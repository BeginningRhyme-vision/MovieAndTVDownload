"""pipeline.py（§12 方案 A′）的离线测试。

重点锁死三件容易写错、且错了会静默丢片/死锁的事：
  - 哨兵与终止：取流异常退出也必须让下载侧能收尾（否则永久挂起）；
  - backlog：断点续跑存量必须发完（取流侧不会再产出它们）；
  - poll 非阻塞：队列空时立刻返回 wait，绝不能卡住主事件循环。
"""

import queue
import threading
import time

import pytest

import pipeline as p


# ------------------------------------------------- QueueEntrySource

def test_poll_returns_wait_when_queue_is_empty_and_never_blocks():
    """队列空必须立刻返回 wait。

    poll 若阻塞，下载侧主事件循环就停摆：已下载完的片无人提交转封装、
    成品堆在 temp、上传信号量不释放（§10.21 B-6 同款事故）。
    """
    source = p.QueueEntrySource(queue.Queue())

    started = time.time()
    state, entry = source.poll()
    elapsed = time.time() - started

    assert (state, entry) == ("wait", None)
    assert elapsed < 0.5, f"poll 阻塞了 {elapsed:.2f}s，会卡死主循环"


def test_sentinel_closes_the_source():
    q = queue.Queue()
    q.put({"tmdbId": "1"})
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q)

    assert source.poll() == ("item", {"tmdbId": "1"})
    assert source.poll() == ("done", None)
    # 关闭后必须稳定返回 done（主循环会重复问）
    assert source.poll() == ("done", None)
    assert source.delivered == 1


def test_queue_items_are_delivered_before_backlog_and_backlog_is_not_lost():
    """🔴 R-A：实时队列优先于存量，但存量必须一条不漏地发完。

    队列里是刚签出的直链，有时效；大存量（几十万部）若先发完，队列会灌满、
    取流线程在 on_result 里等 ENQUEUE_TIMEOUT 后 dropped，新鲜链接本次运行
    根本消费不到——这是直接砍取流成果的路径。
    另一方面下载侧启动时读的 results.jsonl 存量，取流侧 load_processed_ids 会跳过，
    它们**永远不会从队列里出来**，来源若只认队列这批片就被静默丢弃。
    """
    q = queue.Queue()
    q.put({"tmdbId": "new"})
    q.put(p._SENTINEL)
    backlog = [{"tmdbId": "old1"}, {"tmdbId": "old2"}]

    source = p.QueueEntrySource(q, backlog=backlog)

    got = []
    while True:
        state, entry = source.poll()
        if state == "done":
            break
        if state == "item":
            got.append(entry["tmdbId"])

    assert got == ["new", "old1", "old2"]
    assert source.backlog_total == 2
    assert source.delivered == 3


def test_backlog_is_consumed_only_while_queue_is_idle():
    """队列有货就发队列，队列空档才发存量，队列再来货又切回队列。"""
    q = queue.Queue()
    source = p.QueueEntrySource(q, backlog=[{"tmdbId": "b1"}, {"tmdbId": "b2"}])

    q.put({"tmdbId": "q1"})
    assert source.poll()[1]["tmdbId"] == "q1"
    assert source.poll()[1]["tmdbId"] == "b1"    # 队列空档 → 存量
    q.put({"tmdbId": "q2"})
    assert source.poll()[1]["tmdbId"] == "q2"    # 队列又有货 → 队列优先
    assert source.poll()[1]["tmdbId"] == "b2"
    assert source.poll() == ("wait", None)       # 存量发完、队列未关 → wait 不是 done


def test_backlog_is_snapshotted():
    backlog = [{"tmdbId": "1"}]
    source = p.QueueEntrySource(queue.Queue(), backlog=backlog)
    backlog.append({"tmdbId": "2"})

    assert source.poll()[0] == "item"
    assert source.poll()[0] == "wait"  # 队列空，但不该吐出后加的那条


def test_queue_items_already_sent_via_backlog_are_skipped():
    """🔴 审查发现的真 bug：同一部片同时出现在 backlog 和队列里，只能投一次。

    取流线程 start 之后就同时**落盘 + 入队**，而下载侧要稍后才读 results.jsonl
    拿 backlog。这中间取流产出的片会同时进两边。第二份虽会被
    process_one_entry 的 processing_ids 拦下，但要白占一个下载槽位走一遭。
    R-A 后队列优先，故先发队列那份、存量里的同 id 被跳过；反向亦然。
    """
    q = queue.Queue()
    q.put({"tmdbId": "1", "title": "queue-copy"})
    q.put({"tmdbId": "2", "title": "genuinely-new"})
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q, backlog=[{"tmdbId": "1", "title": "backlog"}])

    got = []
    while True:
        state, entry = source.poll()
        if state == "done":
            break
        if state == "item":
            got.append((entry["tmdbId"], entry["title"]))

    assert got == [("1", "queue-copy"), ("2", "genuinely-new")], "同一 id 被投递了两次"
    assert source.skipped_duplicates == 1


def test_backlog_item_already_sent_is_skipped_when_queue_copy_arrives_later():
    """存量先发（队列当时空），随后队列里来了同 id 的副本 → 跳过。"""
    q = queue.Queue()
    source = p.QueueEntrySource(q, backlog=[{"tmdbId": "1", "title": "backlog"}])
    assert source.poll()[1]["title"] == "backlog"
    q.put({"tmdbId": "1", "title": "queue-copy"})
    q.put(p._SENTINEL)
    assert source.poll() == ("done", None)
    assert source.skipped_duplicates == 1


def test_duplicates_inside_the_queue_are_skipped():
    """取流侧就地重取会给同一 id 追加新结果，队列里也可能出现同 id 两次。"""
    q = queue.Queue()
    q.put({"tmdbId": "7", "title": "first"})
    q.put({"tmdbId": "7", "title": "second"})
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q)

    assert source.poll()[1]["title"] == "first"
    assert source.poll() == ("done", None)
    assert source.skipped_duplicates == 1


def test_consecutive_duplicates_do_not_stall_the_source():
    """连续多个重复项要一次性跳完，不能返回 wait 假装"没货"（会白 sleep）。"""
    q = queue.Queue()
    for _ in range(5):
        q.put({"tmdbId": "1"})
    q.put({"tmdbId": "2"})
    source = p.QueueEntrySource(q, backlog=[{"tmdbId": "1"}])

    assert source.poll()[1]["tmdbId"] == "1"      # 队列首条
    # 4 个队列重复项应被连续跳过，直接拿到 id=2，而不是先返回 wait
    assert source.poll()[1]["tmdbId"] == "2"
    assert source.skipped_duplicates == 4
    # 队列空 → 轮到存量，存量里的 1 也是重复 → 跳过 → 无货 → wait
    assert source.poll() == ("wait", None)
    assert source.skipped_duplicates == 5


# ---------------------------------------- QueueEntrySource × 重取结果（R-B）

class _StubRefetcher:
    """只模拟 QueueEntrySource 用到的两个方法：collect / pending_count。"""

    def __init__(self):
        self.results = []
        self.pending = 0

    def collect(self):
        out, self.results = self.results, []
        return out

    def pending_count(self):
        return self.pending


def test_revived_entries_are_delivered_first_and_bypass_seen():
    """🔴 R-B：重取结果时效最紧，排在队列与存量之前；且不受 _seen 拦截。

    重取结果就是同一 id 的新链接——旧链接早已投递过（所以才过期失败），
    若按普通去重处理就会被当成重复丢掉，新链接白换。
    """
    q = queue.Queue()
    r = _StubRefetcher()
    source = p.QueueEntrySource(q, refetcher=r)

    q.put({"tmdbId": "1", "urls": ["old"]})
    assert source.poll()[1]["urls"] == ["old"]

    q.put({"tmdbId": "9", "urls": ["queue"]})
    r.results = [{"tmdbId": "1", "urls": ["new"]}]
    state, entry = source.poll()
    assert (state, entry["tmdbId"], entry["urls"]) == ("item", "1", ["new"])
    assert source.poll()[1]["tmdbId"] == "9"
    assert source.revived_delivered == 1
    assert source.skipped_duplicates == 0


def test_revived_entry_replaces_unsent_backlog_item_in_place():
    """预检投出的重取结果若先于对应存量条目回来，就地替换掉那条旧链接。

    否则旧链接还要白试一遍（它本就是因陈旧才被投的），之后再多发一份新的。
    """
    q = queue.Queue()
    r = _StubRefetcher()
    backlog = [
        {"tmdbId": "a", "urls": ["a-old"]},
        {"tmdbId": "b", "urls": ["b-old"]},
    ]
    source = p.QueueEntrySource(q, backlog=backlog, refetcher=r)
    r.results = [{"tmdbId": "b", "urls": ["b-new"]}]

    got = [source.poll()[1] for _ in range(2)]
    assert [(e["tmdbId"], e["urls"][0]) for e in got] == [("a", "a-old"), ("b", "b-new")]
    assert source.poll() == ("wait", None)
    assert source.revived_delivered == 1
    assert source.delivered == 2


def test_revived_entry_is_held_while_old_link_is_still_downloading():
    """旧链接还在下载线程里时，新链接先压着，等它退出处理态再投。

    此刻投出去会被 process_one_entry 判 "duplicate entry currently processing"
    直接丢掉，新链接白换。
    """
    q = queue.Queue()
    r = _StubRefetcher()
    busy = {"1"}
    source = p.QueueEntrySource(q, refetcher=r, is_busy=lambda k: k in busy)

    q.put({"tmdbId": "1", "urls": ["old"]})
    assert source.poll()[1]["urls"] == ["old"]
    r.results = [{"tmdbId": "1", "urls": ["new"]}]
    assert source.poll() == ("wait", None)     # 压住，不投
    assert source.revived_delivered == 0

    busy.clear()
    state, entry = source.poll()
    assert (state, entry["urls"]) == ("item", ["new"])
    assert source.revived_delivered == 1


def test_source_waits_for_inflight_refetch_after_sentinel_then_finishes():
    """🔴 R-B：队列关了、存量发完，但还有重取在途 → wait 而不是 done。

    否则首轮就此收尾，换回的新链接没有轮次去下（以前只等 120s 的老病）。
    在途归零后要再收一次尾（AsyncRefetcher 先 put 后减计数，故不会漏）。
    """
    q = queue.Queue()
    r = _StubRefetcher()
    source = p.QueueEntrySource(q, refetcher=r)

    q.put({"tmdbId": "1", "urls": ["old"]})
    q.put(p._SENTINEL)
    assert source.poll()[1]["urls"] == ["old"]

    r.pending = 1
    assert source.poll() == ("wait", None)
    assert source.poll() == ("wait", None)

    r.pending = 0
    r.results = [{"tmdbId": "1", "urls": ["new"]}]
    state, entry = source.poll()
    assert (state, entry["urls"]) == ("item", ["new"])
    assert source.poll() == ("done", None)
    assert source.poll() == ("done", None)


def test_source_after_sentinel_waits_while_held_revived_entry_is_busy():
    """队列关闭 + 在途归零，但缓冲里还压着一条等旧链接退出处理态的新链接 → wait。"""
    q = queue.Queue()
    r = _StubRefetcher()
    busy = {"1"}
    source = p.QueueEntrySource(q, refetcher=r, is_busy=lambda k: k in busy)
    q.put({"tmdbId": "1", "urls": ["old"]})
    q.put(p._SENTINEL)
    assert source.poll()[1]["urls"] == ["old"]
    r.results = [{"tmdbId": "1", "urls": ["new"]}]
    assert source.poll() == ("wait", None)
    busy.clear()
    assert source.poll()[1]["urls"] == ["new"]
    assert source.poll() == ("done", None)


def test_broken_refetcher_does_not_break_the_source():
    """collect / pending_count 抛异常只告警，来源照常收尾（绝不能卡死主循环）。"""

    class _Broken:
        def collect(self):
            raise RuntimeError("boom")

        def pending_count(self):
            raise RuntimeError("boom")

    q = queue.Queue()
    q.put({"tmdbId": "1"})
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q, refetcher=_Broken())
    assert source.poll()[1]["tmdbId"] == "1"
    assert source.poll() == ("done", None)


def test_shutdown_unblocks_a_producer_stuck_on_a_full_queue():
    """🔴 审查发现的真问题：队列满时取流线程阻塞在 put 上，shutdown 必须能解救它。

    `_stop` 事件对**已经进入** put 的调用没有唤醒作用，若只 set 再 join，
    会白等满 timeout 秒（实测 3s 超时后线程仍活着）。shutdown 必须排空队列
    给 put 让出空位。
    """
    q = queue.Queue(maxsize=1)
    q.put("occupied")
    worker = p.FetchWorker(q)
    entered = threading.Event()

    def blocking_fetch(argv=None, on_result=None, stop_event=None):
        entered.set()
        on_result({"tmdbId": "1"})  # 队列已满 -> 阻塞在 put

    original = p.fetcher.main
    p.fetcher.main = blocking_fetch
    try:
        worker.start()
        assert entered.wait(timeout=5)
        time.sleep(0.2)

        started = time.time()
        worker.shutdown(timeout=10)
        elapsed = time.time() - started
    finally:
        p.fetcher.main = original

    assert not worker.thread.is_alive(), "shutdown 没能结束阻塞中的取流线程"
    assert elapsed < 5, f"shutdown 白等了 {elapsed:.1f}s，未解除 put 阻塞"


# ------------------------------------------------- FetchWorker

def test_worker_puts_sentinel_even_when_fetch_raises_system_exit(monkeypatch):
    """取流侧抛 SystemExit（缺代理凭证/锁被占）时也必须放哨兵。

    不放哨兵 = 下载侧永远等不到 "done" = 整个进程永久挂起。
    这是本方案最容易写出的死锁，必须锁死。
    """
    def boom(argv=None, on_result=None, stop_event=None):
        raise SystemExit("缺少代理凭证: PROXY_USER")

    monkeypatch.setattr(p.fetcher, "main", boom)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    item = q.get(timeout=5)
    assert item is p._SENTINEL
    assert isinstance(worker.error, SystemExit)
    worker.shutdown(timeout=5)
    assert not worker.thread.is_alive()


def test_sentinel_is_never_dropped_when_the_queue_is_full(monkeypatch):
    """🔴 探针实测的真问题：队列满时哨兵**绝不能**因超时被丢弃。

    普通结果入队超时只是"推迟"——它早已落盘 results.jsonl，下次运行照样读到。
    但哨兵超时是"永久挂死"：下载侧的 source_exhausted 永远为 False，
    `while round_download_futures or not source_exhausted` 出不来，主循环空转到天荒地老。
    两者代价完全不对等，故哨兵必须死等到队列腾出位置为止。

    修复前（put 带 timeout）此用例会失败：哨兵被静默吞掉，poll 只返回 wait。
    """
    monkeypatch.setattr(p, "ENQUEUE_TIMEOUT", 1)
    q = queue.Queue(maxsize=2)
    q.put({"tmdbId": "a"})
    q.put({"tmdbId": "b"})       # 队列已满

    monkeypatch.setattr(p.fetcher, "main",
                        lambda argv=None, on_result=None, stop_event=None: None)
    worker = p.FetchWorker(q)
    worker.start()
    # 等满一个 ENQUEUE_TIMEOUT 还多：修复前哨兵此刻已被丢弃
    time.sleep(1.5)

    source = p.QueueEntrySource(q)
    states = []
    deadline = time.time() + 5
    while time.time() < deadline:
        state, _ = source.poll()
        states.append(state)
        if state == "done":
            break
        if state == "wait":
            time.sleep(0.05)

    assert "done" in states, (
        f"哨兵在队列满时丢失（poll 序列 {states}），"
        f"下载侧将永久空转——哨兵入队不得设超时"
    )
    worker.shutdown(timeout=5)
    assert not worker.thread.is_alive()


def test_worker_puts_sentinel_when_fetch_raises_generic_error(monkeypatch):
    def boom(argv=None, on_result=None, stop_event=None):
        raise RuntimeError("网络炸了")

    monkeypatch.setattr(p.fetcher, "main", boom)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    assert q.get(timeout=5) is p._SENTINEL
    assert isinstance(worker.error, RuntimeError)
    worker.shutdown(timeout=5)


def test_worker_stays_alive_after_main_task_for_standby(monkeypatch):
    """取流主任务跑完后线程必须**留在待命态**，不能直接退出。

    下载侧还在跑，随时可能有 vidlink 直链过期需要重新取流；线程若退了，
    那些片只能退回 refetch_entries 的同步调用老路（§10.21 B-5/B-6 三个坑）。
    """
    def quick(argv=None, on_result=None, stop_event=None):
        on_result({"tmdbId": "1", "urls": []})

    monkeypatch.setattr(p.fetcher, "main", quick)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    assert q.get(timeout=5)["tmdbId"] == "1"
    assert q.get(timeout=5) is p._SENTINEL
    # 主任务已结束（哨兵已出），但线程必须还活着待命
    time.sleep(0.2)
    assert worker.thread.is_alive(), "取流线程过早退出，重取请求将无人服务"

    # 只有下载侧显式 shutdown 才结束——单向终止协议，不存在互等
    worker.shutdown(timeout=5)
    assert not worker.thread.is_alive()


def test_on_result_forwards_to_queue(monkeypatch):
    def produce(argv=None, on_result=None, stop_event=None):
        for i in range(3):
            on_result({"tmdbId": str(i)})

    monkeypatch.setattr(p.fetcher, "main", produce)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    got = [q.get(timeout=5)["tmdbId"] for _ in range(3)]
    assert got == ["0", "1", "2"]
    assert q.get(timeout=5) is p._SENTINEL
    assert worker.enqueued == 3
    worker.shutdown(timeout=5)


def test_on_result_is_ignored_after_stop(monkeypatch):
    """已请求停止后不再入队，避免关停期间还往队列里塞东西。"""
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker._stop.set()

    worker._on_result({"tmdbId": "1"})

    assert q.empty()
    assert worker.enqueued == 0


def test_refetch_failed_flag_is_rejected(monkeypatch):
    """--refetch-failed 是串行模式的人工补救入口，pipeline 下语义不通用，
    必须明确报错而不是默默跑成全量取流。"""
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py", "--refetch-failed"])
    with pytest.raises(SystemExit) as excinfo:
        p.main()
    assert "refetch-failed" in str(excinfo.value)


def test_bad_argv_is_rejected_before_anything_starts(monkeypatch):
    """🔴 探针实测的真问题：参数打错必须在**起线程之前**被拦下。

    argv 是原样透传给取流侧的。若等到取流线程里 argparse 才发现问题，
    那时下载侧已经开跑——它会拿着现有 results.jsonl 跑一整轮全量下载，
    而用户只是想让程序报错停下。全量场景下等于误启动几十万部片的下载。
    """
    started = []
    monkeypatch.setattr(p.FetchWorker, "start",
                        lambda self: started.append(1))
    monkeypatch.setattr(p.downloader, "main",
                        lambda: started.append("downloader"))

    for bad in (["--typo"], ["reupload"]):
        monkeypatch.setattr(p.sys, "argv", ["pipeline.py"] + bad)
        with pytest.raises(SystemExit):
            p.main()

    assert started == [], f"错误参数下仍启动了组件: {started}"


def test_main_wires_refetcher_into_first_round_source(monkeypatch):
    """🔴 R-B 接线：首轮 QueueEntrySource 必须拿到 refetcher 与 is_busy，
    否则重取结果只能等轮末 collect，首轮内根本消费不到。"""
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py"])
    monkeypatch.setattr(p.fetcher, "_parse_args", lambda argv: None)
    monkeypatch.setattr(p.FetchWorker, "start", lambda self: None)
    monkeypatch.setattr(p.FetchWorker, "shutdown", lambda self, timeout=None: None)
    monkeypatch.setattr(p.AsyncRefetcher, "start", lambda self: None)
    monkeypatch.setattr(p.downloader, "AUTO_REFETCH_ENABLED", True)

    captured = {}

    def fake_downloader_main():
        src = p.downloader.ListEntrySource([{"tmdbId": "b"}])
        captured["source"] = src
        captured["hook"] = p.downloader.async_refetch_hook

    monkeypatch.setattr(p.downloader, "main", fake_downloader_main)
    p.main()

    src = captured["source"]
    assert isinstance(src, p.QueueEntrySource)
    assert src._refetcher is captured["hook"]
    assert isinstance(captured["hook"], p.AsyncRefetcher)
    # is_busy 直连下载侧 processing_ids（按 normalize 后的 id 比较）
    p.downloader.processing_ids.add("42")
    try:
        assert src._is_busy("42") is True
        assert src._is_busy(42) is True
        assert src._is_busy("43") is False
    finally:
        p.downloader.processing_ids.discard("42")


def test_queue_and_timeout_come_from_config():
    """两个反压参数必须来自 config，且取值合理。

    硬编码会让"队列满"这类问题只能改代码才能应对；而 §12.10 G 明确
    这两个值的意义在于**触发时能被观察到**，故必须可配。
    """
    import yaml
    from pathlib import Path

    cfg_path = Path(__file__).resolve().parent.parent / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    section = cfg["download_movies"]["pipeline"]

    # config 里写了什么，模块就该读到什么（防"加了配置项但代码没接"）
    assert p.QUEUE_MAXSIZE == section["queue_maxsize"]
    assert p.ENQUEUE_TIMEOUT == section["enqueue_timeout_seconds"]
    # 必须为正：0 或负数会让 Queue(maxsize<=0) 退化成无界，反压彻底失效
    assert p.QUEUE_MAXSIZE > 0
    assert p.ENQUEUE_TIMEOUT > 0


def test_queue_capacity_stays_within_link_lifetime():
    """🔑 队列容量不能超过签名直链的有效期，否则桥反而制造了它要解决的问题。

    队列里每一条都是**等待下载的签名直链**。容量 ÷ 取流速率 = 队首那条最长
    要等多久（下载侧故障积压到上限时的最坏情况）。
    取流实测 ~4.6 部/分钟（§12.3）；vidlink 实测"签发 30h 后仍能 206 拉流"
    （§12.8）——但那只是"测到没失效"，**有效期上界始终未知**，故留大安全边际。

    阈值 6 小时相对当前值（512 ≈ 1.9h）留了 3 倍余量：1024（3.7h）仍放行，
    但再往上（≥1656）就会被拦下——那种深度已经是拿时效换缓冲了。
    """
    fetch_rate_per_minute = 4.6
    hours_of_backlog = p.QUEUE_MAXSIZE / fetch_rate_per_minute / 60

    assert hours_of_backlog < 6, (
        f"队列容量 {p.QUEUE_MAXSIZE} 约合 {hours_of_backlog:.1f} 小时产出量，"
        f"队首直链可能等到过期——这正是 §12 要消除的问题。"
        f"若下载侧跟不上，该查下载侧，而不是加深队列。"
    )


# ------------------------------------------------- AsyncRefetcher

def _make_refetcher(monkeypatch, tmp_path, outcomes, workers=2):
    """建一个 AsyncRefetcher，把取流与落盘都换成假实现。"""
    monkeypatch.setattr(
        p.fetcher, "process_tmdb_id",
        lambda tid, providers=None: outcomes[str(tid)]
    )
    written = []
    monkeypatch.setattr(p.downloader, "INPUT_JSONL", str(tmp_path / "r.jsonl"))
    monkeypatch.setattr(p.downloader, "write_log",
                        lambda path, rec: written.append(rec))
    stop = threading.Event()
    r = p.AsyncRefetcher(workers, stop)
    r.start()
    return r, stop, written


def test_dispatch_returns_immediately_and_does_not_block(monkeypatch, tmp_path):
    """🔑 本方向的全部意义：主循环投递重取时**绝不能阻塞**。

    同步的 refetch_entries 由主事件循环线程跑，期间 wait(pending) 停摆可达
    数十分钟（§10.21 B-6）。dispatch 必须瞬时返回。
    """
    slow = threading.Event()

    def slow_fetch(tid, providers=None):
        slow.wait(timeout=5)  # 取流很慢
        return "ok", {"tmdbId": tid, "urls": [{"url": "u", "type": "mp4"}]}

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", slow_fetch)
    monkeypatch.setattr(p.downloader, "write_log", lambda path, rec: None)
    stop = threading.Event()
    r = p.AsyncRefetcher(2, stop)
    r.start()
    try:
        started = time.time()
        r.dispatch([{"tmdbId": str(i)} for i in range(10)])
        elapsed = time.time() - started
        assert elapsed < 0.5, f"dispatch 阻塞了 {elapsed:.2f}s"
        # 结果还没好时 collect 也必须瞬时返回
        started = time.time()
        assert r.collect() == []
        assert time.time() - started < 0.5
    finally:
        slow.set()
        stop.set()


def test_collect_returns_revived_entries_and_preserves_metadata(
        monkeypatch, tmp_path):
    """重取成功的片要能被收回，且保留 entry 上 result 没有的历史元数据。"""
    outcomes = {
        "1": ("ok", {"tmdbId": "1", "urls": [{"url": "new", "type": "mp4"}],
                     "fetched_at": 999,
                     "captions": [{"url": "new.vtt", "lang": "en"}]}),
    }
    r, stop, written = _make_refetcher(monkeypatch, tmp_path, outcomes)
    try:
        r.dispatch([{"tmdbId": "1", "title": "T", "year": 2020,
                     "urls": [{"url": "old", "type": "mp4"}],
                     "captions": [{"url": "old.vtt", "lang": "en"}]}])
        deadline = time.time() + 5
        got = []
        while time.time() < deadline and not got:
            got = r.collect()
            time.sleep(0.02)

        assert len(got) == 1
        entry = got[0]
        assert entry["urls"] == [{"url": "new", "type": "mp4"}]
        assert entry["fetched_at"] == 999
        # 字幕列表要跟 urls 一起换新，不能留着旧节点的字幕地址
        assert entry["captions"] == [{"url": "new.vtt", "lang": "en"}]
        # 历史元数据必须保留（逐键覆盖而非整体替换）
        assert entry["title"] == "T"
        assert entry["year"] == 2020
        # 必须落盘：本次没赶上消费时，靠它让下次运行用上新链接
        assert len(written) == 1
        assert r.revived == 1
    finally:
        stop.set()


def test_dead_and_retry_outcomes_are_not_revived(monkeypatch, tmp_path):
    """dead（真无源）与 retry（瞬时耗尽）都救不回来，不能当成新链接投出去。"""
    outcomes = {"1": ("dead", None), "2": ("retry", None)}
    r, stop, written = _make_refetcher(monkeypatch, tmp_path, outcomes)
    try:
        r.dispatch([{"tmdbId": "1"}, {"tmdbId": "2"}])
        time.sleep(0.5)
        assert r.collect() == []
        assert written == []
        assert r.revived == 0
    finally:
        stop.set()


def test_refetch_worker_survives_system_exit_from_fetcher(monkeypatch, tmp_path):
    """取流侧的 SystemExit（配置校验）不得杀掉重取线程，更不能带塌整批。"""
    def boom(tid, providers=None):
        if str(tid) == "1":
            raise SystemExit("缺少代理凭证")
        return "ok", {"tmdbId": tid, "urls": [{"url": "u", "type": "mp4"}]}

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", boom)
    monkeypatch.setattr(p.downloader, "write_log", lambda path, rec: None)
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop)   # 单线程：保证两条走同一个 worker
    r.start()
    try:
        r.dispatch([{"tmdbId": "1"}, {"tmdbId": "2"}])
        deadline = time.time() + 5
        got = []
        while time.time() < deadline and not got:
            got = r.collect()
            time.sleep(0.02)
        # 第一条抛 SystemExit，第二条仍要被正常处理
        assert [e["tmdbId"] for e in got] == ["2"]
    finally:
        stop.set()


def test_refetch_threads_exit_on_stop(monkeypatch, tmp_path):
    """stop 事件必须能结束全部重取线程，否则进程退出时线程泄漏。"""
    r, stop, _ = _make_refetcher(monkeypatch, tmp_path, {}, workers=3)
    assert all(t.is_alive() for t in r._threads)

    stop.set()
    deadline = time.time() + 5
    while time.time() < deadline and any(t.is_alive() for t in r._threads):
        time.sleep(0.05)

    assert not any(t.is_alive() for t in r._threads), "重取线程未退出"


def test_pending_count_drops_to_zero_on_every_outcome(monkeypatch, tmp_path):
    """🔴 在途计数必须在**所有**分支归零：成功/无果/异常都要减。

    漏减会让主循环一直以为"还有货没回来"，白等满 ASYNC_REFETCH_WAIT_SECONDS
    （120s）才继续——每一轮都白等两分钟。
    """
    def mixed(tid, providers=None):
        if str(tid) == "ok":
            return "ok", {"tmdbId": tid, "urls": [{"url": "u", "type": "mp4"}]}
        if str(tid) == "dead":
            return "dead", None
        raise RuntimeError("boom")

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", mixed)
    monkeypatch.setattr(p.downloader, "write_log", lambda path, rec: None)
    stop = threading.Event()
    r = p.AsyncRefetcher(2, stop)
    r.start()
    try:
        r.dispatch([{"tmdbId": "ok"}, {"tmdbId": "dead"}, {"tmdbId": "boom"}])
        assert r.pending_count() == 3

        deadline = time.time() + 5
        while time.time() < deadline and r.pending_count() > 0:
            time.sleep(0.02)

        assert r.pending_count() == 0, "有分支漏减在途计数"
        assert r.revived == 1  # 只有 ok 那条救回来了
    finally:
        stop.set()


def test_entry_without_tmdb_id_still_clears_inflight(monkeypatch, tmp_path):
    """脏数据（缺 tmdbId）也要减在途计数，否则同样会卡住主循环的等待。"""
    r, stop, _ = _make_refetcher(monkeypatch, tmp_path, {})
    try:
        r.dispatch([{"title": "no id"}])
        deadline = time.time() + 5
        while time.time() < deadline and r.pending_count() > 0:
            time.sleep(0.02)
        assert r.pending_count() == 0
    finally:
        stop.set()


def test_duplicate_dispatch_while_inflight_is_merged(monkeypatch, tmp_path):
    """B-2：同一 id 在途期间再次 dispatch 必须被拒收——预检投一次、旧链接失败
    又投一次是常态，第二次拿到的还是同一批源的新链接，纯烧取流配额。
    在途归零后再投则要正常接收（这是每片上限 refetch_counts 的事，不归钩子管）。
    """
    gate = threading.Event()
    calls = []

    def slow_fetch(tid, providers=None):
        calls.append(str(tid))
        gate.wait(5)
        return "ok", {"tmdbId": tid, "urls": [{"url": "u", "type": "mp4"}]}

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", slow_fetch)
    monkeypatch.setattr(p.downloader, "write_log", lambda path, rec: None)
    stop = threading.Event()
    r = p.AsyncRefetcher(2, stop)
    r.start()
    try:
        assert r.dispatch([{"tmdbId": "1"}]) == 1
        assert r.dispatch([{"tmdbId": "1"}, {"tmdbId": 1}]) == 0, "在途期间重复投递必须拒收"
        assert r.dispatch([{"tmdbId": "2"}, {"tmdbId": "1"}]) == 1
        assert r.pending_count() == 2
        assert r.dispatched == 2
        assert r.skipped_inflight == 3

        gate.set()
        deadline = time.time() + 5
        while time.time() < deadline and r.pending_count() > 0:
            time.sleep(0.02)
        assert r.pending_count() == 0
        assert sorted(calls) == ["1", "2"], "拒收的不能再进取流"
        assert len(r.collect()) == 2

        # 在途归零后再投同一 id：正常接收
        assert r.dispatch([{"tmdbId": "1"}]) == 1
    finally:
        gate.set()
        stop.set()


def test_inflight_id_is_released_even_when_fetch_raises(monkeypatch, tmp_path):
    """B-2 护栏：取流抛异常/无果也要释放在途 id，否则该片此后永远投不进去。"""
    def boom(tid, providers=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", boom)
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop)
    r.start()
    try:
        assert r.dispatch([{"tmdbId": "1"}]) == 1
        deadline = time.time() + 5
        while time.time() < deadline and r.pending_count() > 0:
            time.sleep(0.02)
        assert r.pending_count() == 0
        assert r.dispatch([{"tmdbId": "1"}]) == 1
    finally:
        stop.set()


def test_refetcher_passes_providers_through(monkeypatch, tmp_path):
    """B-1：命令行 --providers 覆盖时，重取必须用同一份源列表；未覆盖时传 None
    （让取流侧用 config 的 ACTIVE_PROVIDERS），不能各说各话。"""
    seen = []

    def spy(tid, providers=None):
        seen.append(providers)
        return "ok", {"tmdbId": tid, "urls": [{"url": "u", "type": "mp4"}]}

    monkeypatch.setattr(p.fetcher, "process_tmdb_id", spy)
    monkeypatch.setattr(p.downloader, "write_log", lambda path, rec: None)
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop, providers=["vidlink"])
    r.start()
    r2 = p.AsyncRefetcher(1, stop)
    r2.start()
    try:
        r.dispatch([{"tmdbId": "1"}])
        r2.dispatch([{"tmdbId": "2"}])
        deadline = time.time() + 5
        while time.time() < deadline and (r.pending_count() or r2.pending_count()):
            time.sleep(0.02)
        assert len(seen) == 2
        assert ["vidlink"] in seen
        assert None in seen
    finally:
        stop.set()


def test_main_passes_cli_providers_to_refetcher(monkeypatch):
    """B-1 接线：pipeline.py --providers X 时，AsyncRefetcher 拿到的就是 X。"""
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py", "--providers", "vidlink,vidup"])
    monkeypatch.setattr(p.FetchWorker, "start", lambda self: None)
    monkeypatch.setattr(p.FetchWorker, "shutdown", lambda self, timeout=None: None)
    monkeypatch.setattr(p.AsyncRefetcher, "start", lambda self: None)
    monkeypatch.setattr(p.downloader, "AUTO_REFETCH_ENABLED", True)
    captured = {}
    monkeypatch.setattr(
        p.downloader, "main",
        lambda: captured.setdefault("hook", p.downloader.async_refetch_hook),
    )
    p.main()
    assert captured["hook"]._providers == ["vidlink", "vidup"]

    # 不带 --providers：None，交给取流侧用 config 默认
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py"])
    captured.clear()
    p.main()
    assert captured["hook"]._providers is None
