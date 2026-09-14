"""pipeline.py（取流↔下载同进程重叠）的离线测试（TV 版，不联网）。

重点锁死四件容易写错、且错了会静默丢集 / 死锁的事：
  - 哨兵与终止：取流异常退出也必须让下载侧能收尾（否则永久挂起）；
  - backlog：断点续跑存量必须发完（取流侧不会再产出它们）；
  - poll 非阻塞：队列空时立刻返回 wait，绝不能卡住主事件循环；
  - 去重按**集级 key**：TV 的一部剧有几十上百集共用同一个 tmdbId，
    按剧去重会让一部剧只下得了一集。
"""

import queue
import threading
import time

import pytest

import download_tv as d
import pipeline as p


def _ep(tid="1", season=1, episode=1, **extra):
    """构造一条与 results.jsonl 同构的取流结果。"""
    return {"tmdbId": tid, "season": season, "episode": episode,
            "urls": ["u"], **extra}


# ------------------------------------------------- QueueEntrySource

def test_poll_returns_wait_when_queue_is_empty_and_never_blocks():
    """队列空必须立刻返回 wait。

    poll 若阻塞，下载侧主事件循环就停摆：已下载完的集无人提交转封装、
    成品堆在 temp、上传信号量不释放。
    TV 侧尤其关键——季集展开阶段（全新部署约 2.5 小时）会一直停在这个状态。
    """
    source = p.QueueEntrySource(queue.Queue())

    started = time.time()
    state, entry = source.poll()
    elapsed = time.time() - started

    assert (state, entry) == ("wait", None)
    assert elapsed < 0.5, f"poll 阻塞了 {elapsed:.2f}s，会卡死主循环"


def test_wait_is_stable_during_the_long_expand_phase():
    """季集展开期间取流一条结果都不产出：来源必须持续 wait 而不是 done。

    若此时返回 done，下载侧会认为"没片可下"直接收尾——整条流水线只剩取流在跑。
    """
    source = p.QueueEntrySource(queue.Queue())
    for _ in range(5):
        assert source.poll() == ("wait", None)


def test_sentinel_closes_the_source():
    q = queue.Queue()
    q.put(_ep("1", 1, 1))
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q)

    assert source.poll() == ("item", _ep("1", 1, 1))
    assert source.poll() == ("done", None)
    # 关闭后必须稳定返回 done（主循环会重复问）
    assert source.poll() == ("done", None)
    assert source.delivered == 1


def test_queue_items_are_delivered_before_backlog_and_backlog_is_not_lost():
    """🔴 实时队列优先于存量，但存量必须一条不漏地发完。

    队列里是刚签出的直链，有时效；大存量若先发完，队列会灌满、取流线程在
    on_result 里等 ENQUEUE_TIMEOUT 后 dropped，新鲜链接本次运行根本消费不到。
    另一方面下载侧启动时读的 results.jsonl 存量，取流侧 load_processed 会跳过，
    它们**永远不会从队列里出来**，来源若只认队列这批集就被静默丢弃。
    """
    q = queue.Queue()
    q.put(_ep("9", 1, 1))
    q.put(p._SENTINEL)
    backlog = [_ep("1", 1, 1), _ep("1", 1, 2)]

    source = p.QueueEntrySource(q, backlog=backlog)

    got = []
    while True:
        state, entry = source.poll()
        if state == "done":
            break
        if state == "item":
            got.append(d.record_episode_key(entry))

    assert got == ["9_S01E01", "1_S01E01", "1_S01E02"]
    assert source.backlog_total == 2
    assert source.delivered == 3


def test_backlog_is_consumed_only_while_queue_is_idle():
    """队列有货就发队列，队列空档才发存量，队列再来货又切回队列。"""
    q = queue.Queue()
    source = p.QueueEntrySource(q, backlog=[_ep("1", 1, 1), _ep("1", 1, 2)])

    # 队列空 -> 发存量
    assert d.record_episode_key(source.poll()[1]) == "1_S01E01"
    # 队列来货 -> 优先发队列
    q.put(_ep("9", 1, 1))
    assert d.record_episode_key(source.poll()[1]) == "9_S01E01"
    # 队列又空 -> 继续发剩下的存量
    assert d.record_episode_key(source.poll()[1]) == "1_S01E02"
    # 存量发完、取流未收工 -> wait
    assert source.poll() == ("wait", None)


def test_dedup_is_per_episode_not_per_show():
    """🔴 TV 核心差异：去重必须精确到 (剧, 季, 集)。

    同一部剧的几十上百集共用同一个 tmdbId，若照搬电影版按 tmdbId 去重，
    一部剧只会下得了一集，其余全被当成"重复"丢弃。
    """
    q = queue.Queue()
    q.put(_ep("1", 1, 1))
    q.put(_ep("1", 1, 2))     # 同剧不同集：必须都发
    q.put(_ep("1", 2, 1))     # 同剧不同季：必须都发
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q)

    got = []
    while True:
        state, entry = source.poll()
        if state == "done":
            break
        if state == "item":
            got.append(d.record_episode_key(entry))

    assert got == ["1_S01E01", "1_S01E02", "1_S02E01"]
    assert source.skipped_duplicates == 0


def test_same_episode_from_queue_and_backlog_is_delivered_once():
    """取流启动到下载侧读 results.jsonl 之间有重叠窗口：同一集会同时出现在两边。

    第二份虽会被 process_one_entry 的 processing_ids 拦下，但仍要白占一个
    下载槽位走一遭，故来源层就该去掉。
    """
    q = queue.Queue()
    q.put(_ep("1", 1, 1))
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q, backlog=[_ep("1", 1, 1), _ep("1", 1, 2)])

    got = []
    while True:
        state, entry = source.poll()
        if state == "done":
            break
        if state == "item":
            got.append(d.record_episode_key(entry))

    assert got == ["1_S01E01", "1_S01E02"]
    assert source.skipped_duplicates == 1


def test_consecutive_duplicates_in_queue_do_not_recurse():
    """队列里连着多个重复项时用循环跳过，不能递归（会白白加深栈）。"""
    q = queue.Queue()
    for _ in range(200):
        q.put(_ep("1", 1, 1))
    q.put(_ep("1", 1, 2))
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q)

    assert d.record_episode_key(source.poll()[1]) == "1_S01E01"
    assert d.record_episode_key(source.poll()[1]) == "1_S01E02"
    assert source.skipped_duplicates == 199


def test_entry_without_identity_is_still_delivered():
    """缺身份字段的条目不参与去重，但仍要发下去。

    下载侧 process_one_entry 会把它判成确定性失败并如实记一条 FAILED_LOG——
    在来源层静默丢弃反而会让问题不可见。
    """
    q = queue.Queue()
    q.put({"urls": ["u"]})
    q.put({"urls": ["u"]})
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q)

    assert source.poll()[0] == "item"
    assert source.poll()[0] == "item"
    assert source.poll() == ("done", None)
    assert source.skipped_duplicates == 0


# ------------------------------------------------- FetchWorker

def test_sentinel_is_put_even_when_fetch_raises(monkeypatch):
    """🔴 取流异常退出也必须放哨兵，否则下载侧永远等不到 done、主循环空转。"""
    def boom(argv=None, on_result=None, stop_event=None):
        raise RuntimeError("fetch blew up")

    monkeypatch.setattr(p.fetcher, "main", boom)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    item = q.get(timeout=5)
    assert item is p._SENTINEL
    assert isinstance(worker.error, RuntimeError)
    worker.shutdown()


def test_sentinel_is_put_when_fetch_exits_via_systemexit(monkeypatch):
    """取流用 SystemExit 做致命退出（熔断 DeadStreakBreaker → exit 2）。

    线程里的 SystemExit 会静默杀掉线程，下载侧毫不知情 —— 必须兜住并放哨兵。
    """
    def bail(argv=None, on_result=None, stop_event=None):
        raise SystemExit(2)

    monkeypatch.setattr(p.fetcher, "main", bail)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    assert q.get(timeout=5) is p._SENTINEL
    assert isinstance(worker.error, SystemExit)
    worker.shutdown()


def test_results_flow_through_the_queue(monkeypatch):
    """取流产出的每一集都应经回调进入队列，最后跟一个哨兵。"""
    def fake_main(argv=None, on_result=None, stop_event=None):
        for i in (1, 2, 3):
            on_result(_ep("1", 1, i))

    monkeypatch.setattr(p.fetcher, "main", fake_main)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()

    got = [q.get(timeout=5) for _ in range(4)]
    assert [d.record_episode_key(x) for x in got[:3]] == [
        "1_S01E01", "1_S01E02", "1_S01E03"
    ]
    assert got[3] is p._SENTINEL
    assert worker.enqueued == 3
    worker.shutdown()


def test_worker_stays_alive_until_shutdown(monkeypatch):
    """主任务跑完后转入待命态，不退出；shutdown 才结束。"""
    def fake_main(argv=None, on_result=None, stop_event=None):
        return

    monkeypatch.setattr(p.fetcher, "main", fake_main)
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker.start()
    assert q.get(timeout=5) is p._SENTINEL

    time.sleep(0.2)
    assert worker.thread.is_alive(), "主任务结束后应转入待命态而不是退出"

    worker.shutdown()
    assert not worker.thread.is_alive()


def test_shutdown_drains_queue_to_release_a_blocked_put(monkeypatch):
    """🔴 队列满时取流线程阻塞在 put 上，_stop 唤不醒它。

    shutdown 必须排空队列给 put 让出空位，否则 join 会白等满 timeout。
    """
    started = threading.Event()

    def fake_main(argv=None, on_result=None, stop_event=None):
        started.set()
        # 队列容量 1：第二条必定阻塞在 put 上
        for i in (1, 2, 3):
            on_result(_ep("1", 1, i))

    monkeypatch.setattr(p.fetcher, "main", fake_main)
    monkeypatch.setattr(p, "ENQUEUE_TIMEOUT", 30)
    q = queue.Queue(maxsize=1)
    worker = p.FetchWorker(q)
    worker.start()
    assert started.wait(5)
    time.sleep(0.2)   # 让它撞上满队列

    began = time.time()
    worker.shutdown(timeout=10)
    elapsed = time.time() - began

    assert not worker.thread.is_alive()
    assert elapsed < 5, f"shutdown 花了 {elapsed:.1f}s，说明没解开阻塞的 put"


def test_enqueue_timeout_falls_back_to_file_without_losing_the_episode(
    monkeypatch, capsys,
):
    """队列满超时只是"推迟"不是"丢失"：结果已落盘 results.jsonl。"""
    def fake_main(argv=None, on_result=None, stop_event=None):
        on_result(_ep("1", 1, 1))
        on_result(_ep("1", 1, 2))   # 队列已满 -> 超时 -> dropped

    monkeypatch.setattr(p.fetcher, "main", fake_main)
    monkeypatch.setattr(p, "ENQUEUE_TIMEOUT", 1)
    q = queue.Queue(maxsize=1)
    worker = p.FetchWorker(q)
    worker.start()
    time.sleep(2.0)

    assert worker.dropped == 1
    out = capsys.readouterr().out
    assert "results.jsonl 落盘承接" in out
    worker.shutdown()


def test_stop_event_is_passed_to_the_fetcher(monkeypatch):
    """取流必须拿到 stop_event，否则 Ctrl+C 后会把整批几百万集跑完才罢休。"""
    seen = {}

    def fake_main(argv=None, on_result=None, stop_event=None):
        seen["stop_event"] = stop_event
        seen["argv"] = argv

    monkeypatch.setattr(p.fetcher, "main", fake_main)
    q = queue.Queue()
    worker = p.FetchWorker(q, argv=["--providers", "vidlink"])
    worker.start()
    q.get(timeout=5)

    assert isinstance(seen["stop_event"], threading.Event)
    assert seen["argv"] == ["--providers", "vidlink"]
    worker.shutdown()
    assert seen["stop_event"].is_set()


def test_on_result_is_ignored_after_stop(monkeypatch):
    """停止后到达的结果不再入队（它们已落盘，下次运行照样读得到）。"""
    q = queue.Queue()
    worker = p.FetchWorker(q)
    worker._stop.set()
    worker._on_result(_ep("1", 1, 1))
    assert worker.enqueued == 0
    assert q.empty()


# ------------------------------------------------- main 的接线

def test_recheck_dead_is_rejected_in_pipeline_mode(monkeypatch):
    """--recheck-dead 只复查历史真无源集、不产出新任务，与流水线语义冲突。"""
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py", "--recheck-dead"])
    with pytest.raises(SystemExit) as ei:
        p.main()
    assert "--recheck-dead" in str(ei.value)


def test_bad_argv_fails_before_any_thread_starts(monkeypatch):
    """🔴 参数必须在起线程**之前**校验。

    否则打错参数时下载侧已经开跑，会拿着现有 results.jsonl 跑一整轮全量下载，
    而用户只是想让程序报错停下。
    """
    started = []
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py", "--bogus"])
    monkeypatch.setattr(p.FetchWorker, "start",
                        lambda self: started.append(1))
    monkeypatch.setattr(p.downloader, "main", lambda: started.append("dl"))

    with pytest.raises(SystemExit):
        p.main()
    assert started == []


def test_main_installs_queue_source_for_first_round_only(monkeypatch):
    """首轮用队列来源，第二轮起回落到 list —— 多轮语义零改动。"""
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py"])
    monkeypatch.setattr(p.fetcher, "main",
                        lambda argv=None, on_result=None, stop_event=None: None)
    real_list_source = p.downloader.ListEntrySource
    sources = []

    def fake_download_main():
        # 模拟 _run_pipeline：首轮传存量，第二轮传重试批次
        sources.append(p.downloader.ListEntrySource([_ep("1", 1, 1)]))
        sources.append(p.downloader.ListEntrySource([_ep("1", 1, 2)]))

    monkeypatch.setattr(p.downloader, "main", fake_download_main)
    p.main()

    assert isinstance(sources[0], p.QueueEntrySource)
    assert isinstance(sources[1], real_list_source)
    # 钩子必须还原，否则污染后续单独运行
    assert p.downloader.ListEntrySource is real_list_source


def test_hook_is_restored_even_when_download_raises(monkeypatch):
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py"])
    monkeypatch.setattr(p.fetcher, "main",
                        lambda argv=None, on_result=None, stop_event=None: None)
    real_list_source = p.downloader.ListEntrySource

    def boom():
        raise RuntimeError("download blew up")

    monkeypatch.setattr(p.downloader, "main", boom)
    with pytest.raises(RuntimeError):
        p.main()
    assert p.downloader.ListEntrySource is real_list_source


def test_interrupt_returns_130_and_still_prints_summary(monkeypatch, capsys):
    """Ctrl+C 时收尾统计最该被看到（跑了多久、取到多少、留给下次多少）。"""
    monkeypatch.setattr(p.sys, "argv", ["pipeline.py"])
    monkeypatch.setattr(p.fetcher, "main",
                        lambda argv=None, on_result=None, stop_event=None: None)

    def interrupted():
        raise KeyboardInterrupt()

    monkeypatch.setattr(p.downloader, "main", interrupted)
    assert p.main() == 130
    out = capsys.readouterr().out
    assert "已中断" in out
    assert "重跑本命令即可从断点继续" in out


# ------------------------------------------------- AsyncRefetcher（异步重取）

class _FakeFetcher:
    """替身取流模块：按集级 key 返回预设结果，并记录调用与 providers。"""

    def __init__(self, results=None, providers_sink=None):
        self.results = results or {}
        self.calls = []
        self.providers_sink = (
            providers_sink if providers_sink is not None else []
        )

    def process_episode(self, tid, season, episode, providers=None):
        key = f"{tid}_S{season:02d}E{episode:02d}"
        self.calls.append(key)
        self.providers_sink.append(providers)
        return self.results.get(key, ("dead", None))


def _ok_result(tid, season=1, episode=1, urls=("new",), fetched_at=999):
    return ("ok", {"tmdbId": tid, "season": season, "episode": episode,
                   "urls": list(urls), "fetched_at": fetched_at})


def _drain(refetcher, timeout=3.0):
    """等到在途归零（worker 先 put 结果再减计数，故归零后 collect 不会漏）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if refetcher.pending_count() == 0:
            return True
        time.sleep(0.02)
    return False


def test_async_refetcher_revives_and_persists(monkeypatch, tmp_path):
    """重取成功要做三件事：落盘、逐键覆盖 urls/fetched_at、放进 _done。

    落盘是关键：即使本次运行没赶上消费，下次启动也能按 fetched_at 择新用上。
    """
    fake = _FakeFetcher({"7_S01E03": _ok_result("7", 1, 3)})
    monkeypatch.setattr(p, "fetcher", fake)
    out = tmp_path / "results.jsonl"
    monkeypatch.setattr(p.downloader, "INPUT_JSONL", str(out))

    stop = threading.Event()
    r = p.AsyncRefetcher(2, stop)
    r.start()
    try:
        entry = _ep("7", 1, 3, title="旧标题", year="2020")
        assert r.dispatch([entry]) == 1
        assert _drain(r)
        got = r.collect()
    finally:
        stop.set()

    assert len(got) == 1
    assert got[0]["urls"] == ["new"]          # 换成了新链接
    assert got[0]["fetched_at"] == 999
    assert got[0]["title"] == "旧标题"        # 历史元数据必须保留（逐键覆盖）
    assert got[0]["year"] == "2020"
    assert r.revived == 1
    # 落盘：下次运行的兜底
    assert out.exists() and "new" in out.read_text(encoding="utf-8")


def test_async_refetcher_refreshes_captions(monkeypatch, tmp_path):
    """🔴 captions 必须跟 urls 一起刷新：旧字幕地址与旧节点同批签发、一起过期。

    留着旧 captions 会让下载侧拿着过期地址去拉字幕，白白浪费请求；
    新结果没给 captions 时要置空，绝不能回落到旧值。
    """
    monkeypatch.setattr(p.downloader, "INPUT_JSONL", str(tmp_path / "r.jsonl"))

    def _run(result):
        monkeypatch.setattr(p, "fetcher", _FakeFetcher({"7_S01E03": result}))
        stop = threading.Event()
        r = p.AsyncRefetcher(1, stop)
        r.start()
        try:
            r.dispatch([_ep("7", 1, 3,
                            captions=[{"language": "en", "url": "old"}])])
            assert _drain(r)
            return r.collect()[0]
        finally:
            stop.set()

    ok, payload = _ok_result("7", 1, 3)
    payload["captions"] = [{"language": "zh", "url": "fresh"}]
    assert _run((ok, payload))["captions"] == [
        {"language": "zh", "url": "fresh"}
    ]
    # 新结果没带 captions -> 置空，不保留旧的
    assert _run(_ok_result("7", 1, 3))["captions"] == []


def test_async_refetcher_dedupes_inflight_by_episode_key(monkeypatch):
    """🔴 同一集在途时拒收重复投递，且去重必须按**集级 key** 而非 tmdbId。

    同一部剧几十上百集共用一个 tmdbId：按剧去重会让一部剧只重取得了一集，
    其余全被当成"已在途"拒收 —— 与 §0.07 那个"一部剧只下得了一集"同源的坑。
    """
    gate = threading.Event()

    class _Blocking(_FakeFetcher):
        def process_episode(self, tid, season, episode, providers=None):
            gate.wait(3)
            return super().process_episode(tid, season, episode, providers)

    monkeypatch.setattr(p, "fetcher", _Blocking())
    stop = threading.Event()
    r = p.AsyncRefetcher(4, stop)
    r.start()
    try:
        # 同一集投两次 → 第二次被拒
        assert r.dispatch([_ep("9", 1, 1)]) == 1
        assert r.dispatch([_ep("9", 1, 1)]) == 0
        assert r.skipped_inflight == 1
        # 同剧不同集 → 必须放行（按集级 key 去重的核心断言）
        assert r.dispatch([_ep("9", 1, 2)]) == 1
        assert r.dispatch([_ep("9", 2, 1)]) == 1
    finally:
        gate.set()
        _drain(r)
        stop.set()


def test_async_refetcher_inflight_drops_to_zero_on_every_path(monkeypatch,
                                                              tmp_path):
    """🔴 无论成功/无果/抛异常，在途计数都必须减掉。

    漏减会让主循环与 QueueEntrySource 永远以为"还有货没回来"：前者白等满
    ASYNC_REFETCH_WAIT_SECONDS，后者永不返回 done → 主循环空转、永不收尾。

    ⚠️ 三条分支的异常必须分别覆盖到**不同的层**，否则测不到 _loop 的 finally：
      - process_episode 抛的异常被 _handle 内部的 except 吃掉，走不到 _loop；
      - 故第 4 条让 **write_log 落盘时**抛，那是 _handle 里唯一没被包住的动作，
        异常会真正冒泡到 _loop —— 只有它能验证 finally 的必要性。
    """
    class _Mixed(_FakeFetcher):
        def process_episode(self, tid, season, episode, providers=None):
            if season == 2:
                raise RuntimeError("源站 502")
            return super().process_episode(tid, season, episode, providers)

    monkeypatch.setattr(p, "fetcher", _Mixed({
        "1_S01E01": _ok_result("1"),
        "4_S03E01": _ok_result("4", 3, 1),
    }))
    monkeypatch.setattr(
        p.downloader, "INPUT_JSONL", str(tmp_path / "r.jsonl")
    )

    real_write_log = p.downloader.write_log

    def flaky_write_log(path, data):
        # 只让第 4 条（S03）落盘失败，异常冒泡到 _loop。
        if data.get("season") == 3:
            raise OSError("磁盘写失败")
        return real_write_log(path, data)

    monkeypatch.setattr(p.downloader, "write_log", flaky_write_log)

    stop = threading.Event()
    r = p.AsyncRefetcher(3, stop)
    r.start()
    try:
        r.dispatch([
            _ep("1", 1, 1),   # ok
            _ep("2", 1, 1),   # dead（无果）
            _ep("3", 2, 1),   # process_episode 抛异常（_handle 内部兜住）
            _ep("4", 3, 1),   # 落盘抛异常（冒泡到 _loop，只有 finally 能兜）
        ])
        assert _drain(r), "在途计数没有归零，主循环会永久空转"
    finally:
        stop.set()
    assert r.pending_count() == 0
    # 冒泡那条也必须把 key 从在途集合里摘掉，否则该集此后再也投递不进来
    assert r.dispatch([_ep("4", 3, 1)]) == 1


def test_async_refetcher_survives_system_exit(monkeypatch):
    """取流侧用 SystemExit 做配置校验，单集重取绝不能带塌整批。"""
    class _Exiting(_FakeFetcher):
        def process_episode(self, tid, season, episode, providers=None):
            raise SystemExit("缺少代理凭证: PROXY_USER")

    monkeypatch.setattr(p, "fetcher", _Exiting())
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop)
    r.start()
    try:
        r.dispatch([_ep("1", 1, 1)])
        assert _drain(r)
    finally:
        stop.set()
    assert r.collect() == []
    assert r.revived == 0


def test_async_refetcher_passes_providers_through(monkeypatch):
    """--providers 必须透传给重取，否则会绕过用户明确指定的源。"""
    sink = []
    monkeypatch.setattr(p, "fetcher", _FakeFetcher(providers_sink=sink))
    stop = threading.Event()
    r = p.AsyncRefetcher(1, stop, providers=["vidlink"])
    r.start()
    try:
        r.dispatch([_ep("1", 1, 1)])
        assert _drain(r)
    finally:
        stop.set()
    assert sink == [["vidlink"]]


# ------------------------------------------ QueueEntrySource 的重取通道

class _StubRefetcher:
    """只实现 collect/pending_count 的替身，用于驱动来源侧的重取分支。"""

    def __init__(self, results=(), pending=0):
        self._results = list(results)
        self._pending = pending

    def collect(self):
        out, self._results = self._results, []
        return out

    def pending_count(self):
        return self._pending

    def set_pending(self, n):
        self._pending = n


def test_revived_entries_are_delivered_first(monkeypatch):
    """🔴 投递优先级：重取结果 > 实时队列 > backlog。

    重取结果是**刚签出**的直链，时效最紧，且是失败集当次运行唯一的救回机会。
    """
    q = queue.Queue()
    q.put(_ep("queued", 1, 1))
    revived = _ep("revived", 1, 1, urls=["fresh"])
    source = p.QueueEntrySource(
        q, backlog=[_ep("backlog", 1, 1)],
        refetcher=_StubRefetcher([revived]),
    )
    assert source.poll() == ("item", revived)          # 重取最先
    state, entry = source.poll()
    assert entry["tmdbId"] == "queued"                 # 其次队列
    state, entry = source.poll()
    assert entry["tmdbId"] == "backlog"                # 最后 backlog


def test_revived_replaces_pending_backlog_entry_in_place(monkeypatch):
    """重取结果若先于对应 backlog 条目回来，就地替换掉那条旧链接。

    否则同一集会被投两次：旧的白占一个下载槽位，且必然因过期而失败。
    """
    q = queue.Queue()
    old = _ep("5", 1, 1, urls=["old"])
    new = _ep("5", 1, 1, urls=["new"])
    source = p.QueueEntrySource(
        q, backlog=[old], refetcher=_StubRefetcher([new])
    )
    # 队列空、backlog 那条已被就地换成新链接 → 发出来的是新的
    state, entry = source.poll()
    assert (state, entry["urls"]) == ("item", ["new"])
    assert source.revived_delivered == 1
    # 只发一份
    q.put(p._SENTINEL)
    assert source.poll() == ("done", None)


def test_revived_waits_for_busy_episode(monkeypatch):
    """旧链接还在下载时，新链接先压在缓冲里，等它离开处理态再投。

    此刻投出去会被 process_one_entry 判"重复条目正在处理"直接丢掉，
    新链接就白换了。
    """
    q = queue.Queue()
    q.put(p._SENTINEL)
    busy = {"3_S01E01"}
    new = _ep("3", 1, 1, urls=["new"])
    source = p.QueueEntrySource(
        q, backlog=[], refetcher=_StubRefetcher([new]),
        is_busy=lambda key: key in busy,
    )
    # 该集正忙 → 不投，但也不能报 done（缓冲里还压着货）
    assert source.poll() == ("wait", None)
    busy.clear()
    state, entry = source.poll()
    assert (state, entry["urls"]) == ("item", ["new"])


def test_source_waits_while_refetch_is_inflight():
    """🔴 取流收工后，只要还有重取在途就必须 wait 而不是 done。

    max_rounds=1 下这是新直链**唯一**的消费窗口：过早报 done 会让主循环收尾，
    重取回来的链接只能等下次运行，本次白救。
    """
    q = queue.Queue()
    q.put(p._SENTINEL)
    stub = _StubRefetcher([], pending=1)
    source = p.QueueEntrySource(q, backlog=[], refetcher=stub)
    assert source.poll() == ("wait", None)     # 在途 → 等
    stub.set_pending(0)
    assert source.poll() == ("done", None)     # 归零且无结果 → 收工


def test_source_without_refetcher_behaves_as_before():
    """未装重取钩子时行为与改动前完全一致（单独跑 download_tv.py 的路径）。"""
    q = queue.Queue()
    q.put(p._SENTINEL)
    source = p.QueueEntrySource(q, backlog=[])
    assert source.poll() == ("done", None)
