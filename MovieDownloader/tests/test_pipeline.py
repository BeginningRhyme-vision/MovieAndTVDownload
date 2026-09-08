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


def test_backlog_is_delivered_before_queue_items():
    """断点续跑存量必须先发、且一条不漏。

    下载侧启动时把既有 results.jsonl 读成 entries；取流侧的 load_processed_ids
    会跳过这些 id，它们**永远不会从队列里出来**。来源若只认队列，这批片就被
    静默丢弃——跑一次全量会凭空少掉一批本该下载的片。
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

    assert got == ["old1", "old2", "new"]
    assert source.backlog_total == 2
    assert source.delivered == 3


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
            got.append(entry["tmdbId"])

    assert got == ["1", "2"], "同一 id 被投递了两次"
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

    assert source.poll()[1]["tmdbId"] == "1"      # backlog
    # 5 个重复项应被连续跳过，直接拿到 id=2，而不是先返回 wait
    assert source.poll()[1]["tmdbId"] == "2"
    assert source.skipped_duplicates == 5


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

    def blocking_fetch(argv=None, on_result=None):
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
    def boom(argv=None, on_result=None):
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


def test_worker_puts_sentinel_when_fetch_raises_generic_error(monkeypatch):
    def boom(argv=None, on_result=None):
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
    def quick(argv=None, on_result=None):
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
    def produce(argv=None, on_result=None):
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
