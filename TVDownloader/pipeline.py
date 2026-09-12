"""pipeline.py —— 取流↔下载 同进程流水线（TV 版）

把原本串行的两个阶段重叠起来：取流线程一取到集就交给下载侧，不必等整轮取完。

    python pipeline.py            # 本文件：取流与下载在同一进程内重叠
    python tv_ids_to_links.py     # 旧入口：只跑取流（一行未改）
    python download_tv.py         # 旧入口：只跑下载（一行未改）

为什么要重叠：
  - vidlink 出的是**带时效签名的 mp4 直链**。串行模式下最早取到的那批，
    等轮到下载时可能已经放了几十小时；重叠后从产出到消费只隔几分钟。
  - 两阶段的资源画像几乎不重叠（取流吃请求数，下载吃带宽+CPU），
    串行等于一半时间在浪费另一半资源。

⚠️ TV 版特有：取流的**第一步是季集展开**（调 TMDB 把每部剧拆成 (剧, 季, 集)）。
   全新部署、缓存为空时这一步要跑约 2.5 小时，期间**一条取流结果都不产出**，
   下载侧会持续拿到 "wait"（按 stream_idle_poll_seconds 让出 CPU，不空烧）。
   续跑时 seasons_cache.jsonl 已在，展开只需几十秒。这是预期行为，不是卡死。

本文件**不实现任何业务逻辑**——取流怎么取、下载怎么下、怎么判死、怎么重试，
全部复用两个旧模块的既有代码。这里只负责：起线程、搭队列、定终止协议。
"""

import queue
import sys
import threading
import time

import download_tv as downloader
import tv_ids_to_links as fetcher


_PIPELINE_CFG = (downloader._CFG.get("pipeline", {}) or {})

# 队列容量：满了之后取流侧的 on_result 回调会阻塞，天然形成反压。
# 取流侧才是瓶颈（每集要打多个 provider，而有源率只有个位数百分比），
# 故队列长期偏空、这个上限基本不会碰到；它是保险丝，不是主调节手段。
#
# ⚠️ 队列里的每一条都是**等待被下载的签名直链**，而 vidlink 的 mp4 直链带时效。
# 容量越大 = 允许取流跑在下载前面越多 = 队首那条在队列里待得越久。
# 故本值真正决定的不是吞吐（正常时队列本就偏空），而是**下载侧出故障、
# 积压到上限时最坏能坏到什么程度**。
QUEUE_MAXSIZE = max(1, int(_PIPELINE_CFG.get("queue_maxsize", 512)))

# 取流侧入队的最长等待（秒）。只在**队列满**时才起作用：队列满且下载侧长时间
# 不消费时，不能让取流线程永久卡死（那样 Ctrl+C 都退不出去），
# 超时就放弃走快车道——结果**已经落盘 results.jsonl**，下次运行仍能读到，
# 是「推迟」而非「丢失」。
#
# 按设计它**基本不该触发**（取流是瓶颈、队列长期偏空）。真频繁触发说明
# "取流是瓶颈"的判断错了，那是要重新设计的信号，不是靠调这个数字解决。
ENQUEUE_TIMEOUT = max(1, int(_PIPELINE_CFG.get("enqueue_timeout_seconds", 120)))


# 队列终止哨兵：取流线程主任务跑完后放入，下载侧读到即知"不会再有新集了"。
_SENTINEL = object()


class QueueEntrySource:
    """把取流线程的产出包装成下载侧的 entry 来源（三态 poll 接口）。

    poll() 必须**非阻塞**：主事件循环若卡在这里，wait(pending) 就停摆，
    已下载完的集无人提交转封装、成品堆在 temp。
    故队列空时立刻返回 "wait"，由主循环去推进在途任务、稍后再问。

    🔴 backlog 不能省：下载侧启动时会把已有的 results.jsonl 读成一批 entries
    （断点续跑的存量——上次取到但没下完的集）。取流侧的 load_processed 会
    跳过这些集，**它们永远不会再从队列里出来**；若来源只认队列，这批集就被
    静默丢弃了。故存量必须一条不漏地发完。

    🔴 投递顺序：实时队列 > backlog。
    队列里是**刚签出的直链**，有时效；backlog 是躺在 results.jsonl 里的旧链接，
    再等一会儿也不会更坏。若先把存量发完再看队列，大存量场景下队列会灌满、
    取流线程在 on_result 里等 ENQUEUE_TIMEOUT 后被迫 dropped，新鲜链接本次
    运行根本消费不到，等下次运行从文件读出来时早已过期——这是直接砍取流
    成果的路径。故队列有货就先发队列，队列空档才发 backlog。

    🔴 backlog 与队列必须去重：取流线程在 worker.start() 后就同时**落盘 + 入队**，
    而下载侧要稍后才读 results.jsonl 拿 backlog。这中间取流产出的集会**同时**
    出现在两边。第二份虽会被 process_one_entry 的 processing_ids 拦下，
    但仍要白占一个下载槽位走一遭。故用 _seen 记住已投递过的集级 key，
    两边遇到重复都直接跳过。
    """

    def __init__(self, q, backlog=()):
        self._q = q
        self._backlog = list(backlog)
        self._backlog_next = 0
        self._closed = False
        self._seen = set()
        self.delivered = 0
        self.skipped_duplicates = 0
        self.backlog_total = len(self._backlog)

    @staticmethod
    def _key(entry):
        """集级唯一 key `{tid}_S01E03`。与下载侧去重口径完全一致。

        这是 TV 版与电影版最核心的差异：电影按 tmdbId 去重，剧集必须精确到
        (剧, 季, 集) —— 同一部剧的几十上百集都带着同一个 tmdbId，按剧去重
        会让一部剧只下得了一集。
        """
        return downloader.record_episode_key(entry) or None

    def poll(self):
        # ① 实时队列：刚签出的直链，比 backlog 更经不起等。
        # 队列侧可能重复：把已投递过的直接丢弃，继续取下一个。
        # 用循环而非递归——队列里可能连着多个重复项，递归会白白加深栈。
        while not self._closed:
            try:
                item = self._q.get_nowait()
            except queue.Empty:
                break
            if item is _SENTINEL:
                # 取流侧已收工且存货取尽。哨兵是**最后一个**入队的元素，
                # 故此刻队列里不可能还有真结果。
                self._closed = True
                break
            key = self._key(item)
            if key is not None and key in self._seen:
                # 已由队列里更早的一条（或 backlog）投递过，跳过以免白占槽位。
                self.skipped_duplicates += 1
                continue
            if key is not None:
                self._seen.add(key)
            self.delivered += 1
            return "item", item

        # ② 队列空档（或已关闭）：发一条断点续跑存量。
        while self._backlog_next < len(self._backlog):
            entry = self._backlog[self._backlog_next]
            self._backlog_next += 1
            key = self._key(entry)
            if key is not None and key in self._seen:
                # 同一集已从队列里先到过（取流启动到下载侧读 results.jsonl
                # 之间的重叠窗口），跳过。
                self.skipped_duplicates += 1
                continue
            if key is not None:
                self._seen.add(key)
            self.delivered += 1
            return "item", entry

        # ③ 两处都没货：取流没收工就 wait（季集展开阶段会长期停在这里），
        #    收工了才 done。
        return ("done", None) if self._closed else ("wait", None)


class FetchWorker:
    """在后台线程里跑取流主流程，产出实时推入队列。

    ⚠️ 生命周期：取流主任务（季集展开 + 最多 max_rounds 轮取流）跑完后线程
    **不退出**，而是转入**待命态**——这样下载侧还在跑时，取流线程仍然活着，
    日志与统计口径也保持完整。

    终止协议是**单向**的，避免两边互等造成死锁：
        下载侧跑完全部轮次 → 调 shutdown() → 取流线程退出
    取流线程**从不**等待下载侧的任何状态；下载侧也**从不**等待取流线程结束
    （它只认队列里的哨兵）。两个方向都不阻塞，故不存在互等。
    """

    def __init__(self, q, argv=None):
        self._q = q
        self._argv = argv or []
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="fetch-worker",
                                       daemon=True)
        self.error = None
        self.enqueued = 0
        self.dropped = 0

    def _on_result(self, result):
        """取流侧每拿到一条 ok 结果就回调这里（在其写盘锁**之外**）。

        允许阻塞：队列满说明下载侧严重滞后，此时让取流线程等一等正是我们要的
        反压。取流侧已把该回调移出临界区，故这里的等待只影响当前这一条，
        不会堵住其余取流线程。
        """
        if self._stop.is_set():
            return
        try:
            self._q.put(result, timeout=ENQUEUE_TIMEOUT)
            self.enqueued += 1
        except queue.Full:
            # 结果已落盘 results.jsonl，走不了快车道也不算丢：
            # 下次运行会从文件里读到它。只是本次跑不到而已。
            self.dropped += 1
            label = downloader.record_episode_key(result) or result.get("tmdbId")
            print(
                f"⚠️ [pipeline] 队列满 {ENQUEUE_TIMEOUT}s，"
                f"{label} 改由 results.jsonl 落盘承接（不丢集）",
                flush=True,
            )

    def _run(self):
        try:
            # _stop 同时作为取流侧的协作式停止信号：下载侧收工或用户 Ctrl+C 后，
            # 取流不再开新一轮、不再发起新请求、季集展开与退避立刻醒来。
            # 没有它的话，取流会把整批几百万集跑完才罢休。
            fetcher.main(argv=self._argv, on_result=self._on_result,
                         stop_event=self._stop)
        except SystemExit as exc:
            # 取流侧用 SystemExit 做致命退出（熔断 DeadStreakBreaker → exit 2、
            # 配置校验失败等）。绝不能让它静默杀掉线程而下载侧毫不知情。
            self.error = exc
            print(f"\n⚠️ [pipeline] 取流提前终止: {exc}", flush=True)
        except Exception as exc:  # noqa: BLE001
            self.error = exc
            print(f"\n⚠️ [pipeline] 取流异常终止: {exc}", flush=True)
        finally:
            # 无论正常收尾还是异常退出，都必须放哨兵——否则下载侧会一直
            # "等下一集"而永不收尾（这是最容易写出的死锁）。
            #
            # ⚠️ 这里**不能**用带超时的 put：队列满时超时会让哨兵直接丢失，
            # 下载侧的 source_exhausted 永远为 False、主循环永久空转。
            # 结果条目超时了只是"推迟"（有 results.jsonl 兜底），哨兵超时却是
            # "永久挂死"，二者代价完全不对等。
            # 故死等——下载侧只要还在消费就一定能腾出位置；它彻底不再消费时
            # 会调 shutdown()，那里会排空队列把这个 put 解开。
            self._q.put(_SENTINEL)
            print("\n[pipeline] 取流主任务结束，转入待命态（等待下载侧收尾）",
                  flush=True)
            self._stop.wait()

    def start(self):
        self.thread.start()

    def shutdown(self, timeout=30):
        """由下载侧在跑完全部轮次后调用，结束待命态。

        ⚠️ 不能只 set 事件就 join：取流线程此刻可能正**阻塞在 put 上**
        （队列满、下载侧已不再消费），`_stop` 对已经进入 put 的调用没有唤醒作用，
        join 会白等满 timeout 秒。故先排空队列给 put 让出空位，
        让取流线程能走完 _on_result 回到 `_stop.wait()` 并立即退出。
        """
        self._stop.set()
        deadline = time.time() + timeout
        while self.thread.is_alive() and time.time() < deadline:
            # 排空队列：既解除 put 阻塞，也丢掉不会再被消费的残留结果
            # （它们都已落盘 results.jsonl，下次运行照样能读到，不丢集）。
            try:
                while True:
                    self._q.get_nowait()
            except queue.Empty:
                pass
            self.thread.join(timeout=0.1)


def main():
    argv = sys.argv[1:]
    if "--recheck-dead" in argv:
        raise SystemExit(
            "pipeline 模式不支持 --recheck-dead（它是串行模式下的人工补救入口，"
            "只复查历史真无源集、不产出新任务）。\n"
            "请单独运行 python tv_ids_to_links.py --recheck-dead，"
            "跑完后再用 pipeline 或 download_tv 消费结果。"
        )

    # ⚠️ 必须在起线程**之前**校验参数。argv 是原样透传给取流侧的，若等到取流
    # 线程里 argparse 才发现打错（如 --typo），那时下载侧已经开跑了：它会拿着
    # 现有 results.jsonl 跑一整轮全量下载，而用户只是想让程序报错停下。
    fetcher._parse_args(argv)

    print("=" * 70)
    print("pipeline 模式：取流与下载在同一进程内重叠运行")
    print(f"队列容量 {QUEUE_MAXSIZE}，满时取流侧自动降速（反压）")
    print("⚠️ 取流要先做季集展开（全新部署约 2.5 小时且无产出），"
          "期间下载侧只消费断点续跑存量，属正常现象")
    print("=" * 70, flush=True)

    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    worker = FetchWorker(q, argv=argv)
    # source 在 source_factory 里才建得出来——首轮的 backlog（断点续跑存量）
    # 要等 _run_pipeline 读完 results.jsonl 才知道。
    holder = {"source": None}

    # 把首轮来源换成队列；第二轮起仍是 list（重试批次量小且已确定），
    # 故多轮语义、轮次冷却全部不变。
    real_list_source = downloader.ListEntrySource

    def source_factory(entries):
        if holder["source"] is None:
            # entries = 下载侧从既有 results.jsonl 读出的存量，必须发完，
            # 否则这批集会被静默丢掉（取流侧不会再产出它们）。
            # 但投递顺序是 实时队列 > 存量：队列里的直链最新鲜，且队列满会让
            # 取流侧超时丢条目，不能让大存量把它们挤掉。
            holder["source"] = QueueEntrySource(q, backlog=entries)
            if holder["source"].backlog_total:
                print(
                    f"[pipeline] 断点续跑存量 {holder['source'].backlog_total} 集，"
                    f"实时流有货时优先下实时流，空档时消费存量",
                    flush=True,
                )
            return holder["source"]
        return real_list_source(entries)

    downloader.ListEntrySource = source_factory

    started = time.time()
    worker.start()
    interrupted = False
    try:
        downloader.main()
    except KeyboardInterrupt:
        interrupted = True
        print("\n\n⚠️ [pipeline] 收到中断信号，正在收尾...", flush=True)
    finally:
        downloader.ListEntrySource = real_list_source
        worker.shutdown()
        # ⚠️ 收尾统计必须在 finally 里：Ctrl+C 时它才是最该被看到的东西
        # （跑了多久、取到多少、下了多少、有多少留给下次）。
        # 放在 try 之外的话，中断路径会整段跳过，用户只看到一个光秃秃的 traceback。
        _print_summary(worker, holder["source"], started, interrupted)

    return 130 if interrupted else 0


def _print_summary(worker, source, started, interrupted=False):
    delivered = source.delivered if source else 0
    elapsed = time.time() - started
    print("\n" + "=" * 70)
    print(f"[pipeline] {'已中断' if interrupted else '结束'}："
          f"取流入队 {worker.enqueued} 集"
          + (f"（{worker.dropped} 集改走文件承接）" if worker.dropped else "")
          + f"，下载侧消费 {delivered} 集，总耗时 {elapsed / 60:.1f} 分钟")
    if source is not None and source.skipped_duplicates:
        print(f"[pipeline] 队列与存量重叠去重 {source.skipped_duplicates} 集"
              f"（避免白占下载槽位）")
    if worker.error is not None:
        print(f"⚠️ 取流侧曾异常终止：{worker.error}")
        print("   已取到的集仍已下载；重跑本命令可继续未完成的部分。")
    if interrupted:
        print("   进度已全部落盘（results.jsonl / success.jsonl），"
              "重跑本命令即可从断点继续。")
    print("=" * 70, flush=True)


if __name__ == "__main__":
    sys.exit(main())
