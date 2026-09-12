"""pipeline.py —— 取流↔下载 同进程流水线（AGENTS §12 方案 A′）

把原本串行的两个阶段重叠起来：取流线程一取到片就交给下载侧，不必等整轮取完。

    python pipeline.py            # 本文件：取流与下载在同一进程内重叠
    python tmdb_ids_to_links.py   # 旧入口：只跑取流（一行未改）
    python download_movies.py     # 旧入口：只跑下载（一行未改）

为什么要重叠（§12.1）：
  - vidlink 出的是**带时效签名的 mp4 直链**。串行模式下最早取到的那批，
    等轮到下载时可能已经放了几十小时；重叠后从产出到消费只隔几分钟。
  - 两阶段的资源画像几乎不重叠（取流吃请求数、下载吃带宽+CPU），
    串行等于一半时间在浪费另一半资源。

本文件**不实现任何业务逻辑**——取流怎么取、下载怎么下、怎么判死、怎么重试，
全部复用两个旧模块的既有代码。这里只负责：起线程、搭队列、定终止协议。
"""

import queue
import sys
import threading
import time

import download_movies as downloader
import tmdb_ids_to_links as fetcher


_PIPELINE_CFG = (downloader._CFG.get("pipeline", {}) or {})

# 队列容量：满了之后取流侧的 on_result 回调会阻塞，天然形成反压。
# 取流侧才是瓶颈（§12.3：取流 ~4.6 部/分钟 < 下载 ~6.4 部/分钟），
# 故队列长期偏空、这个上限基本不会碰到；它是保险丝，不是主调节手段。
#
# ⚠️ 队列里的每一条都是**等待被下载的签名直链**，而 vidlink 的 mp4 直链带时效。
# 容量越大 = 允许取流跑在下载前面越多 = 队首那条在队列里待得越久。
# 故本值真正决定的不是吞吐（正常时队列本就偏空），而是**下载侧出故障、
# 积压到上限时最坏能坏到什么程度**：512 条按 ~4.6 部/分钟约合 1.9 小时产出量。
QUEUE_MAXSIZE = max(1, int(_PIPELINE_CFG.get("queue_maxsize", 512)))

# 取流侧入队的最长等待（秒）。只在**队列满**时才起作用：队列满且下载侧长时间
# 不消费时，不能让取流线程永久卡死（那样 Ctrl+C 都退不出去），
# 超时就放弃走快车道——结果**已经落盘 results.jsonl**，下次运行仍能读到，
# 是「推迟」而非「丢失」。
#
# 按设计它**基本不该触发**（取流是瓶颈、队列长期偏空）。真频繁触发说明
# "取流是瓶颈"的判断错了，那是要重新设计的信号，不是靠调这个数字解决。
ENQUEUE_TIMEOUT = max(1, int(_PIPELINE_CFG.get("enqueue_timeout_seconds", 120)))


class QueueEntrySource:
    """把取流线程的产出包装成下载侧的 entry 来源（配合 §12 第 1 步的三态接口）。

    poll() 必须**非阻塞**：主事件循环若卡在这里，wait(pending) 就停摆，
    已下载完的片无人提交转封装、成品堆在 temp（§10.21 B-6 踩过的坑）。
    故队列空时立刻返回 "wait"，由主循环去推进在途任务、稍后再问。

    🔴 backlog 不能省：下载侧启动时会把已有的 results.jsonl 读成一批 entries
    （断点续跑的存量——上次取到但没下完的片）。取流侧的 load_processed_ids 会
    跳过这些 id，**它们永远不会再从队列里出来**；若来源只认队列，这批片就被
    静默丢弃了。故存量必须一条不漏地发完。

    🔴 投递顺序：重取结果 > 实时队列 > backlog（R-A）。
    队列里是**刚签出的直链**，有时效；backlog 是躺在 results.jsonl 里的旧链接，
    再等一会儿也不会更坏。若先把存量发完再看队列，大存量（几十万部）场景下
    队列会灌满、取流线程在 on_result 里等 ENQUEUE_TIMEOUT 后被迫 dropped，
    新鲜链接本次运行根本消费不到，等下次运行从文件读出来时早已过期——
    这是直接砍取流成果的路径。故队列有货就先发队列，队列空档才发 backlog。
    重取结果（AsyncRefetcher 换回的新直链）时效最紧、且是救回失败片的唯一
    机会，排最前。

    🔴 backlog 与队列必须去重：取流线程在 worker.start() 后就同时**落盘 + 入队**，
    而下载侧要稍后才读 results.jsonl 拿 backlog。这中间取流产出的片会**同时**
    出现在两边。第二份虽会被 process_one_entry 的 processing_ids 拦下，
    但仍要白占一个下载槽位走一遭（与 merge_next_batch 规避的是同一类浪费）。
    故用 _seen 记住已投递过的 id，两边遇到重复都直接跳过。
    重取结果**不受 _seen 拦截**：它就是同一 id 的新链接，必须放行。

    🔴 重取结果在首轮内消费（R-B）：以前重取结果只在轮末 collect 并入下一轮，
    首轮只等 ASYNC_REFETCH_WAIT_SECONDS 就收尾，而单片重取常常不止这么久
    ——新链接明明换回来了，本次运行却没有轮次去下。这里让来源直接从
    refetcher 拿结果，随到随投；且 backlog 与队列都发完后，只要还有重取在途
    就返回 wait 而不是 done，让首轮把最后几条也等回来下掉。
    """

    def __init__(self, q, backlog=(), refetcher=None, is_busy=None):
        self._q = q
        self._backlog = list(backlog)
        self._backlog_next = 0
        # backlog 里每个 id 的位置：重取结果若先于对应存量条目回来，就地替换
        # 掉那条旧链接（它本就是因陈旧才被预检投递的），不再多发一份。
        self._backlog_index = {}
        for idx, entry in enumerate(self._backlog):
            key = self._key(entry)
            if key is not None and key not in self._backlog_index:
                self._backlog_index[key] = idx
        self._refetcher = refetcher
        # is_busy(key) -> 该 id 是否正在下载侧处理中（processing_ids）。
        # 预检投出的重取结果可能在旧链接**还在下**时就回来了，此刻投出去会被
        # process_one_entry 判"重复条目"直接丢掉，新链接白换。故先压在缓冲里，
        # 等那部片离开处理态再投。
        self._is_busy = is_busy
        self._revived_buf = []
        self._closed = False
        self._seen = set()
        self.delivered = 0
        self.skipped_duplicates = 0
        self.backlog_total = len(self._backlog)
        # 重取结果中被本来源直接投递（含就地替换进 backlog）的条数。
        self.revived_delivered = 0

    @staticmethod
    def _key(entry):
        tid = entry.get("tmdbId")
        return str(tid) if tid is not None else None

    def _poll_revived(self):
        """取一条可投的重取结果；没有返回 None。绝不阻塞。"""
        if self._refetcher is None:
            return None
        try:
            self._revived_buf.extend(self._refetcher.collect())
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ 收取异步重取结果失败: {exc}", flush=True)
        for i, entry in enumerate(self._revived_buf):
            key = self._key(entry)
            idx = self._backlog_index.get(key) if key is not None else None
            if idx is not None and idx >= self._backlog_next:
                # 对应存量条目还没轮到：直接换成新链接，旧的那条不再投。
                self._backlog[idx] = entry
                self.revived_delivered += 1
                del self._revived_buf[i]
                return self._poll_revived()
            if key is not None and self._is_busy is not None:
                try:
                    if self._is_busy(key):
                        continue
                except Exception:  # noqa: BLE001
                    pass
            del self._revived_buf[i]
            if key is not None:
                self._seen.add(key)
            self.revived_delivered += 1
            self.delivered += 1
            return entry
        return None

    def poll(self):
        # ① 重取结果：时效最紧，且是失败片的唯一救回机会。
        entry = self._poll_revived()
        if entry is not None:
            return "item", entry

        # ② 实时队列：刚签出的直链，比 backlog 更经不起等。
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

        # ③ 队列空档（或已关闭）：发一条断点续跑存量。
        while self._backlog_next < len(self._backlog):
            entry = self._backlog[self._backlog_next]
            self._backlog_next += 1
            key = self._key(entry)
            if key is not None and key in self._seen:
                # 同一部片已从队列里先到过（取流启动到下载侧读 results.jsonl
                # 之间的重叠窗口），跳过。
                self.skipped_duplicates += 1
                continue
            if key is not None:
                self._seen.add(key)
            self.delivered += 1
            return "item", entry

        # ④ 三处都没货。取流没收工 → wait；收工了再看重取：
        #    有在途 / 有压在缓冲里等下载槽位腾出的 → wait，本轮把它们等回来下掉；
        #    否则再收一次尾（worker 是先 put 结果再减在途计数，故在途归零时
        #    结果必已入队，这一收不会漏）→ 有就发，没有才 done。
        if not self._closed:
            return "wait", None
        if self._refetcher is not None:
            try:
                if self._refetcher.pending_count() > 0:
                    return "wait", None
            except Exception as exc:  # noqa: BLE001
                print(f"⚠️ 查询异步重取在途数失败: {exc}", flush=True)
            entry = self._poll_revived()
            if entry is not None:
                return "item", entry
            if self._revived_buf:
                return "wait", None
        return "done", None


# 队列终止哨兵：取流线程主任务跑完后放入，下载侧读到即知"不会再有新片了"。
_SENTINEL = object()


class FetchWorker:
    """在后台线程里跑取流主流程，产出实时推入队列。

    ⚠️ 生命周期（§12.4′ 复杂度①）：取流主任务（最多 max_rounds 轮）跑完后线程**不退出**，
    而是转入**待命态**继续服务下载侧的"直链过期，重新取一条"请求——否则那些片
    只能退回 refetch_entries 的同步调用老路（带着 SystemExit / 阻塞主循环的三个坑）。

    终止协议是**单向**的，避免两边互等造成死锁：
        下载侧跑完全部轮次 → 调 shutdown() → 取流线程退出
    取流线程**从不**等待下载侧的任何状态；下载侧也**从不**等待取流线程结束
    （它只认队列里的哨兵）。两个方向都不阻塞，故不存在互等。
    """

    def __init__(self, q, argv=None):
        self._q = q
        self._argv = argv or []
        self._stop = threading.Event()
        self._main_done = threading.Event()
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
            print(
                f"⚠️ [pipeline] 队列满 {ENQUEUE_TIMEOUT}s，"
                f"{result.get('tmdbId')} 改由 results.jsonl 落盘承接（不丢片）",
                flush=True,
            )

    def _run(self):
        try:
            # _stop 同时作为取流侧的协作式停止信号：下载侧收工或用户 Ctrl+C 后，
            # 取流不再开新一轮、不再发起新请求、退避/冷却立刻醒来。
            # 没有它的话，取流会把整批几十万 ID 跑完才罢休（实测 shutdown 白等满）。
            fetcher.main(argv=self._argv, on_result=self._on_result,
                         stop_event=self._stop)
        except SystemExit as exc:
            # 取流侧用模块级/流程内的 SystemExit 做配置校验（缺代理凭证、
            # 单实例锁被占等）。绝不能让它静默杀掉线程而下载侧毫不知情。
            self.error = exc
            print(f"\n⚠️ [pipeline] 取流提前终止: {exc}", flush=True)
        except Exception as exc:  # noqa: BLE001
            self.error = exc
            print(f"\n⚠️ [pipeline] 取流异常终止: {exc}", flush=True)
        finally:
            # 无论正常收尾还是异常退出，都必须放哨兵——否则下载侧会一直
            # "等下一部片"而永不收尾（这是最容易写出的死锁）。
            #
            # ⚠️ 这里**不能**用带超时的 put：队列满时超时会让哨兵直接丢失，
            # 下载侧的 source_exhausted 永远为 False、主循环永久空转
            # （已探针实测复现）。结果条目超时了只是"推迟"（有 results.jsonl
            # 兜底），哨兵超时却是"永久挂死"，二者代价完全不对等。
            # 故死等——下载侧只要还在消费就一定能腾出位置；它彻底不再消费时
            # 会调 shutdown()，那里会排空队列把这个 put 解开。
            self._main_done.set()
            self._q.put(_SENTINEL)
            print("\n[pipeline] 取流主任务结束，转入待命态（继续服务重新取流请求）",
                  flush=True)
            # 待命态：主任务做完了，但下载侧还在跑，随时可能有直链过期需要重取。
            # 只是"活着待命"，不占 CPU；真正的重取由下载侧直接调
            # fetcher.process_tmdb_id 完成（那是线程安全的纯函数调用）。
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
            # （它们都已落盘 results.jsonl，下次运行照样能读到，不丢片）。
            try:
                while True:
                    self._q.get_nowait()
            except queue.Empty:
                pass
            self.thread.join(timeout=0.1)


class AsyncRefetcher:
    """异步重取流：把"直链过期，换一条新的"交给常驻取流线程，主循环不阻塞。

    替代 download_movies.refetch_entries 的**同步**调用（§10.21 B-6：那条路
    由主事件循环线程跑，期间 wait(pending) 停摆可达数十分钟，只好加
    AUTO_REFETCH_TIMEOUT 硬兜）。这里主循环只做两个瞬时动作：
        dispatch(entries)  投递请求，立即返回
        collect()          取走已完成的结果，没有就返回空

    ⚠️ 结果**不走主队列**：主队列有哨兵语义，取流主任务一结束就被标记 done，
    之后推进去的结果再也取不出来（已实测验证）。故用独立的 _done 队列。
    首轮由 QueueEntrySource.poll() 直接 collect() 随到随投（R-B），
    第二轮起来源是 list，由 _run_pipeline 轮末 collect() 并入下一轮。

    ⚠️ worker 先 _done.put 再减 _inflight：故 pending_count()==0 时，
    结果一定已经在 _done 里，调用方"在途归零后再 collect 一次"就不会漏。

    落盘由 refetch_entries 的同款逻辑保证：新结果写进 INPUT_JSONL，
    即使本次运行没赶上消费，下次启动也能按 fetched_at 择新直接用上。
    """

    def __init__(self, workers, stop_event, providers=None):
        self._in = queue.Queue()
        self._done = queue.Queue()
        self._stop = stop_event
        self._workers = max(1, int(workers))
        self._threads = []
        # 取流源列表（B-1）：与主取流任务保持一致。None 表示用 config 默认
        # （fetcher.ACTIVE_PROVIDERS）；命令行 --providers 覆盖时必须一并透传，
        # 否则重取会绕过用户明确禁用/指定的源，与主任务口径不一致。
        self._providers = list(providers) if providers else None
        # 在途计数（已投递、尚未产出结论）。dispatch 时 +1，worker 处理完 -1，
        # 无论成功/失败/无果都要减——否则主循环会一直以为还有货没回来，
        # 白等满 ASYNC_REFETCH_WAIT_SECONDS。
        self._inflight = 0
        # 在途 id 集合（B-2）：同一部片"预检投了一次、旧链接下载失败又投一次"
        # 是常态（预检不等结果，旧链接照常先试）。第二次投递在第一次没回来
        # 之前毫无意义——拿到的还是同一批源的新链接——纯烧取流配额与源站请求。
        # 故在途期间的重复投递直接拒收；dispatch 返回实际接收条数，
        # 调用方据此决定是否计入每片重取上限。
        self._inflight_ids = set()
        self._lock = threading.Lock()
        self.dispatched = 0
        self.skipped_inflight = 0
        self.revived = 0

    def start(self):
        for i in range(self._workers):
            t = threading.Thread(target=self._loop, name=f"refetch-{i}",
                                 daemon=True)
            t.start()
            self._threads.append(t)

    def dispatch(self, entries):
        """主循环调用：投递重取请求。绝不阻塞（无界队列）。

        返回实际接收的条数：同一 id 已在途的条目被拒收（不计数、不入队）。
        """
        accepted = 0
        for entry in entries:
            tid = entry.get("tmdbId")
            key = str(tid) if tid is not None else None
            with self._lock:
                if key is not None and key in self._inflight_ids:
                    self.skipped_inflight += 1
                    continue
                if key is not None:
                    self._inflight_ids.add(key)
                self._inflight += 1
                self.dispatched += 1
            self._in.put(entry)
            accepted += 1
        return accepted

    def collect(self):
        """主循环调用：取走目前已完成的重取结果。绝不阻塞。"""
        out = []
        while True:
            try:
                out.append(self._done.get_nowait())
            except queue.Empty:
                return out

    def pending_count(self):
        """还有多少条在途。主循环靠它决定"还要不要再等一会儿"。"""
        with self._lock:
            return self._inflight

    def _loop(self):
        while not self._stop.is_set():
            try:
                entry = self._in.get(timeout=0.5)
            except queue.Empty:
                continue
            # 取出后无论走哪条分支，都必须把在途计数减掉，否则主循环会误以为
            # 还有货没回来、白等满等待上限。故整体包在 try/finally 里。
            try:
                self._handle(entry)
            except Exception as exc:  # noqa: BLE001
                print(f"  [重取异常] {entry.get('tmdbId')}: {exc}", flush=True)
            finally:
                tid = entry.get("tmdbId")
                with self._lock:
                    self._inflight -= 1
                    if tid is not None:
                        self._inflight_ids.discard(str(tid))

    def _handle(self, entry):
        tmdb_id = entry.get("tmdbId")
        if tmdb_id is None:
            return
        try:
            status, result = fetcher.process_tmdb_id(
                tmdb_id, providers=self._providers
            )
        except (Exception, SystemExit) as exc:
            # 单片重取失败绝不能带塌整批；SystemExit 一并兜住
            # （取流侧用它做配置校验），但不拦 KeyboardInterrupt。
            print(f"  [重取失败] {tmdb_id}: {exc}", flush=True)
            return
        if status != "ok" or not result or not result.get("urls"):
            # dead（源站确认无此片）与 retry（瞬时错误耗尽）都救不回来。
            print(f"  [重取无果] {tmdb_id}: {status}", flush=True)
            return
        # 落盘，与取流侧行为一致（追加写，下游按 fetched_at 择新）。
        # 即使本次运行没消费到，下次启动也能用上。
        downloader.write_log(downloader.INPUT_JSONL, result)
        # 逐键覆盖而非整体替换：entry 可能带有 result 没有的历史字段
        # （title/year/runtime_minutes 等元数据）。
        new_entry = dict(entry)
        new_entry["urls"] = result["urls"]
        new_entry["fetched_at"] = result.get("fetched_at")
        # 字幕列表同步：旧 captions 指向旧节点的字幕地址，跟旧 urls 一起作废；
        # 取流侧没给就置空，别让下载侧拿着过期地址去拉字幕。
        new_entry["captions"] = result.get("captions") or []
        self._done.put(new_entry)
        with self._lock:
            self.revived += 1
        print(f"  [重取成功] {tmdb_id}: {len(result['urls'])} 个新节点",
              flush=True)


def main():
    argv = sys.argv[1:]
    if "--refetch-failed" in argv:
        raise SystemExit(
            "pipeline 模式不支持 --refetch-failed（它是串行模式下的人工补救入口）。\n"
            "pipeline 已内建实时重取；若只想补跑过期直链，"
            "请单独运行 python tmdb_ids_to_links.py --refetch-failed"
        )

    # ⚠️ 必须在起线程**之前**校验参数。argv 是原样透传给取流侧的，若等到取流
    # 线程里 argparse 才发现打错（如 --typo），那时下载侧已经开跑了：它会拿着
    # 现有 results.jsonl 跑一整轮全量下载，而用户只是想让程序报错停下。
    # 全量场景下这等于误启动几十万部片的下载任务（已探针实测复现）。
    args = fetcher._parse_args(argv)
    # 命令行 --providers 覆盖了 config 时，异步重取也要用同一份源列表（B-1），
    # 否则重取会绕过用户明确禁用/指定的源，与主取流任务口径不一致。
    providers_arg = getattr(args, "providers", None)
    refetch_providers = (
        fetcher._resolve_providers(providers_arg) if providers_arg else None
    )

    print("=" * 70)
    print("pipeline 模式：取流与下载在同一进程内重叠运行")
    print(f"队列容量 {QUEUE_MAXSIZE}，满时取流侧自动降速（反压）")
    print("=" * 70, flush=True)

    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    worker = FetchWorker(q, argv=argv)
    # source 在 source_factory 里才建得出来——首轮的 backlog（断点续跑存量）
    # 要等 _run_pipeline 读完 results.jsonl 才知道。
    holder = {"source": None}

    # 把首轮来源换成队列；第二轮起仍是 list（重试批次量小且已确定），
    # 故多轮语义、轮次冷却、就地重取流的触发时机全部不变（§12.5′ 第 1 步）。
    real_list_source = downloader.ListEntrySource

    # 异步重取流：复用 download_movies.auto_refetch 的开关与并发数（用户拍板
    # 不另设开关）。装上钩子后，下载侧的"直链过期"就不再走同步的
    # refetch_entries，而是丢给这里的常驻线程，主循环一步都不阻塞。
    # 必须先于 source_factory 建好：首轮 QueueEntrySource 要直接消费它的
    # 结果，让新直链在首轮内就被下载（R-B）。
    refetcher = None
    real_hook = downloader.async_refetch_hook
    if downloader.AUTO_REFETCH_ENABLED:
        refetcher = AsyncRefetcher(
            downloader.AUTO_REFETCH_WORKERS, worker._stop,
            providers=refetch_providers,
        )
        refetcher.start()
        downloader.async_refetch_hook = refetcher
        print(f"异步重取流已启用（{downloader.AUTO_REFETCH_WORKERS} 个重取线程，"
              f"每部片最多 {downloader.AUTO_REFETCH_MAX_PER_MOVIE} 次）",
              flush=True)

    def _is_busy(key):
        # 该 id 的旧链接还在下载线程里 → 新链接先压着，等它退出处理态再投，
        # 否则会被 process_one_entry 判成 "duplicate entry currently processing"。
        return downloader.normalize_tmdb_id(key) in downloader.processing_ids

    def source_factory(entries):
        if holder["source"] is None:
            # entries = 下载侧从既有 results.jsonl 读出的存量，必须发完，
            # 否则这批片会被静默丢掉（取流侧不会再产出它们）。
            # 但投递顺序是 重取结果 > 实时队列 > 存量：实时队列里的直链最新鲜，
            # 且队列满会让取流侧 120s 后丢条目，不能让大存量把它们挤掉（R-A）。
            holder["source"] = QueueEntrySource(
                q, backlog=entries, refetcher=refetcher, is_busy=_is_busy
            )
            if holder["source"].backlog_total:
                print(
                    f"[pipeline] 断点续跑存量 {holder['source'].backlog_total} 部，"
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
        downloader.async_refetch_hook = real_hook
        worker.shutdown()
        # ⚠️ 收尾统计必须在 finally 里：Ctrl+C 时它才是最该被看到的东西
        # （跑了多久、取到多少、下了多少、有多少留给下次）。
        # 放在 try 之外的话，中断路径会整段跳过，用户只看到一个光秃秃的 traceback。
        _print_summary(worker, holder["source"], refetcher, started, interrupted)

    return 130 if interrupted else 0


def _print_summary(worker, source, refetcher, started, interrupted=False):
    delivered = source.delivered if source else 0
    elapsed = time.time() - started
    print("\n" + "=" * 70)
    print(f"[pipeline] {'已中断' if interrupted else '结束'}："
          f"取流入队 {worker.enqueued} 部"
          + (f"（{worker.dropped} 部改走文件承接）" if worker.dropped else "")
          + f"，下载侧消费 {delivered} 部，总耗时 {elapsed / 60:.1f} 分钟")
    if refetcher is not None and refetcher.dispatched:
        stranded = refetcher.dispatched - refetcher.revived
        in_round = getattr(source, "revived_delivered", 0) if source else 0
        print(f"[pipeline] 异步重取：投递 {refetcher.dispatched} 部，"
              f"换到新直链 {refetcher.revived} 部"
              + (f"（{in_round} 部在首轮内即刻消费）" if in_round else "")
              + (f"（{refetcher.skipped_inflight} 次因同片重取在途而合并）"
                 if getattr(refetcher, "skipped_inflight", 0) else "")
              + (f"（{stranded} 部未及回收，已落盘 results.jsonl，"
                 f"下次运行自动使用）" if stranded > 0 else ""))
    if worker.error is not None:
        print(f"⚠️ 取流侧曾异常终止：{worker.error}")
        print("   已取到的片仍已下载；重跑本命令可继续未完成的部分。")
    if interrupted:
        print("   进度已全部落盘（results.jsonl / success.jsonl），"
              "重跑本命令即可从断点继续。")
    print("=" * 70, flush=True)


if __name__ == "__main__":
    sys.exit(main())
