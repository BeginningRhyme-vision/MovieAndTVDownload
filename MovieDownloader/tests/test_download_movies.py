"""download_movies.py 多源下载改动的离线用例。

覆盖本轮扩源在下载侧的改动：urls 条目归一（兼容旧的裸字符串形态）、
mp4 直链请求头、失败分类与被拒原因归类。全部为纯函数级测试，不联网。
"""

import json
import os
import threading
import time

import pytest

import download_movies as d


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """把模块级路径全部指到 tmp_path，并重置共享状态。"""
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
    monkeypatch.setattr(d, "INPUT_JSONL", str(tmp_path / "results.jsonl"))
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "success.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    monkeypatch.setattr(d, "FOLDER_PREFIX", "movie_")
    monkeypatch.setattr(d, "START_FOLDER_INDEX", 1)
    monkeypatch.setattr(d, "_current_folder_index", 1)
    monkeypatch.setattr(d, "processing_ids", set())
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    return tmp_path


# -------------------------------------------------------------- urls 条目归一

def test_plain_string_url_is_treated_as_vidup_m3u8():
    """旧 results.jsonl 的 urls 是裸字符串数组。归一后必须还能消费，
    否则换代码后历史数据全部读不动。"""
    node = d._normalize_url_entry("https://a/master.m3u8")
    assert node == {
        "url": "https://a/master.m3u8", "provider": "vidup", "type": "m3u8",
        "headers": {}, "quality": None, "size": None,
    }


def test_dict_entry_keeps_provider_headers_quality_size():
    node = d._normalize_url_entry({
        "url": "https://cdn/1080.mp4", "provider": "vidlink", "type": "mp4",
        "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": 1234,
    })
    assert node["type"] == "mp4"
    assert node["provider"] == "vidlink"
    assert node["headers"] == {"User-Agent": "okhttp/4.9.3"}
    assert node["quality"] == 1080 and node["size"] == 1234


@pytest.mark.parametrize("item", [
    None, 123, [], "", "   ",
    {"url": ""},
    {"url": None},
    {"url": "https://a/x", "type": "torrent"},   # 未知 type 无法下载
])
def test_invalid_entries_are_dropped(item):
    # 非法条目返回 None 由调用方跳过，不能让一条脏数据整片失败
    assert d._normalize_url_entry(item) is None


def test_entry_type_defaults_to_m3u8_and_is_case_insensitive():
    assert d._normalize_url_entry({"url": "https://a/x"})["type"] == "m3u8"
    assert d._normalize_url_entry({"url": "https://a/x", "type": "MP4"})["type"] == "mp4"


def test_non_dict_headers_fall_back_to_empty():
    node = d._normalize_url_entry({"url": "https://a/x", "headers": "oops"})
    assert node["headers"] == {}


@pytest.mark.parametrize("field", ["quality", "size"])
@pytest.mark.parametrize("bad", [0, -1, -1080, "0", "-5"])
def test_non_positive_quality_and_size_become_none(field, bad):
    """results.jsonl 是跨进程的不可信输入。负数/0 会造成实质损害：
    quality<=0 让节点被判定性淘汰；size<=0 作为总长兜底会算出空区间。
    归 None 即"未声明"，交由下游实测，是安全的退化方向。"""
    node = d._normalize_url_entry({"url": "https://a/x", field: bad})
    assert node[field] is None


def test_positive_quality_and_size_survive():
    node = d._normalize_url_entry({"url": "https://a/x", "quality": 1080, "size": 123})
    assert node["quality"] == 1080 and node["size"] == 123


# ------------------------------------------------------------ mp4 直链请求头

def test_mp4_headers_strip_referer_and_xhr():
    """vidlink CDN 带任何 Referer 都会 429、浏览器 UA 无 Referer 会 428，
    只有 okhttp UA + 无 Referer 能拿到 206。None 值用于让 requests 删掉 Session 默认头。"""
    headers = d._mp4_request_headers({"User-Agent": "okhttp/4.9.3"})
    assert headers["Referer"] is None
    assert headers["X-Requested-With"] is None
    assert headers["User-Agent"] == "okhttp/4.9.3"


def test_mp4_headers_work_without_node_headers():
    headers = d._mp4_request_headers(None)
    assert headers["Referer"] is None


# ------------------------------------------------------------------ parse_int

@pytest.mark.parametrize("value,expected", [
    (1080, 1080), ("1080", 1080), ("  1080  ", 1080),
    (True, None),      # bool 是 int 子类，size=true 不能变成 1
    (False, None),
    (1080.0, None),    # 严格解析：float 走 str() 后是 "1080.0"，int() 抛错
    (None, None), ("", None), ("abc", None), ([], None),
])
def test_parse_int_rejects_bool_and_garbage(value, expected):
    assert d.parse_int(value) == expected


# ------------------------------------------------------------------ 失败分类

def test_direct_link_expiry_is_permanent_for_this_script():
    """vidlink 直链带时效签名，403/410 表示这条 url 过期了。

    本脚本读的是固化的 results.jsonl，没有重新取流的能力——多轮重投拿到的还是
    同一条过期 url，必然再挂。故这里判死、只留 failed.jsonl，靠重跑
    tmdb_ids_to_links.py 换一条新直链来修复。
    """
    msg = f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: https://cdn/a.mp4"
    assert d._classify_failure(msg) is False


def test_generic_http_403_does_not_kill_the_whole_movie():
    """对比上一条：非直链的 403 只用于"层内短路"（尽快换下一个节点），
    不能让整片判死——它常常只是源站临时风控，冷却一轮往往就恢复。"""
    assert d._HTTP_PERMANENT_MARKER not in d._PERMANENT_FAILURE_MARKERS


@pytest.mark.parametrize("msg", [
    "分辨率 640x360 低于红线",
    "码率未达到门槛：500 kbps < 1480 kbps",
    "缺少 tmdbId 或 urls",
])
def test_quality_failures_are_permanent(msg):
    # 真不达标的片重试多少轮都一样，不能浪费代理流量
    assert d._classify_failure(msg) is False


@pytest.mark.parametrize("msg", [
    "HTTP 500 Server Error",
    "Read timed out",
    "本轮候选流无一入选（各流原因见上方日志），下一轮重采",
])
def test_transient_failures_are_retriable(msg):
    assert d._classify_failure(msg) is True


def test_reject_reason_rules_are_ordered_before_generic_ones():
    """归类是按顺序首个命中，直链专属类目必须排在通用的"超时/SSL"之前，
    否则含 timeout 字样的直链错误会被归错桶，统计失真。"""
    reason = d.classify_reject_reason(
        f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: https://cdn/a.mp4")
    assert "重新取流" in reason


def test_refetch_label_is_what_classifier_actually_emits():
    """🔒 收尾提示靠 `reject_permanent[_REFETCH_REASON_LABEL]` 判断要不要提醒用户跑
    --refetch-failed。若该常量与分类器实际产出的类别名对不上，计数恒为 0，
    提示永远不出现——而这些片正是"重跑下载无效、必须先重新取流"的那批。

    真正的风险不是改常量（规则表引用同一个常量，会一起变），而是**把规则表里
    那条规则删掉、改判定关键字、或被前面的规则抢先命中**。故断言分类器对真实
    错误文案的产出，而不是断言两个常量相等（那是同义反复）。
    """
    reason = d.classify_reject_reason(
        f"直链已失效（HTTP 410），{d._NEEDS_REFETCH_MARKER}: https://cdn/a.mp4")
    assert reason == d._REFETCH_REASON_LABEL
    # 规则表里必须真有这一类，否则上面的断言在规则被删后会静默退化
    assert any(label == d._REFETCH_REASON_LABEL
               for label, _ in d._REJECT_REASON_RULES)


# ------------------------------------------------ 输入去重按 fetched_at 择新

def _pick_latest(rows):
    """复刻 main() 读入阶段的择新逻辑，用于单测（主流程耦合文件 IO 不便直接调）。"""
    by_id = {}
    for entry in rows:
        tid = d.normalize_tmdb_id(entry.get("tmdbId"))
        if not tid:
            continue
        prev = by_id.get(tid)
        if prev is None:
            by_id[tid] = entry
            continue
        new_ts = d.parse_int(entry.get("fetched_at"))
        old_ts = d.parse_int(prev.get("fetched_at"))
        if (new_ts if new_ts is not None else -1) > (old_ts if old_ts is not None else -1):
            by_id[tid] = entry
    return by_id


def test_duplicate_entries_keep_the_newest_fetch():
    """results.jsonl 是追加写，同一片多轮重试会留下多行且不保证越靠后越新。
    必须按 fetched_at 取最新——vidlink 直链带时效签名，拿到旧的等于白跑一次下载。"""
    rows = [
        {"tmdbId": "1", "fetched_at": 200, "urls": ["new"]},
        {"tmdbId": "1", "fetched_at": 100, "urls": ["old"]},   # 后出现但更旧
    ]
    assert _pick_latest(rows)["1"]["urls"] == ["new"]


def test_entry_with_timestamp_beats_one_without():
    rows = [
        {"tmdbId": "1", "urls": ["no-ts"]},
        {"tmdbId": "1", "fetched_at": 5, "urls": ["has-ts"]},
    ]
    assert _pick_latest(rows)["1"]["urls"] == ["has-ts"]


def test_entries_without_timestamp_keep_the_first():
    # 都无戳时维持旧的位置语义（先出现者胜），不引入随机性
    rows = [
        {"tmdbId": "1", "urls": ["first"]},
        {"tmdbId": "1", "urls": ["second"]},
    ]
    assert _pick_latest(rows)["1"]["urls"] == ["first"]


def test_entries_missing_tmdb_id_are_dropped():
    rows = [{"tmdbId": None, "urls": ["x"]}, {"urls": ["y"]}, {"tmdbId": "7", "urls": ["z"]}]
    assert list(_pick_latest(rows)) == ["7"]


# ---------------------------------------------- 候选流采样提前终止（画质择优）

def _sampling_env(monkeypatch, variants):
    """多 variant 采样场景的公共桩：返回记录被采样流 url 的 list。

    显式置 RESOLUTION_CHECK_ENABLED=True：本组用例测的是"高度剪枝 / 高度择优"
    这类**模式 A 专属**行为，不能依赖模块默认值（默认已是模式 B）。
    """
    sampled = []
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: variants,
    )

    def fake_media(url, headers=None):
        sampled.append(url)
        return seg_urls, [4.0] * 20, None

    monkeypatch.setattr(d, "parse_media_playlist", fake_media)
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    # 红线压到 360：否则 720/480 会在候选过滤阶段（声明高度低于红线）就被剔除，
    # 根本进不了采样循环，测不出"选中后跳过更低流"这条逻辑。
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 360)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        return n * 1_000_000, [], 0   # 2000 kbps，稳过 1000 门槛

    monkeypatch.setattr(d, "download_segments", fake_download)
    return sampled


def test_variant_sampling_stops_after_higher_stream_wins(sandbox, monkeypatch):
    """选中 1080 后，声明高度更低的流不再采样。

    候选已按声明高度降序排，且有声明分辨率的流直接采信声明值（不做 ffprobe），
    择优又是"高度绝对优先"——更低的流即便采样也必然落选，那次"解析 media
    playlist + 下载 N 个分片 + 两次 ffprobe"是纯浪费。一个 master 常有
    1080/720/480/360 四档，白花的是三份采样流量。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _sampling_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
        ("854x480", "https://cdn/480.m3u8", 1500.0),
    ])

    entry = {"tmdbId": "55", "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert job["resolution"] == "1920x1080"
    assert sampled == ["https://cdn/1080.m3u8"]


def test_variant_sampling_still_compares_same_height(sandbox, monkeypatch):
    """同声明高度的流必须全部采样——要比采样码率才能择优。"""
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _sampling_env(monkeypatch, [
        ("1920x1080", "https://cdn/a.m3u8", 5000.0),
        ("1920x1080", "https://cdn/b.m3u8", 4000.0),
        ("1280x720", "https://cdn/c.m3u8", 3000.0),
    ])

    entry = {"tmdbId": "55", "urls": ["u"]}
    _, ok, _job = d.process_one_entry(entry, set())
    assert ok is True
    # 两个 1080 都采样，720 被跳过
    assert sampled == ["https://cdn/a.m3u8", "https://cdn/b.m3u8"]


def test_variant_sampling_still_probes_undeclared(sandbox, monkeypatch):
    """未声明分辨率的流不能跳过：真实高度可能更高，必须采样后 ffprobe。

    master 无 RESOLUTION 属性时这类流被排在末尾（用 -1 排序），若按"声明高度
    更低"一并跳过，就会把实际更清晰的流丢掉，直接违背画质择优目标。
    """
    # 未声明的那条实测为 2160p，应当胜出
    monkeypatch.setattr(d, "probe_resolution", lambda p: (3840, 2160))
    sampled = _sampling_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        (None, "https://cdn/unknown.m3u8", 4000.0),
    ])
    # 门槛按 (h/1080)² 缩放，2160p 需 4 倍基准；压低基准让桩数据能过关，
    # 本用例要验的是采样顺序而非码率曲线。
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 100.0})

    entry = {"tmdbId": "55", "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert sampled == ["https://cdn/1080.m3u8", "https://cdn/unknown.m3u8"]
    assert job["resolution"] == "3840x2160"


def test_variant_sampling_continues_after_failure(sandbox, monkeypatch):
    """最高档采样失败（未选中）时，后续较低流仍要采样。

    跳过条件绑定 best_selected：只有真正选中过某流才生效。否则瞬时抖动让最高
    档挂掉后，整片会因"无一入选"而白白失败，直接损失成功率。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _sampling_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
    ])
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    def flaky_media(url, headers=None):
        sampled.append(url)
        if "1080" in url:
            raise RuntimeError("采样抖动")
        return seg_urls, [4.0] * 20, None

    monkeypatch.setattr(d, "parse_media_playlist", flaky_media)

    entry = {"tmdbId": "55", "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert sampled == ["https://cdn/1080.m3u8", "https://cdn/720.m3u8"]
    assert job["resolution"] == "1280x720"


# ------------------------------------------------------ 落点目录：占位 + 锁外移动

def test_move_places_holder_inside_lock_then_moves_outside(sandbox, monkeypatch):
    """移动必须在锁外执行，锁内只落 0 字节占位定名额。

    base_dir 与 temp_dir 跨盘时 shutil.move 是 copy+delete，一部片几十秒；
    若在锁内做，所有转封装 worker 会被这把全局锁完全串行化。
    """
    seen = {}

    def fake_move(src, dst):
        # 移动进行时锁必须是空闲的（可被别的线程拿到）；同时占位文件已存在。
        seen["lock_free"] = d.folder_lock.acquire(blocking=False)
        if seen["lock_free"]:
            d.folder_lock.release()
        seen["holder_exists"] = os.path.exists(dst)
        seen["holder_size"] = os.path.getsize(dst)
        os.replace(src, dst)

    monkeypatch.setattr(d.shutil, "move", fake_move)
    src = os.path.join(d.TEMP_DIR, "temp_55.mp4")
    with open(src, "wb") as fh:
        fh.write(b"data")

    final_path = d.move_to_target_folder(src, "55")
    assert seen["lock_free"] is True
    assert seen["holder_exists"] is True and seen["holder_size"] == 0
    assert os.path.getsize(final_path) == 4


def test_holder_counts_toward_folder_capacity(sandbox, monkeypatch):
    """占位文件必须被目录容量计数算进去，否则并发下同一目录会超容量。"""
    monkeypatch.setattr(d, "MAX_VIDEOS_PER_FOLDER", 1)
    # 第一次移动卡在锁外（模拟慢速跨盘拷贝未完成），此时只有占位文件在目录里。
    monkeypatch.setattr(d.shutil, "move", lambda src, dst: None)
    src = os.path.join(d.TEMP_DIR, "temp_1.mp4")
    open(src, "wb").close()
    first = d.move_to_target_folder(src, "1")

    # 第二部片必须落到下一个目录，而不是与占位文件挤在同一个已满目录里。
    second = d.move_to_target_folder(src, "2")
    assert os.path.dirname(first) != os.path.dirname(second)


def test_failed_move_removes_holder(sandbox, monkeypatch):
    """移动失败要清掉占位/半成品，否则它既非成品又白占目录名额。"""
    def boom(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(d.shutil, "move", boom)
    src = os.path.join(d.TEMP_DIR, "temp_55.mp4")
    open(src, "wb").close()

    with pytest.raises(OSError):
        d.move_to_target_folder(src, "55")

    folder = os.path.join(d.BASE_DIR, "movie_000001")
    assert os.listdir(folder) == []


# ---------------------------------------------------------- 0 字节孤儿清理

def test_scan_removes_zero_byte_orphans(sandbox):
    """0 字节 mp4 是占位后进程被杀留下的残骸：既要清掉，也绝不能算已下载。

    若只跳过不删，它会永久占住目录名额；若算作已下载，该片会被永久跳过、
    再也不会被重新下载。
    """
    folder = os.path.join(d.BASE_DIR, "movie_000001")
    os.makedirs(folder)
    orphan = os.path.join(folder, "11.mp4")
    open(orphan, "wb").close()
    real = os.path.join(folder, "22.mp4")
    with open(real, "wb") as fh:
        fh.write(b"x")

    ids, dups = d.scan_downloaded_mp4_ids()
    assert ids == {"22"}
    assert dups == {}
    assert not os.path.exists(orphan)
    assert os.path.exists(real)


# ------------------------------------------------ 就地重取流（直链过期自愈）

def test_needs_refetch_only_matches_expired_direct_links():
    """只有"签名过期"才值得重取流。

    画质不达标、结构不支持这类确定性失败重取也是同样结果；404 直链不存在则本就
    不该救。把它们放进来只会白烧取流配额。
    """
    assert d.needs_refetch(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: u")
    assert not d.needs_refetch("分辨率 640x360 低于红线")
    assert not d.needs_refetch("直链块不可用")
    assert not d.needs_refetch("")
    assert not d.needs_refetch(None)


class _FakeFetcher:
    """冒充 tmdb_ids_to_links 模块，按 tmdbId 返回预设的取流结果。"""

    def __init__(self, results):
        self.results = results
        self.seen = []

    def process_tmdb_id(self, tmdb_id):
        self.seen.append(str(tmdb_id))
        return self.results[str(tmdb_id)]


def _install_fake_fetcher(monkeypatch, fetcher):
    import sys
    monkeypatch.setitem(sys.modules, "tmdb_ids_to_links", fetcher)


def test_refetch_replaces_urls_and_persists_new_result(sandbox, monkeypatch):
    """重取成功要做两件事：换掉 entry 的 urls，并把新结果落盘。

    落盘是关键——本次运行若中途被打断，下次启动能按 fetched_at 择新直接用上新
    链接，这次重取就不算白做。
    """
    fetcher = _FakeFetcher({
        "55": ("ok", {
            "tmdbId": "55", "urls": [{"url": "https://new/f.mp4"}],
            "fetched_at": 999,
        }),
    })
    _install_fake_fetcher(monkeypatch, fetcher)

    entry = {"tmdbId": "55", "title": "T", "year": 2020,
             "urls": [{"url": "https://old/f.mp4"}]}
    revived = d.refetch_entries([entry], {})

    assert len(revived) == 1
    assert revived[0]["urls"] == [{"url": "https://new/f.mp4"}]
    assert revived[0]["fetched_at"] == 999
    # 非取流字段必须保留：下游要用 title/year 拼 R2 键
    assert revived[0]["title"] == "T" and revived[0]["year"] == 2020
    # 原 entry 不被就地修改（重投的是新对象）
    assert entry["urls"] == [{"url": "https://old/f.mp4"}]
    # 新结果已追加进 results.jsonl
    with open(d.INPUT_JSONL, encoding="utf-8") as fh:
        assert json.loads(fh.readline())["urls"] == [{"url": "https://new/f.mp4"}]


def test_refetch_skips_movies_over_the_per_movie_cap(sandbox, monkeypatch):
    """重取次数用尽的片不再重取，防"取流-过期-重取"无限空转。"""
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_MOVIE", 2)
    fetcher = _FakeFetcher({"55": ("ok", {"tmdbId": "55", "urls": [{"url": "u"}]})})
    _install_fake_fetcher(monkeypatch, fetcher)

    entry = {"tmdbId": "55", "urls": []}
    assert d.refetch_entries([entry], {"55": 2}) == []
    assert fetcher.seen == []   # 一次取流都不该发生


def test_refetch_counts_attempts_even_when_fetch_fails(sandbox, monkeypatch):
    """失败也要计数，否则"每部最多 N 次"的上限形同虚设、可能无限重试。"""
    fetcher = _FakeFetcher({"55": ("dead", None)})
    _install_fake_fetcher(monkeypatch, fetcher)

    counts = {}
    assert d.refetch_entries([{"tmdbId": "55", "urls": []}], counts) == []
    assert counts == {"55": 1}


def test_refetch_survives_fetcher_exceptions(sandbox, monkeypatch):
    """单部片重取抛异常不能带塌整批：重取是尽力而为的捞回。"""
    class Boom(_FakeFetcher):
        def process_tmdb_id(self, tmdb_id):
            if str(tmdb_id) == "1":
                raise RuntimeError("proxy died")
            return super().process_tmdb_id(tmdb_id)

    fetcher = Boom({"2": ("ok", {"tmdbId": "2", "urls": [{"url": "u2"}]})})
    _install_fake_fetcher(monkeypatch, fetcher)

    revived = d.refetch_entries(
        [{"tmdbId": "1", "urls": []}, {"tmdbId": "2", "urls": []}], {}
    )
    assert [e["tmdbId"] for e in revived] == ["2"]


def test_refetch_returns_empty_when_fetcher_unavailable(sandbox, monkeypatch):
    """取流模块导入失败（缺代理凭证等）只跳过重取，绝不能崩掉下载流水线。"""
    import builtins
    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "tmdb_ids_to_links":
            raise ImportError("no proxy credentials")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    assert d.refetch_entries([{"tmdbId": "55", "urls": []}], {}) == []


def test_mixed_node_failure_is_both_retriable_and_refetchable(sandbox, monkeypatch):
    """m3u8 节点 5xx + mp4 节点直链过期：既要重投，也要换新直链。

    any_retriable 是乐观口径——任一节点可重试整片就 retriable=True。多源下
    "vidup m3u8 挂 5xx + vidlink mp4 签名过期"是常态（5xx 占可重试失败约八成）。
    若两条重投路径互斥，这类片只会被重投而永远不换新直链，那个 mp4 节点在剩余
    所有轮次里都是废的，白白损失一个可用源。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    def boom_master(url, retries=None, headers=None):
        raise RuntimeError(f"请求失败(HTTP Error 503): {url}")

    def expired_mp4(node, output_path, label, runtime_minutes=None, preflight=None):
        raise RuntimeError(
            f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: {node['url']}"
        )

    monkeypatch.setattr(d, "parse_master_playlist", boom_master)
    monkeypatch.setattr(d, "_download_mp4_direct", expired_mp4)

    entry = {"tmdbId": "55", "title": "T", "urls": [
        {"url": "https://a/m.m3u8", "provider": "vidup", "type": "m3u8",
         "headers": {}, "quality": None, "size": None},
        {"url": "https://a/x.mp4", "provider": "vidlink", "type": "mp4",
         "headers": {}, "quality": None, "size": None},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False

    # 关键：把真实失败信息喂给分桶决策，两个桶必须同时命中。
    should_retry, should_refetch = d.plan_retry_buckets(
        info["retriable"], info["error"]
    )
    assert should_retry is True
    assert should_refetch is True


@pytest.mark.parametrize("retriable,error,expected", [
    # 纯瞬时失败：只重投，不该白烧取流配额
    (True, "请求失败(HTTP Error 503)", (True, False)),
    # 纯直链过期：不重投（重投拿到的是同一条 url），只换新链接
    (False, f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}", (False, True)),
    # 混合：两者都要
    (True, f"节点1 503；节点2 {d._NEEDS_REFETCH_MARKER}", (True, True)),
    # 真判死：画质不达标，两个桶都不进
    (False, "分辨率 640x360 低于红线", (False, False)),
    (False, "码率未达到门槛", (False, False)),
    # 404 直链不存在：确定性失败，重取也救不回
    (False, "直链块不可用", (False, False)),
])
def test_retry_bucket_routing(retriable, error, expected):
    """判死的不进重试轮次，非判死的进——逐类锁死路由结果。"""
    assert d.plan_retry_buckets(retriable, error) == expected


def test_refetch_bucket_respects_the_kill_switch(monkeypatch):
    """auto_refetch 关掉后，过期直链不再进重取桶（完全回到旧行为）。"""
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", False)
    assert d.plan_retry_buckets(
        False, f"直链已失效，{d._NEEDS_REFETCH_MARKER}"
    ) == (False, False)


# ------------------------------------------------ 画质概率判死（§12.11 D 修复）

def _quality_env(monkeypatch, per_node):
    """构造多节点场景：per_node 为每个节点要抛的异常（None 表示成功）。

    返回 (entry, 调用记录)。节点全部走 mp4 分支，桩掉 _download_mp4_direct，
    这样不必伪造 playlist 就能精确控制每个节点的失败性质。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    # 择优阶段会对每个 mp4 节点发真实网络请求探总长/采样。本组用例只关心
    # 判死与重投口径，故整体桩掉——返回 None 表示"探不出码率"，_rank_mp4_nodes
    # 会保持上游原始顺序，与改动前的行为一致，用例断言的节点次序才稳定。
    monkeypatch.setattr(d, "_mp4_preflight", lambda *a, **k: None)
    calls = []

    def fake_mp4(node, output_path, label, runtime_minutes=None, preflight=None):
        idx = len(calls)
        calls.append(node["url"])
        exc = per_node[idx]
        if exc is not None:
            raise exc
        return "1920x1080", 3000.0

    monkeypatch.setattr(d, "_download_mp4_direct", fake_mp4)
    entry = {"tmdbId": "55", "title": "T", "urls": [
        {"url": f"https://a/{i}.mp4", "provider": "vidlink", "type": "mp4",
         "headers": {}, "quality": None, "size": None}
        for i in range(len(per_node))
    ]}
    return entry, calls


def test_majority_quality_rejection_kills_the_movie(sandbox, monkeypatch):
    """3 节点里 2 个画质淘汰 + 1 个 502 → 判死，不再进下一轮。

    这正是 §12.11 D 的现场（892515）：改动前 any_retriable 只要有一个 502 就把
    整片标成可重试，而画质声明下一轮一模一样，必然再挂。29 部这样的片白跑两轮
    共 28 分钟只救回 1 部。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    entry, _ = _quality_env(monkeypatch, [
        d.QualityRejectedError("声明分辨率 720p 低于红线 1080"),
        d.QualityRejectedError("声明分辨率 480p 低于红线 1080"),
        RuntimeError("请求失败(HTTP Error 502)"),
    ])

    _, ok, info = d.process_one_entry(entry, set())

    assert ok is False
    assert info["retriable"] is False
    # 文案必须换成汇总口径：改动前这里留的是末节点的 502，会写出
    # "retriable=False 却写着 502" 的自相矛盾记录，且统计会归错类。
    assert "因画质不达标" in info["error"]
    assert d.classify_reject_reason(info["error"]) == "画质整体不达标(判死)"
    # 落盘后只剩字符串，此时也必须仍判死（failed.jsonl 重新载入的路径）。
    assert d._classify_failure(info["error"]) is False
    assert d.plan_retry_buckets(info["retriable"], info["error"]) == (False, False)


def test_minority_quality_rejection_still_retries(sandbox, monkeypatch):
    """3 节点里只有 1 个画质淘汰（未过半）→ 维持乐观口径，照常重投。

    未达阈值的场景一律不动，保证"宁可多下不误杀"在多数情形下仍然成立。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    entry, _ = _quality_env(monkeypatch, [
        d.QualityRejectedError("声明分辨率 480p 低于红线 1080"),
        RuntimeError("请求失败(HTTP Error 502)"),
        RuntimeError("请求失败(HTTP Error 503)"),
    ])

    _, ok, info = d.process_one_entry(entry, set())

    assert ok is False
    assert info["retriable"] is True


def test_all_transient_failures_are_never_killed(sandbox, monkeypatch):
    """全节点都是瞬时失败 → 绝不能判死。

    §12.11 D 里三轮唯一救回的那部片（471998）正是这种形态：前几个节点全挂，
    最后一个节点一次成功。若把瞬时失败也计入画质分子，就会误杀掉唯一的正收益。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    entry, _ = _quality_env(monkeypatch, [
        RuntimeError("请求失败(HTTP Error 502)"),
        RuntimeError("请求失败(HTTP Error 502)"),
    ])

    _, ok, info = d.process_one_entry(entry, set())

    assert ok is False
    assert info["retriable"] is True


def test_kill_ratio_one_point_zero_requires_every_node(sandbox, monkeypatch):
    """阈值设 1.0 = 最保守档：必须全部节点都画质淘汰才判死。"""
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    per_node = [
        d.QualityRejectedError("声明分辨率 720p 低于红线 1080"),
        RuntimeError("请求失败(HTTP Error 502)"),
    ]
    entry, _ = _quality_env(monkeypatch, per_node)
    _, _ok, info = d.process_one_entry(entry, set())
    assert info["retriable"] is True   # 2 个里只有 1 个画质淘汰，未达 1.0

    entry2, _ = _quality_env(monkeypatch, [
        d.QualityRejectedError("声明分辨率 720p 低于红线 1080"),
        d.QualityRejectedError("声明分辨率 480p 低于红线 1080"),
    ])
    _, _ok2, info2 = d.process_one_entry(entry2, set())
    assert info2["retriable"] is False


def test_successful_node_is_never_killed(sandbox, monkeypatch):
    """前两个节点画质淘汰但第三个成功 → 整片成功，判死逻辑不得介入。

    判死只写在失败路径（except）里，这条用例锁死"过半淘汰"不会误伤成功片。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    monkeypatch.setattr(d, "convert_and_upload_enabled", True, raising=False)
    entry, calls = _quality_env(monkeypatch, [
        d.QualityRejectedError("声明分辨率 720p 低于红线 1080"),
        d.QualityRejectedError("声明分辨率 480p 低于红线 1080"),
        None,
    ])

    _, ok, job = d.process_one_entry(entry, set())

    assert ok is True
    assert job["resolution"] == "1920x1080"
    assert len(calls) == 3


def test_expired_link_on_non_last_node_still_triggers_refetch(sandbox, monkeypatch):
    """过期直链排在非末位时，仍必须进重取桶。

    改动前 needs_refetch 只看 error 文案，而 error 取的是**最后一个**节点的错误。
    「节点1 vidlink 403 过期 → 节点2 502」这种排列下，那条过期直链在剩余所有
    轮次里都是废的，且不报任何错——是 §10.21 B-2 在多节点场景下的漏网。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    entry, _ = _quality_env(monkeypatch, [
        RuntimeError(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}"),
        RuntimeError("请求失败(HTTP Error 502)"),
    ])

    _, ok, info = d.process_one_entry(entry, set())

    assert ok is False
    assert info["needs_refetch"] is True
    # 进程内路径：显式标志位。
    assert d.plan_retry_buckets(
        info["retriable"], info["error"], info["needs_refetch"]
    ) == (True, True)
    # 落盘路径：`--refetch-failed` 只能读 failed.jsonl 的文案，marker 必须在。
    assert d._NEEDS_REFETCH_MARKER in info["error"]


def test_quality_kill_keeps_the_refetch_marker(sandbox, monkeypatch):
    """判死改写文案后，过期 marker 不能被顺手抹掉。

    判死说的是"这些节点画质不行"，与"另一个节点的直链该换新的"是两件事：
    换到新直链后画质可能就达标了，抹掉 marker 等于永久放弃这条救援路径。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    entry, _ = _quality_env(monkeypatch, [
        d.QualityRejectedError("声明分辨率 480p 低于红线 1080"),
        d.QualityRejectedError("声明分辨率 360p 低于红线 1080"),
        RuntimeError(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}"),
    ])

    _, _ok, info = d.process_one_entry(entry, set())

    assert info["retriable"] is False
    assert "因画质不达标" in info["error"]
    assert d._NEEDS_REFETCH_MARKER in info["error"]
    assert d.plan_retry_buckets(
        info["retriable"], info["error"], info["needs_refetch"]
    ) == (False, True)


def test_all_streams_quality_rejected_is_deterministic(sandbox, monkeypatch):
    """单节点内：全部候选流都因画质淘汰 → 抛确定性异常，不再报"无一入选"。

    改动前这里一律落"本轮候选流无一入选（可重试）"，把"全部真不达标"和
    "采样抖动全挂"混为一谈。前者下一轮重采结果完全相同，是纯浪费。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    _sampling_env(monkeypatch, [
        ("1920x1080", "https://cdn/a.m3u8", 5000.0),
        ("1920x1080", "https://cdn/b.m3u8", 4000.0),
    ])
    # 门槛抬到采样码率（2000 kbps）之上，让两条流都栽在码率关。
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 9000.0})

    _, ok, info = d.process_one_entry({"tmdbId": "55", "urls": ["u"]}, set())

    assert ok is False
    assert info["retriable"] is False
    assert "因画质不达标" in info["error"]


# -------------------------------------- 分辨率判定开关（resolution_check_enabled）

def test_resolution_check_defaults_to_off():
    """护栏：默认业务语义是「只按码率判断画质」（模式 B）。

    这是 2026-09-09 由用户拍板的口径切换。锁死它有两层意义：
      ① 谁改回 true 都会在这里被拦下，避免默认口径被无意翻转；
      ② 提醒后来者：任何测"高度剪枝 / 高度择优 / 分辨率红线"的用例都必须
         自己显式 monkeypatch 成 True，不能依赖模块默认值。
    """
    import yaml

    with open(os.path.join(os.path.dirname(d.__file__), "config.yaml")) as fh:
        cfg = yaml.safe_load(fh)["download_movies"]

    assert cfg["resolution_check_enabled"] is False
    assert d.RESOLUTION_CHECK_ENABLED is False
    # 码率红线 2 Mbps（用户指定），四档等效基准的相对关系见下面的用例。
    assert cfg["bitrate_h264"] == 2000


def test_bitrate_threshold_scales_with_height_when_resolution_counts(monkeypatch):
    """开关启用：门槛 = 基准 × (h/1080)² × leniency（现行口径）。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 2000.0})
    monkeypatch.setattr(d, "LENIENCY", 0.8)

    assert d.bitrate_threshold(1080, "h264") == pytest.approx(1600)
    assert d.bitrate_threshold(2160, "h264") == pytest.approx(6400)
    # 480p 被缩到很低——这正是模式 A 的设计（低清片由红线关拦，不靠码率关）。
    assert d.bitrate_threshold(480, "h264") == pytest.approx(2000 * (480 / 1080) ** 2 * 0.8)


def test_bitrate_threshold_is_absolute_when_resolution_is_off(monkeypatch):
    """开关禁用：门槛 = 基准 × leniency，与高度**完全无关**。

    🔑 这是"只按码率判断"的核心。若保留 (h/1080)² 缩放，480p 的门槛会被缩到
    约 395 kbps —— 低分辨率片反而更容易过关，等于分辨率以更隐蔽的方式仍在
    参与判定，与开关意图正好相反。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 2000.0})
    monkeypatch.setattr(d, "LENIENCY", 0.8)

    for height in (360, 480, 720, 1080, 2160):
        assert d.bitrate_threshold(height, "h264") == pytest.approx(1600)


def test_codec_baselines_stay_relative_when_resolution_is_off(monkeypatch):
    """禁用分辨率后，各编码之间的等效关系必须仍然成立。

    否则会用 H.264 的绝对线去卡 AV1，把同主观画质的高效编码片全部误杀。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {
        "h264": 2000.0, "hevc": 1189.0, "av1": 1000.0, "vp9": 1514.0,
    })

    assert d.bitrate_threshold(1080, "av1") < d.bitrate_threshold(1080, "hevc")
    assert d.bitrate_threshold(1080, "hevc") < d.bitrate_threshold(1080, "h264")
    # 探测不到编码时回退最严的 H.264 基准，禁用分辨率后也不能变。
    assert d.bitrate_threshold(1080, None) == pytest.approx(2000)


def test_resolution_redline_is_bypassed_when_disabled(monkeypatch):
    """开关禁用后红线关恒真——所有红线关卡靠这一个函数统一失效。"""
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 0.8)

    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    assert d.meets_resolution_redline(360) is False

    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    assert d.meets_resolution_redline(360) is True


def test_declared_480p_gate_follows_the_switch(sandbox, monkeypatch):
    """声明 480p 的 mp4 直链：开关启用时当场判死，禁用时必须放行到下一步。

    真穿过 `_download_mp4_direct` 的第一道关卡，不桩该函数本身——否则改坏关卡
    测试也发现不了（§12.13 I 的教训）。用"下一步抛哨兵异常"来证明确实放行了。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 1.0)

    def sentinel(*_a, **_k):
        raise RuntimeError("已越过声明分辨率关（哨兵）")

    # 关卡的下一步就是探总长，拿它当"是否放行"的探针。
    monkeypatch.setattr(d, "_mp4_probe_total_size", sentinel)

    entry = {"tmdbId": "55", "title": "T", "urls": [
        {"url": "https://a/1.mp4", "provider": "vidlink", "type": "mp4",
         "headers": {}, "quality": 480, "size": None},
    ]}

    # 模式 A：拦在第一关，根本走不到探总长。
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "低于红线" in info["error"]
    assert "哨兵" not in info["error"]

    # 模式 B：红线关放行，走到了探总长（哨兵被触发）。
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "processing_ids", set())
    _, ok2, info2 = d.process_one_entry(entry, set())
    assert ok2 is False
    assert "哨兵" in info2["error"]


def test_selection_prefers_bitrate_when_resolution_is_off(sandbox, monkeypatch):
    """禁用分辨率后择优纯比码率：低分辨率高码率的流可以胜出。

    若择优仍按高度优先，等于分辨率虽然不参与"能不能过关"、却仍主导
    "多条合格流选哪条"，开关就没有真正生效。

    ⚠️ 声明带宽故意与实测码率**反向**设置（1080p 声明高、实测低）：这样
    候选排序会把 1080p 排在前面并先选中它，从而真正走到"按高度剪枝"那条
    路径上。若两者同向，剪枝条件根本不成立，用例就锁不住任何东西。
    """
    picked = _run_two_stream_pick(
        monkeypatch, sandbox, resolution_check=False,
        # (分辨率, 声明带宽, 实测采样码率)
        streams=[("1920x1080", 9000, 1200), ("854x480", 1000, 4000)],
    )
    assert picked == "854x480"


def test_selection_prefers_height_when_resolution_is_on(sandbox, monkeypatch):
    """开关启用时择优仍是高度绝对优先（现行口径不得被改动）。"""
    picked = _run_two_stream_pick(
        monkeypatch, sandbox, resolution_check=True,
        streams=[("1920x1080", 5000, 2500), ("854x480", 1500, 9000)],
    )
    assert picked == "1920x1080"


def test_early_termination_is_off_when_resolution_is_off(sandbox, monkeypatch):
    """禁用分辨率后，"声明高度更低就跳过采样"的剪枝必须停用。

    该剪枝的正确性完全建立在"择优按高度绝对优先"之上。纯比码率时，声明高度
    低不代表实测码率低，继续剪枝会真的丢掉更优的流（本例中 480p 才是赢家）。
    """
    sampled = []
    picked = _run_two_stream_pick(
        monkeypatch, sandbox, resolution_check=False,
        streams=[("1920x1080", 9000, 1200), ("854x480", 1000, 4000)],
        sampled_sink=sampled,
    )
    assert len(sampled) == 2, "两条流都必须采样，不能按高度剪枝"
    assert picked == "854x480", "被剪掉的那条恰恰是更优的流"


def _run_two_stream_pick(monkeypatch, sandbox, resolution_check, streams,
                         sampled_sink=None):
    """跑一次两条候选流的完整择优，返回最终选中流的分辨率字符串。

    streams 为 [(分辨率字符串, 声明 BANDWIDTH, 目标实测采样码率 kbps)]。
    声明带宽只影响候选排序，实测码率靠控制采样字节数精确造出——两者分开是
    刻意的：真实源站的声明带宽本就未必等于实测码率。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", resolution_check)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    # 开关启用时红线要压低，否则 480p 在候选过滤阶段就被剔除、测不到择优。
    if resolution_check:
        monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 360)

    variants = [
        (res, f"https://cdn/{res}.m3u8", float(bw)) for res, bw, _br in streams
    ]
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: variants,
    )
    bitrate_of = {f"https://cdn/{res}.m3u8": br for res, _bw, br in streams}

    def fake_media(url, headers=None):
        if sampled_sink is not None:
            sampled_sink.append(url)
        # 分片 url 里带上所属流，供 download_segments 反查目标码率。
        return [f"{url}#s{i}" for i in range(20)], [4.0] * 20, None

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        stream_url = urls[start_idx].split("#")[0]
        # 码率 = bytes×8/时长/1000，时长 = n×4.0s，反解出应写的字节数。
        target_kbps = bitrate_of[stream_url]
        total_bytes = int(target_kbps * 1000 * (n * 4.0) / 8)
        with open(out, "wb") as fh:
            fh.write(b"x" * 16)
        return total_bytes, [], 0

    monkeypatch.setattr(d, "parse_media_playlist", fake_media)
    monkeypatch.setattr(d, "download_segments", fake_download)
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)

    _, ok, job = d.process_one_entry({"tmdbId": "55", "urls": ["u"]}, set())
    assert ok is True, "两条流都应合格，本用例只检验择优结果"
    return job["resolution"]


# ------- 分辨率作为「前置依赖」的三处误杀（2026-09-09 复盘发现，§12.15）-------
# 开关只解决了"分辨率作为判定标准"，却漏了"分辨率作为必需中间数据"——
# 这三处在探测不到分辨率时会直接放弃，而它们其实只需要码率。

def test_finished_mp4_is_not_discarded_when_resolution_probe_fails(sandbox, monkeypatch):
    """🔴 整片已下完，不能只因探不到分辨率就丢弃（模式 B）。

    分辨率根本不参与判定，height 只会被 bitrate_threshold 忽略。为一个不再需要
    的值丢弃一部下载完成的 GB 级影片，是纯粹的误杀。

    本例真正跑完 `_download_mp4_direct`（只桩网络层），确保走到那处判断。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)   # 探不到分辨率
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")

    # 体积取小值（用例要真写盘），靠调低 duration 维持 2000 kbps 的比例：
    # 900_000 字节 × 8 / 3.6s / 1000 = 2000 kbps。
    total = 900_000
    monkeypatch.setattr(d, "_probe_duration", lambda p: 3.6)
    monkeypatch.setattr(d, "_mp4_probe_total_size",
                        lambda url, headers, declared: (total, True))
    # 跳过头部预检（本例要验的是**整片下完之后**那道复检）。
    monkeypatch.setattr(d, "_mp4_probe_quality_by_sample", lambda *a, **k: None)
    # 桩掉网络层：按 Range 返回等长字节，让复检拿到真实文件大小。
    monkeypatch.setattr(
        d, "_download_mp4_chunk",
        lambda url, headers, start, end, index, abort_event=None:
            b"x" * (end - start + 1),
    )

    entry = {"tmdbId": "55", "title": "T", "runtime_minutes": 60, "urls": [
        {"url": "https://a/1.mp4", "provider": "vidlink", "type": "mp4",
         "headers": {}, "quality": None, "size": None},
    ]}

    _, ok, job = d.process_one_entry(entry, set())

    assert ok is True, "整片已下完且码率达标，不该因探不到分辨率被丢弃"
    assert job["resolution"] == "未知分辨率"
    assert job["bitrate_kbps"] == pytest.approx(2000, rel=0.01)


def test_mp4_precheck_still_runs_without_resolution(sandbox, monkeypatch):
    """🔴 头部预检不能因探不到分辨率就整个作废（模式 B）。

    码率 = total_size×8/duration，与分辨率毫无关系。在这里返回 None 会让本可
    8MB 就淘汰的低码率片白下整片（GB 级），把预检省流量的收益完全架空。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)   # 探不到分辨率
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "_download_mp4_chunk", lambda *a, **k: b"x" * 1024)

    sample = os.path.join(str(sandbox), "s.mp4")
    # runtime_minutes=60 → duration 3600s；total_size 900MB → 2000 kbps
    probed = d._mp4_probe_quality_by_sample(
        "https://a/1.mp4", {}, 900_000_000, sample, "55", 60,
    )

    assert probed is not None, "模式 B 下必须仍能预检出码率"
    resolution, height, bitrate, codec = probed
    assert height == 0
    assert resolution == "未知分辨率"
    assert bitrate == pytest.approx(2000, rel=0.01)
    assert codec == "h264"


def test_mp4_precheck_still_bails_out_in_mode_a(sandbox, monkeypatch):
    """模式 A 下行为不变：探不到分辨率就放弃预检，下整片后再验。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    monkeypatch.setattr(d, "_download_mp4_chunk", lambda *a, **k: b"x" * 1024)

    sample = os.path.join(str(sandbox), "s.mp4")
    probed = d._mp4_probe_quality_by_sample(
        "https://a/1.mp4", {}, 900_000_000, sample, "55", 60,
    )
    assert probed is None


def test_m3u8_stream_survives_failed_resolution_probe(sandbox, monkeypatch):
    """🔴 m3u8 流层：探不到分辨率也应能凭码率入选（模式 B）。

    master 未声明 RESOLUTION 且 ffprobe 探测失败时，模式 B 下不该放弃这条流。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)   # 探不到
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)

    # master 不声明 RESOLUTION（第一项为 None），逼流层去 ffprobe 探测。
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: [
            (None, "https://cdn/only.m3u8", 3000.0),
        ],
    )
    monkeypatch.setattr(
        d, "parse_media_playlist",
        lambda url, headers=None: (
            [f"https://cdn/s{i}.ts" for i in range(20)], [4.0] * 20, None,
        ),
    )

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * 16)
        return n * 1_000_000, [], 0     # 2000 kbps，稳过 1000 门槛

    monkeypatch.setattr(d, "download_segments", fake_download)

    _, ok, job = d.process_one_entry({"tmdbId": "55", "urls": ["u"]}, set())

    assert ok is True, "探不到分辨率不应让这条合格流被放弃"
    assert job["resolution"] == "未知分辨率"


# ---------------------------------------------------------------------------
# 成品分辨率补探（待办 H）
# ---------------------------------------------------------------------------
# 模式 B 下采样探不到分辨率时不再阻断下载（§12.15，那是对的），代价是
# success.jsonl 的 resolution 落成占位文案——500 部实跑里占了 44%。
# 成品 mp4 是完整文件、moov 齐全，此时补探一次几乎必成。

def test_final_resolution_is_probed_when_download_could_not_tell(monkeypatch):
    """🔴 核心：下载阶段没探到的，在成品上补探并写入真值。"""
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1920, 1080))

    got = d._resolve_final_resolution(d.UNKNOWN_RESOLUTION, "/x/55.mp4", "55")

    assert got == "1920x1080", "成品补探必须把占位文案换成真实分辨率"


def test_final_resolution_keeps_placeholder_when_probe_also_fails(monkeypatch):
    """🔑 底线：成品也探不出来时保持占位，**绝不判失败**。

    片子已经下完并转封装好了，为一个元数据丢掉它是本末倒置。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)

    got = d._resolve_final_resolution(d.UNKNOWN_RESOLUTION, "/x/55.mp4", "55")

    assert got == d.UNKNOWN_RESOLUTION


def test_final_resolution_does_not_reprobe_known_value(monkeypatch):
    """已经有真值就不重复探——省一次 ffprobe（每部成品都要走这里）。"""
    called = []
    monkeypatch.setattr(
        d, "probe_resolution", lambda p: called.append(p) or (640, 360),
    )

    got = d._resolve_final_resolution("1920x1080", "/x/55.mp4", "55")

    assert got == "1920x1080", "已知真值不该被覆盖"
    assert called == [], "已有真值时不该再调 ffprobe"


def test_finalize_writes_probed_resolution_into_success_info(
    sandbox, monkeypatch,
):
    """端到端：补探结果要真的落进 success_info，而不只是函数返回值。"""
    monkeypatch.setattr(d, "convert_ts_to_mp4", lambda src, dst: True)
    monkeypatch.setattr(d, "move_to_target_folder", lambda p, i: "/out/55.mp4")
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1280, 720))

    job = {
        "tmdbId": "55", "normalized_id": "55", "title": "T", "year": 2024,
        "url": "https://a/1.m3u8",
        "final_ts": os.path.join(str(sandbox), "t.ts"),
        "temp_mp4": os.path.join(str(sandbox), "t.mp4"),
        "cleanup_paths": [],
        "bitrate_kbps": 2000,
        "resolution": d.UNKNOWN_RESOLUTION,
        "missing_segment_count": 0,
        "missing_segment_indices": [],
    }

    _, ok, info = d.finalize_one_entry(job, set())

    assert ok is True
    assert info["resolution"] == "1280x720", (
        "success.jsonl 里必须是补探到的真实分辨率，不能还是占位文案"
    )


def test_unknown_resolution_constant_matches_literals():
    """护栏：常量与三处产出点用的必须是同一个字符串。

    补探靠 `resolution != UNKNOWN_RESOLUTION` 判断，措辞一旦对不上，
    补探会**静默失效**（不报错、只是永远认为"已有真值"）。
    """
    assert d.UNKNOWN_RESOLUTION == "未知分辨率"


def test_bitrate_reject_message_does_not_lead_with_resolution(monkeypatch):
    """模式 B 的淘汰文案不能以"分辨率 …"开头，但必须保留 marker。

    分辨率没参与判定却写在最前面，日后翻 failed.jsonl 会误以为是分辨率把片子
    卡掉的，从而对着一个根本不生效的 min_resolution_height 反复调参。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    msg = d.bitrate_reject_message("854x480", "h264", 900, 1600)

    assert msg.startswith("码率未达到门槛")
    assert "854x480" in msg          # 仍作为附带信息保留，便于排查
    # marker 不变：判定表、统计表、历史 failed.jsonl 都靠它工作。
    assert d._classify_failure(msg) is False
    assert d.classify_reject_reason(msg) == "码率未达门槛"

    # 模式 A 的措辞保持原样，不得被改动。
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    msg_a = d.bitrate_reject_message("854x480", "h264", 900, 1600)
    assert msg_a.startswith("分辨率 854x480")
    assert d._classify_failure(msg_a) is False


# ---------------- mp4 跨节点码率择优（§12.16）----------------
# 背景：mp4 是"试到第一个成功就 break"，不像 m3u8 会在候选流之间择优。取流侧
# 又按声明分辨率降序给节点，于是「1080p/1700kbps 刚过线」会直接胜出，而
# 「480p/8000kbps」根本不会被看到。

def _mp4_node(url, quality=None, size=None):
    return {"url": url, "provider": "vidlink", "type": "mp4",
            "headers": {}, "quality": quality, "size": size}


def _stub_preflight(monkeypatch, by_url):
    """按 url 给出预检结果。值为 (bitrate, estimated) 或 None（探测失败）。"""
    def fake(node, label, runtime_minutes=None):
        got = by_url.get(node["url"])
        if got is None:
            return None
        bitrate, estimated = got
        return {
            "node": node, "total_size": 1_000_000,
            "probed": None if estimated else ("1920x1080", 1080, bitrate, "h264"),
            "bitrate": bitrate, "estimated": estimated,
        }
    monkeypatch.setattr(d, "_mp4_preflight", fake)


def test_mp4_nodes_are_ranked_by_measured_bitrate(monkeypatch):
    """实测码率最高的节点排第一，哪怕它声明分辨率最低。

    这正是本功能要解决的场景：480p/8000kbps 必须赢过 1080p/1700kbps。
    """
    _stub_preflight(monkeypatch, {
        "https://a/1080.mp4": (1700.0, False),
        "https://a/480.mp4": (8000.0, False),
    })
    nodes = [_mp4_node("https://a/1080.mp4", 1080),
             _mp4_node("https://a/480.mp4", 480)]

    ordered = d._rank_mp4_nodes(nodes, "55", 60)

    assert [n["url"] for n, _i in ordered] == [
        "https://a/480.mp4", "https://a/1080.mp4",
    ]


def test_measured_bitrate_beats_estimated(monkeypatch):
    """实测优先于估算：估算值再高也不能压过实测的。

    估算来自源站声明的 size，未经 ffprobe 校验，可信度低一个量级。
    """
    _stub_preflight(monkeypatch, {
        "https://a/measured.mp4": (2000.0, False),
        "https://a/guessed.mp4": (9000.0, True),
    })
    nodes = [_mp4_node("https://a/guessed.mp4"),
             _mp4_node("https://a/measured.mp4")]

    ordered = d._rank_mp4_nodes(nodes, "55", 60)

    assert ordered[0][0]["url"] == "https://a/measured.mp4"


def test_unprobeable_nodes_sink_to_the_bottom_but_survive(monkeypatch):
    """🔑 底线：预检失败的节点排到最后，但**绝不能被淘汰**。

    探不出码率不代表节点是坏的（moov 不在文件头就会这样）。丢掉它们等于
    白白损失多源 fallback 能力（§10.13 实测靠 fallback 救回过片子）。
    """
    _stub_preflight(monkeypatch, {
        "https://a/ok.mp4": (2000.0, False),
        "https://a/dead.mp4": None,      # 预检失败
    })
    nodes = [_mp4_node("https://a/dead.mp4"), _mp4_node("https://a/ok.mp4")]

    ordered = d._rank_mp4_nodes(nodes, "55", 60)

    assert [n["url"] for n, _i in ordered] == [
        "https://a/ok.mp4", "https://a/dead.mp4",
    ]
    assert len(ordered) == 2, "节点数不能变少——排序只重排，不淘汰"


def test_all_unprobeable_falls_back_to_upstream_order(monkeypatch):
    """全部预检不出时退化为上游原始顺序，行为等同改动前。"""
    _stub_preflight(monkeypatch, {})     # 全部返回 None
    nodes = [_mp4_node("https://a/1.mp4", 1080),
             _mp4_node("https://a/2.mp4", 720),
             _mp4_node("https://a/3.mp4", 480)]

    ordered = d._rank_mp4_nodes(nodes, "55", 60)

    assert [n["url"] for n, _i in ordered] == [
        "https://a/1.mp4", "https://a/2.mp4", "https://a/3.mp4",
    ]


def test_single_mp4_node_skips_preflight(monkeypatch):
    """单节点不做择优预检——那是白花 1~2 个网络请求，且结果无从比较。"""
    called = []
    monkeypatch.setattr(
        d, "_mp4_preflight",
        lambda *a, **k: called.append(1) or None,
    )

    ordered = d._rank_mp4_nodes([_mp4_node("https://a/1.mp4")], "55", 60)

    assert called == [], "单节点不该触发预检"
    assert ordered == [(ordered[0][0], None)]


def test_ranking_preserves_m3u8_positions(sandbox, monkeypatch):
    """🔑 重排只在 mp4 节点之间发生，m3u8 节点必须留在原位。

    否则 is_last_node 的语义会漂移（它决定 master playlist 用长重试还是短重试），
    且 m3u8/mp4 的相对尝试次序被打乱，影响面远超本功能意图。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    _stub_preflight(monkeypatch, {
        "https://a/lo.mp4": (1000.0, False),
        "https://a/hi.mp4": (9000.0, False),
    })

    tried = []

    def record_mp4(node, output_path, label, runtime_minutes=None, preflight=None):
        tried.append(node["url"])
        raise RuntimeError("请求失败(HTTP Error 502)")

    def record_master(url, retries=None, headers=None):
        tried.append(url)
        raise RuntimeError("请求失败(HTTP Error 502)")

    monkeypatch.setattr(d, "_download_mp4_direct", record_mp4)
    monkeypatch.setattr(d, "parse_master_playlist", record_master)

    # 顺序：mp4(lo) → m3u8 → mp4(hi)。择优后两个 mp4 互换，m3u8 仍在中间。
    entry = {"tmdbId": "55", "title": "T", "urls": [
        _mp4_node("https://a/lo.mp4"),
        {"url": "https://a/mid.m3u8", "provider": "vidup", "type": "m3u8",
         "headers": {}, "quality": None, "size": None},
        _mp4_node("https://a/hi.mp4"),
    ]}
    d.process_one_entry(entry, set())

    assert tried == [
        "https://a/hi.mp4",     # 高码率的 mp4 换到了第一个 mp4 位置
        "https://a/mid.m3u8",   # m3u8 仍在中间，位置未动
        "https://a/lo.mp4",
    ]


def test_preflight_result_is_reused_not_reprobed(sandbox, monkeypatch):
    """择优探到的结果要透传给下载，不能让同一节点被探两次。

    否则每部片平白多出一轮 8MB 采样 + 探总长请求，择优的成本翻倍。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    _stub_preflight(monkeypatch, {
        "https://a/1.mp4": (5000.0, False),
        "https://a/2.mp4": (1000.0, False),
    })

    seen = []

    def capture(node, output_path, label, runtime_minutes=None, preflight=None):
        seen.append((node["url"], preflight))
        raise RuntimeError("请求失败(HTTP Error 502)")

    monkeypatch.setattr(d, "_download_mp4_direct", capture)

    entry = {"tmdbId": "55", "title": "T", "urls": [
        _mp4_node("https://a/1.mp4"), _mp4_node("https://a/2.mp4"),
    ]}
    d.process_one_entry(entry, set())

    assert len(seen) == 2
    for url, preflight in seen:
        assert preflight is not None, f"{url} 的预检结果没被透传，会导致重复探测"
        assert preflight["total_size"] == 1_000_000


def test_mixed_stream_failures_stay_retriable(sandbox, monkeypatch):
    """单节点内：一条流画质淘汰 + 一条流网络异常 → 仍判可重试。

    存在瞬时异常时无法断定是"真不达标"还是"抖动全挂"，必须留给下一轮重采。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    _sampling_env(monkeypatch, [
        ("1920x1080", "https://cdn/a.m3u8", 5000.0),
        ("1920x1080", "https://cdn/b.m3u8", 4000.0),
    ])
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 9000.0})

    real_media = d.parse_media_playlist

    def flaky_media(url, headers=None):
        if url.endswith("b.m3u8"):
            raise RuntimeError("请求失败(HTTP Error 502)")
        return real_media(url, headers=headers)

    monkeypatch.setattr(d, "parse_media_playlist", flaky_media)

    _, ok, info = d.process_one_entry({"tmdbId": "55", "urls": ["u"]}, set())

    assert ok is False
    assert info["retriable"] is True
    assert "候选流无一入选" in info["error"]


def test_quality_kill_reason_beats_generic_categories():
    """判死文案里附带了末节点错误，统计归类不能被那条错误抢走。
    classify_reject_reason 是"首个命中者胜出"，若画质判死类目排在
    "候选流无一入选"之后，所有判死片都会被记进后者——正好污染了要用来
    评估本口径是否过激的那份数据。
    """
    msg = (
        "3 个节点中 2 个因画质不达标被确定性淘汰（≥ 阈值 0.50），"
        "判定整片画质不达标；末节点错误：本轮候选流无一入选（各流原因见上方日志）"
    )
    assert d.classify_reject_reason(msg) == "画质整体不达标(判死)"


def test_declared_quality_rejection_reaches_the_kill_path(sandbox, monkeypatch):
    """不桩 _download_mp4_direct，让真实的画质关卡自己抛异常并一路走到判死。

    📌 这条用例是"反向验证时测试意外通过"逼出来的：上面几条都把
    _download_mp4_direct 整个替换掉了，真实 raise 点根本没被执行——把某个
    `raise QualityRejectedError` 改回 `raise RuntimeError` 时测试照样全绿。
    强度不够的用例锁不住"关卡必须抛画质异常"这个约束，故补这一条真正穿过
    _download_mp4_direct 内部声明分辨率关卡的用例。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 0.5)
    # 本例走的是"声明分辨率低于红线"这道**模式 A 专属**关卡，必须显式开启
    # （默认已是模式 B，红线关整关放行）。
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    # 两个 mp4 节点会触发跨节点择优，那会对每个节点发真实网络请求探总长。
    # 本例只验"声明分辨率关 → 判死"这条链路，故桩掉择优的探测（返回 None =
    # 探不出码率，保持上游原始顺序，与本例断言无关）。
    monkeypatch.setattr(d, "_mp4_preflight", lambda *a, **k: None)

    # 两个节点都声明 480p：在 _download_mp4_direct 的第一道关卡就被拒，
    # 探测总长等后续动作根本不会发生，故无需桩其余网络函数。
    entry = {"tmdbId": "55", "title": "T", "urls": [
        {"url": "https://a/1.mp4", "provider": "vidlink", "type": "mp4",
         "headers": {}, "quality": 480, "size": None},
        {"url": "https://a/2.mp4", "provider": "vidlink", "type": "mp4",
         "headers": {}, "quality": 360, "size": None},
    ]}

    _, ok, info = d.process_one_entry(entry, set())

    assert ok is False
    assert info["retriable"] is False
    assert "因画质不达标" in info["error"]


def test_merge_next_batch_prefers_the_refetched_entry():
    """同一部片同时进两个桶时，必须用重取后的新 entry，且只投一份。

    投两份会让第二份在 process_one_entry 的 processing_ids 检查里被判"重复条目"
    丢弃，白占一个下载槽位；用旧 entry 则那个 mp4 节点整轮继续是废的。
    """
    retriable = [{"tmdbId": "55", "urls": ["OLD"]}, {"tmdbId": "66", "urls": ["x"]}]
    revived = [{"tmdbId": "55", "urls": ["NEW"]}]

    merged = d.merge_next_batch(retriable, revived)

    assert len(merged) == 2
    by_id = {e["tmdbId"]: e for e in merged}
    assert by_id["55"]["urls"] == ["NEW"]
    assert by_id["66"]["urls"] == ["x"]


def test_merge_next_batch_keeps_refetch_only_entries():
    """纯过期（不在 retriable 桶里）的片重取成功后也要被投出去。"""
    merged = d.merge_next_batch([], [{"tmdbId": "9", "urls": ["NEW"]}])
    assert [e["tmdbId"] for e in merged] == ["9"]


# ---- 首轮来源抽象（§12 流式化第 1 步）----


def test_list_source_yields_every_entry_in_order_then_done():
    """list 来源必须逐个吐出全部条目、顺序不变，最后返回 done。

    锁死"改造后行为与改造前 `current_batch[next_submit]` 完全一致"：
    首轮/重试轮都靠它，顺序或数量变了会直接影响投递语义。
    """
    entries = [{"tmdbId": str(i)} for i in range(3)]
    source = d.ListEntrySource(entries)

    got = []
    while True:
        state, entry = source.poll()
        if state == "done":
            break
        assert state == "item"
        got.append(entry["tmdbId"])

    assert got == ["0", "1", "2"]
    # 耗尽后必须稳定返回 done（主循环会重复问），不能抛异常或吐出重复条目。
    assert source.poll() == ("done", None)


def test_list_source_never_returns_wait():
    """list 的存货是确定的，绝不能返回 wait。

    若返回 wait，主循环会进入 STREAM_IDLE_POLL_SECONDS 的 sleep 分支空转，
    把"本该立刻投递完"的一轮拖成按秒轮询。
    """
    source = d.ListEntrySource([{"tmdbId": "1"}])
    assert source.poll()[0] == "item"
    assert source.poll()[0] == "done"


def test_empty_list_source_is_done_immediately():
    """空批次必须立刻 done，否则首轮会卡在等待里永不收尾。"""
    assert d.ListEntrySource([]).poll() == ("done", None)


def test_list_source_snapshots_input():
    """来源要对入参做快照：调用方后续改动原 list 不得影响已在跑的一轮。"""
    entries = [{"tmdbId": "1"}]
    source = d.ListEntrySource(entries)
    entries.append({"tmdbId": "2"})

    assert source.poll()[0] == "item"
    assert source.poll() == ("done", None)


def test_stream_idle_poll_is_positive():
    """空闲轮询间隔必须为正：0 会让 pending 空时退化成 100% CPU 忙等。"""
    assert d.STREAM_IDLE_POLL_SECONDS > 0


def test_async_refetch_hook_defaults_to_none():
    """默认必须是 None —— 只跑下载时要走同步的 refetch_entries 老路。

    钩子若被意外留成非 None，download_movies.py 单独运行时会去调一个
    根本没有取流线程在服务的队列，过期片永远救不回来且毫无报错。
    """
    assert d.async_refetch_hook is None


def test_list_source_alias_tracks_the_real_class():
    """`_ListEntrySource` 是判断"当前是否流式模式"的唯一依据，必须指向原类。

    pipeline.py 会把模块级的 ListEntrySource 换成自己的工厂，_run_pipeline 靠
    `ListEntrySource is not _ListEntrySource` 区分两种模式。这个别名若被误改成
    别的东西，判断就会永久失真——单跑下载时会被当成流式（缺 results.jsonl 时
    不再报错退出，而是空跑一场）。
    """
    assert d._ListEntrySource is d.ListEntrySource


def _isolate_logs(tmp_path, monkeypatch):
    """把所有会被写盘的日志/目录指到 tmp_path。

    ⚠️ 不做这件事会**污染真实工作区**：_run_pipeline 内部走 write_log 时用的是
    模块级的 FAILED_LOG 等常量，只 patch INPUT_JSONL 拦不住写出。
    服务器实跑时已被这一疏漏坑过——测试桩造的 {"tmdbId":"1","error":"stub"}
    真的落进了生产的 failed.jsonl。
    """
    for name, filename in (
        ("FAILED_LOG", "failed.jsonl"),
        ("SUCCESS_LOG", "success.jsonl"),
        ("DOWNLOAD_OK_LOG", "download_ok.jsonl"),
        ("DOWNLOAD_FAIL_LOG", "download_fail.jsonl"),
        ("UPLOAD_PENDING_LOG", "upload_pending.jsonl"),
    ):
        monkeypatch.setattr(d, name, str(tmp_path / filename))
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))


def test_missing_input_file_exits_when_not_streaming(tmp_path, monkeypatch, capsys):
    """单独跑下载时，results.jsonl 缺失必须报错退出——它是唯一片源。"""
    _isolate_logs(tmp_path, monkeypatch)
    monkeypatch.setattr(d, "INPUT_JSONL", str(tmp_path / "nope.jsonl"))
    monkeypatch.setattr(d, "clean_temp_directory", lambda: None)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    d._run_pipeline()

    assert "找不到" in capsys.readouterr().out


def test_missing_input_file_is_tolerated_when_streaming(tmp_path, monkeypatch, capsys):
    """🔴 探针实测的真问题：pipeline 模式下 results.jsonl 缺失**不能**退出。

    全新部署时片子由同进程的取流线程实时产出，这个文件本来就还不存在
    （取流要几十秒才写出第一条）。若照旧 return，下载侧会在启动瞬间退出，
    整条流水线只剩取流在跑，表现为"跑完什么都没下"——首次上服务器必现。

    修复前此用例会失败：_run_pipeline 在读到队列之前就返回了。
    """
    _isolate_logs(tmp_path, monkeypatch)
    monkeypatch.setattr(d, "INPUT_JSONL", str(tmp_path / "nope.jsonl"))
    monkeypatch.setattr(d, "clean_temp_directory", lambda: None)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", False)

    delivered = []

    class _QueueLike:
        """模拟取流线程：先说"还没货"，再交出一部片，最后收工。"""
        def __init__(self):
            self.state = 0

        def poll(self):
            self.state += 1
            if self.state == 1:
                return "wait", None
            if self.state == 2:
                return "item", {"tmdbId": "1", "urls": [{"url": "u"}]}
            return "done", None

    def _fake_process(entry, processed_ids):
        delivered.append(entry["tmdbId"])
        return entry["tmdbId"], False, {"error": "stub", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", _fake_process)
    # 装上非原类的工厂 = 进入流式模式（与 pipeline.py 的做法一致）
    monkeypatch.setattr(d, "ListEntrySource", lambda entries: _QueueLike())
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    d._run_pipeline()

    assert delivered == ["1"], (
        "pipeline 模式下 results.jsonl 缺失时下载侧提前退出了，"
        "取流线程后续产出的片全部无人消费"
    )
    assert "尚不存在" in capsys.readouterr().out


def test_streaming_source_does_not_busy_wait_when_producer_is_slow():
    """流式来源返回 wait 时，主循环必须让出 CPU，不能忙等。

    复刻主循环那段"pending 为空 + 来源未耗尽"的分支：此时 wait(空集合) 会立刻
    返回，若不 sleep 就是 100% CPU 空转。这里用一个"前 N 次说 wait、之后才出货"
    的来源，断言轮询次数没有失控（真忙等会在同样时长里转出几万次）。
    """
    class _SlowSource:
        def __init__(self, wait_times):
            self.remaining_waits = wait_times
            self.polls = 0
            self.delivered = False

        def poll(self):
            self.polls += 1
            if self.remaining_waits > 0:
                self.remaining_waits -= 1
                return "wait", None
            if not self.delivered:
                self.delivered = True
                return "item", {"tmdbId": "1"}
            return "done", None

    source = _SlowSource(wait_times=3)
    sleeps = []

    # 复刻主循环的空闲分支：pending 空且来源未耗尽 -> sleep 后重新问来源要货。
    exhausted = False
    collected = []
    while not exhausted:
        state, entry = source.poll()
        if state == "done":
            exhausted = True
        elif state == "item":
            collected.append(entry)
        else:
            sleeps.append(d.STREAM_IDLE_POLL_SECONDS)

    assert collected == [{"tmdbId": "1"}]
    # 三次 wait 必须各对应一次让出 CPU，否则就是忙等。
    assert len(sleeps) == 3
    assert all(s > 0 for s in sleeps)
    # 轮询次数应与"3 次 wait + 1 次出货 + 1 次 done"精确对应，不多不少。
    assert source.polls == 5


def test_refetch_survives_system_exit_from_fetcher(sandbox, monkeypatch):
    """取流模块用模块级 `raise SystemExit` 做配置校验，必须被兜住。

    SystemExit 继承 BaseException，`except Exception` 拦不住。只配了 R2 凭证、
    没配代理凭证的机器（只跑下载，完全合理）一旦遇到直链过期，整条流水线会被
    这个 SystemExit 直接杀掉：pending 里的转封装/上传全丢、processing_ids 不
    释放、temp 里的成品变孤儿。
    """
    import builtins
    real_import = builtins.__import__

    def exiting(name, *args, **kwargs):
        if name == "tmdb_ids_to_links":
            raise SystemExit("缺少代理凭证: PROXY_USER")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", exiting)
    assert d.refetch_entries([{"tmdbId": "55", "urls": []}], {}) == []


def test_refetch_survives_per_movie_system_exit(sandbox, monkeypatch):
    """单片取流内部触发 SystemExit 也不能带塌整批。"""
    class Exiting(_FakeFetcher):
        def process_tmdb_id(self, tmdb_id):
            if str(tmdb_id) == "1":
                raise SystemExit("providers 为空")
            return super().process_tmdb_id(tmdb_id)

    fetcher = Exiting({"2": ("ok", {"tmdbId": "2", "urls": [{"url": "u2"}]})})
    _install_fake_fetcher(monkeypatch, fetcher)

    revived = d.refetch_entries(
        [{"tmdbId": "1", "urls": []}, {"tmdbId": "2", "urls": []}], {}
    )
    assert [e["tmdbId"] for e in revived] == ["2"]


def test_refetch_gives_up_at_the_round_timeout(sandbox, monkeypatch):
    """重取不能无限期占住主事件循环线程。

    refetch_entries 是同步调用，期间 wait(pending) 不再执行：已下载完的片无人
    提交转封装、成品在 temp 里堆积、上传反压僵住——与 UPLOAD_SLOT_WAIT_TIMEOUT
    防的是同一类问题。超时要收下已完成的部分并立刻放行主循环。
    """
    import threading
    monkeypatch.setattr(d, "AUTO_REFETCH_TIMEOUT", 1)
    monkeypatch.setattr(d, "AUTO_REFETCH_WORKERS", 2)
    release = threading.Event()

    class Hanging(_FakeFetcher):
        def process_tmdb_id(self, tmdb_id):
            if str(tmdb_id) == "slow":
                release.wait(30)      # 远超 AUTO_REFETCH_TIMEOUT
                return ("retry", None)
            return super().process_tmdb_id(tmdb_id)

    fetcher = Hanging({"fast": ("ok", {"tmdbId": "fast", "urls": [{"url": "u"}]})})
    _install_fake_fetcher(monkeypatch, fetcher)

    started = time.monotonic()
    try:
        revived = d.refetch_entries(
            [{"tmdbId": "fast", "urls": []}, {"tmdbId": "slow", "urls": []}], {}
        )
    finally:
        release.set()   # 放掉挂住的桩线程，避免拖慢整个测试进程
    elapsed = time.monotonic() - started

    # 快的那部照常收下，慢的被放弃
    assert [e["tmdbId"] for e in revived] == ["fast"]
    # 必须在超时附近返回，而不是等满 30s
    assert elapsed < 10


# ------------------------------------------------- 单实例锁

def test_main_lock_refuses_a_second_downloader(tmp_path, monkeypatch):
    """🔴 探针实测的真问题：下载侧的单实例锁必须真的互斥。

    早期 acquire_main_lock 只是覆盖写 PID、不做任何检查，形同虚设。
    两个下载进程并发跑会读同一份 results.jsonl、写同一个 downloads/，
    把同一部片下两遍——三层去重拦不住：success.jsonl 与磁盘扫描都只在
    **启动时**读一次，processing_ids 更是进程内的集合，跨进程无效。
    """
    lock = tmp_path / "download_movies.main.lock"
    monkeypatch.setattr(d, "MAIN_LOCK_FILE", str(lock))
    # 伪造"别的活进程"持有锁
    lock.write_text("999999", encoding="utf-8")
    monkeypatch.setattr(d, "_pid_alive", lambda pid: True)

    with pytest.raises(SystemExit) as excinfo:
        d.acquire_main_lock()

    assert "已有下载进程在运行" in str(excinfo.value)
    # 必须原样保留别人的锁，绝不能覆盖
    assert lock.read_text(encoding="utf-8").strip() == "999999"


def test_main_lock_clears_a_stale_lock(tmp_path, monkeypatch):
    """陈旧锁（上次崩溃留下、PID 已死）必须能自动接管，否则要人工删文件才能重跑。"""
    lock = tmp_path / "download_movies.main.lock"
    monkeypatch.setattr(d, "MAIN_LOCK_FILE", str(lock))
    lock.write_text("999999", encoding="utf-8")
    monkeypatch.setattr(d, "_pid_alive", lambda pid: False)

    d.acquire_main_lock()

    assert lock.read_text(encoding="utf-8").strip() == str(os.getpid())
    d.release_main_lock()
    assert not lock.exists()


def test_release_main_lock_leaves_other_owners_alone(tmp_path, monkeypatch):
    """只删属于本进程的锁：被拒的第二个进程退出时不得清掉第一个进程的锁。

    main() 里 acquire 失败后 SystemExit 往上抛，若某条路径又调了 release
    且它不认主，第二个进程反而会把正在跑的那个的锁删掉，互斥立刻失效。
    """
    lock = tmp_path / "download_movies.main.lock"
    monkeypatch.setattr(d, "MAIN_LOCK_FILE", str(lock))
    lock.write_text("999999", encoding="utf-8")

    d.release_main_lock()

    assert lock.exists(), "误删了别的进程持有的锁"


# ------------------------------------------------- 中断与重试预算

@pytest.fixture(autouse=True)
def _clear_interrupt():
    """每个用例前后都清掉全局中断信号，避免相互污染。"""
    d.interrupted.clear()
    yield
    d.interrupted.clear()


def test_segment_retry_aborts_immediately_when_interrupted(monkeypatch):
    """🔴 服务器实跑的真问题：中断后分片重试必须立刻收手。

    Ctrl+C 只打断主线程，线程池里退避中的 worker 毫不知情。退避第 7 次起
    封顶 60s、单分片最多 20 次，于是"中断统计都打印完了，进程还挂着 31 个
    线程继续刷失败日志"，kill -INT 形同虚设，只能 kill -9。

    修复前此用例会失败：sleep 期间没人叫得醒它，要跑满 20 次才返回。
    """
    def always_502(*a, **kw):
        raise RuntimeError("502 Server Error: Bad Gateway")

    monkeypatch.setattr(d, "request_with_retry", always_502)

    result = {}

    def worker():
        started = time.monotonic()
        try:
            d.download_single_segment("http://x/s.ts", 0, 20, 1)
        except Exception as exc:
            result["error"] = str(exc)
        result["elapsed"] = time.monotonic() - started

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    # 等到退避已经拉长（第 5 次起 ≥16s），此时若不可打断就必然超时
    time.sleep(8)
    d.interrupted.set()      # 模拟 Ctrl+C
    thread.join(timeout=10)

    assert not thread.is_alive(), "中断后线程仍在死磕重试"
    assert result["elapsed"] < 12, (
        f"中断后又跑了 {result['elapsed']:.1f}s，退避没有被打断"
    )
    assert "已取消" in result["error"]


def test_segment_refuses_to_start_once_interrupted(monkeypatch):
    """中断后连第一次请求都不该再发出去。"""
    calls = []

    def spy(*a, **kw):
        calls.append(1)
        return b"data"

    monkeypatch.setattr(d, "request_with_retry", spy)
    d.interrupted.set()

    with pytest.raises(RuntimeError, match="已取消"):
        d.download_single_segment("http://x/s.ts", 0, 20, 1)

    assert calls == [], "中断后仍发起了请求"


def test_abort_event_stops_one_movie_without_touching_others(monkeypatch):
    """单部片的 abort_event 必须能打断**退避中**的重试，且不影响全局。

    刻意在第一次请求之后才置位，绕开入口处的前置检查——只有退避路径也认
    abort_event，这个用例才会通过。
    """
    abort = threading.Event()
    calls = []

    def fail_then_abort(*a, **kw):
        calls.append(1)
        abort.set()          # 第一次失败后，这部片被判定放弃
        raise RuntimeError("502 Server Error: Bad Gateway")

    monkeypatch.setattr(d, "request_with_retry", fail_then_abort)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="已取消"):
        d.download_single_segment("http://x/s.ts", 0, 20, 1, abort_event=abort)
    elapsed = time.monotonic() - started

    assert len(calls) == 1, f"abort 置位后仍重试了 {len(calls)} 次"
    assert elapsed < 5, f"退避没有响应 abort_event，耗时 {elapsed:.1f}s"
    # 全局信号未被误置位
    assert not d.interrupted.is_set()


def test_sample_retry_budget_is_far_smaller_than_full_download():
    """🔴 采样阶段的重试预算必须远小于正片。

    采样只是"测个码率决定要不要下"，探不到就该换下一条流；正片死磕才值得
    （已投入大量带宽）。两者共用 20 次，会把判断成本抬到执行成本的量级——
    服务器实跑时 10 部片在采样里空转了 90 分钟（单条流采样理论上界 35 分钟）。
    """
    assert d.SAMPLE_SEG_RETRY_MAX < d.SEG_RETRY_MAX

    # 按指数退避封顶 60s 估算单分片最长等待
    def backoff_total(attempts):
        return sum(min(1 * (2 ** (i - 1)), 60) for i in range(1, attempts))

    sample_seconds = backoff_total(d.SAMPLE_SEG_RETRY_MAX)
    assert sample_seconds <= 60, (
        f"采样单分片最长退避 {sample_seconds}s，太久了——"
        f"采样阶段应当快速失败并换下一条流"
    )


def test_download_segments_defaults_to_full_retry_budget(monkeypatch, tmp_path):
    """不传 retry_max 时必须沿用正片预算，保持既有行为不变。"""
    seen = []

    def fake_seg(url, index, retry_max, delay, headers=None, abort_event=None):
        seen.append(retry_max)
        return b"x" * 10

    monkeypatch.setattr(d, "download_single_segment", fake_seg)
    out = tmp_path / "out.ts"

    d.download_segments(["u1", "u2"], str(out), concurrency=1)
    assert seen == [d.SEG_RETRY_MAX, d.SEG_RETRY_MAX]

    seen.clear()
    d.download_segments(["u1"], str(out), concurrency=1, retry_max=3)
    assert seen == [3]
