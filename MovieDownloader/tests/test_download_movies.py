"""download_movies.py 多源下载改动的离线用例。

覆盖本轮扩源在下载侧的改动：urls 条目归一（兼容旧的裸字符串形态）、
mp4 直链请求头、失败分类与被拒原因归类。全部为纯函数级测试，不联网。
"""

import json
import os
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
    """多 variant 采样场景的公共桩：返回记录被采样流 url 的 list。"""
    sampled = []
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
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
                      init_url=None, force_init=False, headers=None):
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

    def expired_mp4(node, output_path, label, runtime_minutes=None):
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
