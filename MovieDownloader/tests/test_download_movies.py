"""download_movies.py 多源下载改动的离线用例。

覆盖本轮扩源在下载侧的改动：urls 条目归一（兼容旧的裸字符串形态）、
mp4 直链请求头、失败分类与被拒原因归类。全部为纯函数级测试，不联网。
"""

import os

import pytest

import download_movies as d


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """把模块级路径全部指到 tmp_path，并重置共享状态。"""
    monkeypatch.setattr(d, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(d, "TEMP_DIR", str(tmp_path / "temp"))
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
