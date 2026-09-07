"""Offline unit tests for download_tv.py.

Focus: episode-level key contract, local folder layout, R2 key mapping,
success/failed/pending log bookkeeping, playlist parsing and reupload flow.
No network / ffmpeg / boto3 calls are made.
"""

import json
import os

import pytest

import download_tv as d


def _read_jsonl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point every module-level path at tmp_path and reset shared state."""
    base = tmp_path / "downloads"
    temp = tmp_path / "temp"
    monkeypatch.setattr(d, "BASE_DIR", str(base))
    monkeypatch.setattr(d, "TEMP_DIR", str(temp))
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "success.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    monkeypatch.setattr(d, "MAIN_LOCK_FILE", str(tmp_path / "main.lock"))
    monkeypatch.setattr(d, "FOLDER_PREFIX", "tv_")
    monkeypatch.setattr(d, "START_FOLDER_INDEX", 1)
    monkeypatch.setattr(d, "_current_folder_index", 1)
    monkeypatch.setattr(d, "S3_PREFIX", "")
    monkeypatch.setattr(d, "processing_ids", set())
    return tmp_path


# ---------------------------------------------------------------- key helpers
@pytest.mark.parametrize("value,expected", [
    (3, 3), ("3", 3), (" 12 ", 12), ("0", 0), (0, 0),
    (None, None), (True, None), (False, None), ("", None),
    ("1.5", None), ("abc", None), ([], None),
])
def test_parse_int(value, expected):
    assert d.parse_int(value) == expected


@pytest.mark.parametrize("tid,s,e,expected", [
    (12345, 1, 3, "12345_S01E03"),
    ("12345", "1", "3", "12345_S01E03"),
    (12345, 0, 7, "12345_S00E07"),          # specials season
    (12345, 12, 105, "12345_S12E105"),      # >2 digits not truncated
    (None, 1, 1, ""),
    ("", 1, 1, ""),
    (1, None, 1, ""),
    (1, 1, None, ""),
    (1, -1, 1, ""),
    (1, 1, -1, ""),
    (1, "x", 1, ""),
    (1, True, 1, ""),
])
def test_episode_key(tid, s, e, expected):
    assert d.episode_key(tid, s, e) == expected


@pytest.mark.parametrize("stem,expected", [
    ("12345_S01E03", ("12345", 1, 3)),
    ("12345_S00E00", ("12345", 0, 0)),
    ("12345_S12E105", ("12345", 12, 105)),
    ("  12345_S01E03 ", ("12345", 1, 3)),
    ("12345", None),
    ("12345_S1E3x", None),
    ("12345_E03", None),
    ("_S01E03", None),
    ("temp_12345_S01E03", None),
])
def test_parse_episode_key(stem, expected):
    assert d.parse_episode_key(stem) == expected


def test_episode_key_roundtrip():
    key = d.episode_key("98765", 3, 14)
    assert d.episode_key(*d.parse_episode_key(key)) == key


def test_record_episode_key_and_identity():
    rec = {"tmdbId": 5, "season": "2", "episode": 9, "title": "T", "junk": 1}
    assert d.record_episode_key(rec) == "5_S02E09"
    assert d.record_episode_key("not a dict") == ""
    assert d.record_episode_key({"tmdbId": 5}) == ""
    assert d._entry_identity(rec) == {
        "tmdbId": 5, "season": 2, "episode": 9, "title": "T",
    }
    assert d._entry_identity(None) == {
        "tmdbId": None, "season": None, "episode": None, "title": "",
    }


# ---------------------------------------------------------------- R2 key mapping
def test_build_s3_key_layout(sandbox, monkeypatch):
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    assert d.build_s3_key(12345, 1, 3, 2008) == "2008/12345/S01/20260905/E03.mp4"
    assert d.build_s3_key("12345", "0", "12", "2008") == "2008/12345/S00/20260905/E12.mp4"


def test_build_s3_key_year_sanitised_and_prefix(sandbox, monkeypatch):
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    assert d.build_s3_key(1, 1, 1, " 20/08 ").startswith("2008/1/")
    assert d.build_s3_key(1, 1, 1, None).startswith("unknown_year/1/")
    assert d.build_s3_key(1, 1, 1, "").startswith("unknown_year/1/")
    assert d.build_s3_key(1, 1, 1, "n/a").startswith("unknown_year/1/")
    monkeypatch.setattr(d, "S3_PREFIX", "tv")
    assert d.build_s3_key(1, 1, 1, 2008) == "tv/2008/1/S01/20260905/E01.mp4"


@pytest.mark.parametrize("tid,s,e", [
    (None, 1, 1), ("", 1, 1), (1, None, 1), (1, 1, None), (1, "x", 1),
])
def test_build_s3_key_missing_fields(sandbox, tid, s, e):
    with pytest.raises(ValueError):
        d.build_s3_key(tid, s, e, 2008)


def test_build_s3_key_multiday_same_season_does_not_collide(sandbox, monkeypatch):
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    a = d.build_s3_key(1, 1, 1, 2008)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260906")
    b = d.build_s3_key(1, 1, 1, 2008)
    assert a != b
    assert a.rsplit("/", 2)[0] == b.rsplit("/", 2)[0]  # same S01 prefix


# ---------------------------------------------------------------- local layout
def test_scan_downloaded_mp4_ids(sandbox):
    base = sandbox / "downloads"
    f1 = base / "tv_000001"
    f2 = base / "tv_000002"
    other = base / "movie_000001"
    for folder in (f1, f2, other):
        folder.mkdir(parents=True)
    (f1 / "100_S01E01.mp4").write_bytes(b"x")
    (f1 / "100_S01E02.mp4").write_bytes(b"")          # empty -> ignored
    (f1 / "100_S01E03.MP4").write_bytes(b"x")         # case-insensitive ext
    (f1 / "100.mp4").write_bytes(b"x")                # movie-style name ignored
    (f1 / "temp_100_S01E04.ts").write_bytes(b"x")     # wrong ext
    (f2 / "100_S01E01.mp4").write_bytes(b"x")         # duplicate across folders
    (f2 / "200_S00E05.mp4").write_bytes(b"x")
    (other / "300_S01E01.mp4").write_bytes(b"x")      # wrong prefix ignored

    ids, dups = d.scan_downloaded_mp4_ids()
    assert ids == {"100_S01E01", "100_S01E03", "200_S00E05"}
    assert set(dups) == {"100_S01E01"}
    assert len(dups["100_S01E01"]) == 2


def test_scan_downloaded_mp4_ids_missing_base(sandbox):
    assert d.scan_downloaded_mp4_ids() == (set(), {})


def test_move_to_target_folder_rolls_over_and_overwrites(sandbox, monkeypatch):
    monkeypatch.setattr(d, "MAX_VIDEOS_PER_FOLDER", 2)
    base = sandbox / "downloads"
    temp = sandbox / "temp"
    temp.mkdir()

    def mk(name):
        p = temp / name
        p.write_bytes(b"v")
        return str(p)

    p1 = d.move_to_target_folder(mk("a.mp4"), "1_S01E01")
    p2 = d.move_to_target_folder(mk("b.mp4"), "1_S01E02")
    p3 = d.move_to_target_folder(mk("c.mp4"), "1_S01E03")
    assert p1 == str(base / "tv_000001" / "1_S01E01.mp4")
    assert p2 == str(base / "tv_000001" / "1_S01E02.mp4")
    assert p3 == str(base / "tv_000002" / "1_S01E03.mp4")
    assert d._current_folder_index == 2

    # Same key already present in a full folder is overwritten in place.
    monkeypatch.setattr(d, "_current_folder_index", 1)
    p1b = d.move_to_target_folder(mk("d.mp4"), "1_S01E01")
    assert p1b == p1
    assert sorted(os.listdir(base / "tv_000001")) == ["1_S01E01.mp4", "1_S01E02.mp4"]
    assert not (temp / "d.mp4").exists()


def test_move_to_target_folder_cleans_partial_on_failure(sandbox, monkeypatch):
    base = sandbox / "downloads"
    temp = sandbox / "temp"
    temp.mkdir()
    src = temp / "a.mp4"
    src.write_bytes(b"v")

    def boom(src_path, dst_path):
        with open(dst_path, "wb") as fh:
            fh.write(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(d.shutil, "move", boom)
    with pytest.raises(OSError):
        d.move_to_target_folder(str(src), "1_S01E01")
    assert not (base / "tv_000001" / "1_S01E01.mp4").exists()


def test_move_to_target_folder_places_holder_before_moving(sandbox, monkeypatch):
    """移动在锁外执行，锁内先落 0 字节占位把目录名额定死。

    这样并发的其它 worker 在锁内计数时就能看到它、不会把同一目录算成未满，
    从而在跨盘（shutil.move 退化为 copy+del）时既不串行化也不超容量。
    """
    base = sandbox / "downloads"
    temp = sandbox / "temp"
    temp.mkdir()
    src = temp / "a.mp4"
    src.write_bytes(b"v")
    seen = {}

    real_move = d.shutil.move

    def spy(src_path, dst_path):
        # 移动发生时占位文件已存在，且此刻不再持有 folder_lock。
        seen["holder_exists"] = os.path.exists(dst_path)
        seen["lock_free"] = d.folder_lock.acquire(blocking=False)
        if seen["lock_free"]:
            d.folder_lock.release()
        return real_move(src_path, dst_path)

    monkeypatch.setattr(d.shutil, "move", spy)
    final = d.move_to_target_folder(str(src), "1_S01E01")
    assert seen == {"holder_exists": True, "lock_free": True}
    assert final == str(base / "tv_000001" / "1_S01E01.mp4")
    assert (base / "tv_000001" / "1_S01E01.mp4").read_bytes() == b"v"


def test_clean_temp_directory(sandbox):
    temp = sandbox / "temp"
    temp.mkdir()
    keep = ["other.txt", "temp_x.log", "sample.mp4"]
    drop = ["temp_1_S01E01.ts", "temp_1_S01E01.mp4", "sample_1_S01E01_1080p.ts"]
    for name in keep + drop:
        (temp / name).write_bytes(b"x")
    d.clean_temp_directory()
    assert sorted(os.listdir(temp)) == sorted(keep)


def test_safe_file_token():
    assert d.safe_file_token("12345_S01E03") == "12345_S01E03"
    assert d.safe_file_token("1920x1080") == "1920x1080"
    assert d.safe_file_token("a/b c") == "a_b_c"
    assert d.safe_file_token(None) == "unknown"


# ---------------------------------------------------------------- logs
def test_load_success_log_ids(sandbox):
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": 1, "season": 1, "episode": 1}) + "\n"
        + "not json\n"
        + json.dumps({"tmdbId": 1}) + "\n"                    # movie-style -> ignored
        + json.dumps({"tmdbId": "2", "season": "0", "episode": "3"}) + "\n"
        + "\n",
        encoding="utf-8",
    )
    assert d.load_success_log_ids() == {"1_S01E01", "2_S00E03"}


def test_update_success_log_dedups_by_episode_key(sandbox):
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": 1, "season": 1, "episode": 1, "uploaded": False}) + "\n"
        + json.dumps({"tmdbId": 1, "season": 1, "episode": 2, "uploaded": False}) + "\n"
        + "garbage line\n"
        + json.dumps({"tmdbId": 1, "season": 1, "episode": 1, "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    d.update_success_log("1_S01E01", {
        "tmdbId": 1, "season": 1, "episode": 1, "uploaded": True,
    })
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[2] == "garbage line"
    records = [json.loads(l) for l in lines if l.startswith("{")]
    assert [d.record_episode_key(r) for r in records] == ["1_S01E01", "1_S01E02"]
    assert records[0]["uploaded"] is True
    assert records[1]["uploaded"] is False

    d.update_success_log("9_S02E02", {"tmdbId": 9, "season": 2, "episode": 2})
    assert d.load_success_log_ids() == {"1_S01E01", "1_S01E02", "9_S02E02"}


def test_remove_upload_failure_from_log(sandbox):
    path = sandbox / "failed.jsonl"
    recs = [
        {"tmdbId": 1, "season": 1, "episode": 1, "stage": "upload"},
        {"tmdbId": 1, "season": 1, "episode": 1, "stage": "download"},
        {"tmdbId": 1, "season": 1, "episode": 2, "stage": "upload"},
        {"stage": "preflight", "error": "x"},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in recs) + "bad\n", encoding="utf-8")
    d.remove_upload_failure_from_log("1_S01E01")
    kept = path.read_text(encoding="utf-8").splitlines()
    assert "bad" in kept
    parsed = [json.loads(l) for l in kept if l.startswith("{")]
    assert parsed == recs[1:]

    # No-op when nothing matches (file untouched).
    before = path.read_text(encoding="utf-8")
    d.remove_upload_failure_from_log("nope")
    assert path.read_text(encoding="utf-8") == before


def test_write_log_and_pending(sandbox):
    d.write_log(d.FAILED_LOG, {"tmdbId": 1, "season": 1, "episode": 1, "title": "剧"})
    d.write_pending({"tmdbId": 1, "season": 1, "episode": 1})
    assert _read_jsonl(d.FAILED_LOG)[0]["title"] == "剧"
    assert _read_jsonl(d.UPLOAD_PENDING_LOG) == [{"tmdbId": 1, "season": 1, "episode": 1}]
    d.truncate_log(d.FAILED_LOG)
    assert _read_jsonl(d.FAILED_LOG) == []


# ---------------------------------------------------------------- failure classification
@pytest.mark.parametrize("msg,retriable", [
    ("缺少 tmdbId/season/episode 或 urls", False),
    ("没有找到媒体播放列表或清晰度变体", False),
    ("不支持的播放列表结构：含加密分片", False),
    ("没有找到高度达标（≥ 1080×0.80）的流", False),
    ("分辨率 1280x720 低于红线 1080", False),
    ("码率未达到门槛：500 kbps < 900 kbps", False),
    ("服务器返回的不是视频分片: http://x", False),
    ("缺片率过高：缺 30/100 片", True),
    ("本轮候选流无一入选（各流原因见上方日志），下一轮重采", True),
    ("采样探测分辨率失败（可重试）", True),
    ("HTTPSConnectionPool: Read timed out", True),
    ("", True),
    (None, True),
])
def test_classify_failure(msg, retriable):
    assert d._classify_failure(msg) is retriable


def test_permanent_markers_match_movie_version():
    """TV markers must mirror the movie pipeline except for the field-name message."""
    from pathlib import Path

    movie = Path(__file__).resolve().parents[2] / "MovieDownloader" / "download_movies.py"
    src = movie.read_text(encoding="utf-8")
    # mp4 直链相关的 marker 是 TV 侧多源接入（vidlink）独有的，MovieDownloader
    # 目前只有 m3u8 一条路径，故不参与对齐校验。
    tv_only = {
        "需重新取流", "直链块不可用", "直链不支持 Range",
        "服务器未按 Range 响应", "直链总长异常", "直链下载长度不符",
    }
    for marker in d._PERMANENT_FAILURE_MARKERS:
        if marker.startswith("缺少 ") or marker in tv_only:
            continue
        assert marker in src, marker


@pytest.mark.parametrize("msg,category", [
    ("缺少 tmdbId/season/episode 或 urls", "缺少字段/无媒体列表"),
    ("本轮候选流无一入选", "候选流无一入选"),
    ("分辨率 720 低于红线", "分辨率低于红线"),
    ("码率未达到门槛", "码率未达门槛"),
    ("缺片率过高", "正片缺片率过高"),
    ("503 Server Error", "源站5xx"),
    ("Read timed out", "超时"),
    ("SSLError", "SSL/连接错误"),
    ("something else", "其他"),
    ("", "其他"),
])
def test_classify_reject_reason(msg, category):
    assert d.classify_reject_reason(msg) == category


# ---------------------------------------------------------------- playlist parsing
def test_parse_master_playlist(monkeypatch):
    text = (
        "#EXTM3U\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n"
        "\n"
        "1080/index.m3u8\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=1500000\n"
        "720/index.m3u8\n"
    )
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    variants = d.parse_master_playlist("https://cdn/x/master.m3u8")
    assert variants == [
        ("1920x1080", "https://cdn/x/1080/index.m3u8", 5000.0),
        ("unknown", "https://cdn/x/720/index.m3u8", 1500.0),
    ]


def test_parse_master_playlist_attribute_order_and_quoted_codecs(monkeypatch):
    # First attribute follows ":" not ","; CODECS contains commas inside quotes.
    text = (
        "#EXTM3U\n"
        '#EXT-X-STREAM-INF:RESOLUTION=1280x720,CODECS="avc1.4d401f,mp4a.40.2",BANDWIDTH=2500000\n'
        "720/index.m3u8\n"
        '#EXT-X-STREAM-INF:CODECS="avc1.4d401f,mp4a.40.2",BANDWIDTH=800000,RESOLUTION=640x360\n'
        "360/index.m3u8\n"
    )
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    variants = d.parse_master_playlist("https://cdn/x/master.m3u8")
    assert variants == [
        ("1280x720", "https://cdn/x/720/index.m3u8", 2500.0),
        ("640x360", "https://cdn/x/360/index.m3u8", 800.0),
    ]


def test_parse_master_playlist_media_fallback(monkeypatch):
    text = "#EXTM3U\n#EXTINF:4.0,\nseg0.ts\n"
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    assert d.parse_master_playlist("https://cdn/x/index.m3u8") == [
        ("unknown", "https://cdn/x/index.m3u8", 0.0)
    ]


def test_parse_media_playlist_ts(monkeypatch):
    text = "#EXTM3U\n#EXTINF:4.0,\nseg0.ts\n#EXTINF:3.5,\nseg1.ts\n#EXT-X-ENDLIST\n"
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    urls, durations, init = d.parse_media_playlist("https://cdn/x/index.m3u8")
    assert urls == ["https://cdn/x/seg0.ts", "https://cdn/x/seg1.ts"]
    assert durations == [4.0, 3.5]
    assert init is None


def test_playlist_parsers_forward_node_headers(monkeypatch):
    """取流阶段记录的节点专属请求头必须透传到 m3u8 两级 playlist 请求。

    某些源站没有正确的 Referer/UA 会直接 403/428，丢头等于白白损失一个可用源。
    """
    seen = []

    def fake_request(method, url, **kwargs):
        seen.append(kwargs.get("headers"))
        return "#EXTM3U\n#EXTINF:4.0,\nseg0.ts\n"

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    node_headers = {"Referer": "https://src/"}
    d.parse_master_playlist("https://cdn/x/i.m3u8", headers=node_headers)
    d.parse_media_playlist("https://cdn/x/i.m3u8", headers=node_headers)
    assert seen == [node_headers, node_headers]


def test_download_segments_forwards_headers(sandbox, monkeypatch):
    """分片下载同样要带节点头，否则正片阶段会整片 403。"""
    seen = []

    def fake_single(url, index, retry_max, delay, headers=None):
        seen.append(headers)
        return b"x"

    monkeypatch.setattr(d, "download_single_segment", fake_single)
    out = os.path.join(str(sandbox), "o.ts")
    node_headers = {"User-Agent": "okhttp/4.9.3"}
    d.download_segments(
        ["u0", "u1"], out, init_url="i", headers=node_headers, concurrency=1
    )
    # init 段 + 2 个媒体分片都带上了节点头
    assert seen == [node_headers] * 3


def test_parse_media_playlist_fmp4_init(monkeypatch):
    text = '#EXTM3U\n#EXT-X-MAP:URI="init.mp4"\n#EXTINF:4.0,\nseg0.m4s\n'
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    urls, _, init = d.parse_media_playlist("https://cdn/x/index.m3u8")
    assert init == "https://cdn/x/init.mp4"
    assert urls == ["https://cdn/x/seg0.m4s"]


@pytest.mark.parametrize("text", [
    '#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="k"\n#EXTINF:4,\ns.ts\n',
    "#EXTM3U\n#EXT-X-BYTERANGE:100@0\n#EXTINF:4,\ns.ts\n",
    '#EXTM3U\n#EXT-X-MAP:URI="i.mp4",BYTERANGE="1@0"\n#EXTINF:4,\ns.m4s\n',
    "#EXTM3U\n#EXT-X-MAP:FOO=1\n#EXTINF:4,\ns.m4s\n",
])
def test_parse_media_playlist_unsupported(monkeypatch, text):
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    with pytest.raises(d.UnsupportedPlaylistError):
        d.parse_media_playlist("https://cdn/x/index.m3u8")


def test_parse_media_playlist_key_none_allowed(monkeypatch):
    text = "#EXTM3U\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:4,\ns.ts\n"
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    urls, _, _ = d.parse_media_playlist("https://cdn/x/index.m3u8")
    assert len(urls) == 1


def test_parse_media_playlist_empty(monkeypatch):
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: "#EXTM3U\n")
    with pytest.raises(RuntimeError):
        d.parse_media_playlist("https://cdn/x/index.m3u8")


# ---------------------------------------------------------------- quality gates
def test_parse_resolution():
    assert d.parse_resolution("1920x1080") == (1920, 1080)
    assert d.parse_resolution("1920X1080") == (1920, 1080)
    assert d.parse_resolution("unknown") is None
    assert d.parse_resolution("") is None
    assert d.parse_resolution("abc") is None


def test_bitrate_threshold_scaling(monkeypatch):
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 2000.0, "hevc": 1000.0})
    assert d.bitrate_threshold(1080, "h264") == pytest.approx(2000.0)
    assert d.bitrate_threshold(540, "h264") == pytest.approx(500.0)
    assert d.bitrate_threshold(1080, "hevc") == pytest.approx(1000.0)
    assert d.bitrate_threshold(1080, "unknown-codec") == pytest.approx(2000.0)
    assert d.bitrate_threshold(1080, None) == pytest.approx(2000.0)


def test_meets_resolution_redline(monkeypatch):
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 0.8)
    assert d.meets_resolution_redline(864)
    assert not d.meets_resolution_redline(863)


def test_validate_segment_content():
    d.validate_segment_content(b"\x47binary", "u")
    for bad in (b"", b"  <!DOCTYPE html>", b"<html>", b"#EXTM3U"):
        with pytest.raises(RuntimeError):
            d.validate_segment_content(bad, "u")


# ---------------------------------------------------------------- process_one_entry guards
def test_process_one_entry_rejects_missing_key(sandbox):
    for entry in (
        {"tmdbId": 1, "season": 1, "urls": ["u"]},
        {"tmdbId": 1, "episode": 1, "urls": ["u"]},
        {"season": 1, "episode": 1, "urls": ["u"]},
        {"tmdbId": 1, "season": 1, "episode": 1, "urls": []},
        {"tmdbId": 1, "season": 1, "episode": 1},
    ):
        label, ok, info = d.process_one_entry(entry, set())
        assert ok is False
        assert info["retriable"] is False
        assert d._classify_failure(info["error"]) is False
    assert d.processing_ids == set()


def test_process_one_entry_label_falls_back_to_tmdb_id(sandbox):
    label, ok, _ = d.process_one_entry({"tmdbId": 77, "urls": ["u"]}, set())
    assert label == "77"


def test_process_one_entry_skips_processed_and_in_flight(sandbox):
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": ["u"]}
    label, ok, info = d.process_one_entry(entry, {"1_S01E01"})
    assert (label, ok, info["error"]) == ("1_S01E01", False, "already processed successfully")

    d.processing_ids.add("1_S01E01")
    _, ok, info = d.process_one_entry(entry, set())
    assert info["error"] == "duplicate entry currently processing"
    assert "1_S01E01" in d.processing_ids  # not released by the duplicate path


def test_process_one_entry_node_fallback_and_cleanup(sandbox, monkeypatch):
    """Two nodes: first raises a retriable error, second a permanent one -> still retriable."""
    calls = []

    def fake_master(url, retries=None, headers=None):
        calls.append((url, retries))
        if url == "u1":
            raise RuntimeError("Read timed out")
        raise RuntimeError("没有找到高度达标的流")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    entry = {"tmdbId": 1, "season": 2, "episode": 3, "urls": ["u1", "u2"], "title": "t"}
    label, ok, info = d.process_one_entry(entry, set())
    assert label == "1_S02E03"
    assert ok is False
    assert info["retriable"] is True
    assert calls == [("u1", d.PLAYLIST_RETRY_FALLBACK), ("u2", None)]
    assert d.processing_ids == set()
    assert not os.path.exists(os.path.join(d.TEMP_DIR, "temp_1_S02E03.ts"))


def test_process_one_entry_permanent_only(sandbox, monkeypatch):
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: (_ for _ in ()).throw(
            RuntimeError("没有找到媒体播放列表")
        ),
    )
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": ["u"]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False and info["retriable"] is False


def test_process_one_entry_happy_path_builds_job(sandbox, monkeypatch):
    """Drive the full selection path with fakes: one 1080p variant, good bitrate."""
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]
    durations = [4.0] * 20

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: [
            ("1920x1080", "https://cdn/i.m3u8", 5000.0)
        ],
    )
    monkeypatch.setattr(
        d, "parse_media_playlist",
        lambda url, headers=None: (seg_urls, durations, None),
    )
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        # 1 MB per 4 s segment => 2000 kbps, above 1000 threshold
        return n * 1_000_000, [3] if start_idx == 0 else [], 0

    monkeypatch.setattr(d, "download_segments", fake_download)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"],
             "title": "Show", "year": 2010}
    label, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert label == "55_S01E02"
    assert job["normalized_id"] == "55_S01E02"
    assert (job["tmdbId"], job["season"], job["episode"], job["year"]) == ("55", 1, 2, 2010)
    assert job["resolution"] == "1920x1080"
    assert job["missing_segment_indices"] == [3]
    assert job["final_ts"].endswith("temp_55_S01E02.ts")
    assert job["temp_mp4"].endswith("temp_55_S01E02.mp4")
    # Sample file removed, ID lock retained for the conversion stage.
    assert not any(n.startswith("sample_") for n in os.listdir(d.TEMP_DIR))
    assert "55_S01E02" in d.processing_ids


# ---------------------------------------------------------------- multi-source: url entries / mp4 direct
def test_normalize_url_entry_str_and_dict():
    assert d._normalize_url_entry("  https://a/m.m3u8 ") == {
        "url": "https://a/m.m3u8", "provider": "vidup", "type": "m3u8",
        "headers": {}, "quality": None, "size": None,
    }
    node = d._normalize_url_entry({
        "url": "https://cdn/f.mp4", "provider": "vidlink", "type": "MP4",
        "headers": {"User-Agent": "okhttp/4.9.3", "X": None},
        "quality": "1080", "size": "123",
    })
    assert node == {
        "url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4",
        "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": 123,
    }
    # 缺 type/headers 时补默认；非法条目返回 None
    assert d._normalize_url_entry({"url": "u"})["type"] == "m3u8"
    assert d._normalize_url_entry({"url": "u", "headers": "bad"})["headers"] == {}
    for bad in ("", "  ", None, 5, {}, {"url": ""}, {"url": "u", "type": "dash"}):
        assert d._normalize_url_entry(bad) is None


def test_mp4_request_headers_drop_referer_and_xhr():
    headers = d._mp4_request_headers({"User-Agent": "okhttp/4.9.3"})
    assert headers == {
        "Referer": None, "X-Requested-With": None, "User-Agent": "okhttp/4.9.3",
    }
    # 条目显式给 Referer 时以条目为准
    assert d._mp4_request_headers({"Referer": "https://x/"})["Referer"] == "https://x/"


def test_process_one_entry_dict_urls_use_m3u8_path(sandbox, monkeypatch):
    calls = []

    def fake_master(url, retries=None, headers=None):
        calls.append((url, retries))
        raise RuntimeError("Read timed out")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    entry = {
        "tmdbId": 1, "season": 2, "episode": 3,
        "urls": [
            {"url": "u1", "provider": "vidup", "type": "m3u8"},
            "u2",
            {"url": "", "provider": "broken"},  # 非法条目被跳过
        ],
    }
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False and info["retriable"] is True
    assert calls == [("u1", d.PLAYLIST_RETRY_FALLBACK), ("u2", None)]


def test_process_one_entry_mp4_branch_builds_job(sandbox, monkeypatch):
    seen = {}

    def fake_direct(node, output_path, label, runtime_minutes=None):
        seen["node"] = node
        seen["runtime"] = runtime_minutes
        with open(output_path, "wb") as fh:
            fh.write(b"x")
        return "1920x1080", 4321.4

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: pytest.fail(
            "mp4 节点不应走 playlist 解析"
        ),
    )
    node = {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4",
            "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": 1}
    entry = {"tmdbId": "9", "season": 1, "episode": 1, "urls": [node],
             "title": "Show", "year": 2011, "runtime_minutes": 45}
    label, ok, job = d.process_one_entry(entry, set())
    assert ok is True and label == "9_S01E01"
    assert seen["node"]["url"] == node["url"] and seen["runtime"] == 45
    assert job["url"] == "https://cdn/f.mp4"  # str，保持 finalize/success_info 契约
    assert job["resolution"] == "1920x1080" and job["bitrate_kbps"] == 4321
    assert job["missing_segment_count"] == 0 and job["missing_segment_indices"] == []
    assert job["final_ts"].endswith("temp_9_S01E01.ts")
    assert "9_S01E01" in d.processing_ids


def test_process_one_entry_mp4_then_m3u8_fallback(sandbox, monkeypatch):
    """mp4 直链 403 过期属确定性失败；切到备用 m3u8 节点，残留 ts 被清理。

    末节点是瞬时错误 -> 整集仍判可重试（any_retriable 不被首节点的确定性失败连坐）。
    """
    calls = []

    def fake_direct(node, output_path, label, runtime_minutes=None):
        with open(output_path, "wb") as fh:
            fh.write(b"partial")
        raise RuntimeError(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: x")

    def fake_master(url, retries=None, headers=None):
        calls.append((url, retries))
        raise RuntimeError("Read timed out")

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
        {"url": "https://v/m.m3u8", "provider": "vidfast", "type": "m3u8"},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert info["retriable"] is True
    assert calls == [("https://v/m.m3u8", None)]
    assert not os.path.exists(os.path.join(d.TEMP_DIR, "temp_1_S01E01.ts"))
    assert d.processing_ids == set()


def test_process_one_entry_mp4_expired_only_is_permanent(sandbox, monkeypatch):
    """唯一节点是过期直链时整集判确定性失败：本脚本无法重新取流，重投同一条 url 必然再挂。"""
    def fake_direct(node, output_path, label, runtime_minutes=None):
        raise RuntimeError(f"直链已失效（HTTP 410），{d._NEEDS_REFETCH_MARKER}: x")

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False and info["retriable"] is False


def test_refetch_marker_is_permanent_and_classified():
    """403/410 过期直链：判死（交由上游重跑取流），但统计上单列一类便于观测。"""
    msg = f"直链已失效（HTTP 410），{d._NEEDS_REFETCH_MARKER}: u"
    assert d._classify_failure(msg) is False
    assert d.classify_reject_reason(msg) == "直链失效需重新取流"


@pytest.mark.parametrize("msg,category", [
    ("直链块不可用（HTTP 404）: u", "直链块不可用(404/416)"),
    ("直链不支持 Range 分块下载: u", "直链不支持Range"),
    ("服务器未按 Range 响应（HTTP 200）", "直链不支持Range"),
    ("直链总长异常(0): u", "直链总长异常"),
    ("直链下载长度不符：1 != 2", "直链总长异常"),
])
def test_mp4_failures_are_permanent_and_classified(msg, category):
    """mp4 直链的确定性失败：同一条 url 重下必然复现，不该占用下一轮下载槽位。"""
    assert d._classify_failure(msg) is False
    assert d.classify_reject_reason(msg) == category


def test_mp4_probe_failure_stays_retriable():
    """探测失败文案同时覆盖网络异常与"无法确定总长"两种来源，无法区分。

    按"宁可多试一轮也不误杀"的取向保持可重试；统计上单列"直链探测失败"类目，
    便于跑完后从占比判断到底是源站抖动还是直链本身不可用。
    """
    for msg in ("直链探测失败: u; Read timed out",
                "直链探测失败：无法确定文件总长: u"):
        assert d._classify_failure(msg) is True
        assert d.classify_reject_reason(msg) == "直链探测失败"


# ---------------------------------------------------------------- retry shortcut
@pytest.mark.parametrize("status,permanent", [
    (401, True), (403, True), (404, True), (410, True), (416, True),
    (429, False), (500, False), (502, False), (503, False), (504, False),
])
def test_is_permanent_http_failure_by_status(status, permanent):
    """429/5xx 是限流/临时故障，退避后有机会成功，必须继续重试；
    401/403/404/410/416 重试必然复现，应立刻短路去换下一个节点。"""
    exc = d.requests.HTTPError("boom", response=_FakeResp(status))
    assert d.is_permanent_http_failure(exc) is permanent


def test_download_single_segment_skips_retry_on_permanent_status(sandbox, monkeypatch):
    """分片层遇 404 立即上抛，不再走 20 次退避（否则白等十几分钟占死下载窗口）。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 20)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise d.requests.HTTPError("boom", response=_FakeResp(404))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    with pytest.raises(RuntimeError, match="重试 20 次后仍失败"):
        d.download_single_segment("u", 0, 20, 1)
    assert len(calls) == 1 and sleeps == []


def test_download_single_segment_still_retries_transient(sandbox, monkeypatch):
    """瞬时错误（503）仍要重试满次数——这才是重试真正能救回来的场景。"""
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise d.requests.HTTPError("boom", response=_FakeResp(503))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    with pytest.raises(RuntimeError):
        d.download_single_segment("u", 0, 3, 1)
    assert len(calls) == 3 and len(sleeps) == 2


def test_request_with_retry_short_circuits_permanent_status(monkeypatch):
    """playlist 层同样短路：404 不该重试 10 次（约 4 分钟）才换节点。"""
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    calls = []

    class _Session:
        def request(self, method, url, **kwargs):
            calls.append(url)
            return _FakeResp(404)

    monkeypatch.setattr(d, "get_session", lambda: _Session())
    with pytest.raises(RuntimeError, match=d._HTTP_PERMANENT_MARKER) as exc:
        d.request_with_retry("GET", "https://cdn/x.m3u8", retries=10)
    assert len(calls) == 1 and sleeps == []
    # 层内短路，但整集仍判可重试：403/404 可能是临时风控，判死会误杀。
    assert d._classify_failure(str(exc.value)) is True
    assert d.classify_reject_reason(str(exc.value)) == "确定性4xx"


# ---------------------------------------------------------------- upload retry
def test_upload_to_r2_skips_retry_on_permanent_error(sandbox, monkeypatch):
    """凭证/权限/桶不存在属配置问题，重试 5 次只是白占反压槽位。"""
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    attempts = []

    class _Client:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            exc = Exception("denied")
            exc.response = {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            }
            raise exc

    monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
    ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is False and "不重试" in reason
    assert len(attempts) == 1 and sleeps == []


def test_upload_to_r2_retries_transient_with_exponential_backoff(sandbox, monkeypatch):
    """网络类错误仍重试，且退避是指数（3/6/12...）而非线性。"""
    monkeypatch.setattr(d, "UPLOAD_RETRY_MAX", 4)
    monkeypatch.setattr(d, "UPLOAD_RETRY_DELAY", 3)
    monkeypatch.setattr(d.random, "uniform", lambda a, b: 0.0)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    attempts = []

    class _Client:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            raise OSError("connection reset")

    monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
    ok, _ = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is False
    assert len(attempts) == 4
    assert sleeps == [3, 6, 12]


# ---------------------------------------------------------------- orphan cleanup
def test_scan_downloaded_removes_zero_byte_orphans(sandbox):
    """0 字节 mp4 是移动中断留下的占位残骸：既不算已下载，也要清掉不占名额。"""
    folder = sandbox / "downloads" / "tv_000001"
    folder.mkdir(parents=True)
    real = folder / "1_S01E01.mp4"
    real.write_bytes(b"video")
    orphan = folder / "2_S01E02.mp4"
    orphan.write_bytes(b"")

    ids, duplicates = d.scan_downloaded_mp4_ids()
    assert ids == {"1_S01E01"} and duplicates == {}
    assert real.exists()
    assert not orphan.exists()


class _FakeResp:
    def __init__(self, status, headers=None, content=b""):
        self.status_code = status
        self.headers = headers or {}
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise d.requests.HTTPError(f"HTTP {self.status_code}", response=self)


class _FakeRangeSession:
    """按 Range 头切片返回 206；status 指定时统一返回该状态码。"""

    def __init__(self, data, status=None):
        self.data = data
        self.status = status
        self.calls = []

    def request(self, method, url, headers=None, **kwargs):
        headers = dict(headers or {})
        self.calls.append((url, headers.get("Range"), headers))
        if self.status is not None:
            return _FakeResp(self.status)
        start, end = (int(x) for x in headers["Range"][6:].split("-"))
        end = min(end, len(self.data) - 1)
        return _FakeResp(
            206,
            {"Content-Range": f"bytes {start}-{end}/{len(self.data)}"},
            self.data[start:end + 1],
        )


@pytest.fixture
def mp4_env(sandbox, monkeypatch):
    monkeypatch.setattr(d, "MP4_CHUNK_SIZE", 4)
    monkeypatch.setattr(d, "MP4_CONCURRENCY", 2)
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 1)
    # 样本 4 字节：预检只对 total_size > MP4_SAMPLE_SIZE*2 的文件生效，
    # 10 字节的测试数据正好越过该阈值。
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 4)
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1920, 1080))
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "_probe_duration", lambda p: None)
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 0.0)
    monkeypatch.setattr(d, "record_block_status", lambda s: None)
    return sandbox


def _install_range_session(monkeypatch, data, status=None):
    session = _FakeRangeSession(data, status)
    monkeypatch.setattr(d, "get_session", lambda: session)
    return session


def test_download_mp4_direct_chunks_and_headers(mp4_env, monkeypatch):
    data = b"abcdefghij"
    session = _install_range_session(monkeypatch, data)
    out = os.path.join(d.TEMP_DIR, "temp_x.ts")
    node = {"url": "https://cdn/f.mp4", "type": "mp4",
            "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": None}
    resolution, bitrate = d._download_mp4_direct(node, out, "x", runtime_minutes=1)
    assert resolution == "1920x1080"
    assert bitrate == pytest.approx(len(data) * 8 / 60 / 1000)
    with open(out, "rb") as fh:
        assert fh.read() == data
    ranges = sorted(r for _, r, _ in session.calls)
    # bytes=0-0 探测总长；bytes=0-3 画质预检样本（MP4_SAMPLE_SIZE=4）；
    # 其余是 MP4_CHUNK_SIZE=4 的正片分块（0-3 与样本 range 相同，去重后可见）。
    assert ranges == [
        "bytes=0-0", "bytes=0-3", "bytes=0-3", "bytes=4-7", "bytes=8-9",
    ]
    for _, _, headers in session.calls:
        assert headers["User-Agent"] == "okhttp/4.9.3"
        assert headers["Referer"] is None and headers["X-Requested-With"] is None


def test_download_mp4_direct_overwrites_stale_file(mp4_env, monkeypatch):
    """残留 ts（无论长度）一律重下，不做续传。"""
    data = b"abcdefghij"
    session = _install_range_session(monkeypatch, data)
    out = os.path.join(d.TEMP_DIR, "temp_x.ts")
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    for stale in (data[:4], data, data + b"zz"):
        with open(out, "wb") as fh:
            fh.write(stale)
        session.calls.clear()
        d._download_mp4_direct({"url": "u", "type": "mp4", "headers": {},
                                "quality": None, "size": None}, out, "x", runtime_minutes=1)
        with open(out, "rb") as fh:
            assert fh.read() == data
        assert sorted(r for _, r, _ in session.calls) == [
            "bytes=0-0", "bytes=0-3", "bytes=0-3", "bytes=4-7", "bytes=8-9"]


def test_download_mp4_direct_no_range_support_fails_fast(mp4_env, monkeypatch):
    """探测返回 200（服务端不支持 Range）时：即便带 declared size 也直接判不支持，
    不进入分块下载；块级 200 亦不做退避重试。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))

    class _FullSession:
        def __init__(self):
            self.calls = 0

        def request(self, method, url, headers=None, **kwargs):
            self.calls += 1
            return _FakeResp(200, {}, b"abc")

    session = _FullSession()
    monkeypatch.setattr(d, "get_session", lambda: session)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": 10}
    with pytest.raises(RuntimeError, match="不支持 Range"):
        d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    assert session.calls == 1

    with pytest.raises(RuntimeError, match="未按 Range 响应"):
        d._download_mp4_chunk("u", {}, 0, 2, 0)
    assert session.calls == 2 and sleeps == []


def test_download_mp4_chunk_non_video_first_block_no_retry(mp4_env, monkeypatch):
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    session = _install_range_session(monkeypatch, b"<html>nope</html>")
    with pytest.raises(RuntimeError, match="不是视频分片"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    assert len(session.calls) == 1 and sleeps == []
    # 非首块不做内容校验
    assert d._download_mp4_chunk("u", {}, 10, 16, 1) == b"</html>"


@pytest.mark.parametrize("status", [404, 416])
def test_download_mp4_chunk_404_416_no_retry(mp4_env, monkeypatch, status):
    # 404 直链不存在 / 416 Range 越界：同一 url 重试无意义，块级不退避
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    session = _install_range_session(monkeypatch, b"", status=status)
    with pytest.raises(RuntimeError, match=f"直链块不可用（HTTP {status}）"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    assert len(session.calls) == 1 and sleeps == []


def test_download_mp4_chunk_aborts_immediately_when_event_set(mp4_env, monkeypatch):
    """整片已判失败时，在途块必须立刻放弃、不再发请求也不再退避。

    否则同片其它块会跑满 SEG_RETRY_MAX 次退避（最长十几分钟），
    占死下载槽位、拖慢换下一个取流节点。
    """
    import threading

    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    session = _install_range_session(monkeypatch, b"", status=500)
    abort = threading.Event()
    abort.set()
    with pytest.raises(RuntimeError, match="已随整片失败取消"):
        d._download_mp4_chunk("u", {}, 0, 9, 0, abort)
    assert session.calls == []


def test_download_mp4_chunk_abort_wakes_up_backoff(mp4_env, monkeypatch):
    """退避等待期间整片判失败：用 Event.wait 立刻醒来，不空等完整个退避。"""
    import threading

    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    monkeypatch.setattr(d, "SEG_RETRY_DELAY", 30)
    abort = threading.Event()
    session = _install_range_session(monkeypatch, b"", status=500)
    # 首次请求失败进入退避，退避中 abort 被置位 -> 直接结束，不再重试。
    monkeypatch.setattr(abort, "wait", lambda timeout: True)
    monkeypatch.setattr(
        d.time, "sleep", lambda s: pytest.fail("有 abort_event 时不该用 sleep")
    )
    with pytest.raises(RuntimeError, match="重试后仍失败"):
        d._download_mp4_chunk("u", {}, 0, 9, 0, abort)
    assert len(session.calls) == 1


def test_download_mp4_direct_expired_link_needs_refetch(mp4_env, monkeypatch):
    _install_range_session(monkeypatch, b"", status=403)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": 10}
    with pytest.raises(RuntimeError, match=d._NEEDS_REFETCH_MARKER) as exc:
        d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    # 判死：本脚本读固化的 results.jsonl，无法重新取流，重投同一条 url 必然再挂。
    assert d._classify_failure(str(exc.value)) is False


def test_download_mp4_direct_quality_prefilter(mp4_env, monkeypatch):
    session = _install_range_session(monkeypatch, b"abc")
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": 480, "size": None}
    with pytest.raises(RuntimeError, match="低于红线") as exc:
        d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    assert d._classify_failure(str(exc.value)) is False
    assert session.calls == []  # 声明画质不达标：一个字节都不下


def test_download_mp4_direct_bitrate_gate(mp4_env, monkeypatch):
    session = _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 5000.0)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    with pytest.raises(RuntimeError, match="码率未达到") as exc:
        d._download_mp4_direct(
            node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
        )
    assert d._classify_failure(str(exc.value)) is False
    # 关键：码率不达标在「预检」阶段就淘汰，不该下载任何正片分块。
    # 只应有 bytes=0-0（探总长）与 bytes=0-3（头部样本）两次请求。
    assert sorted(r for _, r, _ in session.calls) == ["bytes=0-0", "bytes=0-3"]


def test_mp4_sample_prefilter_rejects_low_resolution(mp4_env, monkeypatch):
    """预检判分辨率不达标：整片一个分块都不下，省下 GB 级流量。"""
    session = _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "probe_resolution", lambda p: (640, 480))
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    with pytest.raises(RuntimeError, match="低于红线") as exc:
        d._download_mp4_direct(
            node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
        )
    assert d._classify_failure(str(exc.value)) is False
    assert sorted(r for _, r, _ in session.calls) == ["bytes=0-0", "bytes=0-3"]


def test_mp4_sample_unprobeable_falls_through_to_full_download(mp4_env, monkeypatch):
    """moov 在文件尾部导致样本探测失败时必须放行整片下载，绝不误杀。

    预检只是省流量的优化，探不出结果时应回退到"下完整片再验"的老路径，
    否则会把本可下载成功的片错判为失败，与"尽可能提高成功率"相悖。
    """
    data = b"abcdefghij"
    session = _install_range_session(monkeypatch, data)
    calls = {"n": 0}

    def flaky_probe(path):
        # 第一次（样本）探测失败，第二次（整片）成功。
        calls["n"] += 1
        return None if calls["n"] == 1 else (1920, 1080)

    monkeypatch.setattr(d, "probe_resolution", flaky_probe)
    out = os.path.join(d.TEMP_DIR, "t.ts")
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    resolution, _ = d._download_mp4_direct(node, out, "x", runtime_minutes=1)
    assert resolution == "1920x1080"
    with open(out, "rb") as fh:
        assert fh.read() == data
    # 正片分块照常下载
    assert "bytes=4-7" in [r for _, r, _ in session.calls]


def test_mp4_sample_file_is_always_removed(mp4_env, monkeypatch):
    """无论预检通过与否，样本文件都不得残留在 temp 目录。"""
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "_probe_duration", lambda p: 100.0)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    assert not any(
        n.startswith("mp4sample_") for n in os.listdir(d.TEMP_DIR)
    )


def test_mp4_sample_prefers_upstream_runtime_over_sample_duration(mp4_env, monkeypatch):
    """码率必须用上游 runtime_minutes 算，不能用样本自身时长。

    样本是被截断的文件，ffprobe 从残缺 moov 读出的可能是"样本时长"而非整片
    时长。若用它做分母，bitrate 会虚高几十倍，让本该淘汰的低码率片通过预检、
    预检形同虚设。这里让 _probe_duration 返回一个极小值，断言它未被采用。
    """
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "_probe_duration", lambda p: 0.01)  # 若被采用码率会爆表
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    probed = d._mp4_probe_quality_by_sample(
        "u", {}, 10, os.path.join(d.TEMP_DIR, "s.mp4"), "x", runtime_minutes=45
    )
    assert probed is not None
    _, _, bitrate, _ = probed
    # 用 runtime_minutes=45 -> 2700s：10 字节 × 8 / 2700 / 1000
    assert bitrate == pytest.approx(10 * 8 / 2700 / 1000)


def test_mp4_sample_rejects_suspicious_sample_duration(mp4_env, monkeypatch):
    """无上游时长时，样本探测出的可疑短时长不可信 -> 返回 None 放行整片下载。

    绝不用可疑值去淘汰片子（宁可多下也不误杀）。
    """
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 4)
    monkeypatch.setattr(d, "MP4_MIN_TRUSTED_DURATION", 600)
    # 样本 4 字节 / 总长 10 字节 = 40% < 50%，且 5s < 600s -> 判不可信
    monkeypatch.setattr(d, "_probe_duration", lambda p: 5.0)
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    probed = d._mp4_probe_quality_by_sample(
        "u", {}, 10, os.path.join(d.TEMP_DIR, "s.mp4"), "x", runtime_minutes=None
    )
    assert probed is None


def test_mp4_sample_accepts_plausible_sample_duration(mp4_env, monkeypatch):
    """样本时长足够长（像整片时长）时可以采用。"""
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 4)
    monkeypatch.setattr(d, "MP4_MIN_TRUSTED_DURATION", 600)
    monkeypatch.setattr(d, "_probe_duration", lambda p: 2700.0)
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    probed = d._mp4_probe_quality_by_sample(
        "u", {}, 10, os.path.join(d.TEMP_DIR, "s.mp4"), "x", runtime_minutes=None
    )
    assert probed is not None
    assert probed[2] == pytest.approx(10 * 8 / 2700 / 1000)


def test_mp4_skips_prefilter_for_small_files(mp4_env, monkeypatch):
    """总长不足样本 2 倍时跳过预检：采样等于把整片下一遍，双倍流量零收益。"""
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 8)
    session = _install_range_session(monkeypatch, b"abcdefghij")  # 10 字节 < 16
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    d._download_mp4_direct(
        node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
    )
    # 只有探测总长 + 正片分块，没有额外的样本请求
    assert sorted(r for _, r, _ in session.calls) == [
        "bytes=0-0", "bytes=0-3", "bytes=4-7", "bytes=8-9"]


# ---------------------------------------------------------------- finalize / upload
def test_finalize_one_entry_success_and_failure(sandbox, monkeypatch):
    temp = sandbox / "temp"
    temp.mkdir()
    ts = temp / "temp_1_S01E01.ts"
    mp4 = temp / "temp_1_S01E01.mp4"
    sample = temp / "sample_1_S01E01_1080.ts"
    for p in (ts, sample):
        p.write_bytes(b"x")

    def fake_convert(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"mp4")
        return True

    monkeypatch.setattr(d, "convert_ts_to_mp4", fake_convert)
    d.processing_ids.add("1_S01E01")
    job = {
        "tmdbId": "1", "season": 1, "episode": 1, "normalized_id": "1_S01E01",
        "title": "T", "year": 2001, "url": "u", "final_ts": str(ts),
        "temp_mp4": str(mp4), "cleanup_paths": [str(ts), str(mp4), str(sample)],
        "bitrate_kbps": 1234, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
    }
    processed = set()
    key, ok, info = d.finalize_one_entry(job, processed)
    assert (key, ok) == ("1_S01E01", True)
    assert info["final_path"] == str(sandbox / "downloads" / "tv_000001" / "1_S01E01.mp4")
    assert os.path.exists(info["final_path"])
    assert (info["season"], info["episode"], info["year"]) == (1, 1, 2001)
    assert processed == {"1_S01E01"}
    assert d.processing_ids == set()
    assert not ts.exists() and not sample.exists()

    # Failure path: ffmpeg fails -> lock released, nothing registered.
    ts.write_bytes(b"x")
    monkeypatch.setattr(d, "convert_ts_to_mp4", lambda s, t: False)
    d.processing_ids.add("1_S01E01")
    key, ok, info = d.finalize_one_entry(job, processed2 := set())
    assert ok is False and "FFmpeg" in info["error"]
    assert processed2 == set() and d.processing_ids == set()
    assert not ts.exists()


def _success_info(sandbox, key="1_S01E01"):
    folder = sandbox / "downloads" / "tv_000001"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{key}.mp4"
    path.write_bytes(b"mp4")
    tid, s, e = d.parse_episode_key(key)
    return {
        "tmdbId": tid, "season": s, "episode": e, "title": "T", "year": 2001,
        "url": "u", "final_path": str(path), "bitrate_kbps": 1, "resolution": "r",
        "missing_segment_count": 0, "missing_segment_indices": [],
    }


def test_upload_one_entry_s3_disabled(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", False)
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert (key, ok) == ("1_S01E01", True)
    assert out["uploaded"] is False
    assert os.path.exists(info["final_path"])
    assert _read_jsonl(d.SUCCESS_LOG)[0]["uploaded"] is False


def test_upload_one_entry_success_deletes_local(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    uploaded = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (uploaded.append((p, k)) or (True, None)))
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert ok is True
    assert uploaded == [(info["final_path"], "2001/1/S01/20260905/E01.mp4")]
    assert not os.path.exists(info["final_path"])
    rec = _read_jsonl(d.SUCCESS_LOG)[0]
    assert rec["uploaded"] is True and rec["s3_key"] == "2001/1/S01/20260905/E01.mp4"
    assert not os.path.exists(d.UPLOAD_PENDING_LOG)


def test_upload_one_entry_failure_keeps_local_and_writes_pending(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (False, "boom"))
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert ok is False and "boom" in out["error"]
    assert os.path.exists(info["final_path"])
    assert _read_jsonl(d.SUCCESS_LOG)[0]["uploaded"] is False
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)[0]
    assert d.record_episode_key(pend) == "1_S01E01"
    assert pend["local_path"] == info["final_path"]
    assert pend["s3_key"].endswith("/E01.mp4")
    assert pend["fail_reason"] == "boom"


def test_upload_one_entry_exception_is_contained(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", True)

    def explode(p, k):
        raise RuntimeError("client crashed")

    monkeypatch.setattr(d, "upload_to_r2", explode)
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert ok is False and "client crashed" in out["error"]
    assert os.path.exists(info["final_path"])
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)[0]
    assert d.record_episode_key(pend) == "1_S01E01"


# ---------------------------------------------------------------- main lock
def test_main_lock_lifecycle(sandbox):
    assert d.is_main_running() is False
    d.acquire_main_lock()
    assert d.is_main_running() is True
    d.release_main_lock()
    assert not os.path.exists(d.MAIN_LOCK_FILE)

    # Stale lock (dead pid) is cleaned up.
    with open(d.MAIN_LOCK_FILE, "w") as fh:
        fh.write("999999999")
    assert d.is_main_running() is False
    assert not os.path.exists(d.MAIN_LOCK_FILE)

    # release only removes a lock owned by this process.
    with open(d.MAIN_LOCK_FILE, "w") as fh:
        fh.write("1")
    d.release_main_lock()
    assert os.path.exists(d.MAIN_LOCK_FILE)


def test_acquire_main_lock_rejects_concurrent_run(sandbox, monkeypatch):
    """已有存活主流程时必须 fail-fast：并发跑会重复下载并互相覆盖日志。"""
    monkeypatch.setattr(d, "is_main_running", lambda: True)
    with pytest.raises(SystemExit) as exc:
        d.acquire_main_lock()
    assert exc.value.code == 1
    assert not os.path.exists(d.MAIN_LOCK_FILE)


# ---------------------------------------------------------------- pipeline batching
def test_run_pipeline_batches_download_submissions(sandbox, monkeypatch):
    """分批投递：同时在途的下载 future 不超过 DOWNLOAD_QUEUE_DEPTH，但每一集都要跑到。

    一次性把整轮全部集 submit 进 pending，会让 wait(FIRST_COMPLETED) 每次都对
    全部未完成 future 挂/摘 waiter，几万集时主循环退化成 O(N²)。分批后 wait
    规模恒定在槽位量级，语义（每集都处理、失败分类不变）必须完全一致。
    """
    entries = [
        {"tmdbId": str(i), "season": 1, "episode": 1, "urls": ["u"]}
        for i in range(20)
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MAX_WORKERS", 2)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 4)
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    lock = __import__("threading").Lock()
    state = {"inflight": 0, "peak": 0, "seen": []}

    def fake_process(entry, processed_ids):
        with lock:
            state["inflight"] += 1
            state["peak"] = max(state["peak"], state["inflight"])
            state["seen"].append(entry["tmdbId"])
        try:
            # 确定性失败：不进转封装，也不触发下一轮。
            return entry["tmdbId"], False, {
                "error": "没有找到媒体播放列表", "retriable": False,
            }
        finally:
            with lock:
                state["inflight"] -= 1

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    assert sorted(state["seen"], key=int) == [str(i) for i in range(20)]
    assert state["peak"] <= d.DOWNLOAD_QUEUE_DEPTH
    failed = _read_jsonl(d.FAILED_LOG)
    assert len(failed) == 20 and all(r["stage"] == "download" for r in failed)


def test_run_pipeline_retries_only_retriable_entries(sandbox, monkeypatch):
    """分批投递不影响多轮语义：只有可重试的失败才进下一轮。"""
    entries = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["u"]},
        {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["u"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", True)
    monkeypatch.setattr(d, "MAX_ROUNDS", 3)
    monkeypatch.setattr(d, "ROUND_COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 1)

    attempts = []

    def fake_process(entry, processed_ids):
        attempts.append(entry["tmdbId"])
        if entry["tmdbId"] == "1":
            return "1", False, {"error": "Read timed out", "retriable": True}
        return "2", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    # tmdbId=1 可重试 -> 跑满 3 轮；tmdbId=2 确定性失败 -> 只跑第一轮。
    assert attempts.count("1") == 3 and attempts.count("2") == 1


def test_run_pipeline_degrades_when_upload_slots_exhausted(sandbox, monkeypatch):
    """R2 长时间消化不动时不得冻结主循环：降级为留本地 + 写 pending，继续跑下载。

    upload_semaphore 是在**主事件循环线程**里 acquire 的，若无限等待，下载完成的
    future 也没人处理、转封装同步停摆，整条流水线冻结。这里把信号量占满模拟该
    场景，断言：不提交上传、成品仍在本地、pending 有记录、SUCCESS_LOG 标
    uploaded=false（防下次重新下载），且主循环正常收尾。
    """
    entry = {"tmdbId": "7", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entry, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "S3_ENABLED", True)
    # 信号量容量 1 且预先占满 -> acquire 必然超时。
    monkeypatch.setattr(d, "upload_semaphore", __import__("threading").Semaphore(1))
    d.upload_semaphore.acquire()
    monkeypatch.setattr(d, "UPLOAD_SLOT_WAIT_TIMEOUT", 0.05)

    final_path = str(sandbox / "downloads" / "tv_000001" / "7_S01E01.mp4")

    monkeypatch.setattr(
        d, "process_one_entry",
        lambda e, ids: ("7_S01E01", True, {"cleanup_paths": []}),
    )
    monkeypatch.setattr(
        d, "finalize_one_entry",
        lambda info, ids: ("7_S01E01", True, {
            "tmdbId": "7", "season": 1, "episode": 1, "title": "S",
            "year": 2020, "final_path": final_path,
        }),
    )
    monkeypatch.setattr(
        d, "upload_one_entry",
        lambda info: pytest.fail("槽位耗尽时不应提交上传"),
    )

    d._run_pipeline()

    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert len(pend) == 1
    assert pend[0]["local_path"] == final_path
    assert "降级为留本地待补传" in pend[0]["fail_reason"]
    success = _read_jsonl(d.SUCCESS_LOG)
    assert len(success) == 1 and success[0]["uploaded"] is False


def test_degrade_writes_no_pending_when_s3_disabled(sandbox, monkeypatch):
    """纯本地模式（s3.enabled=false）降级时不得写 pending。

    本地模式的成品本来就不需要补传，塞进 pending 只会污染 reupload 的输入。
    但仍要写 SUCCESS_LOG(uploaded=false) 以免下次运行重新下载。
    """
    entry = {"tmdbId": "8", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entry, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "S3_ENABLED", False)
    monkeypatch.setattr(d, "upload_semaphore", __import__("threading").Semaphore(1))
    d.upload_semaphore.acquire()
    monkeypatch.setattr(d, "UPLOAD_SLOT_WAIT_TIMEOUT", 0.05)

    final_path = str(sandbox / "downloads" / "tv_000001" / "8_S01E01.mp4")
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda e, ids: ("8_S01E01", True, {"cleanup_paths": []}),
    )
    monkeypatch.setattr(
        d, "finalize_one_entry",
        lambda info, ids: ("8_S01E01", True, {
            "tmdbId": "8", "season": 1, "episode": 1, "title": "S",
            "year": 2020, "final_path": final_path,
        }),
    )

    d._run_pipeline()

    assert not os.path.exists(d.UPLOAD_PENDING_LOG)
    success = _read_jsonl(d.SUCCESS_LOG)
    assert len(success) == 1 and success[0]["uploaded"] is False


def test_degrade_success_log_is_deduped_by_key(sandbox, monkeypatch):
    """降级写 SUCCESS_LOG 必须按集级 key 覆盖，保持"每集一条"。

    若用追加写，同一集在降级后又被 reupload 补传成功，会残留两条记录，
    违背 SUCCESS_LOG 的设计意图。
    """
    monkeypatch.setattr(d, "S3_ENABLED", True)
    # 预置一条旧记录，模拟该集此前已被写过。
    (sandbox / "success.jsonl").write_text(
        json.dumps({
            "tmdbId": "9", "season": 1, "episode": 1, "uploaded": True,
            "s3_key": "old/key.mp4",
        }) + "\n",
        encoding="utf-8",
    )
    d.update_success_log("9_S01E01", {
        "tmdbId": "9", "season": 1, "episode": 1,
        "final_path": "/tmp/x.mp4", "uploaded": False,
    })
    records = _read_jsonl(d.SUCCESS_LOG)
    assert len(records) == 1
    assert records[0]["uploaded"] is False
    assert "s3_key" not in records[0]


def test_run_pipeline_keeps_latest_duplicate_and_skips_invalid(sandbox, monkeypatch):
    """输入去重保留最后一条；缺身份字段的行读入即跳过。

    同一集在多次运行/复扫后会有多行，越晚写入的 url 越新 —— 对 vidlink 这类
    带时效签名的直链，保留首次出现等于拿一条早已过期的链接白跑一趟。
    缺 tmdbId/season/episode 的行无法定位到集，放进流水线只会放大成等量的
    确定性失败记录，故在读入阶段就计数跳过。
    """
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["old"]},
        {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["only"]},
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["new"]},  # 覆盖首条
        {"urls": ["x"]},                       # 缺全部身份字段
        {"tmdbId": "3", "urls": ["y"]},        # 缺 season/episode
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    seen = []

    def fake_process(entry, processed_ids):
        seen.append((d.record_episode_key(entry), entry.get("urls")))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    keys = sorted(k for k, _ in seen)
    assert keys == ["1_S01E01", "2_S01E01"]      # 残缺行未进入流水线
    urls_of_1 = dict(seen)["1_S01E01"]
    assert urls_of_1 == ["new"]                   # 保留的是最后一条


def test_run_pipeline_preserves_input_order(sandbox, monkeypatch):
    """去重改用 dict 后仍须保持输入文件顺序（dict 保插入序）。"""
    lines = [
        {"tmdbId": str(i), "season": 1, "episode": 1, "urls": ["u"]}
        for i in (5, 3, 9, 1)
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 1)
    monkeypatch.setattr(d, "MAX_WORKERS", 1)

    order = []

    def fake_process(entry, processed_ids):
        order.append(entry["tmdbId"])
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert order == ["5", "3", "9", "1"]


def test_run_pipeline_duplicate_picks_newest_fetched_at(sandbox, monkeypatch):
    """同集多行时按 fetched_at 取最新，而非文件里最后一条。

    取流侧多轮重试会让"更早成功、后写入"的情况真实存在（例如某集第 1 轮
    拿到 url 后写入，第 3 轮复扫又补写了一条更旧缓存）。vidlink 直链带时效
    签名，用旧链接下载等于确定失败，所以时间戳必须压过文件位置。
    """
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["newest"],
         "fetched_at": 2000},
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["older"],
         "fetched_at": 1000},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(entry.get("urls"))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert seen == [["newest"]]


def test_run_pipeline_duplicate_stamped_beats_unstamped(sandbox, monkeypatch):
    """有 fetched_at 的一定胜过无戳的旧格式行，即便无戳行排在后面。"""
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["stamped"],
         "fetched_at": 100},
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["legacy"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(entry.get("urls"))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert seen == [["stamped"]]


def test_run_pipeline_skips_processed_ids_at_read_time(sandbox, monkeypatch):
    """已处理集在读入阶段就被滤掉，不进线程池。

    第二次全量重跑时 success.jsonl 已有数万条，逐个提交再由
    process_one_entry 跳过等于白白付出等量的调度与锁竞争开销。
    """
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["a"]},
        {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["b"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: {"1_S01E01"})
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(d.record_episode_key(entry))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert seen == ["2_S01E01"]


# ---------------------------------------------------------------- reupload
def test_reupload_pending_flow(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    folder = sandbox / "downloads" / "tv_000001"
    folder.mkdir(parents=True)
    ok_file = folder / "1_S01E01.mp4"
    ok_file.write_bytes(b"a")
    bad_file = folder / "2_S01E01.mp4"
    bad_file.write_bytes(b"b")

    # Pre-existing logs to be reconciled.
    (sandbox / "success.jsonl").write_text(
        json.dumps({"tmdbId": "1", "season": 1, "episode": 1, "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    (sandbox / "failed.jsonl").write_text(
        json.dumps({"tmdbId": "1", "season": 1, "episode": 1, "stage": "upload"}) + "\n"
        + json.dumps({"tmdbId": "2", "season": 1, "episode": 1, "stage": "upload"}) + "\n",
        encoding="utf-8",
    )
    pend = [
        # older duplicate for same episode (should be superseded)
        {"tmdbId": "1", "season": 1, "episode": 1, "local_path": "/nope", "s3_key": "old"},
        {"tmdbId": "1", "season": 1, "episode": 1, "local_path": str(ok_file),
         "s3_key": "", "year": 2001, "title": "T"},
        {"tmdbId": "2", "season": 1, "episode": 1, "local_path": str(bad_file),
         "s3_key": "2002/2/S01/20260901/E01.mp4"},
        {"tmdbId": "3", "season": 1, "episode": 1, "local_path": "/gone/3.mp4",
         "s3_key": "x"},                                        # orphan
        {"tmdbId": "4", "local_path": "/x"},                    # movie-style -> ignored
        {"tmdbId": "5", "season": "x", "episode": 1, "local_path": "/x"},  # bad key -> ignored
    ]
    (sandbox / "pending.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in pend), encoding="utf-8",
    )

    calls = []

    def fake_upload(path, key):
        calls.append((path, key))
        return (True, None) if path == str(ok_file) else (False, "still down")

    monkeypatch.setattr(d, "upload_to_r2", fake_upload)
    d.reupload_pending()

    # Episode 1: rebuilt key from year, uploaded, local removed, logs reconciled.
    assert calls == [
        (str(ok_file), "2001/1/S01/20260905/E01.mp4"),
        (str(bad_file), "2002/2/S01/20260901/E01.mp4"),   # existing s3_key reused
    ]
    assert not ok_file.exists()
    assert bad_file.exists()
    succ = _read_jsonl(d.SUCCESS_LOG)
    assert len(succ) == 1
    assert succ[0]["uploaded"] is True and succ[0]["reupload"] is True
    assert succ[0]["year"] == 2001 and succ[0]["season"] == 1
    failed = _read_jsonl(d.FAILED_LOG)
    assert [d.record_episode_key(r) for r in failed] == ["2_S01E01"]
    remaining = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert [d.record_episode_key(r) for r in remaining] == ["2_S01E01"]
    assert remaining[0]["fail_reason"] == "still down"
    out = capsys.readouterr().out
    assert "孤儿(本地已无)清理 1" in out
    assert "成功 1" in out and "仍失败 1" in out


def test_reupload_pending_refuses_when_main_running(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    d.acquire_main_lock()
    (sandbox / "pending.jsonl").write_text("{}\n", encoding="utf-8")
    called = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: called.append(1))
    d.reupload_pending()
    assert called == []
    assert "正在运行" in capsys.readouterr().out
    d.release_main_lock()


def test_reupload_pending_disabled_or_missing(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(d, "S3_ENABLED", False)
    d.reupload_pending()
    assert "无需补传" in capsys.readouterr().out
    monkeypatch.setattr(d, "S3_ENABLED", True)
    d.reupload_pending()
    assert "无待补传文件" in capsys.readouterr().out


# ---------------------------------------------------------------- path resolution
def test_resolve_file_and_dir_anchor_to_script_dir():
    root = str(d._SCRIPT_DIR)
    assert d.resolve_file(None, "x.jsonl") == os.path.join(root, "x.jsonl")
    assert d.resolve_file("  ", "x.jsonl") == os.path.join(root, "x.jsonl")
    assert d.resolve_file("sub/y.jsonl", "x") == os.path.join(root, "sub", "y.jsonl")
    assert d.resolve_dir("/abs/dir", "x") == "/abs/dir"
    assert d.BASE_DIR.startswith(root)
    assert d.MAIN_LOCK_FILE == os.path.join(root, "download_tv.main.lock")
