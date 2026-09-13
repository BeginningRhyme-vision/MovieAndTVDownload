"""Offline unit tests for download_tv.py.

Focus: episode-level key contract, local folder layout, R2 key mapping,
success/failed/pending log bookkeeping, playlist parsing and reupload flow.
No network / ffmpeg / boto3 calls are made.
"""

import builtins
import json
import os
import sys
import threading
import time
import types

import pytest

import download_tv as d


def _read_jsonl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class _FakeInterrupt:
    """替身中断事件：永不置位，但把每次退避时长记进 sink，供断言退避轮数。"""

    def __init__(self, sink):
        self.sink = sink

    def is_set(self):
        return False

    def wait(self, timeout=None):
        self.sink.append(timeout)
        return False

    def set(self):
        raise AssertionError("测试替身不应被置位")


def _always_set_event():
    ev = threading.Event()
    ev.set()
    return ev


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point every module-level path at tmp_path and reset shared state."""
    base = tmp_path / "downloads"
    temp = tmp_path / "temp"
    monkeypatch.setattr(d, "BASE_DIR", str(base))
    monkeypatch.setattr(d, "TEMP_DIR", str(temp))
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "success.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    # 判死账本也必须指向 tmp_path：它由 handle_done_future 在画质判死时自动写入，
    # 不隔离的话测试会把记录写进真实工作目录，而下一个用例启动时
    # load_dead_keys() 会把这些集并进跳过集 —— 表现为"待处理条目莫名变 0"，
    # 极难排查（本夹具漏掉它时确实让 5 个既有用例连锁失败过）。
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "download_dead.jsonl"))
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    monkeypatch.setattr(d, "MAIN_LOCK_FILE", str(tmp_path / "main.lock"))
    monkeypatch.setattr(d, "FOLDER_PREFIX", "tv")
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
def test_build_s3_key_layout(sandbox):
    assert d.build_s3_key(12345, 1, 3, 2008) == "tv/2008/12345/S01/E03/E03.mp4"
    assert d.build_s3_key("12345", "0", "12", "2008") == (
        "tv/2008/12345/S00/E12/E12.mp4"
    )


def test_build_s3_key_asset_shares_prefix_with_video(sandbox):
    """字幕/meta 与视频必须同前缀：前端与字幕脚本都按同前缀一次列举。"""
    video = d.build_s3_key(12345, 1, 3, 2008)
    meta = d.build_s3_key(12345, 1, 3, 2008, "meta.json")
    sub = d.build_s3_key(12345, 1, 3, 2008, "subs/en.srt")
    prefix = "tv/2008/12345/S01/E03"
    assert video == f"{prefix}/E03.mp4"
    assert meta == f"{prefix}/meta.json"
    assert sub == f"{prefix}/subs/en.srt"


def test_build_s3_key_is_stable_across_days(sandbox, monkeypatch):
    """对象键不含上传日期：同一集重传即幂等覆盖，而不是留下两份。

    这是"字幕脚本能由 (tid, s, e, year) 纯计算出前缀"的前提。
    """
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    first = d.build_s3_key(1, 1, 1, 2008)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260906")
    assert d.build_s3_key(1, 1, 1, 2008) == first


def test_build_s3_key_year_sanitised_and_prefix(sandbox, monkeypatch):
    assert d.build_s3_key(1, 1, 1, " 20/08 ").startswith("tv/2008/1/")
    assert d.build_s3_key(1, 1, 1, None).startswith("tv/unknown_year/1/")
    assert d.build_s3_key(1, 1, 1, "").startswith("tv/unknown_year/1/")
    assert d.build_s3_key(1, 1, 1, "n/a").startswith("tv/unknown_year/1/")
    monkeypatch.setattr(d, "S3_PREFIX", "bucketroot")
    assert d.build_s3_key(1, 1, 1, 2008) == "bucketroot/tv/2008/1/S01/E01/E01.mp4"


@pytest.mark.parametrize("tid,s,e", [
    (None, 1, 1), ("", 1, 1), (1, None, 1), (1, 1, None), (1, "x", 1),
])
def test_build_s3_key_missing_fields(sandbox, tid, s, e):
    with pytest.raises(ValueError):
        d.build_s3_key(tid, s, e, 2008)


def test_local_and_remote_layouts_are_isomorphic(sandbox):
    """本地目录与 R2 对象键必须严格同构，否则两侧无法互相换算。"""
    local = d.episode_dir(12345, 1, 3, 2008)
    rel = d.asset_rel_path(12345, 1, 3, 2008)
    assert rel == "tv/2008/12345/S01/E03"
    assert local == os.path.join(str(sandbox / "downloads"), *rel.split("/"))
    assert d.build_s3_key(12345, 1, 3, 2008, "meta.json") == f"{rel}/meta.json"


# ---------------------------------------------------------------- local layout
def _put_episode(base, year, tid, season, episode, data=b"x"):
    """在新布局下造一个成品：{base}/tv/{year}/{tid}/S{ss}/E{ee}/E{ee}.mp4。"""
    folder = base / "tv" / year / tid / f"S{season:02d}" / f"E{episode:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"E{episode:02d}.mp4"
    path.write_bytes(data)
    return path


def test_scan_downloaded_mp4_ids(sandbox):
    base = sandbox / "downloads"
    _put_episode(base, "2008", "100", 1, 1)
    _put_episode(base, "2008", "100", 1, 2, data=b"")     # empty -> cleaned
    _put_episode(base, "2008", "200", 0, 5)
    # 同一集出现在两个 year 目录下（脏数据）：只报告，不删。
    _put_episode(base, "2009", "100", 1, 1)
    # 旁车资产与非集目录不参与去重判定。
    ep_dir = base / "tv" / "2008" / "100" / "S01" / "E01"
    (ep_dir / "meta.json").write_text("{}", encoding="utf-8")
    (ep_dir / "subs").mkdir()
    (ep_dir / "subs" / "en.srt").write_text("x", encoding="utf-8")
    (base / "tv" / "2008" / "100" / "Extras").mkdir()
    (base / "tv" / "2008" / "notanid").mkdir()

    ids, dups = d.scan_downloaded_mp4_ids()
    assert ids == {"100_S01E01", "200_S00E05"}
    assert set(dups) == {"100_S01E01"}
    assert len(dups["100_S01E01"]) == 2
    # 0 字节孤儿已被清理。
    assert not (
        base / "tv" / "2008" / "100" / "S01" / "E02" / "E02.mp4"
    ).exists()


def test_scan_downloaded_mp4_ids_missing_base(sandbox):
    assert d.scan_downloaded_mp4_ids() == (set(), {})


def test_move_to_target_folder_uses_episode_dir(sandbox):
    base = sandbox / "downloads"
    temp = sandbox / "temp"
    temp.mkdir()

    def mk(name):
        p = temp / name
        p.write_bytes(b"v")
        return str(p)

    p1 = d.move_to_target_folder(mk("a.mp4"), 1, 1, 1, 2008)
    p2 = d.move_to_target_folder(mk("b.mp4"), 1, 1, 2, 2008)
    p3 = d.move_to_target_folder(mk("c.mp4"), 1, 2, 1, 2008)
    assert p1 == str(base / "tv" / "2008" / "1" / "S01" / "E01" / "E01.mp4")
    assert p2 == str(base / "tv" / "2008" / "1" / "S01" / "E02" / "E02.mp4")
    assert p3 == str(base / "tv" / "2008" / "1" / "S02" / "E01" / "E01.mp4")

    # 同一集重复落盘就地覆盖，不额外造目录。
    p1b = d.move_to_target_folder(mk("d.mp4"), 1, 1, 1, 2008)
    assert p1b == p1
    assert os.listdir(base / "tv" / "2008" / "1" / "S01" / "E01") == ["E01.mp4"]
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
        d.move_to_target_folder(str(src), 1, 1, 1, 2008)
    assert not (
        base / "tv" / "2008" / "1" / "S01" / "E01" / "E01.mp4"
    ).exists()


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
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
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


def _z1_env(monkeypatch, variants):
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
    # 根本进不了采样循环，测不出"选中后跳过更低流"这条新逻辑。
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


def test_sampling_uses_tiered_retry_budget(sandbox, monkeypatch):
    """采样用 SAMPLE_SEG_RETRY_MAX，正片仍用 SEG_RETRY_MAX（默认值）。

    采样只是为择优探码率，探不到就该快速换下一条候选流；正片才需要死磕。
    两者共用同一个预算会让一条烂流把下载窗口占满，直接拖垮成功率。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    monkeypatch.setattr(d, "SAMPLE_SEG_RETRY_MAX", 3)
    budgets = []
    _z1_env(monkeypatch, [("1920x1080", "https://cdn/1080.m3u8", 5000.0)])
    inner = d.download_segments

    def spy(*args, **kwargs):
        budgets.append(kwargs.get("retry_max"))
        return inner(*args, **kwargs)

    monkeypatch.setattr(d, "download_segments", spy)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, _job = d.process_one_entry(entry, set())
    assert ok is True
    # 第一次是采样（分层预算），第二次是正片（None -> 回落 SEG_RETRY_MAX）
    assert budgets == [3, None]


def test_variant_sampling_stops_after_higher_stream_wins(sandbox, monkeypatch):
    """选中 1080 后，声明高度更低的流不再采样。

    候选已按声明高度降序排，且有声明分辨率的流直接采信声明值（不做 ffprobe），
    择优又是"高度绝对优先"——更低的流即便采样也必然落选，那次"解析 media
    playlist + 下载 N 个分片 + 两次 ffprobe"是纯浪费。一个 master 常有
    1080/720/480/360 四档，白花的是三份采样流量。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
        ("854x480", "https://cdn/480.m3u8", 1500.0),
    ])

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert job["resolution"] == "1920x1080"
    assert sampled == ["https://cdn/1080.m3u8"]


def test_variant_sampling_still_compares_same_height(sandbox, monkeypatch):
    """同声明高度的流必须全部采样——要比采样码率才能择优。"""
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/a.m3u8", 5000.0),
        ("1920x1080", "https://cdn/b.m3u8", 4000.0),
        ("1280x720", "https://cdn/c.m3u8", 3000.0),
    ])

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
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
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        (None, "https://cdn/unknown.m3u8", 4000.0),
    ])
    # 门槛按 (h/1080)² 缩放，2160p 需 4 倍基准；压低基准让桩数据能过关，
    # 本用例要验的是采样顺序而非码率曲线。
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 100.0})

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert sampled == ["https://cdn/1080.m3u8", "https://cdn/unknown.m3u8"]
    assert job["resolution"] == "3840x2160"


def test_variant_sampling_continues_after_failure(sandbox, monkeypatch):
    """最高档采样失败（未选中）时，后续较低流仍要采样。

    跳过条件绑定 best_selected：只有真正选中过某流才生效。否则瞬时抖动让最高
    档挂掉后，整集会因"无一入选"而白白失败，直接损失成功率。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
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

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert sampled == ["https://cdn/1080.m3u8", "https://cdn/720.m3u8"]
    assert job["resolution"] == "1280x720"


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


def test_conversion_job_carries_provider_attribution(sandbox, monkeypatch):
    """🔑 成功的 job 必须带上是哪家 provider、第几个节点下成的。

    没有这几个字段，多节点 fallback 的全部价值都无法量化——成品里看不出
    哪家救回来的，也就无从判断某个源该留该撤（§0.12 待复验 vidfast 去留）。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(
        d, "_download_mp4_direct",
        lambda node, out, label, runtime_minutes=None: ("1920x1080", 4000),
    )
    entry = {"tmdbId": 9, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
    ]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert job["provider"] == "vidlink" and job["node_type"] == "mp4"
    assert job["node_index"] == 1 and job["node_total"] == 1


def test_attribution_records_which_node_won_the_fallback(sandbox, monkeypatch):
    """首节点挂掉、次节点救回时，node_index 必须是 2。

    node_index > 1 正是"多源到底有没有用"最直接的证据：它说明这一集
    **只靠 fallback 才拿到**，是判断某个源价值的核心信号。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    def fake_direct(node, output_path, label, runtime_minutes=None):
        raise RuntimeError("直链块不可用（HTTP 404）")

    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: [("1920x1080", "https://m/v", 5000.0)],
    )
    monkeypatch.setattr(
        d, "parse_media_playlist",
        lambda url, headers=None: ([f"https://s/{i}.ts" for i in range(8)],
                                   [4.0] * 8, None),
    )
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1920, 1080))
    monkeypatch.setattr(d, "SAMPLE_COUNT", 2)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 100.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        n = (len(urls) if end_idx is None else end_idx) - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        return n * 1_000_000, [], 0

    monkeypatch.setattr(d, "download_segments", fake_download)
    entry = {"tmdbId": 9, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
        {"url": "https://m/master.m3u8", "provider": "vidfast", "type": "m3u8"},
    ]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    # 第二个节点才成功 —— 这一集是被 vidfast 靠 fallback 救回来的
    assert job["provider"] == "vidfast" and job["node_type"] == "m3u8"
    assert job["node_index"] == 2 and job["node_total"] == 2


def test_attribution_of_extracts_all_keys():
    """归因字段集中定义，缺键补 None（reupload 等路径可能没有这些键）。"""
    got = d._attribution_of({
        "provider": "vidup", "node_type": "m3u8",
        "node_index": 1, "node_total": 3, "irrelevant": "x",
    })
    assert got == {
        "provider": "vidup", "node_type": "m3u8",
        "node_index": 1, "node_total": 3,
    }
    assert d._attribution_of({}) == {
        "provider": None, "node_type": None,
        "node_index": None, "node_total": None,
    }
    assert d._attribution_of(None)["provider"] is None


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
    """瞬时错误（503）仍要重试满次数——这才是重试真正能救回来的场景。

    退避走 `interrupted.wait()` 而非裸 sleep，所以这里桩掉整个事件来记录等待。
    """
    sleeps = []
    monkeypatch.setattr(d, "interrupted", _FakeInterrupt(sleeps))
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise d.requests.HTTPError("boom", response=_FakeResp(503))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    with pytest.raises(RuntimeError):
        d.download_single_segment("u", 0, 3, 1)
    assert len(calls) == 3 and len(sleeps) == 2


def test_download_single_segment_aborts_before_request_when_interrupted(
    sandbox, monkeypatch
):
    """中断已置位：连第一个请求都不该发出去，直接上抛取消。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 20)
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise AssertionError("中断后不应再发请求")

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    monkeypatch.setattr(d, "interrupted", _always_set_event())
    with pytest.raises(RuntimeError, match="已取消（收到中断信号）"):
        d.download_single_segment("u", 0, 20, 1)
    assert calls == []


def test_download_single_segment_backoff_wakes_up_on_interrupt(
    sandbox, monkeypatch
):
    """退避等待期间收到中断：立刻醒来放弃，不空等完整个退避窗口。"""
    monkeypatch.setattr(d, "interrupted", threading.Event())
    monkeypatch.setattr(d, "SEG_RETRY_DELAY", 30)
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        # 首次失败进入退避，退避中主动置位中断。
        threading.Timer(0.05, d.interrupted.set).start()
        raise d.requests.HTTPError("boom", response=_FakeResp(503))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="已取消（收到中断信号）"):
        d.download_single_segment("u", 0, 5, 30)
    # 裸 sleep 的旧实现这里要等满 30s。
    assert time.monotonic() - started < 5
    assert len(calls) == 1


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
    """0 字节 mp4 是移动中断留下的残骸：既不算已下载，也要清掉。"""
    base = sandbox / "downloads"
    real = _put_episode(base, "2001", "1", 1, 1, data=b"video")
    orphan = _put_episode(base, "2001", "2", 1, 2, data=b"")

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


# ------------------------------------------------ CDN 主机级 429 熔断
@pytest.fixture
def circuit_env(monkeypatch):
    """每个用例独立的熔断计数（模块级字典会跨用例污染）。"""
    monkeypatch.setattr(d, "_mp4_host_429", {})
    monkeypatch.setattr(d, "_mp4_host_tripped", set())
    monkeypatch.setattr(d, "MP4_HOST_CIRCUIT_THRESHOLD", 3)
    monkeypatch.setattr(d, "record_block_status", lambda s: None)


@pytest.mark.parametrize("url,expected", [
    ("https://bcdn.hakunaymatata.com/a/b.mp4", "bcdn.hakunaymatata.com"),
    ("http://HOST.example.com/x", "host.example.com"),   # 归一小写
    ("https://h.example.com", "h.example.com"),          # 无路径段
    ("not-a-url", ""),
    (None, ""),
])
def test_host_of(url, expected):
    assert d._host_of(url) == expected


def test_mp4_host_circuit_trips_at_threshold(circuit_env, capsys):
    url = "https://bcdnxw.hakunaymatata.com/a.mp4"
    for _ in range(2):
        d._mp4_host_record_429(url)
    assert d._mp4_host_is_tripped(url) is False   # 未达阈值不熔断
    d._mp4_host_record_429(url)
    assert d._mp4_host_is_tripped(url) is True
    assert "主机熔断" in capsys.readouterr().out


def test_mp4_host_circuit_is_per_host(circuit_env):
    """🔒 只跳过同一台主机；同域其它主机与别家域名都不受影响。"""
    bad = "https://bcdnxw.hakunaymatata.com/a.mp4"
    for _ in range(3):
        d._mp4_host_record_429(bad)
    assert d._mp4_host_is_tripped(bad) is True
    # 同域不同主机
    assert d._mp4_host_is_tripped("https://bcdn.hakunaymatata.com/b.mp4") is False
    # 完全不同的域
    assert d._mp4_host_is_tripped("https://sun.peakstorm.top/c.m3u8") is False


def test_probe_total_size_short_circuits_tripped_host(circuit_env, monkeypatch):
    """熔断后连请求都不该发出去——这正是本功能省时间的地方。"""
    url = "https://bad.cdn/a.mp4"
    for _ in range(3):
        d._mp4_host_record_429(url)

    def boom(*a, **kw):
        pytest.fail("熔断主机不应再发起请求")

    monkeypatch.setattr(d, "get_session", boom)
    with pytest.raises(RuntimeError, match=d._MP4_HOST_BLOCKED_MARKER):
        d._mp4_probe_total_size(url, {}, None)


def test_probe_total_size_records_429(circuit_env, monkeypatch):
    """探测层的 429 要计入熔断（不只是块层）。"""
    url = "https://slow.cdn/a.mp4"
    _install_range_session(monkeypatch, b"", status=429)
    for _ in range(3):
        with pytest.raises(Exception):
            d._mp4_probe_total_size(url, {}, None)
    assert d._mp4_host_is_tripped(url) is True


def test_mp4_chunk_429_trips_and_aborts_immediately(mp4_env, circuit_env,
                                                    monkeypatch):
    """🔒 块层 429 立即上抛 + 计入熔断，不走 SEG_RETRY_MAX 次退避。

    实测该 429 是整机故障而非限流：换 IP/签名/冷却后恒定 429，
    继续退避 20 次纯属空耗，必须立刻换节点。
    """
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 20)
    url = "https://bad2.cdn/a.mp4"
    session = _install_range_session(monkeypatch, b"", status=429)
    with pytest.raises(RuntimeError, match=d._MP4_HOST_BLOCKED_MARKER):
        d._download_mp4_chunk(url, {}, 0, 3, 0)
    # 关键：只发了一次请求，没有走 20 次退避重试
    assert len(session.calls) == 1
    # 单次 429 只计数不熔断（阈值 3）；凑满阈值后才跳过该主机
    assert d._mp4_host_429[d._host_of(url)] == 1
    assert d._mp4_host_is_tripped(url) is False
    for _ in range(2):
        with pytest.raises(RuntimeError):
            d._download_mp4_chunk(url, {}, 0, 3, 0)
    assert d._mp4_host_is_tripped(url) is True


def test_host_circuit_marker_is_not_a_permanent_failure():
    """🔒 红线：熔断文案绝不能进整集判死表。

    主机故障是临时的，判整集死会让本可救回的集永久丢失。
    它只进块级短路表（不再退避、尽快换节点）。
    """
    msg = f"{d._MP4_HOST_BLOCKED_MARKER}（HTTP 429，bad.cdn）: https://bad.cdn/a.mp4"
    assert d._MP4_HOST_BLOCKED_MARKER not in d._PERMANENT_FAILURE_MARKERS
    assert d._classify_failure(msg) is True          # 仍可重试
    assert d._MP4_HOST_BLOCKED_MARKER in d._MP4_CHUNK_NO_RETRY_MARKERS
    assert d.classify_reject_reason(msg) == "直链主机熔断(429)"


# ------------------------------------------------ 失败侧逐节点归因
def test_node_failures_record_each_provider(sandbox, monkeypatch):
    """🔒 每个失败节点各记一条：provider / 类目 / retriable / 原文。

    没有它就只能看末节点的 error，**无法区分同为 m3u8 的 vidup 与 vidfast**。
    """
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    # 节点 1（vidup / m3u8）瞬时 5xx；节点 2（vidlink / mp4）签名过期。
    # 走真实的节点循环，只把两条下载路径的入口换成桩。
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("HTTP Error 502")),
    )
    monkeypatch.setattr(
        d, "_download_mp4_direct",
        lambda *a, **kw: (_ for _ in ()).throw(
            RuntimeError("直链已失效（HTTP 403），需重新取流")
        ),
    )

    entry = {
        "tmdbId": "10", "season": 1, "episode": 1,
        "urls": [
            {"url": "https://a/1.m3u8", "provider": "vidup", "type": "m3u8",
             "headers": {}, "quality": None, "size": None},
            {"url": "https://b/2.mp4", "provider": "vidlink", "type": "mp4",
             "headers": {}, "quality": None, "size": None},
        ],
    }
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    nodes = info["node_failures"]
    assert [n["provider"] for n in nodes] == ["vidup", "vidlink"]
    assert [n["node_index"] for n in nodes] == [1, 2]
    assert nodes[0]["reason_class"] == "源站5xx"
    assert nodes[0]["retriable"] is True
    assert nodes[1]["reason_class"] == "直链失效需重新取流"
    # 整集结论仍由既有逻辑给出，不受影响
    assert info["retriable"] is True
    assert info["needs_refetch"] is True


def test_node_failures_absent_on_early_return(sandbox, monkeypatch):
    """缺字段的早退路径在拿 ID 锁之前就 return，本就没有节点上下文。

    此时 info 里没有 node_failures 键是**正确的**——消费端统一用
    `info.get("node_failures") or []` 兜底，落盘为空列表。
    """
    monkeypatch.setattr(d, "processing_ids", set())
    label, ok, info = d.process_one_entry(
        {"tmdbId": "11", "season": 1, "episode": 1, "urls": []}, set()
    )
    assert ok is False
    assert info["error"] == "缺少 tmdbId/season/episode 或 urls"
    assert (info.get("node_failures") or []) == []


def test_node_failures_reach_failed_log(sandbox, monkeypatch):
    """端到端：node_failures 必须落进 failed.jsonl，否则统计侧读不到。"""
    line = {"tmdbId": "12", "season": 1, "episode": 1,
            "urls": [{"url": "https://a/1.m3u8", "provider": "vidup",
                      "type": "m3u8", "headers": {}, "quality": None,
                      "size": None}]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, processed: ("12_S01E01", False, {
            "error": "HTTP Error 502",
            "retriable": True,
            "node_failures": [
                {"node_index": 1, "provider": "vidup", "node_type": "m3u8",
                 "reason_class": "源站5xx", "retriable": True,
                 "error": "HTTP Error 502"},
            ],
        }),
    )
    d._run_pipeline()
    rows = [r for r in _read_jsonl(sandbox / "failed.jsonl")
            if r.get("stage") == "download"]
    assert rows and rows[0]["node_failures"][0]["provider"] == "vidup"


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
    with pytest.raises(RuntimeError, match="已取消（整片已判失败）"):
        d._download_mp4_chunk("u", {}, 0, 9, 0, abort)
    assert session.calls == []


def test_download_mp4_chunk_aborts_immediately_when_interrupted(
    mp4_env, monkeypatch
):
    """进程级中断置位：即便没有 abort_event，块层也要立刻放弃。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    monkeypatch.setattr(d, "interrupted", _always_set_event())
    session = _install_range_session(monkeypatch, b"", status=500)
    with pytest.raises(RuntimeError, match="已取消（收到中断信号）"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    assert session.calls == []


def test_download_mp4_chunk_backoff_wakes_up_on_interrupt(mp4_env, monkeypatch):
    """无 abort_event 时退避也必须可打断（不能退化成裸 sleep）。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    monkeypatch.setattr(d, "SEG_RETRY_DELAY", 30)
    monkeypatch.setattr(d, "interrupted", threading.Event())
    session = _install_range_session(monkeypatch, b"", status=500)
    threading.Timer(0.05, d.interrupted.set).start()
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="重试后仍失败"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    # 裸 sleep 的旧实现这里要等满 30s。
    assert time.monotonic() - started < 5
    assert len(session.calls) == 1


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
    assert info["final_path"] == str(
        sandbox / "downloads" / "tv" / "2001" / "1" / "S01" / "E01" / "E01.mp4"
    )
    assert os.path.exists(info["final_path"])
    assert (info["season"], info["episode"], info["year"]) == (1, 1, 2001)
    assert processed == {"1_S01E01"}
    assert d.processing_ids == set()
    assert not ts.exists() and not sample.exists()
    # 旁车 meta.json 与视频同目录落盘，并在 success_info 里留标记供上传阶段收集。
    assert info["has_meta"] is True
    meta_path = os.path.join(os.path.dirname(info["final_path"]), "meta.json")
    with open(meta_path, encoding="utf-8") as fh:
        meta = json.load(fh)
    assert meta["tmdbId"] == "1"
    assert (meta["season"], meta["episode"]) == (1, 1)
    assert meta["video"]["file"] == "E01.mp4"
    assert meta["subtitles"] == []

    # Failure path: ffmpeg fails -> lock released, nothing registered.
    ts.write_bytes(b"x")
    monkeypatch.setattr(d, "convert_ts_to_mp4", lambda s, t: False)
    d.processing_ids.add("1_S01E01")
    key, ok, info = d.finalize_one_entry(job, processed2 := set())
    assert ok is False and "FFmpeg" in info["error"]
    assert processed2 == set() and d.processing_ids == set()
    assert not ts.exists()


def _success_info(sandbox, key="1_S01E01"):
    tid, _, rest = key.partition("_S")
    season, _, episode = rest.partition("E")
    s, e = int(season), int(episode)
    folder = (
        sandbox / "downloads" / "tv" / "2001" / tid / f"S{s:02d}" / f"E{e:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"E{e:02d}.mp4"
    path.write_bytes(b"mp4")
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
    uploaded = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (uploaded.append((p, k)) or (True, None)))
    info = _success_info(sandbox)
    # 旁车资产与视频同目录，应随视频一并上传并删除本地。
    meta_path = os.path.join(os.path.dirname(info["final_path"]), "meta.json")
    with open(meta_path, "w", encoding="utf-8") as fh:
        fh.write("{}")
    info["has_meta"] = True

    key, ok, out = d.upload_one_entry(info)
    assert ok is True
    video_key = "tv/2001/1/S01/E01/E01.mp4"
    meta_key = "tv/2001/1/S01/E01/meta.json"
    assert uploaded == [(info["final_path"], video_key), (meta_path, meta_key)]
    assert out["asset_keys"] == [meta_key]
    assert not os.path.exists(info["final_path"])
    assert not os.path.exists(meta_path)
    # 视频与资产都已进 R2，空的集目录被清掉，不留空壳吃 inode。
    assert not os.path.isdir(os.path.dirname(info["final_path"]))
    rec = _read_jsonl(d.SUCCESS_LOG)[0]
    assert rec["uploaded"] is True and rec["s3_key"] == video_key
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


# ------------------------------------------------ 陈旧直链启动预检
def _stale_env(monkeypatch, sandbox, *, enabled=True, stale_after=86400):
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", enabled)
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", stale_after)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_EPISODE", 2)
    monkeypatch.setattr(d, "AUTO_REFETCH_WORKERS", 2)
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "results.jsonl"))


def _entry(tid, *, fetched_at=None, url="old"):
    e = {"tmdbId": tid, "season": 1, "episode": 2, "urls": [url]}
    if fetched_at is not None:
        e["fetched_at"] = fetched_at
    return e


def _install_fetcher(monkeypatch, results):
    """把假的 tv_ids_to_links 塞进 sys.modules，供 refetch_entries 延迟导入。

    results: {集级 key: (status, result)}；未列出的 key 返回 ("dead", None)。
    返回记录被调用集的 list。
    """
    calls = []
    module = types.ModuleType("tv_ids_to_links")

    def process_episode(tid, season, episode):
        key = d.episode_key(tid, season, episode)
        calls.append(key)
        return results.get(key, ("dead", None))

    module.process_episode = process_episode
    monkeypatch.setitem(sys.modules, "tv_ids_to_links", module)
    return calls


def test_is_stale_entry_needs_fetched_at(monkeypatch, sandbox):
    """没有 fetched_at 的条目一律判不陈旧——宁可漏判，不可误判。

    旧版 results.jsonl 与手工输入都没有该字段，当成"无限旧"会让整批集在启动时
    全部去重取流，取流配额与耗时双重浪费，且多半徒劳。
    """
    _stale_env(monkeypatch, sandbox)
    now = 1_000_000
    assert d.is_stale_entry(_entry("1"), now) is False
    assert d.is_stale_entry(_entry("1", fetched_at=0), now) is False
    assert d.is_stale_entry(_entry("1", fetched_at=now - 10), now) is False
    assert d.is_stale_entry(_entry("1", fetched_at=now - 86401), now) is True


def test_is_stale_entry_disabled_by_zero(monkeypatch, sandbox):
    """stale_after_seconds=0 关闭判定，再旧也不算陈旧。"""
    _stale_env(monkeypatch, sandbox, stale_after=0)
    assert d.is_stale_entry(_entry("1", fetched_at=1), 10**9) is False


def test_refresh_stale_entries_replaces_only_stale_and_keeps_order(
    monkeypatch, sandbox
):
    """只换陈旧的那条，顺序不变，新鲜条目不该被送去重取。"""
    _stale_env(monkeypatch, sandbox)
    now = int(time.time())
    fresh = _entry("1", fetched_at=now)
    stale = _entry("2", fetched_at=now - 90000)
    revived = {
        "tmdbId": "2", "season": 1, "episode": 2,
        "urls": ["new"], "fetched_at": now,
    }
    calls = _install_fetcher(monkeypatch, {"2_S01E02": ("ok", revived)})

    out = d.refresh_stale_entries([fresh, stale], {})
    # 只有陈旧的那集被重取
    assert calls == ["2_S01E02"]
    # 顺序保持，新鲜条目原样
    assert out[0] is fresh
    assert out[1]["urls"] == ["new"] and out[1]["fetched_at"] == now
    # 新结果落盘 INPUT_JSONL，供下次运行按 fetched_at 择新
    assert _read_jsonl(d.INPUT_JSONL) == [revived]


def test_refresh_stale_entries_keeps_original_when_refetch_fails(
    monkeypatch, sandbox
):
    """🔑 重取无果必须原样保留，绝不丢弃。

    阈值只是经验值、旧链接未必真失效；且没配代理凭证的机器根本取不了流，
    丢弃等于整批集全军覆没。本功能绝不允许反过来降低下载成功率。
    """
    _stale_env(monkeypatch, sandbox)
    stale = _entry("2", fetched_at=1)
    _install_fetcher(monkeypatch, {})          # 全部返回 dead

    out = d.refresh_stale_entries([stale], {})
    assert out == [stale] and out[0]["urls"] == ["old"]


def test_refresh_stale_entries_lets_keyboard_interrupt_escape(
    monkeypatch, sandbox
):
    """Ctrl+C 必须原样逃逸：重取只吞 Exception/SystemExit，绝不笼统吞 BaseException。"""
    _stale_env(monkeypatch, sandbox)

    def boom(entries, counts):
        raise KeyboardInterrupt

    monkeypatch.setattr(d, "refetch_entries", boom)
    with pytest.raises(KeyboardInterrupt):
        d.refresh_stale_entries([_entry("2", fetched_at=1)], {})


def test_refetch_entries_import_systemexit_is_swallowed(monkeypatch, sandbox):
    """import 阶段的 SystemExit 被吞掉并返回空列表（沿用旧链接继续下载）。"""
    _stale_env(monkeypatch, sandbox)
    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "tv_ids_to_links":
            raise SystemExit("缺少代理凭证")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert d.refetch_entries([_entry("2", fetched_at=1)], {}) == []


def test_refetch_entries_respects_per_episode_cap(monkeypatch, sandbox):
    """达到每集重取上限后不再重取，防'取流-过期-重取'反复空转。"""
    _stale_env(monkeypatch, sandbox)
    calls = _install_fetcher(monkeypatch, {})
    counts = {"2_S01E02": 2}          # 已达上限 AUTO_REFETCH_MAX_PER_EPISODE
    assert d.refetch_entries([_entry("2", fetched_at=1)], counts) == []
    assert calls == []


def test_refetch_entries_counts_are_per_episode_not_per_show(
    monkeypatch, sandbox
):
    """🔑 记数粒度必须是集级：同一部剧的几十集共用一个 tmdbId。

    若按剧记数，同剧只要有 max_per_episode 集被重取过，**剩下所有集都会被
    永久挡掉**——一部 100 集的剧只救得回 2 集。这里跑两轮来暴露它：第一轮
    把计数填起来，第二轮换别的集，按集记数时它们仍应被重取。
    """
    _stale_env(monkeypatch, sandbox)
    counts = {}
    mk = lambda ep: {  # noqa: E731
        "tmdbId": "7", "season": 1, "episode": ep, "urls": ["o"],
        "fetched_at": 1,
    }
    calls = _install_fetcher(monkeypatch, {})

    # 第一轮：S01E01/E02 各被重取一次 → 按剧记数的话 counts["7"] 会累到 2
    d.refetch_entries([mk(1), mk(2)], counts)
    calls.clear()
    # 第二轮：换两集全新的。按集记数它们的计数是 0，必须照样被重取。
    d.refetch_entries([mk(3), mk(4)], counts)

    assert sorted(calls) == ["7_S01E03", "7_S01E04"]
    # 计数按集分开存放，不会挤在一个剧级键上
    assert counts.get("7") is None
    assert counts["7_S01E01"] == 1 and counts["7_S01E03"] == 1


def test_refetch_entries_single_failure_does_not_kill_batch(
    monkeypatch, sandbox
):
    """单集重取抛异常不影响同批其余集。"""
    _stale_env(monkeypatch, sandbox)
    now = int(time.time())
    good = {"tmdbId": "9", "season": 1, "episode": 2,
            "urls": ["new"], "fetched_at": now}
    module = types.ModuleType("tv_ids_to_links")

    def process_episode(tid, season, episode):
        if str(tid) == "8":
            raise RuntimeError("boom")
        return "ok", good

    module.process_episode = process_episode
    monkeypatch.setitem(sys.modules, "tv_ids_to_links", module)

    out = d.refetch_entries(
        [_entry("8", fetched_at=1), _entry("9", fetched_at=1)], {}
    )
    assert [e["tmdbId"] for e in out] == ["9"]


def test_refetch_entries_preserves_extra_metadata(monkeypatch, sandbox):
    """逐键覆盖而非整体替换：entry 上的历史元数据必须保留。"""
    _stale_env(monkeypatch, sandbox)
    now = int(time.time())
    entry = _entry("2", fetched_at=1)
    entry["year"] = 2021
    entry["runtime_minutes"] = 42
    revived = {"tmdbId": "2", "season": 1, "episode": 2,
               "urls": ["new"], "fetched_at": now}
    _install_fetcher(monkeypatch, {"2_S01E02": ("ok", revived)})

    out = d.refetch_entries([entry], {})
    assert out[0]["year"] == 2021 and out[0]["runtime_minutes"] == 42
    assert out[0]["urls"] == ["new"]


def test_refresh_stale_entries_noop_when_disabled(monkeypatch, sandbox):
    """关闭开关后原样返回，且绝不碰取流模块。"""
    _stale_env(monkeypatch, sandbox, enabled=False)
    calls = _install_fetcher(monkeypatch, {})
    entries = [_entry("2", fetched_at=1)]
    assert d.refresh_stale_entries(entries, {}) is entries
    assert calls == []


def test_precheck_caps_batch_and_prefers_oldest(monkeypatch, sandbox):
    """🔑 单次限额 + 最旧优先。

    TV 全量下 stale 可达几万到十几万集，一次性全 submit 会让绝大多数排队到
    总超时被丢弃，且它们的 refetch_counts 不会 +1，下次运行又从头再来，
    队尾永远轮不到 —— 等于预检只对前几百集有效。
    """
    _stale_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 2)
    # 故意让最旧的排在列表末尾，验证是按时间而非按出现顺序挑
    entries = [
        _entry("1", fetched_at=500),
        _entry("2", fetched_at=100),
        _entry("3", fetched_at=300),
        _entry("4", fetched_at=200),
    ]
    calls = _install_fetcher(monkeypatch, {})
    d.refresh_stale_entries(entries, {})
    # 只处理 2 集，且是 fetched_at 最小（最旧）的两集
    assert sorted(calls) == ["2_S01E02", "4_S01E02"]


def test_precheck_cap_zero_means_unlimited(monkeypatch, sandbox):
    """max_per_run=0 表示不限额（小批量场景）。"""
    _stale_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 0)
    entries = [_entry(str(i), fetched_at=i + 1) for i in range(5)]
    calls = _install_fetcher(monkeypatch, {})
    d.refresh_stale_entries(entries, {})
    assert len(calls) == 5


def test_precheck_skips_quality_dead_episodes(monkeypatch, sandbox):
    """🔑 上次因画质不达标判死的集不再重取——重取回来仍不达标，白烧配额。

    TV 侧集级有源率个位数，这类集在失败总量里占比很高。
    """
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "分辨率 640x360 低于红线 720（容差 0.95），跳过",
        "stage": "download", "retriable": False,
    })
    calls = _install_fetcher(monkeypatch, {})
    out = d.refresh_stale_entries([_entry("2", fetched_at=1)], {})
    assert calls == []              # 一次取流都不发
    assert out[0]["urls"] == ["old"]  # 条目原样保留，照常用旧链接下载


def test_precheck_does_not_skip_transient_failures(monkeypatch, sandbox):
    """🔴 红线：源站 5xx / 超时**绝不能**被当成画质判死跳过。

    那是"今天源站挂了"，重取完全可能换到好流；跳过等于永久放弃可救回的集。
    """
    _stale_env(monkeypatch, sandbox)
    for err, retriable in [
        ("HTTP Error 502", True),
        ("Read timed out", True),
        ("直链已失效（HTTP 403），需重新取流: x", False),
    ]:
        d.write_log(d.FAILED_LOG, {
            "tmdbId": "2", "season": 1, "episode": 2,
            "error": err, "stage": "download", "retriable": retriable,
        })
        calls = _install_fetcher(monkeypatch, {})
        d.refresh_stale_entries([_entry("2", fetched_at=1)], {})
        assert calls == ["2_S01E02"], f"{err} 不该被跳过"
        os.remove(d.FAILED_LOG)


def test_quality_dead_takes_the_latest_verdict(monkeypatch, sandbox):
    """同一集先画质判死、后来因别的原因失败 → 以最后一条为准，不再跳过。

    画质门槛可能被调松过，旧的判死结论不该永久压住它。
    """
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "码率未达到门槛", "stage": "download", "retriable": False,
    })
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "HTTP Error 502", "stage": "download", "retriable": True,
    })
    assert d.load_quality_dead_keys() == set()


def test_load_quality_dead_keys_honors_node_failures(monkeypatch, sandbox):
    """🔒 落盘重载同口径：failed.jsonl 里有节点没给出画质结论 → 不算判死。

    这条路径决定"下次运行的启动预检要不要跳过这集重取"。若只看
    retriable+文案而忽略 node_failures，进程内正确判为"可重试"的集会在
    **重启后**被当成画质判死永久跳过 —— 判死结论凭空复活。
    """
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "分辨率 640x360 低于红线 1080",
        "stage": "download", "retriable": False,
        "node_failures": [
            {"reason_class": "直链失效需重新取流"},   # 从未给出画质结论
            {"reason_class": "分辨率低于红线"},
        ],
    })
    assert d.load_quality_dead_keys() == set()


def test_load_quality_dead_keys_kills_when_all_nodes_report_quality(monkeypatch,
                                                                     sandbox):
    """对照组：全部节点都是画质结论 → 重载后仍判死（避免上条测试变成平凡通过）。"""
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "分辨率 640x360 低于红线 1080",
        "stage": "download", "retriable": False,
        "node_failures": [
            {"reason_class": "分辨率低于红线"},
            {"reason_class": "码率未达门槛"},
        ],
    })
    assert d.load_quality_dead_keys() == {"2_S01E02"}


def test_is_quality_dead_requires_non_retriable():
    """可重试的失败一律不算画质判死（多节点集里"画质淘汰+502"整集仍可重试）。"""
    assert d.is_quality_dead(False, "分辨率 640x360 低于红线 720") is True
    assert d.is_quality_dead(False, "码率未达到门槛") is True
    # 画质汇总判死（内层全流淘汰 / 外层概率判死）也算
    assert d.is_quality_dead(
        False, "全部 3 条候选流均因画质不达标被淘汰（各流原因见上方日志）"
    ) is True
    # 同样的文案，retriable=True 就不算
    assert d.is_quality_dead(True, "分辨率 640x360 低于红线 720") is False
    # 非画质类的确定性失败也不算
    assert d.is_quality_dead(False, "直链块不可用（HTTP 404）") is False
    assert d.is_quality_dead(False, "HTTP Error 502") is False


# --- 第 3 道闸门：必须**全部节点**都给出画质结论才判死 -----------------------
# 业务红线（用户拍板）：判死必须确定"该集在所有存在资源的节点上都画质不达标"，
# 其余情况一律可重试。有跨运行重试兜底时，误判死 = 永久丢一集，代价远高于多跑一轮。
_QUALITY_NODE = {"reason_class": "分辨率低于红线"}
_BITRATE_NODE = {"reason_class": "码率未达门槛"}
_REFETCH_NODE = {"reason_class": "直链失效需重新取流"}
_5XX_NODE = {"reason_class": "源站5xx"}
_STRUCT_NODE = {"reason_class": "不支持的播放列表结构"}
_CHUNK404_NODE = {"reason_class": "直链块不可用(404/416)"}


def test_quality_dead_requires_every_node_to_report_quality():
    """全部节点都是画质类结论 → 判死（这才是"所有节点都不达标"）。"""
    msg = "分辨率 640x360 低于红线 1080"
    assert d.is_quality_dead(False, msg, [_QUALITY_NODE]) is True
    assert d.is_quality_dead(False, msg, [_QUALITY_NODE, _BITRATE_NODE]) is True


@pytest.mark.parametrize("other_node, label", [
    (_REFETCH_NODE, "直链失效（换新链接可能就是 1080p）"),
    (_5XX_NODE, "源站 5xx（今天挂了、明天可能好）"),
    (_STRUCT_NODE, "结构不支持（该节点从未给出画质结论）"),
    (_CHUNK404_NODE, "直链块 404"),
])
def test_quality_dead_blocked_when_any_node_lacks_quality_verdict(other_node, label):
    """🔒 红线：任一节点没给出画质结论 → 绝不判死，无论它排在第几位。

    这是 2026-09-13 审查实测出的真实误杀（3/8 场景违背语义）。成因：
    error_msg 只留**末节点**文案，而"需重新取流/结构不支持/直链块404"这类
    **非画质的确定性失败**既不会让 any_retriable 变 True、也不计入
    quality_rejected_nodes，于是画质失败恰好落在末位时前两道闸门双双失守。
    """
    msg = "分辨率 640x360 低于红线 1080"
    # 画质在末位（原漏洞触发顺序）
    assert d.is_quality_dead(False, msg, [other_node, _QUALITY_NODE]) is False, label
    # 画质在首位
    assert d.is_quality_dead(False, msg, [_QUALITY_NODE, other_node]) is False, label


def test_quality_dead_without_node_failures_falls_back_to_two_conditions():
    """无逐节点信息（旧记录 / 异常抛在节点循环之外）→ 退回两条件口径。

    不是放水：那些路径本就没有"多节点"语义，单节点集结论与三条件版一致。
    """
    msg = "分辨率 640x360 低于红线 1080"
    assert d.is_quality_dead(False, msg, None) is True
    assert d.is_quality_dead(False, msg, []) is True


def test_refetch_node_plus_quality_node_is_not_dead_end_to_end(quality_env,
                                                               monkeypatch):
    """🔒 端到端红线：节点1 直链失效 + 节点2 画质不达标 → **不判死**。

    走真实的 process_one_entry，验证的是"整条链路"而不只是 is_quality_dead
    的单元行为。这个组合正是审查实测出的误杀场景：两个节点都是确定性失败
    （any_retriable 保持 False），末节点文案又是画质的，第 3 道闸门是唯一防线。

    业务含义：那个直链失效的节点换条新 url 完全可能是 1080p，凭什么替它
    断定"整集画质不达标"。
    """
    calls = []

    def fake_master(url, *a, **kw):
        calls.append(url)
        if len(calls) == 1:
            raise RuntimeError("直链签名已过期，需重新取流")
        raise d.QualityRejectedError("分辨率 640x360 低于红线 1080")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert ok is False
    # 两个节点都是确定性失败，整集 retriable 确实是 False
    assert info["retriable"] is False
    # 但**不是**画质判死：有节点从未给出画质结论
    assert d.is_quality_dead(
        info["retriable"], info["error"], info.get("node_failures")
    ) is False
    # 且仍会进重取桶——这才是这集真正的出路
    assert info.get("needs_refetch") is True


def test_quality_kill_dead_log_skips_when_a_node_lacks_verdict(quality_env,
                                                                monkeypatch):
    """🔒 端到端：上述场景**不能**落进 download_dead.jsonl（否则永久丢集）。"""
    line = {"tmdbId": "70", "season": 1, "episode": 1,
            "urls": _quality_entry(2)["urls"]}
    input_path = quality_env / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(quality_env / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(quality_env / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    calls = []

    def fake_master(url, *a, **kw):
        calls.append(url)
        if len(calls) == 1:
            raise RuntimeError("直链签名已过期，需重新取流")
        raise d.QualityRejectedError("分辨率 640x360 低于红线 1080")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    d._run_pipeline()
    assert d.load_dead_keys() == set()     # 账本必须是空的


def test_stream_outage_is_never_quality_dead():
    """🔒 红线：「候选流无一入选」绝不能被当成画质判死。

    那是内层的**兜底汇总文案**，语义混装——既可能是"全部流真不达标"，
    也可能是"源站 5xx 导致采样全挂"。TV 侧源站 5xx 频发，后者是常态。

    此前它被列在 _QUALITY_REJECT_CATEGORIES 里，只是靠 _classify_failure
    判它可重试才没出事——那是**巧合性**的安全。现在内层已用
    QualityRejectedError 把两条路径拆开，该文案只代表"混合/纯瞬时"。

    ⚠️ 若哪天有人把它加回画质类目或判死表，源站抽风的集会被永久判死。
    本用例就是那道护栏。
    """
    outage = "本轮候选流无一入选（各流原因见上方日志），下一轮重采"
    # ① 不在画质判死类目里
    assert d.classify_reject_reason(outage) == "候选流无一入选"
    assert "候选流无一入选" not in d._QUALITY_REJECT_CATEGORIES
    # ② 不在整集判死表里 → 仍可重试
    assert d._classify_failure(outage) is True
    # ③ 即便强行传 retriable=False，也不该算画质判死
    assert d.is_quality_dead(False, outage) is False


def test_quality_summary_takes_precedence_over_outage_category():
    """🔒 汇总判死文案里嵌着末节点错误，归类必须优先命中"画质整体不达标"。

    外层概率判死的文案形如「…判定整集画质不达标；末节点错误：本轮候选流无一入选」。
    若"候选流无一入选"规则排在前面，判死集会全被记到那个类目下，
    正好污染要用来评估本口径是否过激的那份数据。
    """
    msg = (
        "2 个节点中 2 个因画质不达标被确定性淘汰（≥ 阈值 1.00），"
        "判定整集画质不达标；末节点错误：本轮候选流无一入选"
    )
    assert d.classify_reject_reason(msg) == "画质整体不达标(判死)"
    assert d._classify_failure(msg) is False      # 确定性失败
    assert d.is_quality_dead(False, msg) is True


# ------------------------------------------- 画质判死：内层分流 + 外层概率判死
def _quality_entry(node_count):
    """构造 node_count 个 m3u8 节点的 entry。"""
    return {
        "tmdbId": "70", "season": 1, "episode": 1,
        "urls": [
            {"url": f"https://s{i}/p.m3u8", "provider": f"p{i}",
             "type": "m3u8", "headers": {}, "quality": None, "size": None}
            for i in range(1, node_count + 1)
        ],
    }


@pytest.fixture
def quality_env(sandbox, monkeypatch):
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    return sandbox


def test_all_nodes_quality_rejected_kills_episode(quality_env, monkeypatch):
    """全部节点画质淘汰 → 概率判死：retriable=False，不再进下一轮。"""
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("分辨率 640x360 低于红线 1080（容差 0.80），跳过")
        ),
    )
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert ok is False
    assert info["retriable"] is False                    # 判死
    assert "判定整集画质不达标" in info["error"]
    assert d.is_quality_dead(info["retriable"], info["error"]) is True


def test_partial_quality_rejection_does_not_kill_at_ratio_1(quality_env,
                                                            monkeypatch):
    """🔒 阈值 1.0 下，只有部分节点画质淘汰**绝不能**判死。

    这正是 TV 侧取 1.0 的意义：另一个节点是源站 5xx（今天挂了、明天可能好），
    整集必须留在重投队列里。
    """
    calls = []

    def fake_master(url, *a, **kw):
        calls.append(url)
        if len(calls) == 1:
            raise d.QualityRejectedError("分辨率 640x360 低于红线 1080")
        raise RuntimeError("HTTP Error 502")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert ok is False
    assert info["retriable"] is True                     # 仍可重试
    assert "判定整集画质不达标" not in info["error"]
    assert d.is_quality_dead(info["retriable"], info["error"]) is False


def test_single_node_quality_rejection_kills_at_ratio_1(quality_env, monkeypatch):
    """单节点集画质淘汰：1/1 = 1.0 >= 1.0 → 判死。

    ⚠️ 这说明阈值 1.0 对**单节点集**仍等价于"一次判死"——TV 侧单节点集占多数，
    这是该阈值下唯一激进的场景，调阈值前必须知道这一点。
    但它是正确的：只有一个节点且该节点画质确定性不达标，确实没有别的指望。
    """
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("码率未达到门槛：300 kbps < 1480 kbps")
        ),
    )
    label, ok, info = d.process_one_entry(_quality_entry(1), set())
    assert info["retriable"] is False


def test_transient_failures_never_trigger_quality_kill(quality_env, monkeypatch):
    """🔒 红线：纯瞬时失败（源站 5xx）绝不触发画质判死。"""
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("HTTP Error 502")),
    )
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert info["retriable"] is True
    assert "判定整集画质不达标" not in info["error"]


def test_quality_kill_ratio_above_one_only_disables_the_summary(quality_env,
                                                                monkeypatch):
    """>1.0 关掉的是**汇总改写**，不是"画质失败变可重试"。

    ⚠️ 这里有个容易误解的点（写测试时踩过）：画质文案本身
    （"低于红线"/"码率未达到"）**已经在 `_PERMANENT_FAILURE_MARKERS` 里**，
    所以全节点画质淘汰时 `any_retriable` 本来就是 False —— 那是单节点文案
    决定的，与概率判死无关。

    概率判死真正做的两件事是：
      ① 把 msg 换成汇总文案（否则留的是末节点错误，会出现
         「retriable=False 却写着 502」这类自相矛盾的记录）；
      ② 在**混合场景**下推翻乐观口径（部分画质+部分瞬时时才有区别）。
    故调高阈值只是不再改写文案，判死与否仍由文案决定。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.5)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("分辨率 640x360 低于红线 1080")
        ),
    )
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    # 仍是确定性失败（文案决定），但**没有**汇总改写
    assert info["retriable"] is False
    assert "判定整集画质不达标" not in info["error"]


def test_quality_kill_records_to_dead_log(quality_env, monkeypatch):
    """端到端：概率判死的集要落进 download_dead.jsonl，下次运行直接跳过。"""
    line = {"tmdbId": "70", "season": 1, "episode": 1,
            "urls": _quality_entry(2)["urls"]}
    input_path = quality_env / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(quality_env / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(quality_env / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("分辨率 640x360 低于红线 1080")
        ),
    )
    d._run_pipeline()
    assert d.load_dead_keys() == {"70_S01E01"}


def test_inner_all_streams_quality_rejected_raises_typed_error(sandbox, monkeypatch):
    """🔒 内层分流：全流画质淘汰 → QualityRejectedError（确定性）。

    与"混合/纯瞬时"必须产生**不同的异常类型与文案**，否则源站抽风会被
    误判成画质不达标。
    """
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 0.8)
    # 两条候选流都声明低于红线 → 候选预筛阶段就全被排除
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: [("640x360", "https://a/1.m3u8", 100),
                          ("854x480", "https://a/2.m3u8", 200)],
    )
    node = {"url": "https://a/m.m3u8", "provider": "vidup", "type": "m3u8",
            "headers": {}, "quality": None, "size": None}
    entry = {"tmdbId": "71", "season": 1, "episode": 1, "urls": [node]}
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    # 候选预筛的"没有找到高度达标"本身就是 QualityRejectedError
    assert info["retriable"] is False
    assert d.is_quality_dead(info["retriable"], info["error"]) is True


def _inner_loop_env(monkeypatch, stream_outcomes, node_count=1):
    """驱动**内层候选流循环**的公共桩。

    上面那批画质判死用例都是在 parse_master_playlist 上直接抛异常，内层
    「全流画质淘汰 vs 混合失败」的分流代码其实一行都没跑到（反向验证实测：
    把 `quality_rejected_streams > 0 and other_failed_streams == 0` 改成 `or`，
    整个测试文件依旧全绿）。这里让流真的走完采样→红线→码率，才测得到分流。

    stream_outcomes: [("quality" | "transient"), ...]，按候选流顺序生效。
      quality   —— 采样码率远低于门槛 → 内层抛 QualityRejectedError
      transient —— 采样下载抛 502 → 内层记 other_failed_streams
    """
    variants = [
        ("1920x1080", f"https://cdn/v{i}.m3u8", 5000 - i)
        for i in range(len(stream_outcomes))
    ]
    outcome_of = {v[1]: o for v, o in zip(variants, stream_outcomes)}
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(
        d, "parse_master_playlist", lambda *a, **kw: list(variants)
    )

    current = {"url": None}

    def fake_media(url, headers=None):
        current["url"] = url
        return seg_urls, [4.0] * 20, None

    monkeypatch.setattr(d, "parse_media_playlist", fake_media)

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if outcome_of[current["url"]] == "transient":
            raise RuntimeError("HTTP Error 502")
        with open(out, "wb") as fh:
            fh.write(b"x")
        # 16s 采样 / 100KB → 50 kbps，远低于 1000 门槛 → 确定性画质淘汰
        return 100_000, [], 0

    monkeypatch.setattr(d, "download_segments", fake_download)

    return {
        "tmdbId": "72", "season": 1, "episode": 1,
        "urls": [
            {"url": f"https://n{i}/m.m3u8", "provider": f"p{i}", "type": "m3u8",
             "headers": {}, "quality": None, "size": None}
            for i in range(1, node_count + 1)
        ],
    }


def test_inner_all_streams_bitrate_rejected_kills_episode(sandbox, monkeypatch):
    """🔒 内层全流因**码率**淘汰 → 抛 QualityRejectedError → 外层计入判死。

    这是走完整条链路（采样→红线→码率→内层分流→外层概率判死）的用例。
    断言 "判定整集画质不达标" 还兼做类型护栏：内层若退回普通 RuntimeError，
    外层的 isinstance 计数就不会加，汇总文案随之消失。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    entry = _inner_loop_env(monkeypatch, ["quality", "quality"], node_count=1)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "均因画质不达标被淘汰" in info["error"]     # 内层汇总（确定性）
    assert "判定整集画质不达标" in info["error"]       # 外层判死（依赖异常类型）
    assert info["retriable"] is False
    assert d.is_quality_dead(info["retriable"], info["error"]) is True


def test_inner_mixed_failure_never_becomes_quality_kill(sandbox, monkeypatch):
    """🔒 红线：一条流画质淘汰 + 一条流 502 → **绝不能**判死。

    TV 侧源站 5xx 频发，这条混合路径是常态。把它误判成画质不达标等于
    每次源站抽风就永久丢一集，是本功能最危险的失效模式。
    （反向验证靶子：内层分流条件从 and 改成 or，本用例必须转红。）
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    entry = _inner_loop_env(monkeypatch, ["quality", "transient"], node_count=1)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "候选流无一入选" in info["error"]           # 走可重试汇总
    assert "均因画质不达标被淘汰" not in info["error"]
    assert "判定整集画质不达标" not in info["error"]
    assert info["retriable"] is True                   # 留在重投队列
    assert d.is_quality_dead(info["retriable"], info["error"]) is False


def test_inner_all_streams_transient_never_becomes_quality_kill(sandbox,
                                                                 monkeypatch):
    """🔒 红线：全部流都是 502（源站整体抽风）→ 可重试，不判死。"""
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    entry = _inner_loop_env(monkeypatch, ["transient", "transient"], node_count=1)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "候选流无一入选" in info["error"]
    assert info["retriable"] is True
    assert d.is_quality_dead(info["retriable"], info["error"]) is False


def test_permanent_markers_cover_quality_summary_after_reload():
    """🔒 落盘后只剩字符串：汇总判死文案必须能被 _classify_failure 认出。

    failed.jsonl 重载时拿不到异常类型，只有文案。类型与文案互为双保险，
    缺了文案这一层，重载后的判死记录会被当成可重试。
    """
    inner = "全部 3 条候选流均因画质不达标被淘汰（各流原因见上方日志）"
    outer = "2 个节点中 2 个因画质不达标被确定性淘汰（≥ 阈值 1.00），判定整集画质不达标"
    assert d._classify_failure(inner) is False
    assert d._classify_failure(outer) is False


def test_plan_retry_buckets_are_not_mutually_exclusive(monkeypatch):
    """🔑 重投与重取两个桶不互斥。

    多源下"vidup m3u8 挂 5xx + vidlink mp4 签名过期"是常态。若写成互斥分支，
    这类集只会被重投而永远不换新直链，那个 mp4 节点在剩余轮次里都是废的。
    """
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    # 显式标志优先：error 文案里没有 marker，但节点循环发现过期了
    assert d.plan_retry_buckets(True, "HTTP Error 502", True) == (True, True)
    # 缺显式标志时回退按文案判断
    assert d.plan_retry_buckets(
        True, f"x {d._NEEDS_REFETCH_MARKER} y", None
    ) == (True, True)
    assert d.plan_retry_buckets(True, "HTTP Error 502", None) == (True, False)
    # 关掉开关后重取桶恒空
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", False)
    assert d.plan_retry_buckets(True, "x", True) == (True, False)


def test_needs_refetch_flag_survives_non_last_node(sandbox, monkeypatch):
    """🔑 过期节点排在**非末位**时，标志必须仍然为真。

    error 文案只保留最后一个节点的错误。若只按文案判断，
    「节点1 vidlink 403 过期 → 节点2 502」这种常见组合永远读不到过期 marker，
    该直链在剩余所有轮次里都是废的。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    def fake_direct(node, output_path, label, runtime_minutes=None):
        raise RuntimeError(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: x")

    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: (_ for _ in ()).throw(
            RuntimeError("HTTP Error 502")
        ),
    )
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
        {"url": "https://m/x.m3u8", "provider": "vidup", "type": "m3u8"},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    # 末节点是 502，文案里本来没有过期 marker
    assert info["needs_refetch"] is True
    assert info["retriable"] is True
    # 补挂到文案里，落盘的 failed.jsonl 也能看出来
    assert d._NEEDS_REFETCH_MARKER in info["error"]
    assert d.plan_retry_buckets(
        info["retriable"], info["error"], info["needs_refetch"]
    ) == (True, True)


def test_merge_next_batch_dedupes_per_episode(sandbox):
    """🔑 合并两个桶时按**集级 key** 去重，重取后的新 entry 优先。

    按 tmdbId 去重会让一部剧每轮只剩一集能重投 —— 这是 TV 侧最容易踩的坑。
    """
    old_e1 = {"tmdbId": "7", "season": 1, "episode": 1, "urls": ["old"]}
    old_e2 = {"tmdbId": "7", "season": 1, "episode": 2, "urls": ["old"]}
    new_e1 = {"tmdbId": "7", "season": 1, "episode": 1, "urls": ["new"]}

    out = d.merge_next_batch([old_e1, old_e2], [new_e1])
    # 同剧两集都在（没被按 tmdbId 误合成一条）
    assert len(out) == 2
    by_key = {d.record_episode_key(e): e for e in out}
    # E01 取重取后的新 urls，E02 保持原样
    assert by_key["7_S01E01"]["urls"] == ["new"]
    assert by_key["7_S01E02"]["urls"] == ["old"]


def test_startup_precheck_runs_only_in_non_streaming_mode(monkeypatch, sandbox):
    """pipeline（流式）模式刻意不做同步预检。

    它的前提是主循环一步都不阻塞，而 refetch_entries 是同步的、最长堵住
    AUTO_REFETCH_TIMEOUT。此时取流线程已在灌队列，堵住主循环反而会让队列里的
    新鲜直链继续变旧——与预检目的正相反。
    """
    entries = [_entry("2", fetched_at=1)]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entries[0], ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, ids: (
            "x", False, {"error": "没有找到媒体播放列表", "retriable": False}
        ),
    )
    seen = []
    monkeypatch.setattr(
        d, "refresh_stale_entries",
        lambda batch, counts: seen.append(len(batch)) or batch,
    )

    # ① 流式模式：pipeline.py 会把 ListEntrySource 换成自己的工厂
    monkeypatch.setattr(d, "ListEntrySource", lambda es: d._ListEntrySource(es))
    d._run_pipeline()
    assert seen == []            # 预检没被调用

    # ② 非流式（单独跑 download_tv.py）：预检必须执行
    monkeypatch.setattr(d, "ListEntrySource", d._ListEntrySource)
    d._run_pipeline()
    assert seen == [1]


# ------------------------------------------------ 流式来源（pipeline 模式）
def test_list_entry_source_is_three_state():
    """list 来源永不返回 wait —— 它的存货是确定的。"""
    source = d.ListEntrySource([{"tmdbId": "1"}, {"tmdbId": "2"}])
    assert len(source) == 2
    assert source.poll() == ("item", {"tmdbId": "1"})
    assert source.poll() == ("item", {"tmdbId": "2"})
    assert source.poll() == ("done", None)
    # 耗尽后必须稳定返回 done（主循环会重复问）
    assert source.poll() == ("done", None)


def test_missing_input_exits_in_standalone_but_continues_when_streaming(
    sandbox, monkeypatch, capsys,
):
    """results.jsonl 缺失在两种模式下含义完全不同。

    单独跑下载：它是唯一片源，没有就无事可做。
    pipeline 模式：全新部署时它本来就还不存在（TV 侧还要先展开季集才会写出
    第一条），此时若照旧 return，下载侧会在启动瞬间退出、整条流水线只剩取流
    在跑——首次部署必现。
    """
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "nope.jsonl"))
    d._run_pipeline()
    assert "找不到" in capsys.readouterr().out

    # 装上流式来源钩子后，缺文件不再退出，而是等取流实时产出。
    real = d.ListEntrySource
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    class _EmptyStream:
        """立刻收工的流式来源：验的是"缺文件不退出"，不是投递本身。"""

        def __init__(self, entries):
            assert list(entries) == []      # 缺文件 -> 空存量

        def poll(self):
            return "done", None

    monkeypatch.setattr(d, "ListEntrySource", _EmptyStream)
    try:
        d._run_pipeline()
    finally:
        monkeypatch.setattr(d, "ListEntrySource", real)
    assert "等待取流侧实时产出" in capsys.readouterr().out


def test_streaming_source_wait_does_not_block_the_main_loop(sandbox, monkeypatch):
    """来源返回 wait 时主循环必须继续推进在途任务，而不是原地卡住。

    这是 TV 侧季集展开阶段（全新部署约 2.5 小时无产出）的常态：
    poll 一直 wait，此刻 pending 为空，主循环靠 STREAM_IDLE_POLL_SECONDS
    让出 CPU 后重新问来源要货；若写成 wait(空集合) 就是 100% CPU 空转。
    """
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "nope.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    seen = []

    class _SlowStream:
        """前 3 次 wait（模拟展开阶段），然后出一集，再收工。"""

        def __init__(self, entries):
            self.calls = 0

        def poll(self):
            self.calls += 1
            if self.calls <= 3:
                return "wait", None
            if self.calls == 4:
                return "item", {
                    "tmdbId": "1", "season": 1, "episode": 1, "urls": ["u"],
                }
            return "done", None

    monkeypatch.setattr(d, "ListEntrySource", _SlowStream)
    monkeypatch.setattr(d, "process_one_entry", lambda entry, processed: (
        seen.append(d.record_episode_key(entry)),
        (d.record_episode_key(entry), False,
         {"error": "没有找到媒体播放列表", "retriable": False}),
    )[1])

    d._run_pipeline()
    # wait 没有让主循环卡死，后到的那一集仍被消费
    assert seen == ["1_S01E01"]


def test_streaming_new_entry_is_picked_up_while_a_slow_task_is_inflight(
    sandbox, monkeypatch,
):
    """🔑 来源饿着时，新集不能干等在途慢任务完成才被投递。

    「取流侧产出了新集」不是 future 完成事件，无法唤醒 wait(FIRST_COMPLETED)。
    若 wait 无超时，新集就要等某个在途的转封装/上传恰好完成才被顺带发现——
    在途的慢任务恰恰是上传（一集几百 MB 传 R2），最坏几分钟，期间下载槽位全空。

    这构成"假反压"：效果与反压相同却与磁盘压力无关。TV 侧取流是瓶颈，
    "饿着"是常态，损失会被持续放大。

    本例：投出一个 3 秒的在途任务后来源持续 wait，**0.5 秒后**才放出第二集
    （用定时器延后，确保主循环此时已经真正阻塞在 wait 上；若让工作线程立刻
    放行，主线程会在同一次投递循环里就取走它，压根走不到 wait，测不出问题）。
    测量"第二集被投递的时刻"而非总耗时——总耗时必然包含慢任务的 3s。
    """
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "nope.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    released = threading.Event()
    seen = []
    slow_started_at = {}
    fast_started_at = {}

    class _StarvingStream:
        """先出"慢任务"，随后持续 wait；0.5s 后才放出第二集。

        第二集的可见时机与"在途任务完成"完全解耦，正好检验主循环是否靠超时
        轮询主动发现它。
        """

        def __init__(self, entries):
            self.sent_slow = False
            self.sent_fast = False

        def poll(self):
            if not self.sent_slow:
                self.sent_slow = True
                return "item", {
                    "tmdbId": "1", "season": 1, "episode": 1, "urls": ["slow"],
                }
            if not released.is_set():
                return "wait", None       # 来源饿着
            if not self.sent_fast:
                self.sent_fast = True
                return "item", {
                    "tmdbId": "2", "season": 1, "episode": 1, "urls": ["fast"],
                }
            return "done", None

    def fake_process(entry, processed):
        key = d.record_episode_key(entry)
        seen.append(key)
        if entry["urls"] == ["slow"]:
            # 模拟在途慢任务（上传一集大文件）：它完成前，新集不该被它拖着。
            slow_started_at["t"] = time.time()
            time.sleep(3)
        else:
            fast_started_at["t"] = time.time()
        return key, False, {"error": "没有找到媒体播放列表", "retriable": False}

    monkeypatch.setattr(d, "ListEntrySource", _StarvingStream)
    monkeypatch.setattr(d, "process_one_entry", fake_process)

    timer = threading.Timer(0.5, released.set)
    timer.daemon = True
    timer.start()
    try:
        d._run_pipeline()
    finally:
        timer.cancel()

    assert seen == ["1_S01E01", "2_S01E01"]
    # 关键断言：第二集应在它可取之后（慢任务开始 +0.5s）很快被投递，
    # 而不是等慢任务跑完 3s。无超时的旧实现必然 ≈3s。
    lag = fast_started_at["t"] - slow_started_at["t"]
    assert lag < 1.5, (
        f"新集在慢任务开始 {lag:.1f}s 后才被投递，"
        f"说明 wait 没有按 source_waiting 加超时"
    )


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


# --------------------------------------------- 画质判死账本（download_dead.jsonl）
def test_parse_dead_quality_evidence_extracts_numbers():
    """从判死文案回抽码率/门槛/分辨率/编码，供门槛变更后离线复判。"""
    msg = "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps"
    evidence = d.parse_dead_quality_evidence(msg)
    assert evidence == {
        "bitrate_kbps": 372.0,
        "threshold_kbps": 1600.0,
        "resolution": "854x480",
        "codec": "h264",
    }


@pytest.mark.parametrize("msg", [
    "",
    None,
    "分辨率 640x360 低于红线 1080（容差 0.80），跳过",   # 无码率数值
    "候选流无一入选",
])
def test_parse_dead_quality_evidence_returns_none_without_bitrate(msg):
    """没有码率数值就没有可存的依据，--retry-dead 对这些只能整集放回。"""
    assert d.parse_dead_quality_evidence(msg) is None


def test_record_and_load_dead_keys_roundtrip(sandbox):
    """写进账本的集，下次启动会被 load_dead_keys 认出来。"""
    entry = {"tmdbId": "7", "season": 1, "episode": 2, "title": "T"}
    msg = "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps"
    d.record_quality_dead(entry, msg, urls=["u1", "u2"])

    records = _read_jsonl(sandbox / "download_dead.jsonl")
    assert len(records) == 1
    assert records[0]["tmdbId"] == "7"
    assert records[0]["reason_class"] == "quality"
    assert records[0]["node_count"] == 2
    assert records[0]["evidence"]["bitrate_kbps"] == 372.0

    assert d.load_dead_keys() == {"7_S01E02"}


def test_load_dead_keys_uses_conjunction_not_row_order(sandbox):
    """🔒 同一集多行时用**合取**裁决，结论不随追加顺序漂移。

    账本是纯追加的，同一集会有多行。若逐行 add/discard，最终结论取决于哪一行
    排在最后——这里放一条"有依据且仍不达标"和一条"无依据"，无论顺序如何，
    只要有任一条证明仍不达标就必须继续跳过。
    """
    # 第一条有依据且远低于门槛；第二条无依据（单看它会被放回）。
    d.record_quality_dead(
        {"tmdbId": "9", "season": 1, "episode": 1},
        "分辨率 854x480 流（h264）码率未达到门槛：100 kbps < 1600 kbps",
    )
    d.record_quality_dead(
        {"tmdbId": "9", "season": 1, "episode": 1},
        "候选流无一入选",          # parse 不出依据
    )
    # 正常运行：全部判死集一律跳过。
    assert d.load_dead_keys() == {"9_S01E01"}
    # --retry-dead：合取语义下，那条"仍不达标"的记录把它按住。
    assert d.load_dead_keys(d.dead_record_passes_now) == {"9_S01E01"}


def test_dead_record_passes_now_respects_current_threshold(monkeypatch):
    """门槛调松到实测码率之下（且留足余量）时才放回。"""
    monkeypatch.setattr(d, "DEAD_REVIVE_MARGIN", 1.05)
    record = {"evidence": {"bitrate_kbps": 1000.0, "resolution": "1920x1080",
                           "codec": "h264"}}
    # 门槛 2000 → 远高于实测 1000，仍不达标。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 2000.0)
    assert d.dead_record_passes_now(record) is False
    # 门槛降到 900：900 × 1.05 = 945 <= 1000 → 放回。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 900.0)
    assert d.dead_record_passes_now(record) is True
    # 门槛 960：960 × 1.05 = 1008 > 1000 → 余量不足，不放回（防来回震荡）。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 960.0)
    assert d.dead_record_passes_now(record) is False


def test_dead_record_passes_now_revives_when_no_evidence():
    """没有依据就无法证明它现在仍不达标 → 放回（宁可多下不误杀）。"""
    assert d.dead_record_passes_now({"evidence": {}}) is True
    assert d.dead_record_passes_now({}) is True


def test_dead_record_passes_now_blocks_missing_height(monkeypatch):
    """🔒 height 抽不出来时一律不放回。

    bitrate_threshold 按 (h/1080)² 缩放，height=0 会让门槛恒为 0，
    `0 × 余量 <= 任何码率` 恒真 —— 这批记录会被无条件放回去白跑。
    """
    monkeypatch.setattr(d, "DEAD_REVIVE_MARGIN", 1.05)
    record = {"evidence": {"bitrate_kbps": 50.0, "resolution": "未知分辨率"}}
    assert d.dead_record_passes_now(record) is False


def test_quality_dead_is_recorded_and_skipped_next_run(sandbox, monkeypatch):
    """端到端：画质判死的集落账本，下一次运行不再被投递。"""
    line = {"tmdbId": "5", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(d.record_episode_key(entry))
        return "5_S01E01", False, {
            "error": "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps",
            "retriable": False,
        }

    monkeypatch.setattr(d, "process_one_entry", fake_process)

    d._run_pipeline()
    assert seen == ["5_S01E01"]                 # 第一轮确实跑了
    assert d.load_dead_keys() == {"5_S01E01"}   # 判死已落账本

    # 第二次运行：同样的输入，这次应被跳过（不再重新采样求证同一结论）。
    seen.clear()
    d._run_pipeline()
    assert seen == []


def test_transient_failure_is_not_recorded_as_dead(sandbox, monkeypatch):
    """🔒 红线：瞬时失败绝不能进判死账本。

    源站 5xx / 超时是"今天源站挂了"，重取完全可能换到好流；
    把它们当画质判死会永久放弃可救回的集，直接违背"尽可能提高成功率"。
    """
    line = {"tmdbId": "6", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, processed: ("6_S01E01", False,
                                  {"error": "HTTP Error 502", "retriable": True}),
    )
    d._run_pipeline()
    assert d.load_dead_keys() == set()


# ------------------------------------------------ failed.jsonl 轮转
def test_compact_failed_log_keeps_useful_rows(sandbox, monkeypatch):
    """轮转后：历史整体归档，两类仍有用的行原样回填。"""
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 1)   # 任何非空文件都超限
    monkeypatch.setattr(d, "_SCRIPT_DIR", sandbox)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: {"3_S01E01"})

    rows = [
        # ① 待重取：必须回填，否则重取闭环断裂
        {"tmdbId": "1", "season": 1, "episode": 1, "stage": "download",
         "error": "直链失效，需重新取流", "retriable": False},
        # ② 画质判死：必须回填，load_quality_dead_keys 要读
        {"tmdbId": "2", "season": 1, "episode": 1, "stage": "download",
         "error": "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps",
         "retriable": False},
        # ③ 已成功的集：不必再留
        {"tmdbId": "3", "season": 1, "episode": 1, "stage": "download",
         "error": "需重新取流", "retriable": False},
        # ④ 普通瞬时失败：不必留（下次运行本来就会自动重投）
        {"tmdbId": "4", "season": 1, "episode": 1, "stage": "download",
         "error": "HTTP Error 502", "retriable": True},
        # ⑤ 上传阶段失败：与重取/画质都无关
        {"tmdbId": "5", "season": 1, "episode": 1, "stage": "upload",
         "error": "R2 timeout", "retriable": True},
    ]
    with open(sandbox / "failed.jsonl", "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    rotated, archive_path, kept = d.compact_failed_log()
    assert rotated is True and kept == 2
    # 历史零丢失：归档里是完整的 5 行
    assert len(_read_jsonl(archive_path)) == 5
    # 新文件只剩 ①②
    remaining = {r["tmdbId"] for r in _read_jsonl(sandbox / "failed.jsonl")}
    assert remaining == {"1", "2"}


def test_compact_keeps_non_dead_quality_row_via_refetch_marker(sandbox,
                                                                monkeypatch):
    """🔒 轮转口径与判死口径一致：有节点没给出画质结论的行**不是**判死行。

    它靠"需重新取流"这条规则回填（那才是这集真正的出路）。若轮转时忽略
    node_failures，它会被当成判死行回填——回填结果碰巧一样，但 reason 全错，
    且下游 load_quality_dead_keys 会据此永久跳过它。
    """
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 1)
    monkeypatch.setattr(d, "_SCRIPT_DIR", sandbox)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())

    row = {
        "tmdbId": "7", "season": 1, "episode": 1, "stage": "download",
        # 末节点文案是画质的，但节点1 从未给出画质结论
        "error": "分辨率 640x360 低于红线 1080",
        "retriable": False,
        "node_failures": [
            {"reason_class": "源站5xx"},
            {"reason_class": "分辨率低于红线"},
        ],
    }
    with open(sandbox / "failed.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    d.compact_failed_log()
    # 不被判死：重载时不进画质判死集合
    monkeypatch.setattr(d, "FAILED_LOG", str(sandbox / "failed.jsonl"))
    monkeypatch.setattr(d, "AUTO_REFETCH_SKIP_QUALITY_DEAD", True)
    assert d.load_quality_dead_keys() == set()


def test_failed_row_useful_honors_node_failures(sandbox, monkeypatch):
    """🔒 轮转口径必须与判死口径一致（`_failed_row_still_useful`）。

    一行"末节点画质不达标、但另有节点只报 5xx"的记录**不是**判死行：
    它整集仍可重试，下次运行本来就会自动重投，无须占用回填名额。
    忽略 node_failures 的话它会被误当判死行长期回填，且下游
    load_quality_dead_keys 会据此永久跳过它。
    """
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    row = {
        "tmdbId": "7", "season": 1, "episode": 1, "stage": "download",
        "error": "分辨率 640x360 低于红线 1080",   # 末节点文案是画质的
        "retriable": False,
        "node_failures": [
            {"reason_class": "源站5xx"},          # 从未给出画质结论
            {"reason_class": "分辨率低于红线"},
        ],
    }
    assert d._failed_row_still_useful(row, set(), set()) is False

    # 对照组：全部节点都是画质结论 → 真判死行，必须回填
    dead_row = dict(row, node_failures=[
        {"reason_class": "分辨率低于红线"},
        {"reason_class": "码率未达门槛"},
    ])
    assert d._failed_row_still_useful(dead_row, set(), set()) is True


def test_compact_failed_log_skips_when_another_process_runs(sandbox, monkeypatch):
    """🔒 跨进程守卫：别的 downloader 在跑时绝不轮转。

    否则那个进程的 fd 指向旧 inode，os.replace 之后它写的每一条都进了
    archive、新文件里没有 —— 那批待重取记录就此蒸发。
    """
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 1)
    monkeypatch.setattr(d, "_SCRIPT_DIR", sandbox)
    monkeypatch.setattr(d, "is_main_running", lambda: True)
    (sandbox / "failed.jsonl").write_text('{"tmdbId":"1"}\n', encoding="utf-8")

    assert d.compact_failed_log() == (False, None, 0)
    # 原文件原封不动
    assert (sandbox / "failed.jsonl").read_text(encoding="utf-8")


def test_compact_failed_log_disabled_by_zero(sandbox, monkeypatch):
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 0)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    (sandbox / "failed.jsonl").write_text('{"tmdbId":"1"}\n', encoding="utf-8")
    assert d.compact_failed_log() == (False, None, 0)


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
        (str(ok_file), "tv/2001/1/S01/E01/E01.mp4"),
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
