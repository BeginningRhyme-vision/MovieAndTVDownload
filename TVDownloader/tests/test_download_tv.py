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
    for marker in d._PERMANENT_FAILURE_MARKERS:
        if marker.startswith("缺少 "):
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

    def fake_master(url, retries=None):
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
        lambda url, retries=None: (_ for _ in ()).throw(RuntimeError("没有找到媒体播放列表")),
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
        lambda url, retries=None: [("1920x1080", "https://cdn/i.m3u8", 5000.0)],
    )
    monkeypatch.setattr(d, "parse_media_playlist", lambda url: (seg_urls, durations, None))
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False):
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
