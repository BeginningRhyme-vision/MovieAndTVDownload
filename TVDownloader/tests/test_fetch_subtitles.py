"""Offline tests for fetch_subtitles.py (no network)."""

import io
import json
import zipfile

import pytest

import fetch_subtitles as fs


def _zip(files):
    """Build an in-memory zip from {name: bytes}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buf.getvalue()


class _Resp:
    def __init__(self, payload=None, content=b""):
        self._payload = payload
        self.content = content

    def json(self):
        return self._payload


# ------------------------------------------------------------------ key helpers
@pytest.mark.parametrize("value,expected", [
    (3, 3), ("03", 3), (" 7 ", 7), ("-1", -1), (0, 0),
    (True, None), (False, None), (None, None), ("", None), ("1.5", None), ([1], None),
])
def test_parse_int(value, expected):
    assert fs.parse_int(value) == expected


@pytest.mark.parametrize("tid,s,e,expected", [
    ("346", 1, 3, "346_S01E03"),
    (346, "0", "12", "346_S00E12"),
    ("346", 10, 105, "346_S10E105"),
    ("", 1, 1, ""), ("346", None, 1, ""), ("346", 1, -1, ""), ("346", True, 1, ""),
])
def test_episode_key(tid, s, e, expected):
    assert fs.episode_key(tid, s, e) == expected


def test_parse_int_matches_download_tv():
    import download_tv as d
    for v in (3, "03", " 7 ", "-1", True, None, "", "1.5", "abc", 2.0):
        assert fs.parse_int(v) == d.parse_int(v), v


# --------------------------------------------------------------- load_entries
def test_load_entries_dedup_and_folder(tmp_path, monkeypatch):
    log = tmp_path / "success.jsonl"
    lines = [
        {"tmdbId": "346", "season": 1, "episode": 3, "title": "A",
         "final_path": "/x/downloads/tv_000002/346_S01E03.mp4"},
        "not json",
        {"tmdbId": "346", "season": "1", "episode": "3", "title": "A2"},
        {"tmdbId": "346", "season": 1, "episode": 4, "title": "A",
         "final_path": "/x/downloads/tv_000001/346_S01E04.mp4"},
        {"tmdbId": "999", "title": "movie-style"},
        {"tmdbId": "", "season": 1, "episode": 1},
        {"tmdbId": "500", "season": 0, "episode": 1, "title": "special",
         "final_path": "/x/downloads/tv_000001/500_S00E01.mp4"},
    ]
    with open(log, "w", encoding="utf-8") as fh:
        for item in lines:
            fh.write((item if isinstance(item, str) else json.dumps(item)) + "\n")
    monkeypatch.setattr(fs, "SUCCESS_LOG", str(log))

    entries = {e["key"]: e for e in fs.load_entries()}
    assert set(entries) == {"346_S01E03", "346_S01E04", "500_S00E01"}
    # later record wins; missing final_path falls back to DEFAULT_FOLDER
    assert entries["346_S01E03"]["title"] == "A2"
    assert entries["346_S01E03"]["folder"] == fs.DEFAULT_FOLDER
    assert entries["346_S01E04"]["folder"] == "tv_000001"
    assert entries["346_S01E04"]["season"] == 1
    assert entries["346_S01E04"]["episode"] == 4
    assert entries["500_S00E01"]["season"] == 0


def test_load_entries_missing_log(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "SUCCESS_LOG", str(tmp_path / "nope.jsonl"))
    with pytest.raises(SystemExit):
        fs.load_entries()


# ------------------------------------------------------------- request_with_retry
def test_request_with_retry_retries_then_raises(monkeypatch):
    calls = []
    sleeps = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise RuntimeError("boom")

    monkeypatch.setattr(fs.requests, "request", fake_request)
    monkeypatch.setattr(fs.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(fs, "RETRY_MAX", 3)
    monkeypatch.setattr(fs, "RETRY_DELAY", 2)
    with pytest.raises(RuntimeError):
        fs.request_with_retry("GET", "u")
    assert len(calls) == 3
    assert sleeps == [2, 4]


def test_request_with_retry_success_after_failure(monkeypatch):
    attempts = {"n": 0}

    class Ok:
        def raise_for_status(self):
            pass

    def fake_request(method, url, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("first")
        return Ok()

    monkeypatch.setattr(fs.requests, "request", fake_request)
    monkeypatch.setattr(fs.time, "sleep", lambda s: None)
    assert isinstance(fs.request_with_retry("GET", "u"), Ok)
    assert attempts["n"] == 2


# ------------------------------------------------------------- search_subtitles
def test_search_subtitles_params_and_status(monkeypatch):
    seen = {}

    def fake(method, url, **kwargs):
        seen.update(kwargs["params"])
        return _Resp({"status": True, "subtitles": [{"url": "/s.zip"}]})

    monkeypatch.setattr(fs, "request_with_retry", fake)
    assert fs.search_subtitles("346", 1, 3) == [{"url": "/s.zip"}]
    assert seen["type"] == "tv"
    assert seen["tmdb_id"] == "346"
    assert seen["season_number"] == 1
    assert seen["episode_number"] == 3
    assert set(seen["languages"].split(",")) == set(fs.LANGUAGES)

    monkeypatch.setattr(
        fs, "request_with_retry",
        lambda *a, **k: _Resp({"status": False, "error": "bad key"}),
    )
    with pytest.raises(RuntimeError, match="bad key"):
        fs.search_subtitles("346", 1, 3)

    monkeypatch.setattr(fs, "request_with_retry", lambda *a, **k: _Resp({"status": True}))
    assert fs.search_subtitles("346", 1, 3) == []


# -------------------------------------------------------------------- pick_best
def test_pick_best_filters():
    subs = [
        {"language": "en", "season": 1, "episode": 3, "url": "/wrong-lang"},
        {"language": "ZH", "season": 1, "episode": 3, "full_season": True, "url": "/full"},
        {"language": "ZH", "season": 2, "episode": 3, "url": "/wrong-season"},
        {"language": "ZH", "season": 1, "episode": 4, "url": "/wrong-episode"},
        {"language": "ZH", "season": 1, "episode": 3, "url": ""},
        {"language": "ZH", "season": "1", "episode": "3", "url": "/good"},
        {"language": "ZH", "season": 1, "episode": 3, "url": "/later"},
    ]
    assert fs.pick_best(subs, "ZH", 1, 3)["url"] == "/good"
    assert fs.pick_best(subs, "EN", 1, 3)["url"] == "/wrong-lang"
    assert fs.pick_best(subs, "FR", 1, 3) is None


def test_pick_best_trusts_server_when_fields_missing():
    subs = [{"language": "EN", "url": "/x"}]
    assert fs.pick_best(subs, "EN", 1, 3)["url"] == "/x"


def test_pick_best_multi_episode_pack_uses_range():
    pack = {"language": "EN", "season": 1, "episode": 1,
            "episode_from": 1, "episode_end": 5, "url": "/pack"}
    assert fs.pick_best([pack], "EN", 1, 3)["url"] == "/pack"
    assert fs.pick_best([pack], "EN", 1, 1)["url"] == "/pack"
    assert fs.pick_best([pack], "EN", 1, 6) is None
    # single-episode entry preferred when listed first
    single = {"language": "EN", "season": 1, "episode": 3, "url": "/single"}
    assert fs.pick_best([single, pack], "EN", 1, 3)["url"] == "/single"


def test_episode_range():
    assert fs._episode_range({}) is None
    assert fs._episode_range({"episode_from": 3, "episode_end": 3}) is None
    assert fs._episode_range({"episode_from": "1", "episode_end": "4"}) == (1, 4)
    assert fs._episode_range({"episode_from": 4, "episode_end": 1}) == (1, 4)


# ------------------------------------------------------------------ extract_srt
@pytest.mark.parametrize("name", [
    "Show.S01E03.srt", "show.s1e3.srt", "Show 1x03.srt", "Show.S01.E03.srt",
    "Show.S01 E03.srt", "sub/Show.S01E03.WEB.srt",
])
def test_episode_name_pattern_matches(name):
    assert fs._episode_name_pattern(1, 3).search(name)


@pytest.mark.parametrize("name", [
    "Show.S01E13.srt", "Show.S11E03.srt", "Show.S01E030.srt", "Show.S02E03.srt",
    "Show.11x03.srt", "Show.srt", "Show.1080p.srt",
])
def test_episode_name_pattern_rejects(name):
    assert not fs._episode_name_pattern(1, 3).search(name)


def test_extract_srt_single_file_fallback():
    data = _zip({"generic.srt": b"1\n00:00 --> 00:01\nhi\n", "readme.txt": b"x"})
    content, ext = fs.extract_srt(data, 1, 3)
    assert content.startswith(b"1\n")
    assert ext == ".srt"


def test_extract_srt_prefers_largest_without_marker():
    data = _zip({"a.srt": b"short", "b.ass": b"much longer content"})
    content, ext = fs.extract_srt(data)
    assert content == b"much longer content"
    assert ext == ".ass"


def test_extract_srt_picks_matching_episode_in_pack():
    data = _zip({
        "Show.S01E02.srt": b"ep2 content that is longer",
        "Show.S01E03.srt": b"ep3",
        "Show.S01E04.srt": b"ep4 content that is longer",
    })
    content, ext = fs.extract_srt(data, 1, 3, require_match=True)
    assert content == b"ep3"
    assert ext == ".srt"


def test_extract_srt_pack_without_match_returns_none():
    data = _zip({"Show.S01E02.srt": b"ep2", "Show.S01E04.srt": b"ep4"})
    assert fs.extract_srt(data, 1, 3, require_match=True) == (None, None)
    # non-pack: falls back to largest
    content, _ = fs.extract_srt(data, 1, 3, require_match=False)
    assert content in (b"ep2", b"ep4")


def test_extract_srt_no_candidates_and_dirs():
    data = _zip({"readme.txt": b"x", "dir/": b""})
    assert fs.extract_srt(data) == (None, None)


def test_decode_subtitle():
    assert fs.decode_subtitle("你好".encode("utf-8-sig")) == "你好"
    assert fs.decode_subtitle("你好".encode("gb18030")) == "你好"
    assert fs.decode_subtitle(b"caf\xe9") == "café"
    assert isinstance(fs.decode_subtitle(b"\xff\xfe\xfd"), str)


# ----------------------------------------------------------------- download_one
@pytest.fixture
def sub_env(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "SUBTITLE_DIR", str(tmp_path / "subs"))
    monkeypatch.setattr(fs, "STATE_LOG", str(tmp_path / "subtitles.jsonl"))
    monkeypatch.setattr(fs, "LANGUAGES", {"EN": "en", "ZH": "zh"})
    monkeypatch.setattr(fs, "SUBDL_API_KEY", "k")
    return tmp_path


def _entry(season=1, episode=3, folder="tv_000001"):
    return {"key": fs.episode_key("346", season, episode), "tmdbId": "346",
            "season": season, "episode": episode, "title": "T", "folder": folder}


def test_download_one_skips_when_all_present(sub_env, monkeypatch):
    target = sub_env / "subs" / "tv_000001"
    target.mkdir(parents=True)
    (target / "346_S01E03.en.srt").write_text("x")
    (target / "346_S01E03.zh.ass").write_text("x")
    monkeypatch.setattr(fs, "search_subtitles", lambda *a: pytest.fail("should not search"))
    assert fs.download_one(_entry()) == ("346_S01E03", {"status": "skipped"})


def test_download_one_only_fetches_missing_language(sub_env, monkeypatch):
    target = sub_env / "subs" / "tv_000001"
    target.mkdir(parents=True)
    (target / "346_S01E03.en.srt").write_text("x")
    # a sibling episode's file must not count as present
    (target / "346_S01E04.zh.srt").write_text("x")

    monkeypatch.setattr(fs, "search_subtitles", lambda tid, s, e: [
        {"language": "ZH", "season": 1, "episode": 3, "url": "/zh.zip"},
        {"language": "EN", "season": 1, "episode": 3, "url": "/en.zip"},
    ])
    urls = []

    def fake_req(method, url, **kwargs):
        urls.append(url)
        assert kwargs["params"] == {"api_key": "k"}
        return _Resp(content=_zip({"x.srt": "字幕".encode("gb18030")}))

    monkeypatch.setattr(fs, "request_with_retry", fake_req)
    key, result = fs.download_one(_entry())
    assert key == "346_S01E03"
    assert result == {"status": "ok", "saved": ["346_S01E03.zh.srt"], "missing": []}
    assert urls == [fs.DOWNLOAD_BASE + "/zh.zip"]
    assert (target / "346_S01E03.zh.srt").read_text(encoding="utf-8") == "字幕"


def test_download_one_search_failed(sub_env, monkeypatch):
    def boom(*a):
        raise RuntimeError("quota")
    monkeypatch.setattr(fs, "search_subtitles", boom)
    key, result = fs.download_one(_entry())
    assert result == {"status": "search_failed", "error": "quota"}
    assert (sub_env / "subs" / "tv_000001").is_dir()


def test_download_one_missing_and_errors(sub_env, monkeypatch):
    monkeypatch.setattr(fs, "search_subtitles", lambda *a: [
        {"language": "EN", "season": 1, "episode": 3, "url": "/en.zip"},
    ])

    def fake_req(method, url, **kwargs):
        raise RuntimeError("dl fail")

    monkeypatch.setattr(fs, "request_with_retry", fake_req)
    _, result = fs.download_one(_entry())
    assert result["status"] == "ok"
    assert result["saved"] == []
    assert sorted(result["missing"]) == ["EN", "ZH"]
    assert result["errors"] == ["EN: dl fail"]


def test_download_one_pack_without_episode_file_is_missing(sub_env, monkeypatch):
    monkeypatch.setattr(fs, "search_subtitles", lambda *a: [
        {"language": "EN", "season": 1, "episode": 1,
         "episode_from": 1, "episode_end": 5, "url": "/pack.zip"},
    ])
    monkeypatch.setattr(
        fs, "request_with_retry",
        lambda *a, **k: _Resp(content=_zip({"S01E01.srt": b"1", "S01E02.srt": b"2"})),
    )
    _, result = fs.download_one(_entry())
    assert "EN" in result["missing"]
    assert not list((sub_env / "subs" / "tv_000001").glob("*.en.*"))


def test_download_one_pack_extracts_correct_episode(sub_env, monkeypatch):
    monkeypatch.setattr(fs, "LANGUAGES", {"EN": "en"})
    monkeypatch.setattr(fs, "search_subtitles", lambda *a: [
        {"language": "EN", "season": 1, "episode": 1,
         "episode_from": 1, "episode_end": 5, "url": "/pack.zip"},
    ])
    monkeypatch.setattr(
        fs, "request_with_retry",
        lambda *a, **k: _Resp(content=_zip({
            "Show.S01E02.srt": b"two-two-two", "Show.S01E03.srt": b"three",
        })),
    )
    _, result = fs.download_one(_entry())
    assert result["saved"] == ["346_S01E03.en.srt"]
    path = sub_env / "subs" / "tv_000001" / "346_S01E03.en.srt"
    assert path.read_text() == "three"


# ----------------------------------------------------------------------- main
def test_main_writes_state_and_stats(sub_env, monkeypatch, capsys):
    entries = [_entry(1, 1), _entry(1, 2), _entry(1, 3)]
    monkeypatch.setattr(fs, "load_entries", lambda: entries)
    monkeypatch.setattr(fs, "MAX_WORKERS", 2)

    def fake_download(entry):
        if entry["episode"] == 1:
            return entry["key"], {"status": "ok", "saved": ["a", "b"], "missing": []}
        if entry["episode"] == 2:
            return entry["key"], {"status": "skipped"}
        raise RuntimeError("thread crash")

    monkeypatch.setattr(fs, "download_one", fake_download)
    fs.main()

    out = capsys.readouterr().out
    assert "待处理剧集: 3" in out
    assert "字幕文件 2 个" in out
    assert "'ok': 1" in out and "'skipped': 1" in out and "'search_failed': 1" in out

    with open(fs.STATE_LOG, encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
    by_ep = {r["episode"]: r for r in records}
    assert set(by_ep) == {1, 2, 3}
    assert by_ep[1]["season"] == 1 and by_ep[1]["tmdbId"] == "346"
    assert by_ep[3]["status"] == "search_failed" and "thread crash" in by_ep[3]["error"]


def test_main_requires_api_key(monkeypatch):
    monkeypatch.setattr(fs, "SUBDL_API_KEY", "")
    with pytest.raises(SystemExit):
        fs.main()


def test_paths_anchor_to_script_dir():
    assert fs._resolve("x.jsonl", "d") == str(fs._SCRIPT_DIR / "x.jsonl")
    assert fs._resolve("", "d.jsonl") == str(fs._SCRIPT_DIR / "d.jsonl")
    assert fs._resolve(None, "d.jsonl") == str(fs._SCRIPT_DIR / "d.jsonl")
    assert fs._resolve("/abs/p", "d") == "/abs/p"
