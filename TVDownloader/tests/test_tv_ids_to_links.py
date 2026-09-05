"""Offline tests for tv_ids_to_links.py (no network, TMDB/vidup calls are faked)."""

import json
from pathlib import Path

import pytest

import tv_ids_to_links as m


# ---------- _is_retriable ----------
@pytest.mark.parametrize("msg, expected", [
    ("All servers failed for 1 S01E01. Last error: HTTP 404", False),
    ("All servers failed for 1 S01E01. Last error: curl: (35) SSL", True),
    ("All servers failed for 1 S01E01. Last error: timed out", True),
    ("Extract failed (retriable) for 1 S01E01", True),
    ("API Error at x: status=500, error=boom", True),
    ("No servers found", True),
    ("HTTP 403 Forbidden", True),
    ("HTTP 502 Bad Gateway", True),
    ("Proxy CONNECT aborted", True),
    ("HTTP 404 Not Found", False),
    ("Missing url or tmdbId in decrypted data", False),
])
def test_is_retriable(msg, expected):
    assert m._is_retriable(Exception(msg)) is expected


def test_is_retriable_matches_movie_version():
    """TV 版 _is_retriable 必须与电影版语义一致（同一源站、同一 enc-dec 服务）。"""
    movie = Path(__file__).resolve().parents[2] / "MovieDownloader" / "tmdb_ids_to_links.py"
    if not movie.exists():
        pytest.skip("MovieDownloader not present")
    src = movie.read_text(encoding="utf-8")
    for marker in ("curl: (35)", "Extract failed (retriable)", "No servers found", "API Error", "All servers failed"):
        assert marker in src


# ---------- helpers ----------
def test_ep_label_zero_pads():
    assert m._ep_label("123", 1, 2) == "123 S01E02"
    assert m._ep_label("123", "10", "100") == "123 S10E100"


def test_resolve_relative_to_script_dir():
    p = m._resolve("foo.txt")
    assert p == Path(m.__file__).with_name("foo.txt")
    assert m._resolve("/tmp/abs.txt") == Path("/tmp/abs.txt")


def test_validate_ok_and_error():
    assert m.validate({"status": 200, "result": {"a": 1}}, "p") == {"a": 1}
    with pytest.raises(Exception, match="API Error at p"):
        m.validate({"status": 500, "error": "x"}, "p")


def test_build_proxy_random_port(monkeypatch):
    monkeypatch.setattr(m, "USE_PROXY", True)
    monkeypatch.setattr(m, "PROXY_PORT_RANGE", (9000, 9000))
    p = m.build_proxy()
    assert p["http"] == p["https"]
    assert p["http"].endswith(":9000")
    monkeypatch.setattr(m, "USE_PROXY", False)
    assert m.build_proxy() is None


# ---------- _tmdb_get ----------
class _Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload or {}
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _fake_session(monkeypatch, responses):
    calls = []

    class S:
        def get(self, url, params=None, timeout=None):
            calls.append((url, dict(params or {})))
            r = responses.pop(0)
            if isinstance(r, Exception):
                raise r
            return r

    monkeypatch.setattr(m, "_tmdb_session", S())
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)
    return calls


def test_tmdb_get_404_raises_not_found(monkeypatch):
    _fake_session(monkeypatch, [_Resp(404)])
    with pytest.raises(m.TmdbNotFound):
        m._tmdb_get("/tv/1")


def test_tmdb_get_429_does_not_consume_attempts(monkeypatch):
    monkeypatch.setattr(m, "TMDB_RETRIES", 1)
    calls = _fake_session(monkeypatch, [
        _Resp(429, headers={"Retry-After": "0"}),
        _Resp(429, headers={"Retry-After": "0"}),
        _Resp(200, {"ok": True}),
    ])
    assert m._tmdb_get("/tv/1") == {"ok": True}
    assert len(calls) == 3
    assert calls[0][1]["api_key"] == m.TMDB_API_KEY


def test_tmdb_get_429_has_upper_bound(monkeypatch):
    monkeypatch.setattr(m, "_TMDB_MAX_429", 2)
    _fake_session(monkeypatch, [_Resp(429)] * 3)
    with pytest.raises(Exception, match="429"):
        m._tmdb_get("/tv/1")


def test_tmdb_get_retries_then_raises_with_reason(monkeypatch):
    monkeypatch.setattr(m, "TMDB_RETRIES", 2)
    calls = _fake_session(monkeypatch, [_Resp(500), _Resp(503)])
    with pytest.raises(Exception, match="HTTP 503"):
        m._tmdb_get("/tv/1")
    assert len(calls) == 2


# ---------- fetch_seasons_from_tmdb ----------
def _season(n, eps):
    return {"episodes": [{"episode_number": e} for e in eps]}


def test_fetch_seasons_basic(monkeypatch):
    info = {
        "name": "Show",
        "first_air_date": "2011-04-17",
        "seasons": [{"season_number": 0}, {"season_number": 2}, {"season_number": 1}],
    }

    def fake_get(path, params=None):
        if params is None:
            return info
        assert params["append_to_response"] == "season/0,season/1,season/2"
        return {"season/0": _season(0, [1]), "season/1": _season(1, [3, 1, 2, 2]), "season/2": _season(2, [])}

    monkeypatch.setattr(m, "_tmdb_get", fake_get)
    monkeypatch.setattr(m, "INCLUDE_SPECIALS", True)
    out = m.fetch_seasons_from_tmdb(99)
    assert out["tmdbId"] == "99"
    assert out["name"] == "Show"
    assert out["year"] == 2011
    # 排序、去重、空季跳过
    assert out["seasons"] == [{"season": 0, "episodes": [1]}, {"season": 1, "episodes": [1, 2, 3]}]


def test_fetch_seasons_excludes_specials(monkeypatch):
    info = {"seasons": [{"season_number": 0}, {"season_number": 1}], "first_air_date": ""}

    def fake_get(path, params=None):
        if params is None:
            return info
        assert "season/0" not in params["append_to_response"]
        return {"season/1": _season(1, [5, 7])}  # 不连续集号以 episode_number 为准

    monkeypatch.setattr(m, "_tmdb_get", fake_get)
    monkeypatch.setattr(m, "INCLUDE_SPECIALS", False)
    out = m.fetch_seasons_from_tmdb("7")
    assert out["year"] is None
    assert out["seasons"] == [{"season": 1, "episodes": [5, 7]}]


def test_fetch_seasons_chunks_over_append_limit(monkeypatch):
    n_seasons = m._TMDB_APPEND_LIMIT + 3
    info = {"seasons": [{"season_number": i} for i in range(1, n_seasons + 1)]}
    batches = []

    def fake_get(path, params=None):
        if params is None:
            return info
        keys = params["append_to_response"].split(",")
        batches.append(len(keys))
        return {k: _season(int(k.split("/")[1]), [1]) for k in keys}

    monkeypatch.setattr(m, "_tmdb_get", fake_get)
    out = m.fetch_seasons_from_tmdb(1)
    assert batches == [m._TMDB_APPEND_LIMIT, 3]
    assert [s["season"] for s in out["seasons"]] == list(range(1, n_seasons + 1))


# ---------- seasons cache / expand_seasons ----------
def test_load_seasons_cache_tolerates_bad_lines(tmp_path):
    f = tmp_path / "c.jsonl"
    f.write_text('{"tmdbId": 1, "seasons": []}\n\nnot json\n{"noid": 1}\n', encoding="utf-8")
    cache = m.load_seasons_cache(f)
    assert list(cache) == ["1"]
    assert m.load_seasons_cache(tmp_path / "missing.jsonl") == {}


def test_expand_seasons_writes_cache_and_marks_dead(tmp_path, monkeypatch):
    cache_file = tmp_path / "cache.jsonl"
    fail_file = tmp_path / "fail.txt"
    cache_file.write_text(json.dumps({"tmdbId": "10", "name": "cached", "seasons": []}) + "\n", encoding="utf-8")

    def fake_fetch(tid):
        if tid == "20":
            return {"tmdbId": "20", "name": "new", "year": 2000, "seasons": [{"season": 1, "episodes": [1, 2]}]}
        if tid == "30":
            raise m.TmdbNotFound("/tv/30")
        raise Exception("transient")

    monkeypatch.setattr(m, "fetch_seasons_from_tmdb", fake_fetch)
    dead = {"50"}
    cache = m.expand_seasons(["10", "20", "30", "40", "50"], cache_file, fail_file, dead)

    assert set(cache) == {"10", "20"}
    assert dead == {"50", "30"}
    assert fail_file.read_text(encoding="utf-8") == "30\t-\t-\n"
    lines = cache_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2 and json.loads(lines[1])["tmdbId"] == "20"


# ---------- load_processed ----------
def test_load_processed_parses_both_formats(tmp_path):
    results = tmp_path / "results.jsonl"
    fail = tmp_path / "fail.txt"
    results.write_text(
        json.dumps({"tmdbId": "1", "season": 1, "episode": 2}) + "\n"
        + "bad json\n"
        + json.dumps({"tmdbId": "1"}) + "\n",
        encoding="utf-8",
    )
    fail.write_text("1\t1\t3\n2\t-\t-\n3\tx\ty\nonly-two\tcols\n\n", encoding="utf-8")
    processed, dead = m.load_processed(results, fail)
    assert processed == {("1", 1, 2), ("1", 1, 3)}
    assert dead == {"2"}


def test_load_processed_missing_files(tmp_path):
    assert m.load_processed(tmp_path / "a", tmp_path / "b") == (set(), set())


# ---------- process_episode (faked vidup + enc-dec) ----------
class _FakeResp:
    def __init__(self, text="", payload=None, status=200):
        self.text = text
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _install_fake_session(monkeypatch, page_html, streams, servers=None, stream_status=200):
    """streams: list of decrypted stream dicts returned in order for each server."""
    seen = {"page_urls": [], "page_headers": None}
    servers = servers if servers is not None else [{"name": f"s{i}", "data": f"d{i}"} for i in range(len(streams))]
    stream_iter = iter(streams)

    class FakeSession:
        def __init__(self, *a, **k):
            self.proxies = None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, timeout=None, headers=None):
            if url.startswith("https://vidup.to/"):
                seen["page_urls"].append(url)
                seen["page_headers"] = headers
                return _FakeResp(text=page_html)
            if "/enc-vidup" in url:
                return _FakeResp(payload={"status": 200, "result": {
                    "servers": "https://x/servers", "stream": "https://x/stream", "token": "tok"}})
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            if url == "https://x/servers":
                return _FakeResp(text="enc-servers")
            if url.startswith("https://x/stream/"):
                return _FakeResp(text="enc-stream", status=stream_status)
            if url.endswith("/dec-vidup"):
                if json["text"] == "enc-servers":
                    return _FakeResp(payload={"status": 200, "result": servers})
                return _FakeResp(payload={"status": 200, "result": next(stream_iter)})
            raise AssertionError(url)

    monkeypatch.setattr(m.requests, "Session", FakeSession)
    monkeypatch.setattr(m, "build_proxy", lambda: None)
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)
    return seen


PAGE = 'window.x = "{\\"en\\":\\"ENC\\"}"'


def test_process_episode_ok_uses_input_key_and_merges_meta(monkeypatch):
    seen = _install_fake_session(monkeypatch, PAGE, [
        {"url": "u1", "tmdbId": "999", "title": "Show Name"},
        {"url": "u1", "tmdbId": "999", "title": "dup"},
        {"url": "u2", "tmdbId": "999"},
    ])
    monkeypatch.setattr(m, "_SERIES_META", {"42": {"year": 2011, "original_title": "Orig"}})
    status, result = m.process_episode("42", 2, 3)
    assert status == "ok"
    # 源站页面路径：/tv/{tid}/{season}/{episode}/
    assert seen["page_urls"] == ["https://vidup.to/tv/42/2/3/"]
    assert "X-Requested-With" not in seen["page_headers"]
    # key 恒用入参，不信任解密返回的 tmdbId
    assert result["tmdbId"] == "42" and result["season"] == 2 and result["episode"] == 3
    assert result["urls"] == ["u1", "u2"]
    assert result["title"] == "Show Name"
    assert result["year"] == 2011 and result["original_title"] == "Orig"


def test_process_episode_dead_on_clean_404(monkeypatch):
    # 每个 server 的 stream 请求都返回 404 → "All servers failed ... HTTP 404" → dead，不重试
    seen = _install_fake_session(monkeypatch, PAGE, [], servers=[{"name": "a", "data": "d"}], stream_status=404)
    status, result = m.process_episode("1", 1, 1)
    assert status == "dead" and result is None
    assert len(seen["page_urls"]) == 1


def test_process_episode_retry_when_stream_5xx(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, PAGE, [], servers=[{"name": "a", "data": "d"}], stream_status=503)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"
    assert len(seen["page_urls"]) == 2


def test_process_episode_retry_when_extract_fails(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, "<html>cloudflare</html>", [])
    status, result = m.process_episode("1", 1, 1)
    assert status == "retry" and result is None
    assert len(seen["page_urls"]) == 2


def test_process_episode_all_servers_404_is_dead(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [{"url": None, "tmdbId": None}] * 2)
    status, _ = m.process_episode("1", 1, 1)
    # 所有 server 都缺 url：非 404 文案 → "All servers failed ... Missing url" 不含 404 → 可重试
    assert status == "retry"


def test_process_episode_token_fallback(monkeypatch):
    page = 'x = "{\\"token\\":\\"TOK\\"}"'
    _install_fake_session(monkeypatch, page, [{"url": "u", "tmdbId": "1"}])
    status, result = m.process_episode("1", 0, 1)
    assert status == "ok" and result["season"] == 0


# ---------- run_batch ----------
def test_run_batch_routes_three_states(tmp_path, monkeypatch):
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"

    def fake(tid, s, e):
        if tid == "ok":
            return "ok", {"urls": ["u"], "tmdbId": tid, "season": s, "episode": e, "title": "t"}
        if tid == "dead":
            return "dead", None
        if tid == "boom":
            raise RuntimeError("x")
        return "retry", None

    monkeypatch.setattr(m, "process_episode", fake)
    items = [("ok", 1, 1), ("dead", 1, 2), ("retry", 1, 3), ("boom", 1, 4)]
    retry = m.run_batch(items, results, fail, max_workers=2)
    assert sorted(retry) == [("boom", 1, 4), ("retry", 1, 3)]
    assert json.loads(results.read_text(encoding="utf-8"))["tmdbId"] == "ok"
    assert fail.read_text(encoding="utf-8") == "dead\t1\t2\n"


# ---------- main ----------
def test_main_expands_and_backfills_year(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n1\n2\n3\n\n", encoding="utf-8")
    results = tmp_path / "results.jsonl"
    fail = tmp_path / "fail.txt"
    cache = tmp_path / "cache.jsonl"
    results.write_text(json.dumps({"tmdbId": "1", "season": 1, "episode": 1}) + "\n", encoding="utf-8")
    fail.write_text("3\t-\t-\n", encoding="utf-8")

    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(results), "fail_file": str(fail),
        "seasons_cache": str(cache), "max_workers": 2, "max_rounds": 2,
    })
    monkeypatch.setattr(m, "expand_seasons", lambda ids, cf, ff, dead: {
        "1": {"tmdbId": "1", "year": 1999, "seasons": [{"season": 1, "episodes": [1, 2]}]},
        "2": {"tmdbId": "2", "year": 2005, "seasons": [{"season": 0, "episodes": [1]}]},
    })
    meta = {"1": {"year": 2011}}
    monkeypatch.setattr(m, "_SERIES_META", meta)
    seen = []

    def fake_batch(pending, rf, ff, mw):
        seen.append(list(pending))
        return [] if len(seen) > 1 else [pending[-1]]

    monkeypatch.setattr(m, "run_batch", fake_batch)
    m.main()

    assert seen[0] == [("1", 1, 2), ("2", 0, 1)]
    assert seen[1] == [("2", 0, 1)]
    assert meta["1"]["year"] == 2011           # tv_series.jsonl 有值不被覆盖
    assert meta["2"]["year"] == 2005           # 缺失时回退 TMDB first_air_date


def test_main_writes_leftover_retries_to_fail(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n", encoding="utf-8")
    fail = tmp_path / "fail.txt"
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(tmp_path / "r.jsonl"), "fail_file": str(fail),
        "seasons_cache": str(tmp_path / "c.jsonl"), "max_rounds": 1,
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a: {"1": {"tmdbId": "1", "seasons": [{"season": 1, "episodes": [1]}]}})
    monkeypatch.setattr(m, "_SERIES_META", {})
    monkeypatch.setattr(m, "run_batch", lambda pending, *a: list(pending))
    m.main()
    assert fail.read_text(encoding="utf-8") == "1\t1\t1\n"
