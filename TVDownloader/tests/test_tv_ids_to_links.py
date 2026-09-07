"""Offline tests for tv_ids_to_links.py (no network, TMDB/vidup calls are faked)."""

import json
from datetime import date
from pathlib import Path

import pytest

import tv_ids_to_links as m


# ---------- _is_retriable：白名单判死，只有 NoSource 才 dead ----------
@pytest.mark.parametrize("exc, expected", [
    (m.NoSource("page 404"), False),
    (m.HttpStatusError(404, "stream"), True),          # 单个 server 404 不等于无源
    (m.HttpStatusError(503, "servers"), True),
    (Exception("All servers failed for 1 S01E01. Last error: HTTP 404"), True),  # 不再靠字符串
    (Exception("curl: (35) SSL"), True),
    (KeyError("servers"), True),
    (ValueError("bad json"), True),
])
def test_is_retriable(exc, expected):
    assert m._is_retriable(exc) is expected


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
    with pytest.raises(Exception, match="unexpected payload type"):
        m.validate(["not", "dict"], "p")


def test_redact_hides_api_key():
    msg = m._redact(Exception("GET https://api.themoviedb.org/3/tv/1?api_key=SECRET123&x=1 failed"))
    assert "SECRET123" not in msg and "api_key=***" in msg
    assert msg.startswith("Exception:")


def test_retry_after_seconds(monkeypatch):
    assert m.TMDB_RETRY_AFTER_MAX == 30.0  # 默认上限 30s
    assert m._retry_after_seconds(_Resp(429, headers={"Retry-After": "7"})) == 7.0
    assert m._retry_after_seconds(_Resp(429, headers={"Retry-After": "9999"})) == 30.0
    assert m._retry_after_seconds(_Resp(429, headers={"Retry-After": "junk"}), default=2.5) == 2.5
    assert m._retry_after_seconds(_Resp(429)) == 2.0
    # 显式 cap 优先；未传 cap 时跟随配置项
    assert m._retry_after_seconds(_Resp(429, headers={"Retry-After": "9999"}), cap=5) == 5.0
    monkeypatch.setattr(m, "TMDB_RETRY_AFTER_MAX", 12.0)
    assert m._retry_after_seconds(_Resp(429, headers={"Retry-After": "9999"})) == 12.0
    # HTTP-date 形式：不崩溃且落在 [0, cap]
    v = m._retry_after_seconds(_Resp(429, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}))
    assert 0 <= v <= 12


@pytest.mark.parametrize("air_date, ended, expected", [
    ("2030-01-01", False, True),
    ("2030-01-01", True, True),          # 已完结但日期在未来（TMDB 偶尔标错）→ 仍视为未播
    ("2020-01-01", False, False),
    ("2026-09-06", False, False),        # 当天播出算已播
    ("2030-01-01T00:00:00", False, True),
    (None, False, True),                 # 在播剧缺日期 → TBA
    ("", False, True),
    (None, True, False),                 # 已完结剧缺日期 → 视为已播
    ("not-a-date", False, False),        # 格式异常不跳过
])
def test_is_unaired(air_date, ended, expected):
    assert m._is_unaired(air_date, date(2026, 9, 6), ended, grace_days=0) is expected


def test_is_unaired_grace_period(monkeypatch):
    today = date(2026, 9, 6)
    # 宽限期 5 天：播出 4 天内视为“源站尚未上架”，跳过不判死
    assert m._is_unaired("2026-09-06", today, False, grace_days=5) is True
    assert m._is_unaired("2026-09-02", today, False, grace_days=5) is True
    assert m._is_unaired("2026-09-01", today, False, grace_days=5) is False
    # 已完结剧同样适用（完结集刚播也要等上架）
    assert m._is_unaired("2026-09-05", today, True, grace_days=5) is True
    # 缺省从配置取
    monkeypatch.setattr(m, "AIR_GRACE_DAYS", 3)
    assert m._is_unaired("2026-09-04", today, False) is True
    assert m._is_unaired("2026-09-03", today, False) is False


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


@pytest.mark.parametrize("status", [401, 403])
def test_tmdb_get_auth_error_fails_fast(monkeypatch, status):
    monkeypatch.setattr(m, "TMDB_RETRIES", 3)
    calls = _fake_session(monkeypatch, [_Resp(status), _Resp(200, {"ok": True})])
    with pytest.raises(m.TmdbAuthError):
        m._tmdb_get("/tv/1")
    assert len(calls) == 1  # 不重试


def test_tmdb_get_error_message_is_redacted(monkeypatch):
    monkeypatch.setattr(m, "TMDB_RETRIES", 1)
    _fake_session(monkeypatch, [Exception("boom url?api_key=SECRET&x")])
    with pytest.raises(Exception) as ei:
        m._tmdb_get("/tv/1")
    assert "SECRET" not in str(ei.value)


# ---------- fetch_seasons_from_tmdb ----------
def _season(n, eps, dates=None):
    dates = dates or {}
    return {"episodes": [{"episode_number": e, "air_date": dates.get(e)} for e in eps]}


def test_fetch_seasons_basic(monkeypatch):
    info = {
        "name": "Show",
        "first_air_date": "2011-04-17",
        "status": "Ended",
        "seasons": [{"season_number": 0}, {"season_number": 2}, {"season_number": 1}],
    }

    def fake_get(path, params=None):
        if params is None:
            return info
        assert params["append_to_response"] == "season/0,season/1,season/2"
        return {
            "season/0": _season(0, [1]),
            "season/1": _season(1, [3, 1, 2, 2], {1: "2011-04-17", 3: "2011-05-01"}),
            "season/2": _season(2, []),
        }

    monkeypatch.setattr(m, "_tmdb_get", fake_get)
    monkeypatch.setattr(m, "INCLUDE_SPECIALS", True)
    out = m.fetch_seasons_from_tmdb(99)
    assert out["tmdbId"] == "99"
    assert out["name"] == "Show"
    assert out["year"] == 2011
    assert out["ended"] is True
    # 排序、去重、空季跳过；air_dates 只收有值的集，key 为字符串（JSON 往返一致）
    assert out["seasons"] == [
        {"season": 0, "episodes": [1], "air_dates": {}},
        {"season": 1, "episodes": [1, 2, 3], "air_dates": {"1": "2011-04-17", "3": "2011-05-01"}},
    ]


def test_fetch_seasons_excludes_specials(monkeypatch):
    info = {"seasons": [{"season_number": 0}, {"season_number": 1}], "first_air_date": "", "status": "Returning Series"}

    def fake_get(path, params=None):
        if params is None:
            return info
        assert "season/0" not in params["append_to_response"]
        return {"season/1": _season(1, [5, 7])}  # 不连续集号以 episode_number 为准

    monkeypatch.setattr(m, "_tmdb_get", fake_get)
    monkeypatch.setattr(m, "INCLUDE_SPECIALS", False)
    out = m.fetch_seasons_from_tmdb("7")
    assert out["year"] is None
    assert out["ended"] is False
    assert out["seasons"] == [{"season": 1, "episodes": [5, 7], "air_dates": {}}]


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


def test_expand_seasons_refresh_ongoing(tmp_path, monkeypatch):
    cache_file = tmp_path / "cache.jsonl"
    fail_file = tmp_path / "fail.txt"
    old = [
        {"tmdbId": "1", "name": "ended", "ended": True, "seasons": [{"season": 1, "episodes": [1]}]},
        {"tmdbId": "2", "name": "ongoing", "ended": False, "seasons": [{"season": 1, "episodes": [1]}]},
        {"tmdbId": "3", "name": "legacy", "seasons": [{"season": 1, "episodes": [1]}]},  # 旧格式缺 ended
        {"tmdbId": "4", "name": "ongoing-dead", "ended": False, "seasons": []},
    ]
    cache_file.write_text("".join(json.dumps(o) + "\n" for o in old), encoding="utf-8")
    fetched = []

    def fake_fetch(tid):
        fetched.append(tid)
        return {"tmdbId": tid, "name": f"fresh{tid}", "year": 2020, "ended": tid in ("3", "9"),
                "seasons": [{"season": 1, "episodes": [1, 2], "air_dates": {}}]}

    monkeypatch.setattr(m, "fetch_seasons_from_tmdb", fake_fetch)
    ids = ["1", "2", "3", "4", "9"]

    # 默认：只展开未缓存的
    cache = m.expand_seasons(ids, cache_file, fail_file, {"4"})
    assert fetched == ["9"]
    assert cache["2"]["name"] == "ongoing"

    # --refresh-ongoing：ended 非 True（含缺字段）重新展开；ended=True 不动；dead_shows 跳过
    fetched.clear()
    cache = m.expand_seasons(ids, cache_file, fail_file, {"4"}, refresh_ongoing=True)
    assert sorted(fetched) == ["2", "3"]
    assert cache["1"]["name"] == "ended"
    assert cache["2"]["name"] == "fresh2" and cache["2"]["seasons"][0]["episodes"] == [1, 2]
    assert cache["3"]["ended"] is True
    # 追加行覆盖旧行：重新读取取最后一行
    reloaded = m.load_seasons_cache(cache_file)
    assert reloaded["2"]["name"] == "fresh2" and reloaded["3"]["name"] == "fresh3"
    assert reloaded["1"]["name"] == "ended"


def test_expand_seasons_propagates_auth_error(tmp_path, monkeypatch):
    def fake_fetch(tid):
        raise m.TmdbAuthError("401")

    monkeypatch.setattr(m, "fetch_seasons_from_tmdb", fake_fetch)
    with pytest.raises(m.TmdbAuthError):
        m.expand_seasons(["1", "2"], tmp_path / "c.jsonl", tmp_path / "f.txt", set())
    assert not (tmp_path / "f.txt").exists()  # 不判死、不写 fail.txt


def test_expand_seasons_auth_error_cancels_remaining(tmp_path, monkeypatch):
    """401 后剩余排队的剧不该继续打 TMDB（只会一路 401）；单线程下最多再漏跑 1 个已取走的。"""
    called = []

    def fake_fetch(tid):
        called.append(tid)
        raise m.TmdbAuthError("401")

    monkeypatch.setattr(m, "fetch_seasons_from_tmdb", fake_fetch)
    monkeypatch.setattr(m, "TMDB_WORKERS", 1)
    with pytest.raises(m.TmdbAuthError):
        m.expand_seasons([str(i) for i in range(40)], tmp_path / "c.jsonl", tmp_path / "f.txt", set())
    assert len(called) <= 2


def test_load_dead_episodes(tmp_path):
    results = tmp_path / "results.jsonl"
    fail = tmp_path / "fail.txt"
    results.write_text(json.dumps({"tmdbId": "1", "season": 1, "episode": 1}) + "\n", encoding="utf-8")
    fail.write_text("1\t1\t1\n1\t1\t2\n1\t1\t3\tretry-exhausted\n2\t-\t-\n", encoding="utf-8")
    # 后来成功的不算；retry-exhausted 与剧级失效不算
    assert m.load_dead_episodes(results, fail) == {("1", 1, 2)}
    assert m.load_dead_episodes(tmp_path / "x", tmp_path / "y") == set()


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
    fail.write_text(
        "1\t1\t3\n2\t-\t-\n3\tx\ty\nonly-two\tcols\n\n"
        "1\t1\t4\tretry-exhausted\n"      # 重试耗尽标记：不计入 processed，下次重跑
        "1\t1\t5\tsomething-else\n",      # 未知第 4 列：格式不认，忽略
        encoding="utf-8",
    )
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


def _install_fake_session(monkeypatch, page_html, streams, servers=None, stream_status=200,
                          page_status=200, servers_status=200, dec_status=200,
                          enc_resp=None, dec_servers_resp=None, token="tok",
                          vidlink_resp=None, vidlink_enc="ENCTID",
                          vidfast_page_status=404, vidfast_streams=None):
    """三家取流源的假 Session，按域名路由。

    vidup（vidup.to / enc-vidup / dec-vidup / https://x/...）：
      streams: list of decrypted stream dicts returned in order for each server.
      enc_resp / dec_servers_resp: 覆盖 enc-vidup / dec-vidup(servers) 的整个 _FakeResp（模拟非 JSON 等）。
      token: enc-vidup 返回的 token（线上 2026-09 起为空串）。
    vidlink（enc-vidlink / vidlink.pro/api/b/tv）：
      vidlink_resp: 整个 _FakeResp；默认 200 + body null（无源）。
    vidfast（vidfast.vc / enc-vidfast / dec-vidfast / https://y/...）：
      vidfast_page_status: 默认 404（无源）；vidfast_streams 同 streams 语义。
    默认 vidlink/vidfast 都无源，旧的 vidup 单源用例语义不变。"""
    seen = {"page_urls": [], "page_headers": None, "enc_urls": [], "servers_headers": None,
            "vidlink_urls": [], "vidlink_headers": None, "vidfast_page_urls": [], "order": []}
    sites = {
        "vidup": {
            "host": "https://vidup.to/", "base": "https://x", "html": page_html,
            "page_status": page_status, "servers": servers, "streams": iter(streams),
            "page_urls": seen["page_urls"],
        },
        "vidfast": {
            "host": "https://vidfast.vc/", "base": "https://y", "html": page_html,
            "page_status": vidfast_page_status, "servers": None, "streams": iter(vidfast_streams or []),
            "page_urls": seen["vidfast_page_urls"],
        },
    }
    for name, site in sites.items():
        n = len(streams if name == "vidup" else (vidfast_streams or []))
        if site["servers"] is None:
            site["servers"] = [{"name": f"s{i}", "data": f"d{i}"} for i in range(n)]

    def _site_by_base(url):
        for name, site in sites.items():
            if url.startswith(site["base"] + "/"):
                return name, site
        raise AssertionError(url)

    class FakeSession:
        def __init__(self, *a, **k):
            self.proxies = None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, timeout=None, headers=None):
            for name, site in sites.items():
                if url.startswith(site["host"]):
                    seen["order"].append(name)
                    site["page_urls"].append(url)
                    if name == "vidup":
                        seen["page_headers"] = headers
                    ps = site["page_status"]
                    if isinstance(ps, list):  # 按次序给出状态码，最后一个重复使用
                        ps = ps.pop(0) if len(ps) > 1 else ps[0]
                    return _FakeResp(text=site["html"], status=ps)
                if f"/enc-{name}" in url:
                    if name == "vidup":
                        seen["enc_urls"].append(url)
                        if enc_resp is not None:
                            return enc_resp
                    base = site["base"]
                    return _FakeResp(payload={"status": 200, "result": {
                        "servers": f"{base}/servers", "stream": f"{base}/stream", "token": token}})
            if "/enc-vidlink" in url:
                seen["order"].append("vidlink")
                return _FakeResp(payload={"status": 200, "result": vidlink_enc})
            if url.startswith("https://vidlink.pro/api/b/tv/"):
                seen["vidlink_urls"].append(url)
                seen["vidlink_headers"] = headers
                return vidlink_resp if vidlink_resp is not None else _FakeResp(payload=None)
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            for name in sites:
                if url.endswith(f"/dec-{name}"):
                    site = sites[name]
                    if json["text"] == "enc-servers":
                        if name == "vidup" and dec_servers_resp is not None:
                            return dec_servers_resp
                        return _FakeResp(payload={"status": 200, "result": site["servers"]})
                    return _FakeResp(payload={"status": 200, "result": next(site["streams"])}, status=dec_status)
            name, site = _site_by_base(url)
            if url == f"{site['base']}/servers":
                if name == "vidup":
                    seen["servers_headers"] = headers
                if token:
                    assert headers["X-CSRF-Token"] == token
                else:
                    assert "X-CSRF-Token" not in headers
                return _FakeResp(text="enc-servers", status=servers_status)
            if url.startswith(f"{site['base']}/stream/"):
                st = stream_status if name == "vidup" else 200
                if isinstance(st, dict):
                    st = st.get(url.rsplit("/", 1)[1], 200)
                return _FakeResp(text="enc-stream", status=st)
            raise AssertionError(url)

    monkeypatch.setattr(m.requests, "Session", FakeSession)
    monkeypatch.setattr(m, "build_proxy", lambda: None)
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)
    return seen


def _urls(result):
    return [u["url"] for u in result["urls"]]


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
    assert _urls(result) == ["u1", "u2"]
    assert result["urls"][0] == {"url": "u1", "provider": "vidup", "type": "m3u8",
                                 "headers": {}, "quality": None, "size": None}
    # vidup 命中后仍打 vidlink / vidfast 汇总候选（默认都无源，不影响 ok）
    assert seen["order"] == ["vidup", "vidlink", "vidfast"]
    assert result["title"] == "Show Name"
    assert result["year"] == 2011 and result["original_title"] == "Orig"


def test_process_episode_meta_cannot_override_identity(monkeypatch):
    """剧级元数据绝不能覆盖本集的身份字段。

    _SERIES_META 来自 tv_series.jsonl（一剧一条）。若该表哪天多出 season/
    episode/tmdbId 之类的键，直接 update 会把本集身份改掉，下游据此拼文件名与
    R2 对象键，成片会静默写到错误位置且极难发现。这里显式塞入污染键做回归。
    """
    _install_fake_session(monkeypatch, PAGE, [{"url": "u1", "title": "Real"}])
    monkeypatch.setattr(m, "_SERIES_META", {"42": {
        # 污染键：全部应被忽略
        "tmdbId": "999", "season": 99, "episode": 99,
        "urls": ["evil"], "title": "EvilTitle",
        # 正常元数据：应被合并
        "year": 2011, "runtime_minutes": 45,
    }})
    status, result = m.process_episode("42", 2, 3)
    assert status == "ok"
    assert result["tmdbId"] == "42"
    assert result["season"] == 2 and result["episode"] == 3
    assert result["title"] == "Real"
    assert _urls(result) == ["u1"]
    # 非身份字段仍正常合并
    assert result["year"] == 2011 and result["runtime_minutes"] == 45


def test_process_episode_stamps_fetched_at(monkeypatch):
    """每条结果必须带取流时刻，且不可被剧级元数据覆盖。

    results.jsonl 是追加写，同一集复扫会留下多行；下游 download_tv 靠该字段
    挑真正最新的一条 url，而不是"文件里最后出现"这种会被手工编辑破坏的位置
    假设。对 vidlink 这类带时效签名的直链，拿到过期 url 等于白跑一次下载。
    """
    _install_fake_session(monkeypatch, PAGE, [{"url": "u1"}])
    monkeypatch.setattr(m, "_SERIES_META", {"7": {"fetched_at": 1, "year": 2020}})
    monkeypatch.setattr(m.time, "time", lambda: 1730000000.9)
    status, result = m.process_episode("7", 1, 1)
    assert status == "ok"
    assert result["fetched_at"] == 1730000000
    assert result["year"] == 2020


def test_load_exhausted_reads_only_tagged_rows(tmp_path):
    """_load_exhausted 只认第 4 列为 retry-exhausted 的行。

    集级真无源（3 列）与剧级 404（tid\\t-\\t-）不属于"重试耗尽"，若被误纳入
    会让本次运行把它们当成已写过而漏记。
    """
    fail_file = tmp_path / "fail.txt"
    fail_file.write_text(
        "1\t1\t1\tretry-exhausted\n"
        "2\t1\t2\n"
        "3\t-\t-\n"
        "4\tx\ty\tretry-exhausted\n"      # 非法数字，忽略
        "\n",
        encoding="utf-8",
    )
    assert m._load_exhausted(fail_file) == {("1", 1, 1)}
    assert m._load_exhausted(tmp_path / "nope.txt") == set()


def test_process_episode_ok_without_tmdbid_in_stream(monkeypatch):
    # 解密结果缺 tmdbId 不再是失败条件，只要有 url 即成功；title 无任何来源时兜底空串（下游按 str 用）
    _install_fake_session(monkeypatch, PAGE, [{"url": "u"}])
    monkeypatch.setattr(m, "_TMDB_NAMES", {})
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and _urls(result) == ["u"] and result["title"] == ""


@pytest.mark.parametrize("token", ["", None])
def test_process_episode_ok_with_empty_token(monkeypatch, token):
    # 线上 2026-09 实测 enc-vidup 返回 token 为空串，servers/stream 正常；不带 X-CSRF-Token 也能取流，
    # 不能因 token 缺失判为瞬时错误（否则整批 0 成功、全部 retry-exhausted）
    seen = _install_fake_session(monkeypatch, PAGE, [{"url": "u"}], token=token)
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and _urls(result) == ["u"]
    assert "X-CSRF-Token" not in seen["servers_headers"]


def test_process_episode_missing_servers_is_retriable(monkeypatch):
    enc = _FakeResp(payload={"status": 200, "result": {"stream": "https://x/stream", "token": ""}})
    _install_fake_session(monkeypatch, PAGE, [{"url": "u"}], enc_resp=enc)
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_title_falls_back_to_tmdb_name(monkeypatch):
    _install_fake_session(monkeypatch, PAGE, [{"url": "u"}])
    monkeypatch.setattr(m, "_TMDB_NAMES", {"1": "TMDB Name"})
    # 源站无 title → 回退 TMDB name
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and result["title"] == "TMDB Name"
    # 源站有 title → 优先源站
    _install_fake_session(monkeypatch, PAGE, [{"url": "u2", "title": "From Source"}])
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and result["title"] == "From Source"


def test_process_episode_encodes_enc_vidup_text(monkeypatch):
    page = 'window.x = "{\\"en\\":\\"a+b/c=d&e f\\"}"'
    seen = _install_fake_session(monkeypatch, page, [{"url": "u"}])
    status, _ = m.process_episode("1", 1, 1)
    assert status == "ok"
    assert seen["enc_urls"] == [f"{m.API}/enc-vidup?text=a%2Bb%2Fc%3Dd%26e%20f"]


def test_process_episode_dead_on_page_404(monkeypatch):
    # 二次确认：连续两次页面 404 才判死（第二次换出口 IP 完整重探）
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404)
    status, result = m.process_episode("1", 1, 1)
    assert status == "dead" and result is None
    assert len(seen["page_urls"]) == 2


def test_process_episode_dead_without_confirm(monkeypatch):
    monkeypatch.setattr(m, "DEAD_CONFIRM", False)
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "dead" and len(seen["page_urls"]) == 1


def test_process_episode_404_then_ok_is_not_dead(monkeypatch):
    # 首次 404 是 CDN/代理抖动，换 IP 后拿到 url → ok，不得误判永久丢集
    seen = _install_fake_session(monkeypatch, PAGE, [{"url": "u"}], page_status=[404, 200])
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and _urls(result) == ["u"]
    assert len(seen["page_urls"]) == 2


def test_process_episode_404_then_transient_gets_extra_budget(monkeypatch):
    # 404 → 503 → 503：命中过 NoSource 后预算 MAX_RETRIES+1，确认跳不挤占常规重试
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=[404, 503])
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry" and len(seen["page_urls"]) == 3


def test_process_episode_retry_on_page_5xx(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=503)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry" and len(seen["page_urls"]) == 2


def test_process_episode_dead_when_all_streams_404(monkeypatch):
    # 所有 server 的 stream 接口都 404 → NoSource；二次确认后 dead
    seen = _install_fake_session(monkeypatch, PAGE, [], servers=[{"name": "a", "data": "d"}] * 2, stream_status=404)
    status, result = m.process_episode("1", 1, 1)
    assert status == "dead" and result is None
    assert len(seen["page_urls"]) == 2


def test_process_episode_partial_404_still_ok(monkeypatch):
    """一个 server 404、另一个有 url → ok（多 server 取并集，单个 404 不是无源证据）。"""
    seen = _install_fake_session(
        monkeypatch, PAGE, [{"url": "u"}],
        servers=[{"name": "a", "data": "d1"}, {"name": "b", "data": "d2"}],
        stream_status={"d1": 404},
    )
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and _urls(result) == ["u"]
    assert len(seen["page_urls"]) == 1


def test_process_episode_retry_when_stream_5xx(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, PAGE, [], servers=[{"name": "a", "data": "d"}], stream_status=503)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"
    assert len(seen["page_urls"]) == 2


def test_process_episode_retry_when_servers_post_fails(monkeypatch):
    # servers POST 5xx 原先没有 raise_for_status，会把错误页当加密文本送去解密；现在直接判瞬时
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    _install_fake_session(monkeypatch, PAGE, [], servers_status=502)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_retry_when_dec_vidup_404(monkeypatch):
    # enc-dec 服务自身的 404 不是“无源”证据 → retry 而非 dead
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    _install_fake_session(monkeypatch, PAGE, [{"url": "u"}], dec_status=404)
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_retry_when_enc_returns_html(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 1)

    class Html(_FakeResp):
        def json(self):
            raise ValueError("Expecting value")

    _install_fake_session(monkeypatch, PAGE, [], enc_resp=Html(text="<html>cf</html>"))
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_retry_when_enc_result_incomplete(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    _install_fake_session(monkeypatch, PAGE, [], enc_resp=_FakeResp(payload={"status": 200, "result": {"servers": "x"}}))
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_retry_when_servers_list_empty(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    _install_fake_session(monkeypatch, PAGE, [], dec_servers_resp=_FakeResp(payload={"status": 200, "result": []}))
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_retry_when_extract_fails(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, "<html>cloudflare</html>", [])
    status, result = m.process_episode("1", 1, 1)
    assert status == "retry" and result is None
    assert len(seen["page_urls"]) == 2


def test_process_episode_all_servers_missing_url_is_retry(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    _install_fake_session(monkeypatch, PAGE, [{"url": None, "tmdbId": None}] * 2)
    status, _ = m.process_episode("1", 1, 1)
    # 缺 url 不是 404 证据 → 瞬时
    assert status == "retry"


def test_process_episode_token_fallback(monkeypatch):
    page = 'x = "{\\"token\\":\\"TOK\\"}"'
    _install_fake_session(monkeypatch, page, [{"url": "u", "tmdbId": "1"}])
    status, result = m.process_episode("1", 0, 1)
    assert status == "ok" and result["season"] == 0


# ---------- 多源编排（vidup → vidlink → vidfast） ----------
VIDLINK_OK = {"stream": {"qualities": {
    "480": {"url": "https://cdn/480.mp4", "size": 100},
    "1080": {"url": "https://cdn/1080.mp4", "size": 900, "codecName": "h264"},
    "720": {"url": "https://cdn/720.mp4", "size": "bad"},
    "360": {"url": "", "size": 10},
}}, "captions": []}


def test_process_episode_falls_through_to_vidlink(monkeypatch):
    # vidup 页面 404（无源）→ vidlink 命中 → ok；vidfast 仍会被探一次（无源），不触发二次确认
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                                 vidlink_resp=_FakeResp(payload=VIDLINK_OK))
    monkeypatch.setattr(m, "_TMDB_NAMES", {"7": "Name"})
    status, result = m.process_episode("7", 1, 2)
    assert status == "ok"
    assert seen["order"] == ["vidup", "vidlink", "vidfast"]
    assert seen["vidlink_urls"] == ["https://vidlink.pro/api/b/tv/ENCTID/1/2"]
    assert seen["vidlink_headers"]["Origin"] == "https://vidlink.pro"
    # 画质降序；size 非法为 None；空 url 丢弃；下载头是 okhttp 且不带 Referer
    assert [(u["quality"], u["size"]) for u in result["urls"]] == [(1080, 900), (720, None), (480, 100)]
    assert all(u["provider"] == "vidlink" and u["type"] == "mp4" for u in result["urls"])
    assert result["urls"][0]["headers"] == {"User-Agent": "okhttp/4.9.3"}
    assert result["title"] == "Name"
    assert len(seen["page_urls"]) == 1


def test_process_episode_falls_through_to_vidfast(monkeypatch):
    # vidup 404 + vidlink null → vidfast 命中
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                                 vidfast_page_status=200, vidfast_streams=[{"url": "vf1", "title": "VF"}])
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok"
    assert seen["order"] == ["vidup", "vidlink", "vidfast"]
    assert seen["vidfast_page_urls"] == ["https://vidfast.vc/tv/1/1/1/"]
    assert result["urls"] == [{"url": "vf1", "provider": "vidfast", "type": "m3u8",
                               "headers": {}, "quality": None, "size": None}]
    assert result["title"] == "VF"


def test_process_episode_dead_only_when_all_providers_nosource(monkeypatch):
    # 三家都无源 → 换 IP 二次确认（三家再跑一遍）→ dead
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404)
    status, result = m.process_episode("1", 1, 1)
    assert status == "dead" and result is None
    assert seen["order"] == ["vidup", "vidlink", "vidfast"] * 2


def test_process_episode_vidlink_transient_blocks_dead(monkeypatch):
    # vidup/vidfast 无源，但 vidlink 5xx：不能判死，走 retry
    monkeypatch.setattr(m, "MAX_RETRIES", 2)
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                                 vidlink_resp=_FakeResp(status=503))
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"
    assert seen["order"] == ["vidup", "vidlink", "vidfast"] * 2


def test_process_episode_vidlink_missing_qualities_is_transient(monkeypatch):
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                          vidlink_resp=_FakeResp(payload={"stream": {}}))
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"


def test_process_episode_vidlink_404_is_transient(monkeypatch):
    # vidlink 404 不是无源证据（路由变更/enc 异常/WAF），只有 200+null 才算 NoSource
    monkeypatch.setattr(m, "MAX_RETRIES", 1)
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                                 vidlink_resp=_FakeResp(status=404))
    status, _ = m.process_episode("1", 1, 1)
    assert status == "retry"
    # 走的是瞬时重试路径而不是 DEAD_CONFIRM 复探：MAX_RETRIES=1 只跑一轮
    assert seen["order"] == ["vidup", "vidlink", "vidfast"]


def test_process_episode_vidlink_enc_is_url_quoted(monkeypatch):
    seen = _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                                 vidlink_resp=_FakeResp(payload=VIDLINK_OK),
                                 vidlink_enc="a/b+c=")
    status, _ = m.process_episode("1", 1, 1)
    assert status == "ok"
    assert seen["vidlink_urls"] == ["https://vidlink.pro/api/b/tv/a%2Fb%2Bc%3D/1/1"]


def test_process_episode_vidlink_size_bool_and_dup_url(monkeypatch):
    payload = {"stream": {"qualities": {
        "1080": {"url": "https://cdn/same.mp4", "size": True},
        "720": {"url": "https://cdn/same.mp4", "size": 5},
        "480": {"url": "https://cdn/480.mp4", "size": 0},
        "360": {"url": "https://cdn/360.mp4", "size": 7.9},
    }}}
    _install_fake_session(monkeypatch, PAGE, [], page_status=404,
                          vidlink_resp=_FakeResp(payload=payload))
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok"
    # size=True 不能被当成 1；重复 url 只保留首个（高画质）；size<=0 为 None；float 截断为 int
    assert [(u["quality"], u["size"]) for u in result["urls"]] == [(1080, None), (480, None), (360, 7)]


def test_process_episode_respects_providers_arg(monkeypatch):
    # 只指定 vidlink：不打 vidup/vidfast
    seen = _install_fake_session(monkeypatch, PAGE, [{"url": "u"}],
                                 vidlink_resp=_FakeResp(payload=VIDLINK_OK))
    status, result = m.process_episode("1", 1, 1, providers=["vidlink"])
    assert status == "ok" and seen["order"] == ["vidlink"]
    assert result["urls"][0]["provider"] == "vidlink"


def test_process_episode_aggregates_all_providers(monkeypatch):
    # 三家同时命中：urls 按 providers 顺序拼接（vidup m3u8 → vidlink mp4 降序 → vidfast m3u8），
    # 跨家 url 去重保留首家；title 取首家非空
    seen = _install_fake_session(monkeypatch, PAGE, [{"url": "u1"}, {"url": "https://cdn/480.mp4"}],
                                 vidlink_resp=_FakeResp(payload=VIDLINK_OK),
                                 vidfast_page_status=200,
                                 vidfast_streams=[{"url": "vf1", "title": "VF"}, {"url": "u1"}])
    monkeypatch.setattr(m, "_TMDB_NAMES", {})
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok"
    assert seen["order"] == ["vidup", "vidlink", "vidfast"]
    assert [(u["provider"], u["url"]) for u in result["urls"]] == [
        ("vidup", "u1"), ("vidup", "https://cdn/480.mp4"),
        ("vidlink", "https://cdn/1080.mp4"), ("vidlink", "https://cdn/720.mp4"),
        ("vidfast", "vf1"),
    ]
    assert result["title"] == "VF"


def test_process_episode_ok_even_if_other_provider_transient(monkeypatch):
    # vidup 命中、vidlink 5xx：已有候选即 ok，不因其它家瞬时错误转 retry
    seen = _install_fake_session(monkeypatch, PAGE, [{"url": "u"}],
                                 vidlink_resp=_FakeResp(status=503))
    status, result = m.process_episode("1", 1, 1)
    assert status == "ok" and _urls(result) == ["u"]
    assert seen["order"] == ["vidup", "vidlink", "vidfast"]


def test_resolve_providers():
    assert m._resolve_providers(["vidfast", " vidup", "vidfast", ""]) == ["vidfast", "vidup"]
    # 逗号分隔字符串（CLI --providers / config 写成字符串）不能被逐字符拆开
    assert m._resolve_providers("vidup, vidlink") == ["vidup", "vidlink"]
    assert m._resolve_providers("vidlink") == ["vidlink"]
    with pytest.raises(SystemExit):
        m._resolve_providers(["nope"])
    with pytest.raises(SystemExit):
        m._resolve_providers([])


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


# ---------- 批量误杀熔断 ----------
def _dead_items(n, tid="d"):
    return [(tid, 1, i) for i in range(1, n + 1)]


def test_breaker_trips_rolls_back_and_raises(tmp_path, monkeypatch):
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    fail.write_text("old\t1\t1\n", encoding="utf-8")
    results.write_text(json.dumps({"tmdbId": "c", "season": 1, "episode": 1}) + "\n", encoding="utf-8")

    probed = []

    def fake(tid, s, e):
        if tid == "c":
            probed.append((tid, s, e))
        return "dead", None   # 全员判死，金丝雀也死 → 上游坏了

    monkeypatch.setattr(m, "process_episode", fake)
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 3)
    with pytest.raises(m.DeadStreakBreaker):
        m.run_batch(_dead_items(10), results, fail, max_workers=1)
    assert probed == [("c", 1, 1)]
    # 窗口内 3 行已回滚，旧行保留
    assert fail.read_text(encoding="utf-8") == "old\t1\t1\n"


def test_breaker_canary_alive_resets_and_continues(tmp_path, monkeypatch):
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    results.write_text(json.dumps({"tmdbId": "c", "season": 1, "episode": 1}) + "\n", encoding="utf-8")
    canary_calls = []

    def fake(tid, s, e):
        if tid == "c":
            canary_calls.append(1)
            return "ok", {"urls": ["u"], "tmdbId": tid, "season": s, "episode": e, "title": "t"}
        return "dead", None

    monkeypatch.setattr(m, "process_episode", fake)
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 3)
    retry = m.run_batch(_dead_items(7), results, fail, max_workers=1)
    assert retry == []
    # 7 集全 dead 全部落盘；每满 3 集探一次金丝雀 → 探了 2 次（第 3、6 集）
    assert len(fail.read_text(encoding="utf-8").splitlines()) == 7
    assert len(canary_calls) == 2


def test_breaker_success_resets_streak(tmp_path, monkeypatch):
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"

    def fake(tid, s, e):
        if tid == "ok":
            return "ok", {"urls": ["u"], "tmdbId": tid, "season": s, "episode": e, "title": "t"}
        return "dead", None

    monkeypatch.setattr(m, "process_episode", fake)
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 3)
    monkeypatch.setattr(m, "_canaries_alive", lambda *a, **k: pytest.fail("不应触发"))
    items = [("d", 1, 1), ("d", 1, 2), ("ok", 1, 1), ("d", 1, 3), ("d", 1, 4)]
    m.run_batch(items, results, fail, max_workers=1)
    assert len(fail.read_text(encoding="utf-8").splitlines()) == 4


def test_breaker_disabled_in_recheck_or_zero(tmp_path, monkeypatch):
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    monkeypatch.setattr(m, "process_episode", lambda *a: ("dead", None))
    monkeypatch.setattr(m, "_canaries_alive", lambda *a, **k: pytest.fail("不应触发"))
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 2)
    m.run_batch(_dead_items(5), results, fail, max_workers=1, write_dead=False)
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 0)
    m.run_batch(_dead_items(5), results, fail, max_workers=1)
    assert len(fail.read_text(encoding="utf-8").splitlines()) == 5


def test_rollback_fail_tail_mismatch_leaves_file(tmp_path):
    fail = tmp_path / "f.txt"
    fail.write_text("a\t1\t1\nb\t1\t1\n", encoding="utf-8")
    assert m._rollback_fail_tail(fail, [("x", 1, 1)]) is False
    assert fail.read_text(encoding="utf-8") == "a\t1\t1\nb\t1\t1\n"
    assert m._rollback_fail_tail(fail, [("b", 1, 1)]) is True
    assert fail.read_text(encoding="utf-8") == "a\t1\t1\n"


def test_canaries_alive_without_history(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "process_episode", lambda *a: pytest.fail("无金丝雀不应探测"))
    assert m._canaries_alive([], tmp_path / "missing.jsonl") is None


def test_canaries_alive_three_states(tmp_path, monkeypatch):
    results = tmp_path / "missing.jsonl"
    # 全瞬时错误 → None
    monkeypatch.setattr(m, "process_episode", lambda *a: ("retry", None))
    assert m._canaries_alive([("c", 1, 1), ("c", 1, 2)], results) is None
    # 探测抛异常不崩，视作瞬时 → None
    def boom(*a):
        raise RuntimeError("x")
    monkeypatch.setattr(m, "process_episode", boom)
    assert m._canaries_alive([("c", 1, 1)], results) is None
    # 一个瞬时 + 一个明确 dead → False
    seq = iter([("retry", None), ("dead", None)])
    monkeypatch.setattr(m, "process_episode", lambda *a: next(seq))
    assert m._canaries_alive([("c", 1, 1), ("c", 1, 2)], results) is False
    # 任一 ok → True
    seq = iter([("dead", None), ("ok", {"urls": ["u"]})])
    monkeypatch.setattr(m, "process_episode", lambda *a: next(seq))
    assert m._canaries_alive([("c", 1, 1), ("c", 1, 2)], results) is True


def test_pick_canaries_prefers_recent_ok(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "_load_ok_keys", lambda *a: pytest.fail("有 recent_ok 时不应读 results"))
    picked = m._pick_canaries([("a", 1, 1), ("b", 1, 1)], tmp_path / "r.jsonl", 5)
    assert sorted(picked) == [("a", 1, 1), ("b", 1, 1)]


def test_breaker_no_history_all_dead_does_not_trip(tmp_path, monkeypatch):
    """全新跑、还没有任何成功集时，连败不应熔断（无从判断上游）。"""
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    monkeypatch.setattr(m, "process_episode", lambda *a: ("dead", None))
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 3)
    retry = m.run_batch(_dead_items(7), results, fail, max_workers=1)
    assert retry == []
    assert len(fail.read_text(encoding="utf-8").splitlines()) == 7


def test_breaker_canary_all_retry_does_not_trip(tmp_path, monkeypatch):
    """金丝雀全是瞬时错误（如限流）→ 放行，不回滚 fail。"""
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    results.write_text(json.dumps({"tmdbId": "c", "season": 1, "episode": 1}) + "\n", encoding="utf-8")

    def fake(tid, s, e):
        return ("retry", None) if tid == "c" else ("dead", None)

    monkeypatch.setattr(m, "process_episode", fake)
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 3)
    retry = m.run_batch(_dead_items(4), results, fail, max_workers=1)
    assert retry == []
    assert len(fail.read_text(encoding="utf-8").splitlines()) == 4


def test_breaker_recent_ok_used_as_canary(tmp_path, monkeypatch):
    """本轮刚成功过的集应优先作金丝雀，而不是读 results.jsonl。"""
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    monkeypatch.setattr(m, "_load_ok_keys", lambda *a: pytest.fail("不应读 results"))
    probed = []

    def fake(tid, s, e):
        if tid == "ok":
            probed.append((tid, s, e))
            return "ok", {"urls": ["u"], "tmdbId": tid, "season": s, "episode": e, "title": "t"}
        return "dead", None

    monkeypatch.setattr(m, "process_episode", fake)
    monkeypatch.setattr(m, "DEAD_STREAK_BREAKER", 3)
    items = [("ok", 1, 1)] + _dead_items(3)
    m.run_batch(items, results, fail, max_workers=1)
    # 第一次是正常处理，第二次是作金丝雀复探
    assert probed == [("ok", 1, 1), ("ok", 1, 1)]


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
    expand_kw = []

    def fake_expand(ids, cf, ff, dead, **kw):
        expand_kw.append(kw)
        return {
            # 旧格式缓存行（无 air_dates / ended）：全部纳入
            "1": {"tmdbId": "1", "name": "Show One", "year": 1999, "seasons": [{"season": 1, "episodes": [1, 2]}]},
            # 新格式：已完结剧缺 air_date 视为已播
            "2": {"tmdbId": "2", "year": 2005, "ended": True,
                  "seasons": [{"season": 0, "episodes": [1], "air_dates": {}}]},
        }

    monkeypatch.setattr(m, "expand_seasons", fake_expand)
    meta = {"1": {"year": 2011}}
    monkeypatch.setattr(m, "_SERIES_META", meta)
    names = {}
    monkeypatch.setattr(m, "_TMDB_NAMES", names)
    sleeps = []
    monkeypatch.setattr(m.time, "sleep", lambda s: sleeps.append(s))
    seen = []

    def fake_batch(pending, rf, ff, mw, write_dead=True):
        assert write_dead is True
        seen.append(list(pending))
        return [] if len(seen) > 1 else [pending[-1]]

    monkeypatch.setattr(m, "run_batch", fake_batch)
    m.main()

    assert expand_kw == [{"refresh_ongoing": False}]   # 不传参数默认不刷新
    assert names == {"1": "Show One"}                  # 缓存 name 进入 title 回退表
    assert seen[0] == [("1", 1, 2), ("2", 0, 1)]
    assert seen[1] == [("2", 0, 1)]
    assert meta["1"]["year"] == 2011           # tv_series.jsonl 有值不被覆盖
    assert meta["2"]["year"] == 2005           # 缺失时回退 TMDB first_air_date
    assert sleeps == [m.ROUND_BACKOFF_BASE]    # 第 1 轮后退避 base*1；最后一轮清零不再等
    assert not fail.read_text(encoding="utf-8").endswith("retry-exhausted\n")


def test_main_skips_unaired_episodes(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n2\n", encoding="utf-8")
    fail = tmp_path / "fail.txt"
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(tmp_path / "r.jsonl"), "fail_file": str(fail),
        "seasons_cache": str(tmp_path / "c.jsonl"),
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: {
        # 在播剧：ep2 未来、ep3 缺日期(TBA) → 跳过；ep1 已播
        "1": {"tmdbId": "1", "ended": False,
              "seasons": [{"season": 1, "episodes": [1, 2, 3], "air_dates": {"1": "2000-01-01", "2": "2999-01-01"}}]},
        # 已完结剧：缺日期视为已播
        "2": {"tmdbId": "2", "ended": True, "seasons": [{"season": 1, "episodes": [1], "air_dates": {}}]},
    })
    monkeypatch.setattr(m, "_SERIES_META", {})
    seen = []
    monkeypatch.setattr(m, "run_batch", lambda pending, *a, **kw: (seen.append(list(pending)), [])[1])
    m.main()
    assert seen == [[("1", 1, 1), ("2", 1, 1)]]
    assert not fail.exists()  # 未播集不写 fail.txt


def test_main_refresh_ongoing_flag_passthrough(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n", encoding="utf-8")
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(tmp_path / "r.jsonl"), "fail_file": str(tmp_path / "f.txt"),
        "seasons_cache": str(tmp_path / "c.jsonl"),
    })
    kws = []
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: (kws.append(kw), {})[1])
    m.main(["--refresh-ongoing"])
    assert kws == [{"refresh_ongoing": True}]
    with pytest.raises(SystemExit):
        m.main(["--bogus"])


def test_main_writes_leftover_retries_with_tag(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n", encoding="utf-8")
    fail = tmp_path / "fail.txt"
    results = tmp_path / "r.jsonl"
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(results), "fail_file": str(fail),
        "seasons_cache": str(tmp_path / "c.jsonl"), "max_rounds": 1,
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: {"1": {"tmdbId": "1", "seasons": [{"season": 1, "episodes": [1]}]}})
    monkeypatch.setattr(m, "_SERIES_META", {})
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)
    monkeypatch.setattr(m, "run_batch", lambda pending, *a, **kw: list(pending))
    m.main()
    assert fail.read_text(encoding="utf-8") == "1\t1\t1\tretry-exhausted\n"
    # 标记行不算已处理：下次运行仍会重跑这一集
    processed, _ = m.load_processed(results, fail)
    assert processed == set()


def test_main_round_backoff_and_outage_detection(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n", encoding="utf-8")
    n = m.OUTAGE_MIN_ITEMS + 10
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(tmp_path / "r.jsonl"), "fail_file": str(tmp_path / "f.txt"),
        "seasons_cache": str(tmp_path / "c.jsonl"), "max_rounds": 4,
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: {
        "1": {"tmdbId": "1", "seasons": [{"season": 1, "episodes": list(range(1, n + 1))}]}})
    monkeypatch.setattr(m, "_SERIES_META", {})
    sleeps = []
    monkeypatch.setattr(m.time, "sleep", lambda s: sleeps.append(s))
    rounds = []

    def fake_batch(pending, *a, **kw):
        rounds.append(len(pending))
        if len(rounds) == 1:
            return list(pending)          # 全部 retry → 疑似故障 → 等上限
        if len(rounds) == 2:
            return list(pending)[:5]      # 少量 retry → 正常退避 base*round
        return []

    monkeypatch.setattr(m, "run_batch", fake_batch)
    m.main()
    assert rounds == [n, n, 5]
    assert sleeps == [m.ROUND_BACKOFF_MAX, min(m.ROUND_BACKOFF_BASE * 2, m.ROUND_BACKOFF_MAX)]


def test_main_exits_2_on_breaker(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n", encoding="utf-8")
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(tmp_path / "r.jsonl"), "fail_file": str(tmp_path / "f.txt"),
        "seasons_cache": str(tmp_path / "c.jsonl"),
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: {
        "1": {"tmdbId": "1", "seasons": [{"season": 1, "episodes": [1, 2]}]}})
    monkeypatch.setattr(m, "_SERIES_META", {})

    def fake_batch(*a, **kw):
        raise m.DeadStreakBreaker("boom")

    monkeypatch.setattr(m, "run_batch", fake_batch)
    with pytest.raises(SystemExit) as ei:
        m.main()
    assert ei.value.code == 2


def test_main_recheck_dead_mode(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("1\n2\n", encoding="utf-8")
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    # 1:1:1 死过但后来成功 → 不复查；1:1:2 死 → 复查；1:1:3 retry-exhausted → 不属于死集；
    # 2 剧级失效 → 其集不复查；9 不在 ids.txt → 不复查
    results.write_text(json.dumps({"tmdbId": "1", "season": 1, "episode": 1}) + "\n", encoding="utf-8")
    fail.write_text("1\t1\t2\n1\t1\t1\n1\t1\t3\tretry-exhausted\n2\t-\t-\n2\t1\t1\n9\t1\t1\n1\t1\t9\n",
                    encoding="utf-8")
    monkeypatch.setattr(m, "_CFG", {
        "input": str(ids), "output": str(results), "fail_file": str(fail),
        "seasons_cache": str(tmp_path / "c.jsonl"),
    })
    monkeypatch.setattr(m, "expand_seasons", lambda *a, **kw: {
        "1": {"tmdbId": "1", "name": "One", "seasons": [{"season": 1, "episodes": [1, 2, 3, 9]}]}})
    monkeypatch.setattr(m, "_SERIES_META", {})
    names = {}
    monkeypatch.setattr(m, "_TMDB_NAMES", names)
    calls = []

    def fake_batch(pending, rf, ff, mw, write_dead=True):
        calls.append((list(pending), write_dead))
        return []

    monkeypatch.setattr(m, "run_batch", fake_batch)
    m.main(["--recheck-dead"])
    assert calls == [([("1", 1, 2), ("1", 1, 9)], False)]
    assert names == {"1": "One"}   # 复查模式仍填充 title 回退表


def test_run_batch_recheck_does_not_rewrite_dead(tmp_path, monkeypatch):
    results = tmp_path / "r.jsonl"
    fail = tmp_path / "f.txt"
    fail.write_text("1\t1\t1\n", encoding="utf-8")
    monkeypatch.setattr(m, "process_episode", lambda tid, s, e: ("dead", None))
    assert m.run_batch([("1", 1, 1)], results, fail, max_workers=1, write_dead=False) == []
    assert fail.read_text(encoding="utf-8") == "1\t1\t1\n"


def test_run_batch_cancels_pending_on_interrupt(tmp_path, monkeypatch):
    """Ctrl+C：已排队但未开始的集必须被取消，不能等数万集跑完才退出。"""
    import threading as th
    started = []
    gate = th.Event()

    def slow(tid, s, e):
        started.append(tid)
        if tid == "first":
            raise KeyboardInterrupt
        gate.wait(0.2)
        return "retry", None

    monkeypatch.setattr(m, "process_episode", slow)
    items = [("first", 1, 1)] + [(f"q{i}", 1, 1) for i in range(50)]
    with pytest.raises(KeyboardInterrupt):
        m.run_batch(items, tmp_path / "r.jsonl", tmp_path / "f.txt", max_workers=1)
    # 单线程：第一个抛 KeyboardInterrupt 后，剩余排队任务被 cancel（最多再漏跑 1 个已被 worker 取走的）
    assert started[0] == "first" and len(started) <= 2
