"""tmdb_ids_to_links.py 多源取流的离线用例。

覆盖本轮扩源改动：provider 抽象与注册表、四家源的协议分支、多源汇总与去重、
白名单判死（NoSource 是唯一判死证据）、判死二次确认、身份字段保护。
全部用假 Session，不联网。
"""

import json

import pytest

import tmdb_ids_to_links as m


# ---------------------------------------------------------------- 假 Session

class _FakeResp:
    def __init__(self, status=200, payload=None, text=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if self._payload is _NOT_JSON:
            raise ValueError("not json")
        return self._payload


_NOT_JSON = object()


def _ok(result):
    """enc-dec.app 的成功信封。"""
    return _FakeResp(200, {"status": 200, "result": result})


def _install_fake_session(monkeypatch, *, vidup_streams=(), vidup_page_status=200,
                          vidfast_streams=(), vidfast_page_status=404,
                          vidlink_resp=None, videasy_seed="SEED",
                          videasy_sources=None, videasy_seed_status=200):
    """四家取流源的假 Session，按域名路由。

    默认：vidup 按 vidup_streams 出流；vidfast 页面 404（无源）；
    vidlink 返回 null（无源）；videasy 各 server 返回空（无源）。
    这样单独测某一家时，其余三家都是"明确无源"而非瞬时错误，语义干净。
    """
    seen = {"order": [], "urls": [], "headers": {}}

    sites = {
        "vidup": {"host": "https://vidup.to/", "base": "https://x",
                  "status": vidup_page_status, "streams": list(vidup_streams)},
        "vidfast": {"host": "https://vidfast.vc/", "base": "https://y",
                    "status": vidfast_page_status, "streams": list(vidfast_streams)},
    }

    class FakeSession:
        def __init__(self, *a, **k):
            self.proxies = None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, timeout=None, headers=None):
            seen["urls"].append(url)
            for name, site in sites.items():
                if url.startswith(site["host"]):
                    seen["order"].append(name)
                    return _FakeResp(site["status"], text="<x>\\\"en\\\":\\\"TXT\\\"</x>")
            if "/enc-vidup" in url:
                return _ok({"servers": "https://x/servers", "stream": "https://x/stream",
                            "token": "tok"})
            if "/enc-vidfast" in url:
                return _ok({"servers": "https://y/servers", "stream": "https://y/stream",
                            "token": "tok"})
            if "/enc-vidlink" in url:
                seen["order"].append("vidlink")
                return _ok("ENCTID")
            if url.startswith("https://vidlink.pro/api/b/movie/"):
                seen["headers"]["vidlink"] = headers
                return vidlink_resp if vidlink_resp is not None else _FakeResp(200, None, "null")
            if "speedracelight.com/seed" in url:
                seen["order"].append("videasy")
                return _FakeResp(videasy_seed_status, {"seed": videasy_seed})
            if "sources-with-title" in url:
                if videasy_sources is None:
                    return _FakeResp(404, text="")
                return videasy_sources
            raise AssertionError(f"unexpected GET {url}")

        def post(self, url, headers=None, json=None, timeout=None):
            seen["urls"].append(url)
            for name, site in sites.items():
                if url == site["base"] + "/servers":
                    return _FakeResp(200, text="ENC")
                if url.startswith(site["base"] + "/stream/"):
                    idx = int(url.rsplit("/", 1)[-1].lstrip("d"))
                    entry = site["streams"][idx]
                    if isinstance(entry, int):  # 用整数表示该 server 的 HTTP 状态码
                        return _FakeResp(entry, text="err")
                    return _FakeResp(200, text=f"ENCSTREAM{idx}")
            for name, site in sites.items():
                api = "vidup" if name == "vidup" else "vidfast"
                if url.endswith(f"/dec-{api}"):
                    text = (json or {}).get("text", "")
                    if text == "ENC":
                        return _ok([{"name": f"s{i}", "data": f"d{i}"}
                                    for i in range(len(site["streams"]))])
                    idx = int(text.replace("ENCSTREAM", ""))
                    return _ok(site["streams"][idx])
            if url.endswith("/dec-videasy"):
                return _ok((json or {}).get("_decoded", {"sources": [{"file": "https://v/a.m3u8"}]}))
            raise AssertionError(f"unexpected POST {url}")

    monkeypatch.setattr(m.requests, "Session", FakeSession)
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)
    return seen


# ------------------------------------------------------- provider 注册表 / 配置

def test_default_providers_cover_all_four_sources():
    # 电影版实测四家都有贡献，默认全开；顺序即下载侧的节点尝试顺序
    assert m.DEFAULT_PROVIDERS == ["vidup", "videasy", "vidlink", "vidfast"]
    assert set(m.PROVIDERS) == {"vidup", "videasy", "vidlink", "vidfast"}


def test_resolve_providers_accepts_comma_string_and_dedupes():
    # config 误写成字符串时不能被逐字符拆成 'v','i','d'...
    assert m._resolve_providers("vidup,vidlink") == ["vidup", "vidlink"]
    assert m._resolve_providers(["vidup", "vidup", "vidlink"]) == ["vidup", "vidlink"]


def test_resolve_providers_rejects_unknown_and_empty():
    with pytest.raises(SystemExit):
        m._resolve_providers(["nope"])
    with pytest.raises(SystemExit):
        m._resolve_providers([])


# ------------------------------------------------------------- 白名单判死语义

def test_is_retriable_only_nosource_is_fatal():
    # 这是本轮最关键的语义改动：旧版靠 "404" in msg 判死，会把 tt0404xxx、
    # 源站 5xx 页面文案、enc-dec 自身 404 全部误判成真无源而永久丢片。
    assert m._is_retriable(m.HttpStatusError(404, "enc-vidup")) is True
    assert m._is_retriable(Exception("All servers failed ... 404 ...")) is True
    assert m._is_retriable(RuntimeError("stream timeout")) is True
    assert m._is_retriable(m.NoSource("page 404")) is False


def test_http_status_error_carries_structured_status():
    err = m.HttpStatusError(503, "stream", "boom")
    assert err.status == 503 and err.where == "stream"


# -------------------------------------------------------------- 各 provider

def test_vidup_collects_all_server_urls_and_dedupes(monkeypatch):
    _install_fake_session(monkeypatch, vidup_streams=[
        {"url": "https://a/m.m3u8", "title": "T"},
        {"url": "https://b/m.m3u8"},
        {"url": "https://a/m.m3u8"},  # 重复 url 应被去掉
    ])
    status, result = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "ok"
    assert [u["url"] for u in result["urls"]] == ["https://a/m.m3u8", "https://b/m.m3u8"]
    assert all(u["type"] == "m3u8" and u["provider"] == "vidup" for u in result["urls"])
    assert result["title"] == "T"


def test_vidup_page_404_is_nosource(monkeypatch):
    _install_fake_session(monkeypatch, vidup_page_status=404)
    status, _ = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "dead"


def test_vidup_all_stream_404_is_nosource(monkeypatch):
    # 全部 server 的 stream 接口 404 == 该片真无源
    _install_fake_session(monkeypatch, vidup_streams=[404, 404])
    status, _ = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "dead"


def test_vidup_partial_stream_5xx_is_retriable(monkeypatch):
    # 不是全部 404，就不能判死——哪怕一个 server 是 5xx 也要留给重试
    _install_fake_session(monkeypatch, vidup_streams=[404, 500])
    status, _ = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "retry"


def test_vidlink_returns_mp4_entries_sorted_by_quality(monkeypatch):
    _install_fake_session(monkeypatch, vidlink_resp=_FakeResp(200, {
        "stream": {"qualities": {
            "720": {"url": "https://cdn/720.mp4", "size": 700},
            "1080": {"url": "https://cdn/1080.mp4", "size": 1500},
            "480": {"url": "https://cdn/480.mp4", "size": True},  # bool 不是合法 size
        }},
    }))
    status, result = m.process_tmdb_id("42", providers=["vidlink"])
    assert status == "ok"
    assert [u["quality"] for u in result["urls"]] == [1080, 720, 480]
    assert all(u["type"] == "mp4" for u in result["urls"])
    # vidlink CDN 只认 okhttp UA 且不能带 Referer，头必须随 url 一起落盘
    assert result["urls"][0]["headers"] == {"User-Agent": "okhttp/4.9.3"}
    assert result["urls"][0]["size"] == 1500
    # size=true 是 bool（int 子类），必须被判为非法而不是 1
    assert result["urls"][2]["size"] is None


def test_vidlink_null_body_is_nosource(monkeypatch):
    _install_fake_session(monkeypatch, vidlink_resp=_FakeResp(200, None, "null"))
    status, _ = m.process_tmdb_id("42", providers=["vidlink"])
    assert status == "dead"


@pytest.mark.parametrize("bad_quality", ["0", "-1"])
def test_vidlink_non_positive_quality_becomes_none(monkeypatch, bad_quality):
    """源站偶发 0/-1 画质。留着会让下游 meets_resolution_redline 把这个节点
    判定性淘汰，白丢一个可用流；归 None 表示未声明、交由实测。"""
    _install_fake_session(monkeypatch, vidlink_resp=_FakeResp(200, {
        "stream": {"qualities": {bad_quality: {"url": "https://cdn/x.mp4"}}}}))
    status, result = m.process_tmdb_id("42", providers=["vidlink"])
    assert status == "ok"
    assert result["urls"][0]["quality"] is None


def test_vidlink_404_is_retriable_not_dead(monkeypatch):
    # 判死必须保守：404 可能是路由变更/enc 异常/WAF，只有 200+null 才是无源证据
    _install_fake_session(monkeypatch, vidlink_resp=_FakeResp(404, text="nope"))
    status, _ = m.process_tmdb_id("42", providers=["vidlink"])
    assert status == "retry"


def test_videasy_extracts_media_urls(monkeypatch):
    _install_fake_session(monkeypatch, videasy_sources=_FakeResp(200, text="PAYLOAD"))
    status, result = m.process_tmdb_id("42", providers=["videasy"])
    assert status == "ok"
    assert result["urls"][0]["provider"] == "videasy"
    assert result["urls"][0]["type"] == "m3u8"


@pytest.mark.parametrize("url,expected", [
    ("https://a/x.m3u8", "m3u8"),
    ("https://a/x.mp4", "mp4"),
    # 带签名参数的直链：只看 ? 之前的路径，否则整串比对会漏判
    ("https://cdn/v.mp4?sign=abc&t=1", "mp4"),
    ("https://cdn/v.m3u8?token=x", "m3u8"),
    ("https://a/X.MP4", "mp4"),          # 大小写
    ("https://a/x.mp4#frag", "mp4"),
    ("https://a/weird", "m3u8"),         # 认不出时保守当 m3u8
])
def test_media_type_is_detected_from_url(url, expected):
    assert m._media_type_of(url) == expected


def test_videasy_mp4_url_is_not_mislabeled_as_m3u8(monkeypatch):
    """_find_media_urls 的正则同时匹配 .m3u8 与 .mp4。若把 mp4 直链标成 m3u8，
    下游会拿它去解析 HLS playlist，必然失败。"""
    _install_fake_session(monkeypatch, videasy_sources=_FakeResp(200, text="PAYLOAD"))
    monkeypatch.setattr(m, "_find_media_urls",
                        lambda _: ["https://cdn/movie.mp4?sign=x", "https://cdn/movie.m3u8"])
    status, result = m.process_tmdb_id("42", providers=["videasy"])
    assert status == "ok"
    assert [u["type"] for u in result["urls"]] == ["mp4", "m3u8"]


def test_videasy_no_streams_500_is_nosource(monkeypatch):
    # 源站用 500 + "No streams available" 表达"没有这部片"，等价于 404
    _install_fake_session(
        monkeypatch,
        videasy_sources=_FakeResp(500, text="No streams available"),
    )
    status, _ = m.process_tmdb_id("42", providers=["videasy"])
    assert status == "dead"


def test_find_media_urls_walks_nested_structures():
    payload = {"sources": [{"file": "https://a/x.m3u8"}],
               "extra": {"deep": ["https://b/y.mp4", "https://a/x.m3u8"]}}
    assert m._find_media_urls(payload) == ["https://a/x.m3u8", "https://b/y.mp4"]


# ------------------------------------------------------------------ 多源汇总

def test_urls_from_all_providers_are_merged_in_order(monkeypatch):
    """多源的核心价值：把各家的 url 全部汇总成候选节点列表，
    下载侧逐个尝试，某家画质/码率不达标还能落到下一家。"""
    _install_fake_session(
        monkeypatch,
        vidup_streams=[{"url": "https://up/m.m3u8", "title": "T"}],
        vidlink_resp=_FakeResp(200, {
            "stream": {"qualities": {"1080": {"url": "https://link/a.mp4"}}}}),
        videasy_sources=_FakeResp(200, text="PAYLOAD"),
    )
    status, result = m.process_tmdb_id("42", providers=["vidup", "videasy", "vidlink"])
    assert status == "ok"
    # 顺序 = providers 顺序，下载侧据此决定节点尝试次序
    assert [u["provider"] for u in result["urls"]] == ["vidup", "videasy", "vidlink"]


def test_one_provider_ok_is_enough_even_if_others_fail(monkeypatch):
    # vidup 无源、videasy 无源，只要 vidlink 给出 url 就算成功
    _install_fake_session(
        monkeypatch,
        vidup_page_status=404,
        vidlink_resp=_FakeResp(200, {
            "stream": {"qualities": {"1080": {"url": "https://link/a.mp4"}}}}),
    )
    status, result = m.process_tmdb_id("42", providers=["vidup", "vidlink"])
    assert status == "ok"
    assert [u["provider"] for u in result["urls"]] == ["vidlink"]


def test_any_transient_prevents_dead(monkeypatch):
    """一家明确无源 + 一家瞬时错误 → 必须重试而不是判死。
    否则源站抖动会把有源片永久写进 fail.txt。"""
    _install_fake_session(
        monkeypatch,
        vidup_page_status=404,                       # 明确无源
        vidlink_resp=_FakeResp(500, text="oops"),    # 瞬时错误
    )
    status, _ = m.process_tmdb_id("42", providers=["vidup", "vidlink"])
    assert status == "retry"


def test_all_providers_nosource_is_dead(monkeypatch):
    _install_fake_session(monkeypatch, vidup_page_status=404)  # 其余三家默认即无源
    status, _ = m.process_tmdb_id("42", providers=["vidup", "videasy", "vidlink", "vidfast"])
    assert status == "dead"


# ---------------------------------------------------------------- 判死二次确认

def test_dead_confirm_probes_twice_before_giving_up(monkeypatch):
    seen = _install_fake_session(monkeypatch, vidup_page_status=404)
    monkeypatch.setattr(m, "DEAD_CONFIRM", True)
    status, _ = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "dead"
    # 首次无源只算"疑似"，换 IP 再完整探一次，两次都无源才判死
    assert seen["order"].count("vidup") == 2


def test_dead_confirm_disabled_dies_on_first_hit(monkeypatch):
    seen = _install_fake_session(monkeypatch, vidup_page_status=404)
    monkeypatch.setattr(m, "DEAD_CONFIRM", False)
    status, _ = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "dead"
    assert seen["order"].count("vidup") == 1


# ------------------------------------------------------------------ 结果契约

def test_result_uses_input_id_and_carries_fetched_at(monkeypatch):
    _install_fake_session(monkeypatch, vidup_streams=[
        {"url": "https://a/m.m3u8", "tmdbId": "999", "title": "T"},
    ])
    status, result = m.process_tmdb_id("42", providers=["vidup"])
    assert status == "ok"
    # key 恒用入参：源站返回的 tmdbId 不可信，下游据此拼文件名与 R2 对象键
    assert result["tmdbId"] == "42"
    assert isinstance(result["fetched_at"], int)


def test_metadata_cannot_override_identity_fields(monkeypatch):
    """静态元数据是按 tmdb_id 查表合并进来的。若它哪天多出 tmdbId/urls 之类的键，
    直接 update 会把本片身份改掉，成品会静默传到错误的 R2 路径且极难发现。"""
    _install_fake_session(monkeypatch, vidup_streams=[{"url": "https://a/m.m3u8"}])
    monkeypatch.setitem(m._MOVIE_META, "42", {
        "year": 1999, "tmdbId": "hacked", "urls": ["hacked"], "title": "hacked",
        "fetched_at": 0,
    })
    _, result = m.process_tmdb_id("42", providers=["vidup"])
    assert result["tmdbId"] == "42"
    assert result["urls"][0]["url"] == "https://a/m.m3u8"
    assert result["year"] == 1999          # 非身份字段正常合并
    assert result["fetched_at"] != 0


def test_url_entry_shape_matches_downstream_contract():
    entry = m._url_entry("https://a/x.mp4", "vidlink", "mp4",
                         {"User-Agent": "okhttp/4.9.3"}, 1080, 123)
    assert set(entry) == {"url", "provider", "type", "headers", "quality", "size"}


# ------------------------------------------------- fail.txt 与 unresolved.txt

def test_unresolved_ids_are_not_treated_as_processed(tmp_path):
    """多轮跑满的 ID 落在 unresolved.txt，绝不能被当成已处理而跳过。

    load_processed_ids 只认 results（成功过）与 fail（确认真无源）两个来源；
    unresolved 里的 ID 从未被判过 NoSource，下次运行必须自动重试。
    """
    results = tmp_path / "results.jsonl"
    fail = tmp_path / "fail.txt"
    unresolved = tmp_path / "unresolved.txt"

    results.write_text('{"tmdbId": "11"}\n', encoding="utf-8")
    fail.write_text("22\n", encoding="utf-8")
    unresolved.write_text("33\n", encoding="utf-8")

    processed = m.load_processed_ids(results, fail)
    assert processed == {"11", "22"}
    assert "33" not in processed


def test_write_unresolved_overwrites_and_clears(tmp_path):
    """该文件描述"最近一次运行结束时仍未解决的 ID"，故必须覆盖写、且能清空。

    若只在非空时才写，上次运行的残留会一直留在文件里骗人说它们还没解决。
    """
    path = tmp_path / "unresolved.txt"

    m.write_unresolved(path, ["7", "8"])
    assert path.read_text(encoding="utf-8").split() == ["7", "8"]

    # 本次全部捞回 -> 文件应被清空，而不是保留上次的 7/8
    m.write_unresolved(path, [])
    assert path.read_text(encoding="utf-8") == ""


def test_write_unresolved_failure_does_not_raise(tmp_path):
    """写不进去只是丢一份给人看的清单，不该让整轮取流的成果白费。"""
    # 传目录路径，open(..., 'w') 必然抛 IsADirectoryError（OSError 子类）
    m.write_unresolved(tmp_path, ["1"])
