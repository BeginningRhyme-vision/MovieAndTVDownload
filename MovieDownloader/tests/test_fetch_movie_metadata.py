"""fetch_movie_metadata.py 的离线用例。

覆盖从电视版移植过来的四条修复（详见 AGENTS §0 待办与 TVDownloader 的
`f3a6319`），这四条都不是可有可无的健壮性改进：

  ① API Key 经 _redact 脱敏，绝不落进 fetch.log；
  ② TMDB 查询失败与"确实查不到"严格区分，前者绝不 mark_done（静默丢数据）；
  ③ 401/403 快速失败，不对几十万条各自重试；
  ④ 429 按 HTTP 状态码判定，不再字符串匹配（id 里含 429 会误判）。

全部不联网：tmdb.Find 被替换成假对象。
"""

import json

import pytest

import fetch_movie_metadata as f


# ---------------------------------------------------------------- 假 TMDB

class _FakeResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


class _HttpError(Exception):
    """模拟 requests.HTTPError：带 response 属性，消息里含完整 URL。"""

    def __init__(self, status_code, headers=None, message=None):
        super().__init__(message or f"{status_code} Error for url: "
                                    f"https://api.themoviedb.org/3/find/tt1?"
                                    f"api_key=SECRET123&external_source=imdb_id")
        self.response = _FakeResponse(status_code, headers)


def _install_find(monkeypatch, behaviours):
    """behaviours 为可调用列表，按调用次序依次生效；返回记录调用次数的 dict。"""
    calls = {"n": 0}

    class _FakeFind:
        def __init__(self, imdb_id):
            self.imdb_id = imdb_id

        def info(self, **_kwargs):
            index = min(calls["n"], len(behaviours) - 1)
            calls["n"] += 1
            behaviour = behaviours[index]
            if isinstance(behaviour, Exception):
                raise behaviour
            return behaviour

    monkeypatch.setattr(f.tmdb, "Find", _FakeFind)
    monkeypatch.setattr(f.time, "sleep", lambda *_: None)
    return calls


# ------------------------------------------------------- ① api_key 脱敏

def test_redact_hides_api_key():
    """tmdbsimple 的 HTTPError 消息里带完整 URL（含 api_key=），
    原样写进 fetch.log 就等于把密钥落盘。"""
    message = _redacted = f._redact(_HttpError(500))
    assert "SECRET123" not in message
    assert "api_key=***" in _redacted


def test_redact_keeps_exception_type():
    """脱敏不能把排查信息一起抹掉——异常类型要保留。"""
    assert "_HttpError" in f._redact(_HttpError(500))


def test_redact_handles_message_without_api_key():
    exc = ValueError("connection reset by peer")
    assert f._redact(exc) == "ValueError: connection reset by peer"


# --------------------------------------- ② 查询失败 ≠ 查不到（最严重的一条）

def test_lookup_failure_raises_instead_of_returning_none(monkeypatch):
    """🔴 重试耗尽必须抛异常。旧版 `return None` 会被 process 当成"TMDB 没有这部片"
    而 mark_done，一次几分钟的 TMDB 故障就能让那批条目永久跳过、静默丢失。"""
    _install_find(monkeypatch, [_HttpError(500)])
    with pytest.raises(f.TMDBLookupError):
        f.get_tmdb_id("tt0000001", retry=2)


def test_genuinely_absent_returns_none(monkeypatch):
    """TMDB 确实没有 -> None（可以 mark_done 永久跳过），与上面的失败严格区分。"""
    _install_find(monkeypatch, [{"movie_results": []}])
    assert f.get_tmdb_id("tt0000001") is None


def test_successful_lookup_returns_id(monkeypatch):
    _install_find(monkeypatch, [{"movie_results": [{"id": 550}]}])
    assert f.get_tmdb_id("tt0137523") == 550


def test_transient_failure_then_success(monkeypatch):
    """瞬时错误会重试，不该因为第一次失败就放弃。"""
    calls = _install_find(monkeypatch, [
        _HttpError(503),
        {"movie_results": [{"id": 27205}]},
    ])
    assert f.get_tmdb_id("tt1375666") == 27205
    assert calls["n"] == 2


def test_lookup_error_message_is_redacted(monkeypatch):
    """抛出的异常消息也会被记进日志，同样必须脱敏。"""
    _install_find(monkeypatch, [_HttpError(500)])
    with pytest.raises(f.TMDBLookupError) as excinfo:
        f.get_tmdb_id("tt0000001", retry=1)
    assert "SECRET123" not in str(excinfo.value)


# --------------------------------------------------- ③ 401/403 快速失败

@pytest.mark.parametrize("status", [401, 403])
def test_auth_error_fails_fast_without_retry(monkeypatch, status):
    """API Key 失效是全局性问题：必须立刻抛，且只请求一次。
    旧版会对每个 id 老实重试 3 次，几十万条全部空转跑到底。"""
    calls = _install_find(monkeypatch, [_HttpError(status)])
    with pytest.raises(f.TMDBAuthError):
        f.get_tmdb_id("tt0000001", retry=3)
    assert calls["n"] == 1


def test_auth_error_is_a_lookup_error():
    """TMDBAuthError 继承 TMDBLookupError：调用方按 LookupError 兜底时不会漏掉它
    （"没查成"的语义成立，同样不能 mark_done）。"""
    assert issubclass(f.TMDBAuthError, f.TMDBLookupError)


def test_auth_error_message_has_no_api_key(monkeypatch):
    _install_find(monkeypatch, [_HttpError(401)])
    with pytest.raises(f.TMDBAuthError) as excinfo:
        f.get_tmdb_id("tt0000001")
    assert "SECRET123" not in str(excinfo.value)


# ------------------------------------------------ ④ 429 按状态码而非字符串

def test_rate_limit_detected_by_status_not_message(monkeypatch):
    """429 判定必须看 response.status_code。"""
    calls = _install_find(monkeypatch, [
        _HttpError(429, {"Retry-After": "1"}),
        {"movie_results": [{"id": 1}]},
    ])
    assert f.get_tmdb_id("tt0000001") == 1
    assert calls["n"] == 2


def test_imdb_id_containing_429_is_not_mistaken_for_rate_limit(monkeypatch):
    """🔑 旧版 `"429" in str(e)` 的真实误判场景：tt0429493 这类 id 本身含 429，
    一旦报错会被当成限速白等 10 秒，真正的错误信息被吞掉。
    现在按状态码判定：500 就该走重试路径并最终抛 TMDBLookupError。"""
    _install_find(monkeypatch, [
        _HttpError(500, message="500 Server Error for tt0429493"),
    ])
    with pytest.raises(f.TMDBLookupError):
        f.get_tmdb_id("tt0429493", retry=1)


def test_rate_limit_does_not_consume_retry_budget(monkeypatch):
    """限速是外部节奏问题、不是本条目的问题，不该占用重试预算。
    retry=1 时若 429 消耗预算，第二次调用就不会发生。"""
    calls = _install_find(monkeypatch, [
        _HttpError(429, {"Retry-After": "1"}),
        _HttpError(429, {"Retry-After": "1"}),
        {"movie_results": [{"id": 7}]},
    ])
    assert f.get_tmdb_id("tt0000001", retry=1) == 7
    assert calls["n"] == 3


def test_rate_limit_hits_are_capped(monkeypatch):
    """TMDB 长时间限速时不能让线程无限空转，命中上限后放弃。"""
    _install_find(monkeypatch, [_HttpError(429, {"Retry-After": "1"})])
    with pytest.raises(f.TMDBLookupError):
        f.get_tmdb_id("tt0000001", retry=3)


@pytest.mark.parametrize("headers,expected", [
    ({"Retry-After": "5"}, 5),
    ({"Retry-After": "999"}, 60),      # 超上限收敛到 cap
    ({"Retry-After": "0"}, 1),         # 下限至少 1s
    ({"Retry-After": "abc"}, 10),      # 解析失败用 default
    ({}, 10),                          # 缺头用 default
])
def test_retry_after_parsing(headers, expected):
    assert f._retry_after(_HttpError(429, headers)) == expected


def test_http_status_returns_none_for_non_http_errors():
    """超时/连接失败没有 response，不能因此崩掉判定逻辑。"""
    assert f._http_status(ValueError("boom")) is None
    assert f._http_status(_HttpError(429)) == 429


# ------------------------------------------------------- NaN 数值守卫

def test_num_converts_and_guards_nan():
    """pandas 缺失值是 NaN：int(NaN) 抛 ValueError，float(NaN) 会让 json.dumps
    写出裸 NaN 字面量——非法 JSON，下游整行解析失败等于静默丢记录。"""
    import numpy as np

    assert f._num(None, int) is None
    assert f._num(np.nan, int) is None
    assert f._num(float("nan"), float) is None
    assert f._num("1994", int) == 1994
    assert f._num(8.8, float) == pytest.approx(8.8)


def test_num_output_is_json_serialisable():
    """守卫的最终目的：产出的记录必须能被 json.dumps 写成合法 JSON。"""
    import numpy as np

    record = {"start_year": f._num(np.nan, int), "rating": f._num(np.nan, float)}
    assert json.loads(json.dumps(record)) == {"start_year": None, "rating": None}


# --------------------------------------------------------- keep_types 契约

def test_keep_types_includes_tv_movie():
    """tvMovie（电视电影）此前掉在两个 pipeline 的缝里：电影版只收 movie，
    TV 版因它无季集结构而有意排除。它在 TMDB Find 里落 movie_results 桶，
    正是本脚本读取的那个，所以由电影版接收。"""
    assert "movie" in f.KEEP_TYPES
    assert "tvMovie" in f.KEEP_TYPES


def test_filter_allow_list_covers_upstream_keep_types():
    """🔒 跨文件配置契约：filter_config.yaml 的 title_type.allow 必须覆盖上游
    keep_types。两处不一致时，上游抓来的条目会在筛选阶段被无声丢弃——
    该项默认 enabled: false 所以现在不发作，但一旦有人打开就会静默丢片。"""
    import yaml
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "filter_config.yaml"
    rule = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("title_type") or {}
    allow = set(rule.get("allow") or [])
    missing = set(f.KEEP_TYPES) - allow
    assert not missing, f"filter_config.yaml 的 title_type.allow 缺少: {missing}"
