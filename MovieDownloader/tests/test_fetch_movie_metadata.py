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


# ------------------------------------------------------- 进度一致性

@pytest.fixture
def io_paths(tmp_path, monkeypatch):
    out = tmp_path / "movies.jsonl"
    prog = tmp_path / "progress.txt"
    monkeypatch.setattr(f, "OUTPUT", out)
    monkeypatch.setattr(f, "PROGRESS", prog)
    return out, prog


def test_write_jsonl_marks_progress_in_same_step(io_paths):
    out, prog = io_paths
    f.write_jsonl({"imdb_id": "tt0000001", "primary_title": "A"})
    assert json.loads(out.read_text().splitlines()[0])["imdb_id"] == "tt0000001"
    assert prog.read_text().splitlines() == ["tt0000001"]


def test_write_jsonl_rejects_nan(io_paths):
    out, prog = io_paths
    with pytest.raises(ValueError):
        f.write_jsonl({"imdb_id": "tt1", "rating": float("nan")})
    assert not out.exists() and not prog.exists()


def test_load_done_backfills_from_jsonl(io_paths):
    """jsonl 里有、progress 里没有的 id 必须被补记，否则重跑会重复查 TMDB 并重复追加。"""
    out, prog = io_paths
    prog.write_text("tt1\n")
    out.write_text(
        json.dumps({"imdb_id": "tt1"}) + "\n"
        + json.dumps({"imdb_id": "tt2"}) + "\n"
        + json.dumps({"imdb_id": "tt3", "primary_title": "有 \"imdb_id\": 字样"}) + "\n"
    )
    done = f.load_done()
    assert done == {"tt1", "tt2", "tt3"}
    assert prog.read_text().splitlines() == ["tt1", "tt2", "tt3"]
    # 再次加载不应重复补记
    assert f.load_done() == {"tt1", "tt2", "tt3"}
    assert prog.read_text().splitlines() == ["tt1", "tt2", "tt3"]


def test_load_done_without_progress_rebuilds_from_jsonl(io_paths):
    """progress.txt 丢失时可从 movies.jsonl 完整重建，补跑不会重复已抓过的条目。"""
    out, prog = io_paths
    out.write_text("".join(json.dumps({"imdb_id": f"tt{i}"}) + "\n" for i in range(5)))
    assert f.load_done() == {f"tt{i}" for i in range(5)}
    assert prog.exists() and len(prog.read_text().splitlines()) == 5


def test_load_done_empty(io_paths):
    assert f.load_done() == set()


# ------------------------------------------------------- 滑动窗口提交

def test_main_uses_bounded_submission(monkeypatch, io_paths, tmp_path):
    """一次只提交 MAX_WORKERS*4 个 future：几十万条时不把全部 future 常驻内存。"""
    import pandas as pd

    n = 50
    monkeypatch.setattr(f, "MAX_WORKERS", 2)  # window = 8
    monkeypatch.setattr(f, "ensure_dataset", lambda key: None)
    monkeypatch.setattr(f, "build_index", lambda: None)
    monkeypatch.setattr(f, "load_basics",
                        lambda: pd.DataFrame(index=[f"tt{i}" for i in range(n)]))
    monkeypatch.setattr(f, "load_ratings", lambda: None)
    monkeypatch.setattr(f, "load_crew", lambda: None)
    monkeypatch.setattr(f, "load_names_dict", lambda: {})

    processed = []
    monkeypatch.setattr(f, "process", lambda iid, *a: (processed.append(iid) or True))

    peak = {"n": 0}
    real_submit = f.ThreadPoolExecutor.submit
    live = {"n": 0}
    lock = __import__("threading").Lock()

    def counting_submit(self, fn, *args, **kw):
        with lock:
            live["n"] += 1
            peak["n"] = max(peak["n"], live["n"])

        def wrapped(*a, **k):
            try:
                return fn(*a, **k)
            finally:
                with lock:
                    live["n"] -= 1
        return real_submit(self, wrapped, *args, **kw)

    monkeypatch.setattr(f.ThreadPoolExecutor, "submit", counting_submit)
    f.main()
    assert sorted(processed) == sorted(f"tt{i}" for i in range(n))
    assert peak["n"] <= f.MAX_WORKERS * 4 + 1


# ------------------------------------------------------- 标题 NaN 守卫

def _basics_tsv(tmp_path, rows):
    header = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
    path = tmp_path / "title.basics.tsv"
    path.write_text(header + "".join("\t".join(r) + "\n" for r in rows), encoding="utf-8")
    return path


def test_load_basics_keeps_na_like_titles(monkeypatch, tmp_path):
    """pandas 默认把 "NA"/"None"/"null"/"nan" 当缺失；它们都是真实存在的片名，
    只有 IMDB 自己的 `\\N` 才是缺失。"""
    path = _basics_tsv(tmp_path, [
        ("tt1", "movie", "NA", "N/A", "0", "2000", "\\N", "90", "Drama"),
        ("tt2", "movie", "None", "null", "0", "2001", "\\N", "\\N", "\\N"),
        ("tt3", "movie", "nan", "NaN", "1", "\\N", "\\N", "\\N", "\\N"),
        ("tt4", "movie", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"),
    ])
    monkeypatch.setattr(f, "ensure_dataset", lambda key: path)
    df = f.load_basics()
    assert df.loc["tt1", "primaryTitle"] == "NA"
    assert df.loc["tt1", "originalTitle"] == "N/A"
    assert df.loc["tt2", "primaryTitle"] == "None"
    assert df.loc["tt2", "originalTitle"] == "null"
    assert df.loc["tt3", "primaryTitle"] == "nan"
    assert df.loc["tt3", "originalTitle"] == "NaN"
    # 真缺失仍是 NaN；数值列 / isAdult 语义不变
    assert f._num(df.loc["tt4", "primaryTitle"], str) is None
    assert f._num(df.loc["tt2", "runtimeMinutes"], int) is None
    assert f._num(df.loc["tt1", "runtimeMinutes"], int) == 90
    assert f._num(df.loc["tt3", "isAdult"], bool) is True
    assert f._num(df.loc["tt4", "isAdult"], bool) is None
    assert df.loc["tt2", "genres"] == []


def test_process_writes_record_when_title_missing(monkeypatch, io_paths, tmp_path):
    """标题为 `\\N` 的条目必须能落盘为 null，而不是被 allow_nan=False 拒绝后
    每次运行都重查 TMDB 并反复失败。"""
    import pandas as pd

    out, prog = io_paths
    path = _basics_tsv(tmp_path, [
        ("tt9", "movie", "\\N", "\\N", "0", "1999", "\\N", "\\N", "\\N"),
    ])
    monkeypatch.setattr(f, "ensure_dataset", lambda key: path)
    basics = f.load_basics()
    empty = pd.DataFrame(index=pd.Index([], name="tconst"))
    monkeypatch.setattr(f, "get_tmdb_id", lambda iid: 123)
    monkeypatch.setattr(f.time, "sleep", lambda *_: None)
    monkeypatch.setattr(f, "query_principals", lambda *a: [])
    monkeypatch.setattr(f, "query_akas", lambda *a: [])

    assert f.process("tt9", basics, empty, empty, {}) == "tt9"
    rec = json.loads(out.read_text(encoding="utf-8").strip())
    assert rec["primary_title"] is None
    assert rec["original_title"] is None
    assert rec["start_year"] == 1999
    assert prog.read_text(encoding="utf-8").strip() == "tt9"


# ------------------------------------------------------- ensure_dataset 清理

def test_ensure_dataset_cleans_gz_when_decompress_fails(monkeypatch, tmp_path):
    """解压中途失败：.part 与完整 gz 都不能残留，下次运行才不会误判。"""
    monkeypatch.setattr(f, "DATA_DIR", tmp_path)

    class _Resp:
        raw = __import__("io").BytesIO(b"not-really-gzip")
        def raise_for_status(self): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(f.requests, "get", lambda *a, **k: _Resp())
    with pytest.raises(Exception):
        f.ensure_dataset("ratings")
    assert list(tmp_path.iterdir()) == []


# ------------------------------------------------------- _resolve

def test_resolve_expands_user_and_keeps_absolute(monkeypatch):
    from pathlib import Path

    home = Path.home()
    assert f._resolve("~/x", "d") == home / "x"
    assert f._resolve("/abs/x", "d") == Path("/abs/x")
    assert f._resolve(" rel ", "d") == f._SCRIPT_DIR / "rel"
    assert f._resolve("", "d") == f._SCRIPT_DIR / "d"
    assert f._resolve(None, "d") == f._SCRIPT_DIR / "d"
