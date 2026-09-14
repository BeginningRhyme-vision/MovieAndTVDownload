"""Offline unit tests for fetch_tv_metadata.py (no network, no real datasets)."""

import sqlite3

import pandas as pd
import pytest

import fetch_tv_metadata as m


# ---------------------------------------------------------------- helpers
def _make_ratings(rows):
    """rows: list of (tconst, averageRating, numVotes)."""
    df = pd.DataFrame(rows, columns=["tconst", "averageRating", "numVotes"])
    return df.set_index("tconst")


@pytest.fixture
def episode_db(monkeypatch):
    """In-memory SQLite with the `episode` table; patched into get_conn()."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(
        "CREATE TABLE episode (tconst TEXT PRIMARY KEY, parentTconst TEXT, "
        "seasonNumber TEXT, episodeNumber TEXT)"
    )
    monkeypatch.setattr(m, "get_conn", lambda: conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------- _to_int_or_none
@pytest.mark.parametrize("value,expected", [
    (None, None),
    ("", None),
    ("\\N", None),
    ("3", 3),
    ("0", 0),
    (7, 7),
    ("abc", None),
    ("1.5", None),
])
def test_to_int_or_none(value, expected):
    assert m._to_int_or_none(value) == expected


# ---------------------------------------------------------------- query_principals ordering
def test_query_principals_orders_numerically_not_lexically(monkeypatch):
    """ordering 列是 TEXT，裸 ORDER BY 是字典序（"10" < "2"），会把第 10 位排到第 2 位前。"""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(
        "CREATE TABLE principals (tconst TEXT, ordering TEXT, nconst TEXT, "
        "category TEXT, job TEXT, characters TEXT)"
    )
    conn.executemany(
        "INSERT INTO principals VALUES (?,?,?,?,?,?)",
        [("tt1", "10", "nm10", "actor", "\\N", "\\N"),
         ("tt1", "2", "nm2", "actor", "\\N", "\\N"),
         ("tt1", "1", "nm1", "actor", "\\N", "\\N")],
    )
    monkeypatch.setattr(m, "get_conn", lambda: conn)
    try:
        got = m.query_principals("tt1", {"nm1": "A", "nm2": "B", "nm10": "C"})
    finally:
        conn.close()
    assert [p["ordering"] for p in got] == ["1", "2", "10"]
    assert [p["name"] for p in got] == ["A", "B", "C"]


# ---------------------------------------------------------------- query_episodes
def test_query_episodes_empty(episode_db):
    """🔴 无分集数据时 total_episodes 必须是 None，不是 0。

    与 total_seasons 用同一套缺失语义。下游 _check_numeric 只对 None 走
    keep_if_missing，给 0 会让它去比 min/max —— 同一部剧被两个字段判出相反结论。
    """
    out = m.query_episodes("tt0000000", _make_ratings([]))
    assert out == {"total_seasons": None, "total_episodes": None, "episodes": []}


def test_query_episodes_never_returns_zero_total(episode_db):
    """锁死不变量：total_episodes 要么是 None，要么 >= 1，永远不会是 0。

    这是"用 None 不丢信息"的前提 —— 有分集数据时 len(episodes) 至少为 1，
    所以 0 只可能来自"查不到"，与 None 表达同一件事。
    """
    # 查不到 -> None
    assert m.query_episodes("tt_nothing", _make_ratings([]))["total_episodes"] is None
    # 有一条 -> 1
    episode_db.execute("INSERT INTO episode VALUES (?,?,?,?)",
                       ("tt1", "ttQ", "1", "1"))
    episode_db.commit()
    assert m.query_episodes("ttQ", _make_ratings([]))["total_episodes"] == 1


def test_query_episodes_sorting_and_none_last(episode_db):
    # Insert out of order, with unnumbered episodes (\N) and season 0 (specials).
    episode_db.executemany(
        "INSERT INTO episode VALUES (?,?,?,?)",
        [
            ("tt2", "ttP", "1", "2"),
            ("tt5", "ttP", "\\N", "\\N"),   # unnumbered -> must sort last
            ("tt1", "ttP", "1", "1"),
            ("tt4", "ttP", "2", "1"),
            ("tt0", "ttP", "0", "1"),       # specials -> first
            ("tt3", "ttP", "1", "\\N"),     # season known, episode unknown
            ("ttX", "ttOther", "1", "1"),   # different show, must be excluded
        ],
    )
    ratings = _make_ratings([("tt1", 8.5, 100), ("tt4", 7.0, 50)])
    out = m.query_episodes("ttP", ratings)

    assert out["total_episodes"] == 6
    # seasons counted only for numbered, non-special seasons: {1, 2}; S0 excluded
    assert out["total_seasons"] == 2

    order = [(e["season"], e["episode"]) for e in out["episodes"]]
    assert order == [(0, 1), (1, 1), (1, 2), (1, None), (2, 1), (None, None)]

    by_id = {e["episode_imdb_id"]: e for e in out["episodes"]}
    assert by_id["tt1"]["rating"] == 8.5 and by_id["tt1"]["votes"] == 100
    assert by_id["tt4"]["rating"] == 7.0 and by_id["tt4"]["votes"] == 50
    # episodes without a rating row -> None, not NaN
    assert by_id["tt2"]["rating"] is None and by_id["tt2"]["votes"] is None
    assert by_id["tt5"]["rating"] is None
    # JSON-native types (not numpy scalars)
    assert type(by_id["tt1"]["rating"]) is float
    assert type(by_id["tt1"]["votes"]) is int
    assert "ttX" not in by_id


def test_query_episodes_only_unnumbered(episode_db):
    episode_db.execute("INSERT INTO episode VALUES ('tt9','ttP','\\N','\\N')")
    out = m.query_episodes("ttP", _make_ratings([]))
    assert out["total_seasons"] is None
    assert out["total_episodes"] == 1
    assert out["episodes"][0]["season"] is None


def test_query_episodes_only_specials_has_no_seasons(episode_db):
    # A show whose IMDB episodes are all in season 0 has 0 "real" seasons -> None,
    # but the specials still count toward total_episodes (they are downloadable).
    episode_db.executemany(
        "INSERT INTO episode VALUES (?,?,?,?)",
        [("tt1", "ttP", "0", "1"), ("tt2", "ttP", "0", "2")],
    )
    out = m.query_episodes("ttP", _make_ratings([]))
    assert out["total_seasons"] is None
    assert out["total_episodes"] == 2
    assert [e["season"] for e in out["episodes"]] == [0, 0]


# ---------------------------------------------------------------- load_basics
def test_load_basics_filters_by_keep_types(monkeypatch, tmp_path):
    tsv = tmp_path / "title.basics.tsv"
    tsv.write_text(
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "tt1\ttvSeries\tA\tA\t0\t2001\t\\N\t45\tDrama,Crime\n"
        "tt2\tmovie\tB\tB\t0\t1999\t\\N\t120\tAction\n"
        "tt3\ttvMiniSeries\tC\tC\t1\t\\N\t2010\t\\N\t\\N\n"
        "tt4\ttvEpisode\tD\tD\t0\t2001\t\\N\t45\tDrama\n"
        "tt1\ttvSeries\tA-dup\tA\t0\t2001\t\\N\t45\tDrama\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "ensure_dataset", lambda key: tsv)
    monkeypatch.setattr(m, "KEEP_TYPES", {"tvSeries", "tvMiniSeries", "tvSpecial", "tvShort"})

    df = m.load_basics()

    assert list(df.index) == ["tt1", "tt3"]          # movie/tvEpisode dropped, dup removed
    assert df.loc["tt1", "primaryTitle"] == "A"       # first occurrence kept
    assert df.loc["tt1", "genres"] == ["Drama", "Crime"]
    assert df.loc["tt3", "genres"] == []
    assert df.loc["tt1", "isAdult"] is False or df.loc["tt1", "isAdult"] == False  # noqa: E712
    assert bool(df.loc["tt3", "isAdult"]) is True
    assert pd.isna(df.loc["tt3", "startYear"]) and int(df.loc["tt3", "endYear"]) == 2010


@pytest.mark.parametrize("title", ["NA", "N/A", "None", "nan", "null", "NULL"])
def test_load_basics_keeps_na_like_titles_as_text(monkeypatch, tmp_path, title):
    """🔴 IMDB **只用 `\\N`** 表示缺失，"NA"/"None"/"null" 都是真实剧名。

    pandas 默认会把这批字符串一并当缺失（keep_default_na=True）。读成 NaN 后
    primary_title 写出 null，下游 filter_to_ids 的 `show.get("primary_title") or ""`
    拿到空串，**按标题筛选的规则对这些剧全部静默失效**——日志上毫无痕迹。
    """
    tsv = tmp_path / "title.basics.tsv"
    tsv.write_text(
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear"
        "\tendYear\truntimeMinutes\tgenres\n"
        f"tt1\ttvSeries\t{title}\t{title}\t0\t2001\t\\N\t45\tDrama\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "ensure_dataset", lambda key: tsv)
    monkeypatch.setattr(m, "KEEP_TYPES", {"tvSeries"})

    df = m.load_basics()
    assert df.loc["tt1", "primaryTitle"] == title
    assert df.loc["tt1", "originalTitle"] == title
    # 真正的缺失标记仍必须变成 NaN，别把 \N 也一起当文本留下
    assert pd.isna(df.loc["tt1", "endYear"])


def test_tsv_kw_is_shared_by_every_reader():
    """三处 read_csv 必须共用同一份参数——各写各的必然漂移。"""
    assert m._TSV_KW["keep_default_na"] is False
    assert m._TSV_KW["na_values"] == "\\N"
    assert m._TSV_KW["quoting"] == m._IMDB_QUOTING

    import inspect
    source = inspect.getsource(m)
    # 除 _TSV_KW 定义本身外，不应再有手写 na_values 的 read_csv
    assert source.count("pd.read_csv") == source.count("**_TSV_KW"), \
        "所有 read_csv 都必须走 _TSV_KW"


# ---------------------------------------------------------------- load_crew (pruned to basics)
def test_load_crew_prunes_to_keep_ids(monkeypatch, tmp_path):
    tsv = tmp_path / "title.crew.tsv"
    tsv.write_text(
        "tconst\tdirectors\twriters\n"
        "tt1\tnm1,nm2\tnm3\n"
        "tt2\tnm9\t\\N\n"        # an episode row: not in basics, must be dropped
        "tt3\t\\N\t\\N\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "ensure_dataset", lambda key: tsv)
    df = m.load_crew(pd.Index(["tt1", "tt3"]))
    assert list(df.index) == ["tt1", "tt3"]
    assert df.loc["tt1", "directors"] == ["nm1", "nm2"] and df.loc["tt1", "writers"] == ["nm3"]
    assert df.loc["tt3", "directors"] == [] and df.loc["tt3", "writers"] == []
    # default (no keep_ids) still loads everything
    assert list(m.load_crew().index) == ["tt1", "tt2", "tt3"]


# ---------------------------------------------------------------- paths
def test_paths_resolved_relative_to_script_dir():
    assert m.DATA_DIR.is_absolute()
    assert m.DATA_DIR.parent == m._SCRIPT_DIR
    assert m.INDEX_DB == m.DATA_DIR / "index.db"
    assert m.OUTPUT.parent == m._SCRIPT_DIR


def test_tmdb_timeout_is_set():
    # tmdbsimple defaults to no timeout; a hung connection would pin a worker forever.
    assert m.tmdb.REQUESTS_TIMEOUT is not None


# ---------------------------------------------------------------- get_tmdb_id
class _FakeFind:
    """Stand-in for tmdb.Find: `script` is a list of callables/values consumed per call."""
    script = []
    calls = 0

    def __init__(self, imdb_id):
        self.imdb_id = imdb_id

    def info(self, **kwargs):
        _FakeFind.calls += 1
        step = _FakeFind.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


@pytest.fixture
def fake_find(monkeypatch):
    monkeypatch.setattr(m.tmdb, "Find", _FakeFind)
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    _FakeFind.script = []
    _FakeFind.calls = 0
    return _FakeFind


def _http_err(status, headers=None, url="https://api.themoviedb.org/3/find/tt1?api_key=SECRET123&external_source=imdb_id"):
    """Mimic what tmdbsimple raises: requests.HTTPError whose message embeds the full URL."""
    import requests
    resp = requests.Response()
    resp.status_code = status
    resp.url = url
    if headers:
        resp.headers.update(headers)
    return requests.HTTPError(f"{status} Client Error for url: {url}", response=resp)


def test_get_tmdb_id_found(fake_find):
    fake_find.script = [{"tv_results": [{"id": 1399}], "movie_results": []}]
    assert m.get_tmdb_id("tt0944947") == (1399, None, None)


def test_get_tmdb_id_movie_only_returns_movie_bucket(fake_find):
    # tvSpecial landing in movie_results: tv None, movie id carried back for the side output.
    fake_find.script = [{"tv_results": [], "movie_results": [{"id": 1}]}]
    assert m.get_tmdb_id("tt1") == (None, 1, None)


def test_get_tmdb_id_episode_bucket_returns_show_id(fake_find):
    # A special filed under some show's "Specials" season: only tv_episode_results is populated.
    fake_find.script = [{"tv_results": [], "movie_results": [],
                         "tv_episode_results": [{"id": 55, "show_id": 1399, "season_number": 0}]}]
    assert m.get_tmdb_id("tt1") == (None, None, 1399)


def test_get_tmdb_id_nothing_found(fake_find):
    fake_find.script = [{"tv_results": [], "movie_results": []}]
    assert m.get_tmdb_id("tt1") == (None, None, None)


def test_get_tmdb_id_404_is_not_found_not_retried(fake_find):
    """404 是 TMDB 对该 id 确定性的"查无"，必须按三桶皆空返回（process 会 mark_done），
    而不是消耗 3 次重试后抛 TMDBLookupError —— 后者会让每次续跑都对同一批 id 白打 3 次。"""
    fake_find.script = [_http_err(404)] * 3
    assert m.get_tmdb_id("tt1", retry=3) == (None, None, None)
    assert fake_find.calls == 1


def test_get_tmdb_id_transient_error_then_success(fake_find):
    fake_find.script = [RuntimeError("boom"), {"tv_results": [{"id": 7}]}]
    assert m.get_tmdb_id("tt1") == (7, None, None)
    assert fake_find.calls == 2


def test_get_tmdb_id_persistent_failure_raises_not_none(fake_find):
    # Must NOT return None: None means "mark done & skip forever" in process().
    fake_find.script = [RuntimeError("down")] * 3
    with pytest.raises(m.TMDBLookupError):
        m.get_tmdb_id("tt1", retry=3)
    assert fake_find.calls == 3


def test_get_tmdb_id_429_does_not_consume_retries(fake_find):
    fake_find.script = [_http_err(429)] * 4 + [{"tv_results": [{"id": 9}]}]
    assert m.get_tmdb_id("tt1", retry=3) == (9, None, None)
    assert fake_find.calls == 5


def test_get_tmdb_id_429_forever_eventually_raises(fake_find):
    fake_find.script = [_http_err(429)] * 20
    with pytest.raises(m.TMDBLookupError):
        m.get_tmdb_id("tt1")
    assert fake_find.calls < 20  # bounded, not infinite


def test_get_tmdb_id_429_is_judged_by_status_not_by_message(fake_find):
    # imdb id containing "429" (tt0429493) must not be mistaken for rate limiting:
    # a plain error carrying that id consumes a retry like any other error.
    fake_find.script = [RuntimeError("tt0429493 boom")] * 3
    with pytest.raises(m.TMDBLookupError):
        m.get_tmdb_id("tt0429493", retry=3)
    assert fake_find.calls == 3


def test_get_tmdb_id_429_honours_retry_after(fake_find, monkeypatch):
    slept = []
    monkeypatch.setattr(m.time, "sleep", lambda s: slept.append(s))
    fake_find.script = [_http_err(429, {"Retry-After": "3"}), {"tv_results": [{"id": 9}]}]
    assert m.get_tmdb_id("tt1") == (9, None, None)
    assert slept == [3]


@pytest.mark.parametrize("status", [401, 403])
def test_get_tmdb_id_auth_error_fails_fast(fake_find, status):
    fake_find.script = [_http_err(status)] * 3
    with pytest.raises(m.TMDBAuthError) as ei:
        m.get_tmdb_id("tt1", retry=3)
    assert fake_find.calls == 1               # no retry: the key is broken, not the request
    assert "SECRET123" not in str(ei.value)   # message must not carry the URL/key
    assert isinstance(ei.value, m.TMDBLookupError)


def test_get_tmdb_id_never_logs_api_key(fake_find, monkeypatch):
    warns = []
    monkeypatch.setattr(m.log, "warning", lambda msg, *a, **k: warns.append(str(msg)))
    fake_find.script = [_http_err(500)] * 3
    with pytest.raises(m.TMDBLookupError) as ei:
        m.get_tmdb_id("tt1", retry=3)
    assert warns and all("SECRET123" not in w for w in warns)
    assert "api_key=***" in warns[0]
    assert "SECRET123" not in str(ei.value)


def test_redact_keeps_exception_type_and_strips_key():
    out = m._redact(_http_err(500))
    assert out.startswith("HTTPError:")
    assert "SECRET123" not in out and "api_key=***" in out
    assert "api_key" not in m._redact(RuntimeError("plain")) and "RuntimeError: plain" == m._redact(RuntimeError("plain"))


# ---------------------------------------------------------------- process: failure must not mark done
def test_process_lookup_error_propagates_without_mark_done(monkeypatch):
    marked = []
    monkeypatch.setattr(m, "mark_done", lambda iid: marked.append(iid))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (_ for _ in ()).throw(m.TMDBLookupError("x")))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    with pytest.raises(m.TMDBLookupError):
        m.process("tt1", None, None, None, {})
    assert marked == []


def test_process_none_marks_done_and_skips(monkeypatch):
    marked, written = [], []
    monkeypatch.setattr(m, "mark_done", lambda iid: marked.append(iid))
    monkeypatch.setattr(m, "commit_record", lambda rec, iid: written.append((rec, iid)))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (None, None, None))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    assert m.process("tt1", None, None, None, {}) == "skip"
    assert marked == ["tt1"] and written == []


def test_process_movie_only_goes_to_side_output_not_mark_done(monkeypatch):
    marked, written, side = [], [], []
    monkeypatch.setattr(m, "mark_done", lambda iid: marked.append(iid))
    monkeypatch.setattr(m, "commit_record", lambda rec, iid: written.append((rec, iid)))
    monkeypatch.setattr(m, "commit_as_movie", lambda iid, mid, tt: side.append((iid, mid, tt)))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (None, 123, None))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    basics = pd.DataFrame({"titleType": ["tvSpecial"]}, index=pd.Index(["tt1"], name="tconst"))
    assert m.process("tt1", basics, None, None, {}) == "as_movie"
    assert side == [("tt1", 123, "tvSpecial")]
    assert marked == [] and written == []  # commit_as_movie owns the progress write


def test_process_episode_of_show_marks_done_without_writing(monkeypatch):
    marked, written, side = [], [], []
    monkeypatch.setattr(m, "mark_done", lambda iid: marked.append(iid))
    monkeypatch.setattr(m, "commit_record", lambda rec, iid: written.append((rec, iid)))
    monkeypatch.setattr(m, "commit_as_movie", lambda iid, mid, tt: side.append((iid, mid, tt)))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (None, None, 1399))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    assert m.process("tt1", None, None, None, {}) == "episode_of_show"
    assert marked == ["tt1"] and written == [] and side == []


def test_process_tv_wins_over_movie_and_episode(monkeypatch):
    written = []
    monkeypatch.setattr(m, "commit_record", lambda rec, iid: written.append(rec))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (10, 20, 30))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    monkeypatch.setattr(m, "query_name", lambda n, d: n)
    monkeypatch.setattr(m, "query_principals", lambda iid, d: [])
    monkeypatch.setattr(m, "query_akas", lambda iid: [])
    monkeypatch.setattr(m, "query_episodes", lambda iid, r: {"total_seasons": 0, "total_episodes": 0, "episodes": []})
    basics = pd.DataFrame({"titleType": ["tvSeries"], "primaryTitle": ["A"], "originalTitle": ["A"],
                           "isAdult": [False], "startYear": [2000], "endYear": [None],
                           "runtimeMinutes": [None], "genres": [["Drama"]]},
                          index=pd.Index(["tt1"], name="tconst"))
    ratings = _make_ratings([])
    crew = pd.DataFrame({"directors": [[]], "writers": [[]]}, index=pd.Index(["ttX"], name="tconst"))
    assert m.process("tt1", basics, ratings, crew, {}) == "ok"
    assert written[0]["tmdb_id"] == 10


def test_process_rating_nan_becomes_none(monkeypatch):
    # ratings are to_numeric(errors="coerce"): a dirty row yields NaN; int(NaN) would raise,
    # float(NaN) would emit invalid JSON. Both must become None.
    written = []
    monkeypatch.setattr(m, "commit_record", lambda rec, iid: written.append(rec))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (10, None, None))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    monkeypatch.setattr(m, "query_name", lambda n, d: n)
    monkeypatch.setattr(m, "query_principals", lambda iid, d: [])
    monkeypatch.setattr(m, "query_akas", lambda iid: [])
    monkeypatch.setattr(m, "query_episodes", lambda iid, r: {"total_seasons": 0, "total_episodes": 0, "episodes": []})
    basics = pd.DataFrame({"titleType": ["tvSeries"], "primaryTitle": ["A"], "originalTitle": ["A"],
                           "isAdult": [False], "startYear": [2000], "endYear": [None],
                           "runtimeMinutes": [None], "genres": [[]]},
                          index=pd.Index(["tt1"], name="tconst"))
    ratings = _make_ratings([("tt1", float("nan"), float("nan"))])
    crew = pd.DataFrame({"directors": [[]], "writers": [[]]}, index=pd.Index(["ttX"], name="tconst"))
    assert m.process("tt1", basics, ratings, crew, {}) == "ok"
    assert written[0]["rating"] is None and written[0]["votes"] is None


def test_process_nan_title_becomes_none_and_json_is_valid(monkeypatch, tmp_path):
    # basics reads "\N" as NaN (numpy float). Without a guard json.dumps emits a bare `NaN`
    # literal, and filter_to_ids' `title or ""` would pass NaN to .lower().
    import json
    out, prog = tmp_path / "out.jsonl", tmp_path / "progress.txt"
    monkeypatch.setattr(m, "OUTPUT", out)
    monkeypatch.setattr(m, "PROGRESS", prog)
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (10, None, None))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    monkeypatch.setattr(m, "query_name", lambda n, d: n)
    monkeypatch.setattr(m, "query_principals", lambda iid, d: [])
    monkeypatch.setattr(m, "query_akas", lambda iid: [])
    monkeypatch.setattr(m, "query_episodes", lambda iid, r: {"total_seasons": None, "total_episodes": 0, "episodes": []})
    basics = pd.DataFrame({"titleType": ["tvSeries"], "primaryTitle": [float("nan")], "originalTitle": ["Orig"],
                           "isAdult": [False], "startYear": [2000], "endYear": [None],
                           "runtimeMinutes": [None], "genres": [[]]},
                          index=pd.Index(["tt1"], name="tconst"))
    basics["primaryTitle"] = basics["primaryTitle"].astype(float)
    ratings = _make_ratings([])
    crew = pd.DataFrame({"directors": [[]], "writers": [[]]}, index=pd.Index(["ttX"], name="tconst"))
    assert m.process("tt1", basics, ratings, crew, {}) == "ok"
    line = out.read_text(encoding="utf-8").strip()
    assert "NaN" not in line
    rec = json.loads(line)
    assert rec["primary_title"] is None and rec["original_title"] == "Orig"


# ---------------------------------------------------------------- commit_as_movie (tsv + progress in one lock)
def test_commit_as_movie_writes_tsv_and_progress(monkeypatch, tmp_path):
    side, prog = tmp_path / "tv_as_movie.tsv", tmp_path / "progress.txt"
    monkeypatch.setattr(m, "AS_MOVIE_OUTPUT", side)
    monkeypatch.setattr(m, "PROGRESS", prog)
    m.commit_as_movie("tt1", 123, "tvSpecial")
    m.commit_as_movie("tt2", 456, None)
    assert side.read_text(encoding="utf-8") == "tt1\t123\ttvSpecial\ntt2\t456\t\n"
    assert prog.read_text(encoding="utf-8").split() == ["tt1", "tt2"]


# ---------------------------------------------------------------- run_pool (bounded window + clean Ctrl+C)
def test_run_pool_counts_all_statuses(monkeypatch):
    monkeypatch.setattr(m.log, "info", lambda *a, **k: None)
    outcomes = {"a": "ok", "b": "as_movie", "c": "skip", "d": "error", "e": "ok", "f": "weird", "g": "episode_of_show"}
    stats = m.run_pool(list(outcomes), lambda i: outcomes[i], max_workers=2, window=2)
    assert stats == {"ok": 2, "as_movie": 1, "episode_of_show": 1, "skip": 1, "error": 2}  # unknown status -> error


def test_run_pool_empty_pending():
    assert m.run_pool([], lambda i: "ok", max_workers=2) == {k: 0 for k in m.STATUS_KEYS}


def test_run_pool_window_bounds_in_flight(monkeypatch):
    import threading
    monkeypatch.setattr(m.log, "info", lambda *a, **k: None)
    lock = threading.Lock()
    active, peak = [0], [0]

    def job(i):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        import time as _t
        _t.sleep(0.005)
        with lock:
            active[0] -= 1
        return "ok"

    stats = m.run_pool(list(range(40)), job, max_workers=8, window=3)
    assert stats["ok"] == 40
    assert peak[0] <= 3  # never more than `window` tasks running, even with 8 workers


def test_run_pool_keyboard_interrupt_cancels_pending_and_reraises(monkeypatch):
    import threading
    infos, warns = [], []
    monkeypatch.setattr(m.log, "info", lambda msg, *a, **k: infos.append(msg))
    monkeypatch.setattr(m.log, "warning", lambda msg, *a, **k: warns.append(msg))
    started = []
    gate = threading.Event()

    def job(i):
        started.append(i)
        gate.wait(1)
        return "ok"

    real_wait = m.wait
    calls = [0]

    def wait_then_interrupt(*a, **k):
        # First poll returns normally; second poll simulates Ctrl+C arriving in the main thread.
        calls[0] += 1
        if calls[0] == 1:
            gate.set()
            return real_wait(*a, **k)
        raise KeyboardInterrupt
    monkeypatch.setattr(m, "wait", wait_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        m.run_pool(list(range(100)), job, max_workers=2, window=2)
    # Only the window's worth of tasks (plus refills before the interrupt) ever started; the
    # remaining ~90 were never submitted, so Ctrl+C returned promptly instead of draining 100.
    assert len(started) < 100
    assert any("提前退出" in w for w in warns)


def test_run_pool_interrupt_mid_accounting_does_not_lose_counts(monkeypatch):
    # Ctrl+C landing while the done_set is being accounted: every finished Future must still be
    # counted (the old code only re-scanned in_flight, dropping the rest of done_set).
    warns = []
    monkeypatch.setattr(m.log, "info", lambda *a, **k: None)
    monkeypatch.setattr(m.log, "warning", lambda msg, *a, **k: warns.append(msg))
    real_wait = m.wait

    def wait_all_done(*a, **k):
        # Return the whole window as done in one go so accounting has several futures to walk.
        from concurrent.futures import ALL_COMPLETED
        k["return_when"] = ALL_COMPLETED
        return real_wait(*a, **k)
    monkeypatch.setattr(m, "wait", wait_all_done)

    n = 6

    def job(i):
        return "ok"

    # Interrupt from inside accounting: patch Future.result of the 2nd accounted future.
    from concurrent import futures as cf
    real_result = cf.Future.result
    hit = [0]

    def result_then_interrupt(self, *a, **k):
        hit[0] += 1
        if hit[0] == 2:
            raise KeyboardInterrupt
        return real_result(self, *a, **k)
    monkeypatch.setattr(cf.Future, "result", result_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        m.run_pool(list(range(n)), job, max_workers=n, window=n)
    # 1 accounted before the interrupt + the remaining 5 recovered in the except branch.
    done_line = [w for w in warns if "提前退出" in w][0]
    assert f"已完成 {n:,}/{n:,}" in done_line
    assert "写入:6" in done_line


def test_run_pool_fatal_job_exception_aborts_and_reraises(monkeypatch):
    # A TMDBAuthError raised by job must escape run_pool (not be counted as 'error' and swallowed).
    warns = []
    monkeypatch.setattr(m.log, "info", lambda *a, **k: None)
    monkeypatch.setattr(m.log, "warning", lambda msg, *a, **k: warns.append(msg))
    started = []

    def job(i):
        started.append(i)
        if i == 0:
            raise m.TMDBAuthError("bad key")
        return "ok"

    with pytest.raises(m.TMDBAuthError):
        m.run_pool(list(range(100)), job, max_workers=1, window=1)
    assert len(started) < 100
    assert any("TMDBAuthError" in w for w in warns)


# ---------------------------------------------------------------- commit_record (jsonl + progress in one lock)
def test_commit_record_writes_both_files(monkeypatch, tmp_path):
    import json
    out, prog = tmp_path / "out.jsonl", tmp_path / "progress.txt"
    monkeypatch.setattr(m, "OUTPUT", out)
    monkeypatch.setattr(m, "PROGRESS", prog)
    import numpy as np
    m.commit_record({"imdb_id": "tt1", "n": np.int64(3), "f": np.float64(1.5), "b": np.bool_(True)}, "tt1")
    m.commit_record({"imdb_id": "tt2"}, "tt2")
    lines = out.read_text(encoding="utf-8").splitlines()
    assert [json.loads(l)["imdb_id"] for l in lines] == ["tt1", "tt2"]
    assert json.loads(lines[0]) == {"imdb_id": "tt1", "n": 3, "f": 1.5, "b": True}
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt1", "tt2"]
    assert m.load_done() == {"tt1", "tt2"}


def test_commit_record_serialization_failure_writes_nothing(monkeypatch, tmp_path):
    out, prog = tmp_path / "out.jsonl", tmp_path / "progress.txt"
    monkeypatch.setattr(m, "OUTPUT", out)
    monkeypatch.setattr(m, "PROGRESS", prog)
    with pytest.raises(TypeError):
        m.commit_record({"bad": object()}, "tt1")
    assert not out.exists() and not prog.exists()


def test_commit_record_is_thread_safe(monkeypatch, tmp_path):
    import json
    from concurrent.futures import ThreadPoolExecutor
    out, prog = tmp_path / "out.jsonl", tmp_path / "progress.txt"
    monkeypatch.setattr(m, "OUTPUT", out)
    monkeypatch.setattr(m, "PROGRESS", prog)
    ids = [f"tt{i:05d}" for i in range(400)]
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda i: m.commit_record({"imdb_id": i, "pad": "x" * 500}, i), ids))
    out_ids = [json.loads(l)["imdb_id"] for l in out.read_text(encoding="utf-8").splitlines()]
    prog_ids = prog.read_text(encoding="utf-8").splitlines()
    # no interleaved/corrupted lines, same order in both files
    assert sorted(out_ids) == sorted(ids)
    assert out_ids == prog_ids


def test_commit_record_rejects_nan(monkeypatch, tmp_path):
    """🔴 兜底防线：裸 NaN 不是合法 JSON，绝不能落盘。

    上游逐字段的 _text/pd.isna 守卫是白名单式的，新增字段漏一个就会写出
    `{"x": NaN}`。下游 filter_to_ids 解析失败后按 bad_json **静默跳过**，
    等于丢掉这条剧集。在这里抛错才能被 job 层记为 error、下次重试。
    """
    out, prog = tmp_path / "out.jsonl", tmp_path / "progress.txt"
    monkeypatch.setattr(m, "OUTPUT", out)
    monkeypatch.setattr(m, "PROGRESS", prog)
    with pytest.raises(ValueError):
        m.commit_record({"imdb_id": "tt1", "rating": float("nan")}, "tt1")
    # 序列化先于开文件：失败时两个文件都不该出现
    assert not out.exists() and not prog.exists()


# ---------------------------------------------------------------- load_done 对账
def _done_env(monkeypatch, tmp_path):
    out = tmp_path / "tv_series.jsonl"
    side = tmp_path / "tv_as_movie.tsv"
    prog = tmp_path / "progress.txt"
    monkeypatch.setattr(m, "OUTPUT", out)
    monkeypatch.setattr(m, "AS_MOVIE_OUTPUT", side)
    monkeypatch.setattr(m, "PROGRESS", prog)
    return out, side, prog


def test_load_done_backfills_ids_written_but_not_marked(monkeypatch, tmp_path):
    """🔴 进程死在"写完 jsonl、未写 progress"之间时，必须以产出文件为准补记。

    不补记的代价：重跑时对这条 id 再查一次 TMDB（配额 + 时间），并在
    tv_series.jsonl 里留下重复记录。
    （ids.txt 本身不会重复——filter_to_ids 有 seen 集合兜底，已实测确认。）
    """
    import json
    out, side, prog = _done_env(monkeypatch, tmp_path)
    out.write_text(
        json.dumps({"imdb_id": "tt1", "tmdb_id": 1}) + "\n"
        + json.dumps({"imdb_id": "tt2", "tmdb_id": 2}) + "\n",
        encoding="utf-8",
    )
    prog.write_text("tt1\n", encoding="utf-8")   # tt2 漏标

    assert m.load_done() == {"tt1", "tt2"}
    # 补记必须落盘，否则下次运行还要再对账一遍
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt1", "tt2"]


def test_load_done_backfills_side_output_too(monkeypatch, tmp_path):
    """旁路 TSV 与 progress 同样是两次独立 write，一样要对账。"""
    out, side, prog = _done_env(monkeypatch, tmp_path)
    side.write_text("tt7\t123\ttvSpecial\ntt8\t456\t\n", encoding="utf-8")
    prog.write_text("tt7\n", encoding="utf-8")

    assert m.load_done() == {"tt7", "tt8"}
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt7", "tt8"]


def test_load_done_ignores_episode_imdb_ids(monkeypatch, tmp_path):
    """🔴 episodes[] 里的 episode_imdb_id 绝不能被当成剧集 id 补记。

    一部长寿剧有上万集，误匹配会把这些分集 id 全写进 progress，让它膨胀几个
    数量级，且 load_done 的返回值不再表达"已处理的剧集"这个语义。

    ⚠️ 把 episodes 摆在 imdb_id **之前**：正常 record 里 imdb_id 是第一个字段，
    `search` 天然先命中它，那样测等于什么也没验证（不依赖字段顺序才是要点）。
    真正提供保护的是模式里的**开引号** —— episode_imdb_id 的 imdb_id 前面紧挨
    的是 `_` 而非 `"`。
    """
    import json
    out, side, prog = _done_env(monkeypatch, tmp_path)
    out.write_text(json.dumps({
        "episodes": [{"episode_imdb_id": "tt9001"},
                     {"episode_imdb_id": "tt9002"}],
        "imdb_id": "tt1",
    }) + "\n", encoding="utf-8")

    assert m.load_done() == {"tt1"}
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt1"]


def test_imdb_id_regex_requires_the_opening_quote():
    """直接锁死那个边界：去掉开引号就会吃进 episode_imdb_id。

    上一条用例走的是 load_done 整条链路，而链路里 imdb_id 恰好也在行内出现，
    单看结果分不清"是边界起了作用"还是"碰巧只匹配到一个"。这里直接对正则
    断言，把保护点本身钉住。
    """
    line = ('{"episodes": [{"episode_imdb_id": "tt9001"}], '
            '"imdb_id": "tt1"}')
    assert m._IMDB_ID_RE.findall(line) == ["tt1"]
    # 反证：没有开引号约束时分集 id 会被一并吃进来
    import re
    loose = re.compile(r'imdb_id":\s*"(tt\d+)"')
    assert loose.findall(line) == ["tt9001", "tt1"]


def test_load_done_no_backfill_leaves_progress_untouched(monkeypatch, tmp_path):
    """两边一致时不该重写 progress（避免每次启动都追加一遍）。"""
    import json
    out, side, prog = _done_env(monkeypatch, tmp_path)
    out.write_text(json.dumps({"imdb_id": "tt1"}) + "\n", encoding="utf-8")
    prog.write_text("tt1\n", encoding="utf-8")

    assert m.load_done() == {"tt1"}
    assert prog.read_text(encoding="utf-8") == "tt1\n"


def test_load_done_handles_missing_files(monkeypatch, tmp_path):
    """首跑：三个文件都不存在，返回空集且不建文件。"""
    out, side, prog = _done_env(monkeypatch, tmp_path)
    assert m.load_done() == set()
    assert not prog.exists()


def test_load_done_does_not_backfill_trailing_partial_line(monkeypatch, tmp_path):
    """🔴 jsonl 尾部半行（进程死在 write 与 close 之间）绝不能被当成已完成。

    半行里的 imdb_id 正则照样抠得出来；若补进 progress，下游 filter_to_ids 会把
    这行按 bad_json 跳过，这部剧就**静默丢失**且永不重试。正确做法是把半行截掉
    （否则下一条 record 会紧接其后，一起变成坏行）并让它下次重跑。
    """
    import json
    out, side, prog = _done_env(monkeypatch, tmp_path)
    good = json.dumps({"imdb_id": "tt1", "tmdb_id": 1}) + "\n"
    partial = '{"imdb_id": "tt2", "tmdb_id": 2, "episodes": [{"episode_imdb'
    out.write_text(good + partial, encoding="utf-8")

    assert m.load_done() == {"tt1"}
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt1"]
    # 半行必须被截掉，文件只剩完整行
    assert out.read_text(encoding="utf-8") == good


def test_load_done_truncates_partial_line_in_progress_and_side_output(monkeypatch, tmp_path):
    """progress.txt / tv_as_movie.tsv 同样是 append 写，尾部半行一样要截掉。
    progress 里的半个 id（如 "tt12" 实际是 "tt123456" 的前缀）若被当成已完成，
    会让一个**不存在**的 id 进入 done 集——无害但脏；真正的 tt123456 仍会重跑。"""
    out, side, prog = _done_env(monkeypatch, tmp_path)
    prog.write_text("tt1\ntt12", encoding="utf-8")
    side.write_text("tt7\t123\ttvSpecial\ntt8\t45", encoding="utf-8")

    assert m.load_done() == {"tt1", "tt7"}
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt1", "tt7"]
    assert side.read_text(encoding="utf-8") == "tt7\t123\ttvSpecial\n"


def test_load_done_skips_non_json_line_in_the_middle(monkeypatch, tmp_path):
    """中间的坏行（以 \\n 结尾但不是合法 JSON）不补记：下游会跳过它，
    不认作已完成才能让这条 id 下次重跑覆盖。"""
    import json
    out, side, prog = _done_env(monkeypatch, tmp_path)
    out.write_text(
        '{"imdb_id": "tt1", "broken\n'
        + json.dumps({"imdb_id": "tt2"}) + "\n",
        encoding="utf-8",
    )
    assert m.load_done() == {"tt2"}
    assert prog.read_text(encoding="utf-8").splitlines() == ["tt2"]


def test_truncate_trailing_partial_line_finds_newline_across_chunks(tmp_path):
    """半行超过一个回退块（64KB）时也要能找到最后一个换行。"""
    p = tmp_path / "x.jsonl"
    good = b'{"imdb_id": "tt1"}\n'
    p.write_bytes(good + b"x" * (200_000))
    assert m._truncate_trailing_partial_line(p) is True
    assert p.read_bytes() == good
    assert m._truncate_trailing_partial_line(p) is False


def test_truncate_trailing_partial_line_whole_file_is_partial(tmp_path):
    """整个文件都是半行（首条记录就没写完）：截成空文件，不报错。"""
    p = tmp_path / "x.jsonl"
    p.write_bytes(b'{"imdb_id": "tt1"')
    assert m._truncate_trailing_partial_line(p) is True
    assert p.read_bytes() == b""
    assert m._truncate_trailing_partial_line(p) is False


# ---------------------------------------------------------------- _ensure_dirs (nested paths)
def test_ensure_dirs_creates_nested_parents(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path / "a" / "b" / "imdb_data")
    monkeypatch.setattr(m, "OUTPUT", tmp_path / "out" / "x" / "tv.jsonl")
    monkeypatch.setattr(m, "PROGRESS", tmp_path / "state" / "progress.txt")
    monkeypatch.setattr(m, "LOG_PATH", tmp_path / "logs" / "y" / "fetch.log")
    m._ensure_dirs()
    assert (tmp_path / "a" / "b" / "imdb_data").is_dir()
    assert (tmp_path / "out" / "x").is_dir()
    assert (tmp_path / "state").is_dir()
    assert (tmp_path / "logs" / "y").is_dir()
    m._ensure_dirs()  # idempotent


# ---------------------------------------------------------------- isAdult dirty values
def test_load_basics_is_adult_dirty_values_never_nan(monkeypatch, tmp_path):
    import json
    tsv = tmp_path / "title.basics.tsv"
    tsv.write_text(
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "tt1\ttvSeries\tA\tA\t0\t2001\t\\N\t45\tDrama\n"
        "tt2\ttvSeries\tB\tB\t1\t2001\t\\N\t45\tDrama\n"
        "tt3\ttvSeries\tC\tC\t\\N\t2001\t\\N\t45\tDrama\n"   # missing
        "tt4\ttvSeries\tD\tD\tfoo\t2001\t\\N\t45\tDrama\n"   # garbage
        "tt5\ttvSeries\tE\tE\t\t2001\t\\N\t45\tDrama\n",     # empty
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "ensure_dataset", lambda key: tsv)
    monkeypatch.setattr(m, "KEEP_TYPES", {"tvSeries"})
    df = m.load_basics()
    assert df["isAdult"].dtype == bool
    assert df["isAdult"].tolist() == [False, True, False, False, False]
    # every value must be JSON-encodable to a real boolean (no NaN literal)
    for iid in df.index:
        s = json.dumps({"is_adult": df.loc[iid].get("isAdult")}, cls=m._Encoder)
        assert s in ('{"is_adult": true}', '{"is_adult": false}')


def test_load_basics_is_adult_float_column_after_na(monkeypatch, tmp_path):
    # When \N is present pandas reads the column as float (0.0/1.0); 1.0 must still map to True.
    tsv = tmp_path / "title.basics.tsv"
    tsv.write_text(
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "tt1\ttvSeries\tA\tA\t1\t2001\t\\N\t45\tDrama\n"
        "tt2\ttvSeries\tB\tB\t\\N\t2001\t\\N\t45\tDrama\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "ensure_dataset", lambda key: tsv)
    monkeypatch.setattr(m, "KEEP_TYPES", {"tvSeries"})
    df = m.load_basics()
    assert df["isAdult"].tolist() == [True, False]


# ---------------------------------------------------------------- ensure_dataset: partial-file protection
def _gz_bytes(text: str) -> bytes:
    import gzip, io
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as g:
        g.write(text.encode())
    return buf.getvalue()


class _FakeResp:
    def __init__(self, payload, headers=None):
        import io
        self.raw = io.BytesIO(payload)
        self.headers = dict(headers or {})

    def raise_for_status(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_ensure_dataset_happy_path_leaves_no_temp_files(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(m.requests, "get", lambda *a, **k: _FakeResp(_gz_bytes("a\tb\n1\t2\n")))
    out = m.ensure_dataset("ratings")
    assert out == tmp_path / "title.ratings.tsv"
    assert out.read_text() == "a\tb\n1\t2\n"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["title.ratings.tsv"]


def test_ensure_dataset_interrupted_decompress_leaves_no_truncated_tsv(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(m.requests, "get", lambda *a, **k: _FakeResp(_gz_bytes("a\tb\n1\t2\n")))

    def boom(src, dst, *a, **k):
        dst.write(b"a\tb\n")  # write a partial tsv, then die
        raise KeyboardInterrupt

    monkeypatch.setattr(m.shutil, "copyfileobj", boom)
    with pytest.raises(KeyboardInterrupt):
        m.ensure_dataset("ratings")
    # Final .tsv must not exist (otherwise next run treats a truncated file as complete)
    assert not (tmp_path / "title.ratings.tsv").exists()
    assert not (tmp_path / "title.ratings.tsv.part").exists()


def test_ensure_dataset_skips_when_tsv_exists(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    (tmp_path / "title.ratings.tsv").write_text("x")
    monkeypatch.setattr(m.requests, "get", lambda *a, **k: pytest.fail("must not download"))
    assert m.ensure_dataset("ratings") == tmp_path / "title.ratings.tsv"


def test_ensure_dataset_never_reuses_a_leftover_gz(monkeypatch, tmp_path):
    """🔴 半截 gz 绝不能被复用，否则形成**永久死循环**。

    网络中途断开时 copyfileobj 不报错（无 Content-Length 校验），半截数据会被
    rename 成看似正常的 .gz。若下次运行复用它，gzip 必抛 EOFError，而 gz 又没人
    清理 —— 每次运行都在同一处失败，人工不介入就再也跑不起来。
    """
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    # 上次运行遗留的半截 gz
    (tmp_path / "title.ratings.tsv.gz").write_bytes(_gz_bytes("a\tb\n1\t2\n")[:40])

    downloaded = []

    def fake_get(*a, **k):
        downloaded.append(1)
        return _FakeResp(_gz_bytes("a\tb\n1\t2\n"))

    monkeypatch.setattr(m.requests, "get", fake_get)
    out = m.ensure_dataset("ratings")

    assert downloaded == [1], "必须重新下载，不能复用遗留的 gz"
    assert out.read_text() == "a\tb\n1\t2\n"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["title.ratings.tsv"]


def test_ensure_dataset_cleans_gz_even_when_decompress_fails(monkeypatch, tmp_path):
    """解压失败也要把 gz 删干净，否则下次运行会带着同一个坏文件再失败一次。"""
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        m.requests, "get", lambda *a, **k: _FakeResp(b"not gzip at all")
    )
    with pytest.raises(Exception):
        m.ensure_dataset("ratings")
    # 目录必须是干净的：没有 gz、没有 .part、没有半截 tsv
    assert list(tmp_path.iterdir()) == []


def test_ensure_dataset_rejects_short_download_by_content_length(monkeypatch, tmp_path):
    """🔴 写入字节数 < Content-Length 必须报错并清场。

    截断恰好落在 gzip 成员边界时解压**不会**报错，只会得到少一截的 tsv，
    然后被永久"已存在跳过"。这里用两个 gzip 成员拼接、只回传第一个来模拟。
    """
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    full = _gz_bytes("a\tb\n1\t2\n") + _gz_bytes("3\t4\n")
    first_member = _gz_bytes("a\tb\n1\t2\n")
    monkeypatch.setattr(
        m.requests, "get",
        lambda *a, **k: _FakeResp(first_member, {"Content-Length": str(len(full))}),
    )
    with pytest.raises(IOError, match="下载不完整"):
        m.ensure_dataset("ratings")
    assert list(tmp_path.iterdir()) == []


def test_ensure_dataset_accepts_matching_content_length(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    payload = _gz_bytes("a\tb\n1\t2\n")
    monkeypatch.setattr(
        m.requests, "get",
        lambda *a, **k: _FakeResp(payload, {"Content-Length": str(len(payload))}),
    )
    assert m.ensure_dataset("ratings").read_text() == "a\tb\n1\t2\n"


def test_ensure_dataset_ignores_content_length_when_transfer_encoded(monkeypatch, tmp_path):
    """Content-Encoding 非 identity 时 r.raw 的字节数与 Content-Length 无关，不能校验。"""
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    payload = _gz_bytes("a\tb\n1\t2\n")
    monkeypatch.setattr(
        m.requests, "get",
        lambda *a, **k: _FakeResp(payload, {"Content-Length": "1", "Content-Encoding": "gzip"}),
    )
    assert m.ensure_dataset("ratings").read_text() == "a\tb\n1\t2\n"


def test_ensure_dataset_ignores_garbage_content_length(monkeypatch, tmp_path):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    payload = _gz_bytes("a\tb\n1\t2\n")
    monkeypatch.setattr(
        m.requests, "get",
        lambda *a, **k: _FakeResp(payload, {"Content-Length": "abc"}),
    )
    assert m.ensure_dataset("ratings").read_text() == "a\tb\n1\t2\n"


# ---------------------------------------------------------------- 资源释放
def test_build_index_closes_connection_on_failure(monkeypatch, tmp_path):
    """🔴 建索引中途失败也要关连接，否则连同**写锁**一起泄漏。

    检测手段用"另开一个连接能否拿到写锁"，而不是数 gc 里的 Connection 对象：
    后者会被 GC 回收掉，改坏生产代码后照样全绿（第一版就是这么写的）。
    这里先让 _build_all_tables 在**持有写事务**时抛错 —— 连接没关的话那个
    写事务一直挂着，第二个连接会拿不到锁。
    """
    import sqlite3
    db = tmp_path / "index.db"
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(m, "INDEX_DB", db)

    def start_write_then_boom(conn):
        # 开一个未提交的写事务，模拟"插到一半炸了"
        conn.execute("CREATE TABLE half (x TEXT)")
        conn.execute("INSERT INTO half VALUES ('uncommitted')")
        raise RuntimeError("数据集下载失败")

    monkeypatch.setattr(m, "_build_all_tables", start_write_then_boom)
    with pytest.raises(RuntimeError):
        m.build_index()

    # 连接若没关，这个写操作会因拿不到锁而超时报 "database is locked"
    probe = sqlite3.connect(db, timeout=0.5)
    try:
        probe.execute("CREATE TABLE probe (x TEXT)")
        probe.commit()
    finally:
        probe.close()


def test_load_names_dict_closes_connection_on_failure(monkeypatch, tmp_path):
    """查询失败（如表不存在）也要关连接。"""
    db = tmp_path / "index.db"
    import sqlite3
    sqlite3.connect(db).close()          # 空库：没有 names 表
    monkeypatch.setattr(m, "INDEX_DB", db)
    with pytest.raises(sqlite3.OperationalError):
        m.load_names_dict()
    # 连接已关则库文件可被立即删除/重建（Windows 上尤其明显，POSIX 下退而验证可重连）
    conn = sqlite3.connect(db)
    conn.close()


# ---------------------------------------------------------------- TMDB 节流
def test_process_throttles_even_when_lookup_fails(monkeypatch):
    """🔴 失败路径也必须 sleep：失败往往正是限速引起的，不节流等于火上浇油。

    get_tmdb_id 内部已对 429 退避，但**非 429 的失败**（超时、5xx、连接重置）
    走的是 `2 ** attempt` 那条路，重试耗尽后直接抛出。此时若 process 不节流，
    几十万条里连续失败的那一段会以最快速度反复冲击 TMDB。
    """
    slept = []
    monkeypatch.setattr(m.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(m, "SLEEP", 0.25)

    def boom(iid):
        raise m.TMDBLookupError("TMDB 查询失败（已重试）")

    monkeypatch.setattr(m, "get_tmdb_id", boom)
    with pytest.raises(m.TMDBLookupError):
        m.process("tt1", None, None, None, {})
    assert slept == [0.25], "失败路径漏掉了节流"


def test_process_throttles_on_success_path(monkeypatch):
    """成功路径当然也要节流（这条原本就有，一并锁住防回归）。"""
    slept = []
    monkeypatch.setattr(m.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(m, "SLEEP", 0.25)
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: (None, None, None))
    monkeypatch.setattr(m, "mark_done", lambda iid: None)
    assert m.process("tt1", None, None, None, {}) == "skip"
    assert slept == [0.25]


# ---------------------------------------------------------------- QUOTE_NONE on IMDB tsv
def test_load_basics_does_not_choke_on_leading_double_quote(monkeypatch, tmp_path):
    # IMDB TSV is unquoted; a title starting with `"` must not swallow following rows.
    tsv = tmp_path / "title.basics.tsv"
    tsv.write_text(
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "tt1\ttvSeries\t\"Weird Al\" Show\t\"Weird Al\" Show\t0\t2001\t\\N\t45\tComedy\n"
        "tt2\ttvSeries\tNext\tNext\t0\t2002\t\\N\t30\tDrama\n"
        "tt3\ttvSeries\t\"Unbalanced\tX\t0\t2003\t\\N\t30\tDrama\n"
        "tt4\ttvSeries\tAfter\tAfter\t0\t2004\t\\N\t30\tDrama\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "ensure_dataset", lambda key: tsv)
    monkeypatch.setattr(m, "KEEP_TYPES", {"tvSeries"})
    df = m.load_basics()
    assert list(df.index) == ["tt1", "tt2", "tt3", "tt4"]
    assert df.loc["tt1", "primaryTitle"] == '"Weird Al" Show'
    assert df.loc["tt3", "primaryTitle"] == '"Unbalanced'
