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


# ---------------------------------------------------------------- query_episodes
def test_query_episodes_empty(episode_db):
    out = m.query_episodes("tt0000000", _make_ratings([]))
    assert out == {"total_seasons": None, "total_episodes": 0, "episodes": []}


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
    def __init__(self, payload):
        import io
        self.raw = io.BytesIO(payload)

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
