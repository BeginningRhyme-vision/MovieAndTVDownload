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
    # seasons counted only for numbered seasons: {0, 1, 2}
    assert out["total_seasons"] == 3

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


def test_get_tmdb_id_found(fake_find):
    fake_find.script = [{"tv_results": [{"id": 1399}], "movie_results": []}]
    assert m.get_tmdb_id("tt0944947") == 1399


def test_get_tmdb_id_not_in_tmdb_returns_none(fake_find):
    # Genuine "not found" (e.g. tvSpecial landing in movie_results) -> None, caller marks done.
    fake_find.script = [{"tv_results": [], "movie_results": [{"id": 1}]}]
    assert m.get_tmdb_id("tt1") is None


def test_get_tmdb_id_transient_error_then_success(fake_find):
    fake_find.script = [RuntimeError("boom"), {"tv_results": [{"id": 7}]}]
    assert m.get_tmdb_id("tt1") == 7
    assert fake_find.calls == 2


def test_get_tmdb_id_persistent_failure_raises_not_none(fake_find):
    # Must NOT return None: None means "mark done & skip forever" in process().
    fake_find.script = [RuntimeError("down")] * 3
    with pytest.raises(m.TMDBLookupError):
        m.get_tmdb_id("tt1", retry=3)
    assert fake_find.calls == 3


def test_get_tmdb_id_429_does_not_consume_retries(fake_find):
    fake_find.script = [RuntimeError("429 Too Many Requests")] * 4 + [{"tv_results": [{"id": 9}]}]
    assert m.get_tmdb_id("tt1", retry=3) == 9
    assert fake_find.calls == 5


def test_get_tmdb_id_429_forever_eventually_raises(fake_find):
    fake_find.script = [RuntimeError("429")] * 20
    with pytest.raises(m.TMDBLookupError):
        m.get_tmdb_id("tt1")
    assert fake_find.calls < 20  # bounded, not infinite


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
    monkeypatch.setattr(m, "write_jsonl", lambda rec: written.append(rec))
    monkeypatch.setattr(m, "get_tmdb_id", lambda iid: None)
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    assert m.process("tt1", None, None, None, {}) is None
    assert marked == ["tt1"] and written == []


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
