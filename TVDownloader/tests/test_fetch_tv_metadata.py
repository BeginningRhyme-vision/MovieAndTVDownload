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
