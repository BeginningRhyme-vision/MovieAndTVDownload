"""Offline unit tests for filter_to_ids.py (rule semantics + end-to-end main())."""

import json

import pytest
import yaml

import filter_to_ids as f


def _show(**kw):
    base = {
        "imdb_id": "tt1", "tmdb_id": 100, "title_type": "tvSeries",
        "primary_title": "Breaking Bad", "original_title": "Breaking Bad",
        "is_adult": False, "start_year": 2008, "end_year": 2013,
        "runtime_minutes": 49, "rating": 9.5, "votes": 2000000,
        "total_seasons": 5, "total_episodes": 62,
        "genres": ["Crime", "Drama", "Thriller"],
        "directors": ["Vince Gilligan"], "writers": ["Vince Gilligan"],
        "episodes": [{"episode_imdb_id": "tt2", "season": 1, "episode": 1}],
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------- rule_enabled
def test_rule_enabled_semantics():
    assert f.rule_enabled({}, "rating") is None
    assert f.rule_enabled({"rating": {"enabled": False}}, "rating") is None
    assert f.rule_enabled({"rating": "junk"}, "rating") is None
    assert f.rule_enabled({"rating": {"enabled": True, "min": 7}}, "rating") == {"enabled": True, "min": 7}


# ---------------------------------------------------------------- numeric rules
@pytest.mark.parametrize("field", [
    "start_year", "end_year", "runtime_minutes", "rating", "votes",
    "total_seasons", "total_episodes",
])
def test_numeric_rule_range_and_missing(field):
    check = f.CHECKS[field]
    assert check(_show(**{field: 5}), {"min": 1, "max": 10}) is True
    assert check(_show(**{field: 0}), {"min": 1, "max": 10}) is False
    assert check(_show(**{field: 11}), {"min": 1, "max": 10}) is False
    assert check(_show(**{field: 11}), {"min": 1, "max": None}) is True
    assert check(_show(**{field: None}), {"min": 1}) is True           # default keep
    assert check(_show(**{field: None}), {"min": 1, "keep_if_missing": False}) is False


def test_total_episodes_zero_is_not_missing():
    # total_episodes == 0 must go through range check, not the keep_if_missing branch
    assert f.check_total_episodes(_show(total_episodes=0), {"min": 1, "keep_if_missing": True}) is False


# ---------------------------------------------------------------- title_type / is_adult
def test_title_type_allow():
    rule = {"allow": ["tvSeries", "tvMiniSeries"]}
    assert f.check_title_type(_show(title_type="tvSeries"), rule)
    assert not f.check_title_type(_show(title_type="tvSpecial"), rule)
    assert not f.check_title_type(_show(title_type=None), rule)
    assert not f.check_title_type(_show(), {"allow": []})


def test_is_adult():
    assert not f.check_is_adult(_show(is_adult=True), {"exclude_adult": True})
    assert f.check_is_adult(_show(is_adult=True), {"exclude_adult": False})
    assert f.check_is_adult(_show(is_adult=None), {})


# ---------------------------------------------------------------- genres
def test_genres_include_exclude_case_insensitive():
    rule = {"include": ["drama"], "exclude": ["reality-tv"], "case_insensitive": True}
    assert f.check_genres(_show(genres=["Drama"]), rule)
    assert not f.check_genres(_show(genres=["Drama", "Reality-TV"]), rule)
    assert not f.check_genres(_show(genres=["Comedy"]), rule)
    assert f.check_genres(_show(genres=[]), rule)
    assert not f.check_genres(_show(genres=None), {**rule, "keep_if_missing": False})
    # case-sensitive: lowercase include no longer matches
    assert not f.check_genres(_show(genres=["Drama"]), {**rule, "case_insensitive": False})


# ---------------------------------------------------------------- people
def test_directors_writers():
    rule = {"include": ["vince gilligan"]}
    assert f.check_directors(_show(), rule)
    assert not f.check_directors(_show(directors=["Someone Else"]), rule)
    assert f.check_directors(_show(), {"include": []})            # empty include -> pass
    assert not f.check_directors(_show(directors=[]), rule)      # missing defaults to drop
    assert f.check_writers(_show(writers=[]), {"include": ["x"], "keep_if_missing": True})


# ---------------------------------------------------------------- title_keywords
def test_title_keywords_substring_and_regex():
    assert f.check_title_keywords(_show(), {"include": ["breaking"]})
    assert not f.check_title_keywords(_show(), {"include": ["breaking"], "case_insensitive": False})
    assert not f.check_title_keywords(_show(), {"exclude": ["bad"]})
    assert f.check_title_keywords(_show(original_title="Отчаянные"), {"include": ["отчаян"]})
    assert f.check_title_keywords(_show(), {"include": [r"^break\w+ bad$"], "use_regex": True})
    assert f.check_title_keywords(_show(primary_title=None, original_title=None), {"exclude": ["x"]})


# ---------------------------------------------------------------- passes_all
def test_passes_all_only_enabled_rules_apply():
    cfg = {
        "rating": {"enabled": True, "min": 9.0},
        "votes": {"enabled": False, "min": 10**9},   # disabled -> ignored
    }
    assert f.passes_all(_show(), cfg)
    assert not f.passes_all(_show(rating=8.0), cfg)
    assert f.passes_all(_show(), {})


# ---------------------------------------------------------------- main() end-to-end
def test_main_end_to_end(tmp_path, monkeypatch, capsys):
    series = tmp_path / "tv_series.jsonl"
    rows = [
        _show(imdb_id="tt1", tmdb_id=100),
        _show(imdb_id="tt2", tmdb_id=None),                       # no tmdb_id -> skipped
        _show(imdb_id="tt3", tmdb_id=100),                        # duplicate tmdb_id
        _show(imdb_id="tt4", tmdb_id=200, rating=5.0),            # filtered out by rating
        _show(imdb_id="tt5", tmdb_id=300, genres=["Reality-TV"]), # filtered out by genre
        _show(imdb_id="tt6", tmdb_id=400, rating=None),           # missing rating kept
    ]
    with open(series, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
        fh.write("\n{not json}\n")   # blank + malformed lines must be tolerated

    cfg = tmp_path / "filter_config.yaml"
    cfg.write_text(yaml.safe_dump({
        "rating": {"enabled": True, "min": 7.0, "keep_if_missing": True},
        "genres": {"enabled": True, "exclude": ["Reality-TV"]},
    }), encoding="utf-8")

    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")

    f.main()

    ids = (tmp_path / "ids.txt").read_text().split()
    assert ids == ["100", "400"]

    detail = [json.loads(l) for l in (tmp_path / "filtered.jsonl").read_text().splitlines()]
    assert [d["imdb_id"] for d in detail] == ["tt1", "tt6"]
    assert all("episodes" not in d for d in detail)          # bulky list stripped
    assert all("total_episodes" in d for d in detail)        # totals kept

    out = capsys.readouterr().out
    assert "读取 6 条" in out and "无 tmdb_id 跳过 1" in out and "重复跳过 1" in out and "最终选中 2" in out


def test_paths_resolved_relative_to_script_dir():
    assert f.SERIES.is_absolute() and f.SERIES.parent == f._SCRIPT_DIR
    assert f.CONFIG.parent == f._SCRIPT_DIR


def test_shipped_filter_config_is_consistent_with_checks():
    """Every rule in filter_config.yaml must map to a CHECKS entry and be disabled by default."""
    cfg = yaml.safe_load(open(f.CONFIG, encoding="utf-8"))
    assert set(cfg) == set(f.CHECKS)
    assert all(rule.get("enabled") is False for rule in cfg.values())
