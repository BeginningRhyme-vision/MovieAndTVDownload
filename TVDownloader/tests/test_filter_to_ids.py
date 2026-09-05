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
    # P2-A: the malformed line must be surfaced, not silently dropped
    assert "有 1 行无法解析为 JSON" in out
    # P3-D: no leftover temp files after a successful run
    assert not (tmp_path / "ids.txt.part").exists()
    assert not (tmp_path / "filtered.jsonl.part").exists()


def test_main_no_bad_json_no_warning(tmp_path, monkeypatch, capsys):
    series = tmp_path / "tv_series.jsonl"
    series.write_text(json.dumps(_show()) + "\n", encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", tmp_path / "missing.yaml")
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")
    f.main()
    out = capsys.readouterr().out
    assert "无法解析为 JSON" not in out
    assert (tmp_path / "ids.txt").read_text().split() == ["100"]


def test_main_atomic_write_keeps_old_output_on_failure(tmp_path, monkeypatch):
    """P3-D: if processing blows up midway, the previous ids.txt must survive intact
    and no .part files may be left behind."""
    series = tmp_path / "tv_series.jsonl"
    with open(series, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(_show(tmdb_id=1)) + "\n")
        fh.write(json.dumps(_show(tmdb_id=2)) + "\n")
    ids_out = tmp_path / "ids.txt"
    detail_out = tmp_path / "filtered.jsonl"
    ids_out.write_text("999\n", encoding="utf-8")
    detail_out.write_text("old\n", encoding="utf-8")

    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", tmp_path / "missing.yaml")
    monkeypatch.setattr(f, "OUTPUT_IDS", ids_out)
    monkeypatch.setattr(f, "OUTPUT_DETAIL", detail_out)

    calls = {"n": 0}

    def boom(show, config):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated failure")
        return True

    monkeypatch.setattr(f, "passes_all", boom)

    with pytest.raises(RuntimeError):
        f.main()

    assert ids_out.read_text() == "999\n"
    assert detail_out.read_text() == "old\n"
    assert not (tmp_path / "ids.txt.part").exists()
    assert not (tmp_path / "filtered.jsonl.part").exists()


# ---------------------------------------------------------------- P2-B: config typo warning
def test_warn_unknown_rules(capsys):
    cfg = {
        "rating": {"enabled": True, "min": 7},       # fine
        "vote": {"enabled": True, "min": 100},       # typo -> unknown key
        "votes": {"enable": True, "min": 100},       # typo -> missing 'enabled'
        "genres": "junk",                            # non-dict, not flagged for enabled
    }
    problems = f.warn_unknown_rules(cfg)
    assert len(problems) == 2
    out = capsys.readouterr().out
    assert "'vote'" in out and "未知筛选项" in out
    assert "'votes'" in out and "缺少 enabled" in out
    assert "'rating'" not in out
    # clean config -> silent
    assert f.warn_unknown_rules({"rating": {"enabled": False}}) == []
    assert capsys.readouterr().out == ""


def test_warn_unknown_rules_tolerates_non_string_keys(capsys):
    # YAML `2000:` yields an int key; mixed int/str keys must not TypeError in sorted()
    problems = f.warn_unknown_rules({2000: {"enabled": True}, "rating": {"enabled": True}})
    assert problems == ["未知筛选项 2000（将被忽略）"]
    assert "2000" in capsys.readouterr().out


@pytest.mark.parametrize("body, kind", [
    ("- rating\n- votes\n", "list"),
    ("just a string\n", "str"),
    ("42\n", "int"),
])
def test_load_config_rejects_non_mapping_top_level(tmp_path, capsys, body, kind):
    cfg = tmp_path / "filter_config.yaml"
    cfg.write_text(body, encoding="utf-8")
    assert f.load_config(cfg) == {}
    out = capsys.readouterr().out
    assert "顶层应为键值映射" in out and kind in out and "将不做任何筛选" in out


def test_main_survives_non_mapping_filter_config(tmp_path, monkeypatch, capsys):
    series = tmp_path / "tv_series.jsonl"
    series.write_text(json.dumps(_show()) + "\n", encoding="utf-8")
    cfg = tmp_path / "filter_config.yaml"
    cfg.write_text("- rating\n", encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")
    f.main()  # must not raise
    out = capsys.readouterr().out
    assert "顶层应为键值映射" in out and "未启用任何筛选项" in out
    assert (tmp_path / "ids.txt").read_text().split() == ["100"]


def test_main_warns_on_typo_in_config(tmp_path, monkeypatch, capsys):
    series = tmp_path / "tv_series.jsonl"
    series.write_text(json.dumps(_show()) + "\n", encoding="utf-8")
    cfg = tmp_path / "filter_config.yaml"
    cfg.write_text(yaml.safe_dump({"ratting": {"enabled": True, "min": 9.9}}), encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")
    f.main()
    out = capsys.readouterr().out
    assert "未知筛选项 'ratting'" in out
    assert "未启用任何筛选项" in out          # the typo'd rule did not take effect
    assert (tmp_path / "ids.txt").read_text().split() == ["100"]


# ---------------------------------------------------------------- P3-C: paths from config.yaml
def test_paths_resolved_relative_to_script_dir():
    assert f.SERIES.is_absolute() and f.SERIES.parent == f._SCRIPT_DIR
    assert f.CONFIG.parent == f._SCRIPT_DIR
    assert f.OUTPUT_IDS.parent == f._SCRIPT_DIR
    assert f.OUTPUT_DETAIL.parent == f._SCRIPT_DIR


def test_resolve_falls_back_to_default_on_blank():
    assert f._resolve(None, "a.txt") == (f._SCRIPT_DIR / "a.txt").resolve()
    assert f._resolve("", "a.txt") == (f._SCRIPT_DIR / "a.txt").resolve()
    assert f._resolve("  ", "a.txt") == (f._SCRIPT_DIR / "a.txt").resolve()
    assert f._resolve(123, "a.txt") == (f._SCRIPT_DIR / "a.txt").resolve()
    assert f._resolve("sub/b.txt", "a.txt") == (f._SCRIPT_DIR / "sub" / "b.txt").resolve()


def test_shipped_config_yaml_has_filter_to_ids_section_matching_neighbors():
    """config.yaml's filter_to_ids section must exist and stay consistent with the
    upstream (fetch_tv_metadata.output) and downstream (tv_ids_to_links.input) names."""
    full = yaml.safe_load(open(f.CONFIG_PATH, encoding="utf-8"))
    sec = full["filter_to_ids"]
    assert set(sec) == {"input", "filter_config", "output_ids", "output_detail"}
    assert sec["input"] == full["fetch_tv_metadata"]["output"]
    assert sec["output_ids"] == full["tv_ids_to_links"]["input"]
    assert sec["input"] == full["tv_ids_to_links"]["metadata"]
    # module-level paths were actually derived from that section
    assert f.SERIES.name == sec["input"]
    assert f.CONFIG.name == sec["filter_config"]
    assert f.OUTPUT_IDS.name == sec["output_ids"]
    assert f.OUTPUT_DETAIL.name == sec["output_detail"]


def test_shipped_filter_config_is_consistent_with_checks():
    """Every rule in filter_config.yaml must map to a CHECKS entry and be disabled by default."""
    cfg = yaml.safe_load(open(f.CONFIG, encoding="utf-8"))
    assert set(cfg) == set(f.CHECKS)
    assert all(rule.get("enabled") is False for rule in cfg.values())
    assert f.warn_unknown_rules(cfg) == []
