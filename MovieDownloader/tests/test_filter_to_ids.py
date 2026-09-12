"""filter_to_ids.py 的单元测试：单项筛选语义、正则预编译、主流程原子写。"""

import json
import os
import stat

import pytest

import filter_to_ids as f


# ========== 单项筛选 ==========
class TestTitleType:
    def test_empty_allow_passes(self):
        assert f.check_title_type({"title_type": "tvSeries"}, {"allow": []})
        assert f.check_title_type({"title_type": "tvSeries"}, {"allow": None})
        assert f.check_title_type({"title_type": "tvSeries"}, {})

    def test_non_empty_allow_filters(self):
        rule = {"allow": ["movie", "tvMovie"]}
        assert f.check_title_type({"title_type": "movie"}, rule)
        assert f.check_title_type({"title_type": "tvMovie"}, rule)
        assert not f.check_title_type({"title_type": "tvSeries"}, rule)
        assert not f.check_title_type({}, rule)


class TestIsAdult:
    def test_exclude_adult_default(self):
        assert not f.check_is_adult({"is_adult": True}, {})
        assert f.check_is_adult({"is_adult": False}, {})
        assert f.check_is_adult({}, {})

    def test_exclude_disabled(self):
        assert f.check_is_adult({"is_adult": True}, {"exclude_adult": False})


class TestNumeric:
    @pytest.mark.parametrize("value", [None, True, False, "1999", [1999], {"y": 1}])
    def test_non_numeric_uses_keep_if_missing(self, value):
        movie = {"start_year": value}
        assert f.check_start_year(movie, {"min": 1990, "keep_if_missing": True})
        assert not f.check_start_year(movie, {"min": 1990, "keep_if_missing": False})

    def test_range(self):
        rule = {"min": 1990, "max": 2000}
        assert f.check_start_year({"start_year": 1990}, rule)
        assert f.check_start_year({"start_year": 2000}, rule)
        assert not f.check_start_year({"start_year": 1989}, rule)
        assert not f.check_start_year({"start_year": 2001}, rule)

    def test_open_ended(self):
        assert f.check_rating({"rating": 9.9}, {"min": 7.0})
        assert f.check_rating({"rating": 0.1}, {"max": 5})
        assert f.check_votes({"votes": 123}, {})

    def test_float_and_int_mixed(self):
        assert f.check_rating({"rating": 7.5}, {"min": 7, "max": 8})


class TestGenres:
    def test_missing(self):
        assert f.check_genres({"genres": []}, {"include": ["Drama"]})
        assert not f.check_genres({}, {"include": ["Drama"], "keep_if_missing": False})

    def test_include_exclude(self):
        rule = {"include": ["Drama", "Action"], "exclude": ["Horror"]}
        assert f.check_genres({"genres": ["Drama"]}, rule)
        assert not f.check_genres({"genres": ["Comedy"]}, rule)
        assert not f.check_genres({"genres": ["Drama", "Horror"]}, rule)

    def test_case_insensitive(self):
        assert f.check_genres({"genres": ["drama"]}, {"include": ["DRAMA"]})
        assert not f.check_genres(
            {"genres": ["drama"]}, {"include": ["DRAMA"], "case_insensitive": False})

    def test_non_str_entries_ignored(self):
        assert f.check_genres({"genres": [1, None, "Drama"]}, {"include": ["Drama"]})


class TestPerson:
    def test_missing_default_drop(self):
        assert not f.check_directors({"directors": []}, {"include": ["X"]})
        assert f.check_directors({}, {"include": ["X"], "keep_if_missing": True})

    def test_empty_include_passes(self):
        assert f.check_directors({"directors": ["Anyone"]}, {"include": []})
        assert f.check_writers({"writers": ["Anyone"]}, {})

    def test_include_match(self):
        rule = {"include": ["Christopher Nolan"]}
        assert f.check_directors({"directors": ["christopher nolan"]}, rule)
        assert not f.check_directors({"directors": ["Someone Else"]}, rule)


class TestTitleKeywords:
    def test_substring_ci(self):
        rule = {"include": ["star"], "exclude": ["trek"]}
        assert f.check_title_keywords({"primary_title": "Star Wars"}, rule)
        assert not f.check_title_keywords({"primary_title": "Star Trek"}, rule)
        assert not f.check_title_keywords({"primary_title": "Alien"}, rule)

    def test_substring_case_sensitive(self):
        rule = {"include": ["Star"], "case_insensitive": False}
        assert not f.check_title_keywords({"primary_title": "star wars"}, rule)

    def test_matches_original_title(self):
        rule = {"include": ["seven"]}
        assert f.check_title_keywords(
            {"primary_title": "Seven Samurai", "original_title": "七人の侍"}, rule)
        assert f.check_title_keywords(
            {"primary_title": "七人の侍", "original_title": "Seven Samurai"}, rule)

    def test_regex(self):
        rule = {"use_regex": True, "include": [r"^star\s+wars"], "exclude": [r"\bIII\b"]}
        assert f.check_title_keywords({"primary_title": "Star Wars"}, rule)
        assert not f.check_title_keywords({"primary_title": "Star Wars III"}, rule)
        assert not f.check_title_keywords({"primary_title": "The Star Wars"}, rule)
        # 自动预编译并缓存
        assert "_include_re" in rule and "_exclude_re" in rule

    def test_regex_case_sensitive(self):
        rule = {"use_regex": True, "include": ["^Star"], "case_insensitive": False}
        assert not f.check_title_keywords({"primary_title": "star"}, rule)

    def test_non_str_keywords_ignored(self):
        rule = {"include": [None, 1, "star"]}
        assert f.check_title_keywords({"primary_title": "Star"}, rule)
        rule = {"use_regex": True, "include": [None, "star"]}
        assert f.check_title_keywords({"primary_title": "Star"}, rule)

    def test_empty_titles(self):
        rule = {"include": ["x"]}
        assert not f.check_title_keywords({"primary_title": None, "original_title": ""}, rule)


class TestCompileKeywords:
    def test_invalid_regex_exits(self):
        rule = {"use_regex": True, "include": ["(unclosed"]}
        with pytest.raises(SystemExit) as ei:
            f._compile_keywords(rule)
        assert "title_keywords.include" in str(ei.value)
        assert "(unclosed" in str(ei.value)

    def test_invalid_exclude_regex_exits(self):
        rule = {"use_regex": True, "exclude": ["*bad"]}
        with pytest.raises(SystemExit) as ei:
            f._compile_keywords(rule)
        assert "title_keywords.exclude" in str(ei.value)

    def test_no_regex_noop(self):
        rule = {"include": ["(unclosed"]}
        f._compile_keywords(rule)
        assert "_include_re" not in rule


class TestPassesAll:
    def test_disabled_rules_skipped(self):
        config = {"start_year": {"enabled": False, "min": 3000}}
        assert f.passes_all({"start_year": 1999}, config)

    def test_enabled_rule_applies(self):
        config = {"start_year": {"enabled": True, "min": 3000}}
        assert not f.passes_all({"start_year": 1999}, config)

    def test_multiple_rules_all_must_pass(self):
        config = {
            "start_year": {"enabled": True, "min": 1990},
            "rating": {"enabled": True, "min": 8},
        }
        assert f.passes_all({"start_year": 1999, "rating": 8.5}, config)
        assert not f.passes_all({"start_year": 1999, "rating": 7.9}, config)

    def test_rule_not_dict_ignored(self):
        assert f.rule_enabled({"rating": "yes"}, "rating") is None
        assert f.rule_enabled({"rating": {"enabled": True}}, "rating") == {"enabled": True}


# ========== 主流程 ==========
def _write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(r if isinstance(r, str) else json.dumps(r, ensure_ascii=False))
            fh.write("\n")


@pytest.fixture
def env(tmp_path, monkeypatch):
    movies = tmp_path / "movies.jsonl"
    cfg = tmp_path / "filter_config.yaml"
    ids = tmp_path / "ids.txt"
    detail = tmp_path / "filtered.jsonl"
    monkeypatch.setattr(f, "MOVIES", movies)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", ids)
    monkeypatch.setattr(f, "OUTPUT_DETAIL", detail)
    return tmp_path


def test_main_basic(env, capsys):
    _write_jsonl(env / "movies.jsonl", [
        {"tmdb_id": 1, "primary_title": "A", "start_year": 2000},
        {"tmdb_id": 2, "primary_title": "B", "start_year": 1980},
        {"tmdb_id": 1, "primary_title": "A dup", "start_year": 2001},
        {"tmdb_id": None, "primary_title": "no id", "start_year": 2001},
        "",
        "{not json",
        {"tmdb_id": 3, "primary_title": "C", "start_year": 2010},
    ])
    (env / "filter_config.yaml").write_text(
        "start_year:\n  enabled: true\n  min: 1990\n", encoding="utf-8")

    f.main()

    assert (env / "ids.txt").read_text().splitlines() == ["1", "3"]
    detail = [json.loads(l) for l in (env / "filtered.jsonl").read_text().splitlines()]
    assert [d["primary_title"] for d in detail] == ["A", "C"]
    out = capsys.readouterr().out
    assert "1 行 JSON 解析失败" in out
    assert "读取 5 条" in out
    assert "无 tmdb_id 跳过 1" in out
    assert "重复跳过 1" in out
    assert "最终选中 2" in out
    assert not list(env.glob("*.tmp"))


def test_main_no_config_selects_all(env, capsys):
    _write_jsonl(env / "movies.jsonl", [{"tmdb_id": 5}, {"tmdb_id": 6}])
    f.main()
    assert (env / "ids.txt").read_text().splitlines() == ["5", "6"]
    assert "未启用任何筛选项" in capsys.readouterr().out


def test_main_missing_input_exits(env):
    with pytest.raises(SystemExit):
        f.main()


def test_main_output_permissions(env):
    _write_jsonl(env / "movies.jsonl", [{"tmdb_id": 5}])
    f.main()
    mode = stat.S_IMODE(os.stat(env / "ids.txt").st_mode)
    assert mode & 0o044 == 0o044, oct(mode)


def test_main_invalid_regex_keeps_old_output(env):
    _write_jsonl(env / "movies.jsonl", [{"tmdb_id": 5, "primary_title": "x"}])
    (env / "ids.txt").write_text("old\n")
    (env / "filter_config.yaml").write_text(
        "title_keywords:\n  enabled: true\n  use_regex: true\n  include: ['(bad']\n",
        encoding="utf-8")
    with pytest.raises(SystemExit) as ei:
        f.main()
    assert "无效" in str(ei.value)
    assert (env / "ids.txt").read_text() == "old\n"
    assert not list(env.glob("*.tmp"))


def test_main_exception_mid_run_keeps_old_output(env, monkeypatch):
    _write_jsonl(env / "movies.jsonl", [{"tmdb_id": 5}, {"tmdb_id": 6}])
    (env / "ids.txt").write_text("old\n")
    (env / "filtered.jsonl").write_text("olddetail\n")

    calls = {"n": 0}

    def boom(movie, config):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(f, "passes_all", boom)
    with pytest.raises(RuntimeError):
        f.main()
    assert (env / "ids.txt").read_text() == "old\n"
    assert (env / "filtered.jsonl").read_text() == "olddetail\n"
    assert not list(env.glob("*.tmp"))


def test_main_atomic_replace_overwrites_old(env):
    (env / "ids.txt").write_text("old1\nold2\nold3\n")
    _write_jsonl(env / "movies.jsonl", [{"tmdb_id": 9}])
    f.main()
    assert (env / "ids.txt").read_text() == "9\n"


def test_main_closes_handles_when_setup_fails(env, monkeypatch):
    """第二个 mkstemp 抛异常时，已打开的第一个临时文件必须被关闭并删除。"""
    _write_jsonl(env / "movies.jsonl", [{"tmdb_id": 5}])
    opened = []
    real_fdopen = os.fdopen

    def tracking_fdopen(fd, *a, **k):
        fh = real_fdopen(fd, *a, **k)
        opened.append(fh)
        return fh

    real_mkstemp = f.tempfile.mkstemp
    calls = {"n": 0}

    def failing_mkstemp(*a, **k):
        calls["n"] += 1
        if calls["n"] == 2:
            raise OSError("disk full")
        return real_mkstemp(*a, **k)

    monkeypatch.setattr(f.os, "fdopen", tracking_fdopen)
    monkeypatch.setattr(f.tempfile, "mkstemp", failing_mkstemp)
    with pytest.raises(OSError):
        f.main()
    assert len(opened) == 1 and opened[0].closed
    assert not list(env.glob("*.tmp"))


def test_resolve_expands_user_and_keeps_absolute():
    from pathlib import Path

    assert f._resolve("~/x", "d") == Path.home() / "x"
    assert f._resolve("/abs/x", "d") == Path("/abs/x")
    assert f._resolve(" rel ", "d") == f._SCRIPT_DIR / "rel"
    assert f._resolve(None, "d") == f._SCRIPT_DIR / "d"
