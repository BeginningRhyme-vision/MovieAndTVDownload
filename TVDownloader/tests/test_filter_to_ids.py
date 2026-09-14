"""Offline unit tests for filter_to_ids.py (rule semantics + end-to-end main())."""

import json
import re

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


def test_total_episodes_missing_uses_keep_if_missing():
    """🔴 IMDB 无分集数据时 total_episodes 是 None（不是 0），必须走 keep_if_missing。

    上游 query_episodes 保证：有分集数据时 total_episodes >= 1，查不到时为 None。
    所以 0 这个取值在产出里根本不会出现，而 None 必须与 total_seasons 同样处理 ——
    否则同一部剧会被两个字段判出相反结论（total_seasons 保留、total_episodes 排除），
    把"IMDB 没数据但 TMDB 有"的剧静默筛掉。
    """
    rule = {"min": 1, "keep_if_missing": True}
    assert f.check_total_episodes(_show(total_episodes=None), rule) is True
    assert f.check_total_seasons(_show(total_seasons=None), rule) is True
    # 两个字段在"缺失"这件事上必须给出一致结论
    assert (f.check_total_episodes(_show(total_episodes=None), rule)
            is f.check_total_seasons(_show(total_seasons=None), rule))
    # keep_if_missing=False 时同样一致地排除
    strict = {"min": 1, "keep_if_missing": False}
    assert f.check_total_episodes(_show(total_episodes=None), strict) is False
    assert f.check_total_seasons(_show(total_seasons=None), strict) is False


def test_missing_episode_data_survives_a_min_based_filter():
    """🔴 跨模块契约：上游"无分集数据"的产出，下游必须原样保留。

    这条把生产者(query_episodes)与消费者(check_total_*)绑在一起 —— 单独测任一侧
    都发现不了语义漂移：上游改回 0 时本用例会红，下游改坏 keep_if_missing 也会红。
    用的是 query_episodes 的**真实返回值**，不是手写的字面量。
    """
    import fetch_tv_metadata as meta

    # 直接取上游在"查不到分集"时的真实产出形状
    empty = {"total_seasons": None, "total_episodes": None, "episodes": []}
    import inspect
    source = inspect.getsource(meta.query_episodes)
    assert '"total_episodes": None' in source, \
        "query_episodes 的无数据分支必须返回 None，否则下游 min 筛选会误杀"

    show = _show(**empty)
    # 典型配置：只限上限、不限下限 —— 这类剧必须活下来
    assert f.check_total_episodes(show, {"min": None, "max": 200,
                                         "keep_if_missing": True}) is True
    # 即便有人设了 min，keep_if_missing 也该兜住它
    assert f.check_total_episodes(show, {"min": 1, "max": 200,
                                         "keep_if_missing": True}) is True
    # 整条规则链跑一遍，确认不会被任何一项静默淘汰
    config = {
        "total_seasons": {"enabled": True, "min": 1, "max": 10,
                          "keep_if_missing": True},
        "total_episodes": {"enabled": True, "min": 1, "max": 200,
                           "keep_if_missing": True},
    }
    assert f.passes_all(show, config) is True


# ---------------------------------------------------------------- 脏数据守卫
@pytest.mark.parametrize("junk", ["9.5", "abc", [], {}, True, False])
def test_numeric_rule_treats_junk_as_missing(junk):
    """🔴 数值字段里的脏数据必须按缺失处理，不能崩。

    tv_series.jsonl 是人可编辑的文本文件。字符串与 min/max 比较会 TypeError，
    而 main 的 except 只删临时文件就 raise —— 读到第几行崩就死在第几行，
    ids.txt 一个字节都产不出来。9.7 万行里混进一行脏数据就能让整批筛选失败。

    bool 单列：它是 int 子类，不拦的话 True 会当成 1 去比范围（实测 True < 10000
    返回 False，即被静默淘汰），而 True 出现在 votes 里只可能是脏数据。
    """
    rule = {"min": 1, "max": 10, "keep_if_missing": True}
    assert f.check_rating({"rating": junk}, rule) is True
    assert f.check_votes({"votes": junk}, rule) is True
    # keep_if_missing=False 时一致地排除，而不是去比大小
    strict = {"min": 1, "max": 10, "keep_if_missing": False}
    assert f.check_rating({"rating": junk}, strict) is False


def test_main_survives_a_dirty_row(tmp_path, monkeypatch):
    """端到端：一行脏数据不能让整批筛选颗粒无收。

    只测 _check_numeric 不够 —— 本 bug 的真实伤害是 main 整个挂掉，
    干净的剧也跟着一个都出不来。
    """
    series = tmp_path / "tv.jsonl"
    series.write_text(
        json.dumps(_show(tmdb_id=100, rating=9.5)) + "\n"
        + json.dumps(_show(imdb_id="tt8", tmdb_id=300, rating="N/A")) + "\n"
        + json.dumps(_show(imdb_id="tt9", tmdb_id=400, rating=9.1)) + "\n",
        encoding="utf-8")
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(yaml.safe_dump(
        {"rating": {"enabled": True, "min": 9.0, "keep_if_missing": False}}),
        encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")

    f.main()

    # 脏的那条按 keep_if_missing=False 排除，干净的两条照常产出
    assert (tmp_path / "ids.txt").read_text().split() == ["100", "400"]


# ---------------------------------------------------------------- title_type / is_adult
def test_title_type_allow():
    rule = {"allow": ["tvSeries", "tvMiniSeries"]}
    assert f.check_title_type(_show(title_type="tvSeries"), rule)
    assert not f.check_title_type(_show(title_type="tvSpecial"), rule)
    assert not f.check_title_type(_show(title_type=None), rule)


def test_title_type_empty_allow_passes_everything():
    """🔴 allow 留空 = 不设限，绝不能理解成"全部排除"。

    配置文件里 genres.include / directors.include / title_keywords.include
    统统是"留空 [] 表示不做包含限制"。allow 若反着来，同一个文件里就有两套
    相反的空列表语义 —— 用户注释掉 allow 想临时放开筛选，实际把整个 ids.txt
    清空了，而下游 tv_ids_to_links 读到零个 id 会**正常退出**，全链路无一句报错。
    """
    for empty in ({"allow": []}, {"allow": None}, {}):
        assert f.check_title_type(_show(title_type="tvSeries"), empty) is True
        # 连平时会被 allow 挡掉的类型也放行，才叫"不设限"
        assert f.check_title_type(_show(title_type="tvSpecial"), empty) is True
        assert f.check_title_type(_show(title_type=None), empty) is True


def test_empty_allow_does_not_silently_empty_ids(tmp_path, monkeypatch):
    """端到端锁死：allow 为空跑完整条 main，ids.txt 必须非空。

    单测 check_title_type 只能证明函数返回 True；这条证明"配置写成这样时
    产出文件真的不是空的"—— 静默清空正是本 bug 唯一的可观测症状。
    """
    series = tmp_path / "tv.jsonl"
    series.write_text(
        json.dumps(_show(tmdb_id=100, title_type="tvSeries")) + "\n"
        + json.dumps(_show(imdb_id="tt9", tmdb_id=400, title_type="tvSpecial")) + "\n",
        encoding="utf-8")
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(yaml.safe_dump({"title_type": {"enabled": True, "allow": []}}),
                   encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")

    f.main()

    assert (tmp_path / "ids.txt").read_text().split() == ["100", "400"]


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


@pytest.mark.parametrize("use_regex", [False, True])
def test_title_keywords_ignores_non_string_entries(use_regex):
    """🔴 YAML 里 `include: [2024]` 解析成 int，不过滤就崩。

    子串分支会 AttributeError('int' has no attribute 'lower')，
    正则分支会 TypeError(first argument must be string) —— 两条路都要拦。
    非字符串项被跳过后，列表若因此变空，语义就是"本项不设限"，应放行。
    """
    rule = {"include": [2024, None, {"a": 1}], "use_regex": use_regex}
    assert f.check_title_keywords(_show(), rule) is True
    # 混合列表：合法的那个仍要照常生效
    mixed = {"include": [2024, "breaking"], "use_regex": use_regex}
    assert f.check_title_keywords(_show(), mixed) is True
    assert f.check_title_keywords(_show(primary_title="Other",
                                        original_title="Other"), mixed) is False


def test_invalid_regex_fails_fast_before_reading_any_data(tmp_path, monkeypatch):
    """🔴 无效正则必须在**打开输入文件之前**退出。

    ⚠️ 这条用例的第一版只断言 "抛 SystemExit + 没留下 .part"，反向验证时
    把 main 里的预编译整段删掉后**依然全绿** —— 因为 check_title_keywords
    里还有懒编译回退，它抛的也是 SystemExit，而 main 的 except 又会把 .part
    删干净，两种时机的可观测结果完全一样。

    改成让 SERIES 指向一个目录：open() 会抛 IsADirectoryError。
    预编译在前 → 先拿到 SystemExit；预编译在后 → 先拿到 IsADirectoryError。
    异常类型直接把先后顺序钉死。
    """
    unreadable = tmp_path / "tv.jsonl"
    unreadable.mkdir()                      # exists() 为真，但 open() 必炸
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(yaml.safe_dump({"title_keywords": {
        "enabled": True, "use_regex": True, "include": ["["]}}), encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", unreadable)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")

    with pytest.raises(SystemExit) as exc:
        f.main()
    assert "title_keywords.include" in str(exc.value)
    # 顺带确认：输出文件一个都没建，旧的 ids.txt 不会被覆盖或清空
    assert not (tmp_path / "ids.txt").exists()
    assert not (tmp_path / "ids.txt.part").exists()


def test_regex_is_compiled_once_not_per_show():
    """预编译的另一半价值：hit() 拿到的是 Pattern 对象，不是每部剧现调 re.search。

    9.7 万部剧 × 2 个标题 × N 个关键词，现编译纯属白烧。
    """
    rule = {"include": [r"^break\w+ bad$"], "use_regex": True}
    f._compile_keywords(rule)
    assert all(isinstance(p, re.Pattern) for p in rule["_include_re"])
    assert rule["_exclude_re"] == []
    assert f.check_title_keywords(_show(), rule) is True


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
    # 'enable' 触发两条互补的提示：缺 enabled（后果）+ 子键无效（原因，点名拼错的键）
    assert len(problems) == 3
    out = capsys.readouterr().out
    assert "'vote'" in out and "未知筛选项" in out
    assert "'votes'" in out and "缺少 enabled" in out
    assert "'enable'" in out and "无效" in out
    assert "'rating'" not in out
    # clean config -> silent
    assert f.warn_unknown_rules({"rating": {"enabled": False}}) == []
    assert capsys.readouterr().out == ""


def test_warn_unknown_rules_tolerates_non_string_keys(capsys):
    # YAML `2000:` yields an int key; mixed int/str keys must not TypeError in sorted()
    problems = f.warn_unknown_rules({2000: {"enabled": True}, "rating": {"enabled": True}})
    assert problems == ["未知筛选项 2000（将被忽略）"]
    assert "2000" in capsys.readouterr().out


# ---------------------------------------------------------------- 子键拼写守卫
def test_warn_unknown_rules_catches_misspelled_subkeys(capsys):
    """🔴 项内子键拼错是三类拼写错误里最危险的一种：静默**放宽**。

    `min` 写成 `mim` 时 rule.get("min") 返回 None → _in_range 视作"该侧不设限"
    → 筛选形同虚设。而日志照常打印"已启用的筛选项: rating"，看起来一切正常，
    等发现时垃圾已经下了一堆。顶层键拼错至少还会被原有的检查拦下。
    """
    problems = f.warn_unknown_rules({"rating": {"enabled": True, "mim": 9.0}})
    assert len(problems) == 1
    out = capsys.readouterr().out
    assert "'rating'" in out and "'mim'" in out
    # 提示里要列出本项可用的子键，用户才知道该改成什么
    assert "min" in out and "max" in out and "keep_if_missing" in out


def test_subkey_guard_is_per_rule_not_global(capsys):
    """白名单必须按项区分：allow 只对 title_type 合法，min 只对数值项合法。

    用一张全局大表会放过 `title_type: {min: 1}` 这种串项的写法。
    """
    assert f.warn_unknown_rules({"title_type": {"enabled": True, "allow": ["tvSeries"]}}) == []
    capsys.readouterr()
    # allow 用在数值项上 -> 报错
    assert len(f.warn_unknown_rules({"rating": {"enabled": True, "allow": [1]}})) == 1
    capsys.readouterr()
    # min 用在 title_type 上 -> 报错
    assert len(f.warn_unknown_rules({"title_type": {"enabled": True, "min": 1}})) == 1
    capsys.readouterr()
    # exclude 只对 genres / title_keywords 合法，对 directors 非法
    assert f.warn_unknown_rules({"genres": {"enabled": True, "exclude": ["News"]}}) == []
    capsys.readouterr()
    assert len(f.warn_unknown_rules({"directors": {"enabled": True, "exclude": ["X"]}})) == 1


def test_subkey_guard_ignores_runtime_cache_keys(capsys):
    """_compile_keywords 会把编译结果塞进 rule（_include_re / _exclude_re）。

    main 里预编译发生在 warn_unknown_rules 之后，但 rule 是同一个 dict 对象，
    任何一方调整顺序都可能让缓存键流进检查。下划线前缀必须豁免，
    否则用户会收到一条自己根本没写过的"无效子键"警告。
    """
    rule = {"enabled": True, "use_regex": True, "include": [r"^x$"]}
    f._compile_keywords(rule)
    assert "_include_re" in rule           # 确认缓存键确实被写进去了
    assert f.warn_unknown_rules({"title_keywords": rule}) == []
    assert capsys.readouterr().out == ""


def test_rule_keys_covers_every_key_the_checks_actually_read():
    """🔴 防漂移：RULE_KEYS 必须与 check_* 里实际读的 rule.get(...) 一致。

    这张白名单是手写的，跟生产逻辑没有强制关联 —— 以后给某项加个新参数
    却忘了同步这里，用户正确填写的配置反而会收到"无效子键"警告，
    比不做检查更糟。这条用例扫源码把两者绑死。
    """
    import inspect

    for name, check in f.CHECKS.items():
        source = inspect.getsource(check)
        # check_* 多数是转调 _check_numeric / _check_person 的一行包装，要跟进去
        for helper in ("_check_numeric", "_check_person"):
            if helper in source:
                source += inspect.getsource(getattr(f, helper))
        used = set(re.findall(r'rule\.get\(["\'](\w+)["\']', source))
        used.discard("enabled")            # 对所有项通用，不进白名单
        declared = f.RULE_KEYS[name]
        assert used <= declared, (
            f"{name}: check 里读了 {sorted(used - declared)}，但 RULE_KEYS 没声明")
        assert declared <= used, (
            f"{name}: RULE_KEYS 声明了 {sorted(declared - used)}，但 check 从不读")


def test_misspelled_subkey_silently_widens_the_filter(tmp_path, monkeypatch, capsys):
    """端到端：坐实"静默放宽"这个危害，而不只是验警告文案。

    没有警告时，3.0 分的剧会跟 9.5 分的一起被选中，而日志显示筛选已启用。
    """
    series = tmp_path / "tv.jsonl"
    series.write_text(
        json.dumps(_show(tmdb_id=100, rating=9.5)) + "\n"
        + json.dumps(_show(imdb_id="tt9", tmdb_id=400, rating=3.0)) + "\n",
        encoding="utf-8")
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(yaml.safe_dump({"rating": {"enabled": True, "mim": 9.0}}),
                   encoding="utf-8")
    monkeypatch.setattr(f, "SERIES", series)
    monkeypatch.setattr(f, "CONFIG", cfg)
    monkeypatch.setattr(f, "OUTPUT_IDS", tmp_path / "ids.txt")
    monkeypatch.setattr(f, "OUTPUT_DETAIL", tmp_path / "filtered.jsonl")

    f.main()

    # 行为本身不变（保持向后兼容），但用户必须被告知
    assert (tmp_path / "ids.txt").read_text().split() == ["100", "400"]
    out = capsys.readouterr().out
    assert "'mim'" in out, "筛选被静默放宽却没有任何提示"


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


def test_person_rules_warn_about_their_footgun_default():
    """🔴 directors/writers 的默认值是 include=[] + keep_if_missing=false，
    只要把 enabled 改成 true（启用任何筛选项的标准动作），本项就退化成
    "按有没有导演信息筛选" —— 有导演全放行、没导演全排除，跟任何意图都不符。
    而 IMDB 的剧集恰恰是"很多剧为空"的重灾区。

    这不是代码 bug（行为本身是各子键语义的正确组合），只能靠注释预警。
    用例锁住这段注释，防止后人整理配置时顺手删掉。
    """
    text = f.CONFIG.read_text(encoding="utf-8")
    for section in ("directors", "writers"):
        head = text.split(f"\n{section}:")[0].rsplit("# ---", 2)[-2]
        assert "⚠️" in head and "include" in head, \
            f"{section} 缺少启用前的 include 警告注释"

    # 同时确认默认值确实是那个危险组合 —— 哪天默认值改了，这条注释就该重写
    cfg = yaml.safe_load(open(f.CONFIG, encoding="utf-8"))
    for section in ("directors", "writers"):
        rule = cfg[section]
        assert rule["include"] == [] and rule["keep_if_missing"] is False
        rule = {k: v for k, v in rule.items() if k != "enabled"}
        assert f.check_directors({"directors": ["Someone"]}, rule) is True
        assert f.check_directors({"directors": []}, rule) is False
