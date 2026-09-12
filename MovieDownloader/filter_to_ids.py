"""
按筛选配置从 movies.jsonl 中挑选电影，生成下游需要的 ids.txt。

输入：
    movies.jsonl       —— fetch_movie_metadata.py 的产物，每行一部电影的元数据
    filter_config.yaml —— 筛选配置文件，每个电影元数据字段对应一个筛选开关

输出：
    ids.txt        —— 每行一个 tmdb_id，供 tmdb_ids_to_links.py 消费
    filtered.jsonl —— 通过筛选的电影明细（保留标题/评分等，便于人工复核）

设计原则：
    - 每个筛选项都是独立开关，由各自的 enabled 控制是否生效。
    - 某项 enabled=false 时完全跳过（放行所有电影）。
    - 因此“配置文件里所有开关都关闭” == “不做任何筛选，全部选中”。
    - 数值字段为 null 时的取舍由各项的 keep_if_missing 决定。
    - 流式逐行读取，避免把整个 movies.jsonl 加载进内存。
"""

import json
import os
import re
import tempfile
from contextlib import ExitStack
from numbers import Real
from pathlib import Path
from typing import Optional

import yaml

# ========== 路径配置（从 config.yaml 的 filter_to_ids 段读取）==========
# 与 tmdb_ids_to_links.py / download_movies.py 一致：全部锚定脚本目录，
# 不随进程当前工作目录漂移，避免从别处启动时读写到错误的文件。
_SCRIPT_DIR = Path(__file__).resolve().parent


def _resolve(value, default_name: str) -> Path:
    """相对路径锚定脚本目录；绝对路径与 `~` 开头的路径按原义解析。"""
    raw = value.strip() if isinstance(value, str) else value
    return _SCRIPT_DIR / Path(raw or default_name).expanduser()


def _load_own_config() -> dict:
    """读取 config.yaml 中本脚本对应的段落；缺失时返回空字典（走默认路径）。"""
    path = _SCRIPT_DIR / "config.yaml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("filter_to_ids", {}) or {}


_CFG = _load_own_config()

MOVIES = _resolve(_CFG.get("input"), "movies.jsonl")
# 筛选规则仍单独成文件：项目多、注释长，混进 config.yaml 会喧宾夺主。
CONFIG = _resolve(_CFG.get("filter_config"), "filter_config.yaml")
OUTPUT_IDS = _resolve(_CFG.get("output_ids"), "ids.txt")
OUTPUT_DETAIL = _resolve(_CFG.get("output_detail"), "filtered.jsonl")


# ========== 配置加载 ==========
def load_config(path: Path) -> dict:
    """读取 YAML 筛选配置；文件不存在时返回空配置（等于不筛选）。"""
    if not path.exists():
        print(f"警告: 未找到配置文件 {path}，将不做任何筛选")
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def rule_enabled(config: dict, name: str) -> Optional[dict]:
    """取出某个筛选项；未配置或 enabled 不为 true 时返回 None（表示放行）。"""
    rule = config.get(name)
    if not isinstance(rule, dict):
        return None
    if not rule.get("enabled", False):
        return None
    return rule


# ========== 单项筛选逻辑 ==========
# 约定：每个 check_* 返回 True 表示“通过本项”，False 表示“被本项淘汰”。


def _in_range(value, low, high) -> bool:
    """闭区间判断；low/high 为 None 表示该侧不设限。"""
    if low is not None and value < low:
        return False
    if high is not None and value > high:
        return False
    return True


def check_title_type(movie: dict, rule: dict) -> bool:
    allow = rule.get("allow") or []
    if not allow:
        # 与 _check_person 的 "include 为空即放行" 语义一致；
        # 否则误删 allow 列表会静默清空 ids.txt。
        return True
    return movie.get("title_type") in allow


def check_is_adult(movie: dict, rule: dict) -> bool:
    if not rule.get("exclude_adult", True):
        return True
    return not bool(movie.get("is_adult"))


def _check_numeric(movie: dict, rule: dict, field: str) -> bool:
    value = movie.get(field)
    # bool 是 int 子类，但 True/False 出现在数值字段里只能是脏数据，同样按缺失处理；
    # 字符串等非数值类型若直接与 min/max 比较会 TypeError 拖垮整批。
    if value is None or isinstance(value, bool) or not isinstance(value, Real):
        return bool(rule.get("keep_if_missing", True))
    return _in_range(value, rule.get("min"), rule.get("max"))


def check_start_year(movie: dict, rule: dict) -> bool:
    return _check_numeric(movie, rule, "start_year")


def check_runtime_minutes(movie: dict, rule: dict) -> bool:
    return _check_numeric(movie, rule, "runtime_minutes")


def check_rating(movie: dict, rule: dict) -> bool:
    return _check_numeric(movie, rule, "rating")


def check_votes(movie: dict, rule: dict) -> bool:
    return _check_numeric(movie, rule, "votes")


def _normalize_list(values, case_insensitive: bool) -> list:
    result = []
    for v in values or []:
        if not isinstance(v, str):
            continue
        result.append(v.lower() if case_insensitive else v)
    return result


def check_genres(movie: dict, rule: dict) -> bool:
    ci = rule.get("case_insensitive", True)
    genres = _normalize_list(movie.get("genres") or [], ci)

    if not genres:
        return bool(rule.get("keep_if_missing", True))

    include = _normalize_list(rule.get("include") or [], ci)
    exclude = _normalize_list(rule.get("exclude") or [], ci)

    genre_set = set(genres)
    # include 非空时，至少命中一个才保留
    if include and not (genre_set & set(include)):
        return False
    # exclude 命中任意一个就淘汰
    if exclude and (genre_set & set(exclude)):
        return False
    return True


def _check_person(movie: dict, rule: dict, field: str) -> bool:
    ci = rule.get("case_insensitive", True)
    people = _normalize_list(movie.get(field) or [], ci)

    if not people:
        return bool(rule.get("keep_if_missing", False))

    include = _normalize_list(rule.get("include") or [], ci)
    if not include:
        # include 为空表示本项不设人名限制，直接放行
        return True
    return bool(set(people) & set(include))


def check_directors(movie: dict, rule: dict) -> bool:
    return _check_person(movie, rule, "directors")


def check_writers(movie: dict, rule: dict) -> bool:
    return _check_person(movie, rule, "writers")


def _compile_keywords(rule: dict) -> None:
    """use_regex 时在启动阶段预编译 include/exclude，写错的正则立即报错退出，
    而不是跑到第一条电影才抛 re.error（那时输出文件已被打开）。
    编译结果缓存在 rule["_include_re"] / rule["_exclude_re"]。"""
    if not rule.get("use_regex", False):
        return
    flags = re.IGNORECASE if rule.get("case_insensitive", True) else 0
    for key in ("include", "exclude"):
        compiled = []
        for kw in rule.get(key) or []:
            if not isinstance(kw, str):
                continue
            try:
                compiled.append(re.compile(kw, flags))
            except re.error as e:
                raise SystemExit(
                    f"错误: title_keywords.{key} 中的正则 {kw!r} 无效: {e}"
                )
        rule[f"_{key}_re"] = compiled


def check_title_keywords(movie: dict, rule: dict) -> bool:
    ci = rule.get("case_insensitive", True)
    use_regex = rule.get("use_regex", False)

    titles = [
        movie.get("primary_title") or "",
        movie.get("original_title") or "",
    ]

    def hit(keyword) -> bool:
        for title in titles:
            if not title:
                continue
            if use_regex:
                if keyword.search(title):
                    return True
            else:
                a, b = (title.lower(), keyword.lower()) if ci else (title, keyword)
                if b in a:
                    return True
        return False

    if use_regex:
        if "_include_re" not in rule:
            _compile_keywords(rule)
        include = rule["_include_re"]
        exclude = rule["_exclude_re"]
    else:
        include = [k for k in (rule.get("include") or []) if isinstance(k, str)]
        exclude = [k for k in (rule.get("exclude") or []) if isinstance(k, str)]

    if include and not any(hit(k) for k in include):
        return False
    if exclude and any(hit(k) for k in exclude):
        return False
    return True


# 筛选项名称 -> 检查函数。顺序即执行顺序，任一不通过立即淘汰。
CHECKS = {
    "title_type": check_title_type,
    "is_adult": check_is_adult,
    "start_year": check_start_year,
    "runtime_minutes": check_runtime_minutes,
    "rating": check_rating,
    "votes": check_votes,
    "genres": check_genres,
    "directors": check_directors,
    "writers": check_writers,
    "title_keywords": check_title_keywords,
}


def passes_all(movie: dict, config: dict) -> bool:
    """逐项应用已启用的筛选；全部通过才返回 True。"""
    for name, check in CHECKS.items():
        rule = rule_enabled(config, name)
        if rule is None:
            continue  # 该项未启用，放行
        if not check(movie, rule):
            return False
    return True


# ========== 主流程 ==========
def main():
    if not MOVIES.exists():
        raise SystemExit(f"错误: 找不到 {MOVIES}")

    config = load_config(CONFIG)
    active = [name for name in CHECKS if rule_enabled(config, name) is not None]
    if active:
        print(f"已启用的筛选项: {', '.join(active)}")
    else:
        print("未启用任何筛选项：将选中全部带 tmdb_id 的电影")

    # 正则在启动阶段就编译，配置写错立即退出，不会留下半截输出文件
    kw_rule = rule_enabled(config, "title_keywords")
    if kw_rule is not None:
        _compile_keywords(kw_rule)

    total = kept = no_id = duplicate = bad_json = 0
    seen = set()

    # 先写同目录临时文件，全部完成后再原子替换：中途任何异常都不会
    # 把旧的 ids.txt 清空或留下半截文件给下游 tmdb_ids_to_links 误读。
    tmp_ids = tmp_detail = None
    try:
        # ExitStack：每个句柄一打开就登记关闭，后续任何一步（第二个 mkstemp、
        # chmod）抛异常时前面已打开的文件不会漏关。
        with ExitStack() as stack:
            fin = stack.enter_context(open(MOVIES, encoding="utf-8"))
            fd_ids, tmp_ids = tempfile.mkstemp(
                dir=OUTPUT_IDS.parent, prefix=OUTPUT_IDS.name + ".", suffix=".tmp")
            fids = stack.enter_context(os.fdopen(fd_ids, "w", encoding="utf-8"))
            fd_detail, tmp_detail = tempfile.mkstemp(
                dir=OUTPUT_DETAIL.parent, prefix=OUTPUT_DETAIL.name + ".", suffix=".tmp")
            fdetail = stack.enter_context(os.fdopen(fd_detail, "w", encoding="utf-8"))
            # mkstemp 默认 0600，替换后保持普通文件权限
            os.chmod(tmp_ids, 0o644)
            os.chmod(tmp_detail, 0o644)
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    movie = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue

                total += 1

                # 没有 tmdb_id 的无法下载，直接跳过
                tmdb_id = movie.get("tmdb_id")
                if not tmdb_id:
                    no_id += 1
                    continue

                if not passes_all(movie, config):
                    continue

                tid = str(tmdb_id)
                if tid in seen:  # 去重
                    duplicate += 1
                    continue
                seen.add(tid)

                fids.write(tid + "\n")
                fdetail.write(json.dumps(movie, ensure_ascii=False) + "\n")
                kept += 1

        os.replace(tmp_ids, OUTPUT_IDS)
        tmp_ids = None
        os.replace(tmp_detail, OUTPUT_DETAIL)
        tmp_detail = None
    finally:
        for tmp in (tmp_ids, tmp_detail):
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)

    if bad_json:
        print(f"警告: {bad_json} 行 JSON 解析失败已跳过（请检查 {MOVIES} 是否有损坏/非法行）")
    print(
        f"读取 {total} 条 | 无 tmdb_id 跳过 {no_id} | 重复跳过 {duplicate} | "
        f"最终选中 {kept}"
    )
    print(f"已写入 {OUTPUT_IDS}（{kept} 个 id）和 {OUTPUT_DETAIL}（明细）")


if __name__ == "__main__":
    main()
