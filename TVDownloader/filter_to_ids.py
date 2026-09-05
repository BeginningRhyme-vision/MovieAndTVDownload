"""
按筛选配置从 tv_series.jsonl 中挑选电视剧，生成下游需要的 ids.txt。

输入：
    tv_series.jsonl    —— fetch_tv_metadata.py 的产物，每行一部电视剧的元数据
    filter_config.yaml —— 筛选配置文件，每个元数据字段对应一个筛选开关

输出：
    ids.txt        —— 每行一个 tmdb_id（剧集级），供 tv_ids_to_links.py 消费
    filtered.jsonl —— 通过筛选的电视剧明细（保留标题/评分等，便于人工复核）

设计原则：
    - 每个筛选项都是独立开关，由各自的 enabled 控制是否生效。
    - 某项 enabled=false 时完全跳过（放行所有电视剧）。
    - 因此“配置文件里所有开关都关闭” == “不做任何筛选，全部选中”。
    - 数值字段为 null 时的取舍由各项的 keep_if_missing 决定。
    - 流式逐行读取，避免把整个 tv_series.jsonl 加载进内存。
    - 筛选粒度是“剧”，不是“集”；季/集的展开在 tv_ids_to_links.py 调 TMDB 完成。
"""

import json
import re
from pathlib import Path
from typing import Optional

import yaml

# ========== 路径配置 ==========
SERIES = Path("tv_series.jsonl")
CONFIG = Path("filter_config.yaml")
OUTPUT_IDS = Path("ids.txt")
OUTPUT_DETAIL = Path("filtered.jsonl")


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


def check_title_type(show: dict, rule: dict) -> bool:
    allow = rule.get("allow") or []
    return show.get("title_type") in allow


def check_is_adult(show: dict, rule: dict) -> bool:
    if not rule.get("exclude_adult", True):
        return True
    return not bool(show.get("is_adult"))


def _check_numeric(show: dict, rule: dict, field: str) -> bool:
    value = show.get(field)
    if value is None:
        return bool(rule.get("keep_if_missing", True))
    return _in_range(value, rule.get("min"), rule.get("max"))


def check_start_year(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "start_year")


def check_end_year(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "end_year")


def check_runtime_minutes(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "runtime_minutes")


def check_rating(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "rating")


def check_votes(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "votes")


def check_total_seasons(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "total_seasons")


def check_total_episodes(show: dict, rule: dict) -> bool:
    return _check_numeric(show, rule, "total_episodes")


def _normalize_list(values, case_insensitive: bool) -> list:
    result = []
    for v in values or []:
        if not isinstance(v, str):
            continue
        result.append(v.lower() if case_insensitive else v)
    return result


def check_genres(show: dict, rule: dict) -> bool:
    ci = rule.get("case_insensitive", True)
    genres = _normalize_list(show.get("genres") or [], ci)

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


def _check_person(show: dict, rule: dict, field: str) -> bool:
    ci = rule.get("case_insensitive", True)
    people = _normalize_list(show.get(field) or [], ci)

    if not people:
        return bool(rule.get("keep_if_missing", False))

    include = _normalize_list(rule.get("include") or [], ci)
    if not include:
        # include 为空表示本项不设人名限制，直接放行
        return True
    return bool(set(people) & set(include))


def check_directors(show: dict, rule: dict) -> bool:
    return _check_person(show, rule, "directors")


def check_writers(show: dict, rule: dict) -> bool:
    return _check_person(show, rule, "writers")


def check_title_keywords(show: dict, rule: dict) -> bool:
    ci = rule.get("case_insensitive", True)
    use_regex = rule.get("use_regex", False)
    include = rule.get("include") or []
    exclude = rule.get("exclude") or []

    titles = [
        show.get("primary_title") or "",
        show.get("original_title") or "",
    ]

    def hit(keyword: str) -> bool:
        for title in titles:
            if not title:
                continue
            if use_regex:
                flags = re.IGNORECASE if ci else 0
                if re.search(keyword, title, flags):
                    return True
            else:
                a, b = (title.lower(), keyword.lower()) if ci else (title, keyword)
                if b in a:
                    return True
        return False

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
    "end_year": check_end_year,
    "runtime_minutes": check_runtime_minutes,
    "rating": check_rating,
    "votes": check_votes,
    "total_seasons": check_total_seasons,
    "total_episodes": check_total_episodes,
    "genres": check_genres,
    "directors": check_directors,
    "writers": check_writers,
    "title_keywords": check_title_keywords,
}


def passes_all(show: dict, config: dict) -> bool:
    """逐项应用已启用的筛选；全部通过才返回 True。"""
    for name, check in CHECKS.items():
        rule = rule_enabled(config, name)
        if rule is None:
            continue  # 该项未启用，放行
        if not check(show, rule):
            return False
    return True


# ========== 主流程 ==========
def main():
    if not SERIES.exists():
        raise SystemExit(f"错误: 找不到 {SERIES}")

    config = load_config(CONFIG)
    active = [name for name in CHECKS if rule_enabled(config, name) is not None]
    if active:
        print(f"已启用的筛选项: {', '.join(active)}")
    else:
        print("未启用任何筛选项：将选中全部带 tmdb_id 的电视剧")

    total = kept = no_id = duplicate = 0
    seen = set()

    with open(SERIES, encoding="utf-8") as fin, \
            open(OUTPUT_IDS, "w", encoding="utf-8") as fids, \
            open(OUTPUT_DETAIL, "w", encoding="utf-8") as fdetail:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                show = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1

            # 没有 tmdb_id 的无法下载，直接跳过
            tmdb_id = show.get("tmdb_id")
            if not tmdb_id:
                no_id += 1
                continue

            if not passes_all(show, config):
                continue

            tid = str(tmdb_id)
            if tid in seen:  # 去重
                duplicate += 1
                continue
            seen.add(tid)

            fids.write(tid + "\n")
            fdetail.write(json.dumps(show, ensure_ascii=False) + "\n")
            kept += 1

    print(
        f"读取 {total} 条 | 无 tmdb_id 跳过 {no_id} | 重复跳过 {duplicate} | "
        f"最终选中 {kept}"
    )
    print(f"已写入 {OUTPUT_IDS}（{kept} 个 id）和 {OUTPUT_DETAIL}（明细）")


if __name__ == "__main__":
    main()
