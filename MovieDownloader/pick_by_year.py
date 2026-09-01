#!/usr/bin/env python3
# 从「有源全集」中，按指定年份各挑 N 部电影，输出含下载 url 的完整条目。
import json

RESULTS = "total_results.jsonl"   # 有源全集：{urls, tmdbId, title}
MOVIES = "movies.jsonl"           # 元数据：{tmdb_id, start_year, primary_title, ...}
YEARS = {2000, 2010, 2020}
PER_YEAR = 10
OUT = "picked_by_year.jsonl"

# 1) 加载有源集：tmdbId -> {url, title}
sourced = {}
with open(RESULTS, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        tid = r.get("tmdbId")
        if tid is not None and tid not in sourced:
            sourced[tid] = {"urls": r.get("urls", []), "title": r.get("title")}
print(f"有源集去重后 tmdbId 数：{len(sourced)}")

# 2) 扫元数据，按年份收集「有源」的电影
buckets = {y: [] for y in YEARS}
with open(MOVIES, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        m = json.loads(line)
        y = m.get("start_year")
        if y not in YEARS:
            continue
        tid = m.get("tmdb_id")
        if tid not in sourced:
            continue
        if len(buckets[y]) >= PER_YEAR:
            continue
        s = sourced[tid]
        buckets[y].append({
            "tmdbId": tid,
            "title": s["title"] or m.get("primary_title"),
            "year": y,
            "urls": s["urls"],
        })
        # 三个年份都满了就提前结束
        if all(len(buckets[yy]) >= PER_YEAR for yy in YEARS):
            break

# 3) 输出
with open(OUT, "w", encoding="utf-8") as f:
    for y in sorted(YEARS):
        for item in buckets[y]:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

for y in sorted(YEARS):
    print(f"\n===== {y} 年（{len(buckets[y])} 部）=====")
    for item in buckets[y]:
        print(f"  {item['tmdbId']:>8}  {item['title']}")
print(f"\n已写入 {OUT}")
