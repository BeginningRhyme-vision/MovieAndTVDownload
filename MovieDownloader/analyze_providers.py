#!/usr/bin/env python3
"""分析 probe_sources.py 的输出，评估各取流源的互补性与去留。

回答的核心问题：一个源如果命中的片别家也都命中，它就是冗余的；
只有"独占命中"和"移除后 ANY 的损失"才是接入它的理由。

用法：
    python analyze_providers.py probe_detail.jsonl
    python analyze_providers.py batch1.jsonl batch2.jsonl   # 多批合并看总体
    python analyze_providers.py --compare batch1.jsonl batch2.jsonl  # 分批对比看结论是否稳定
"""
import json
import sys
from collections import defaultdict
from itertools import combinations
from urllib.parse import urlparse


def load(paths):
    """返回 (by_tid, urls_by, providers)。多个文件按 tid 合并。"""
    by_tid = defaultdict(dict)
    urls_by = defaultdict(dict)
    provs = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                p = r["provider"]
                if p not in provs:
                    provs.append(p)
                by_tid[r["tmdbId"]][p] = r["status"]
                if r["status"] == "hit":
                    urls_by[r["tmdbId"]][p] = set(r.get("urls") or [])
    return by_tid, urls_by, provs


def report(by_tid, urls_by, providers, label=""):
    n = len(by_tid)
    if not n:
        print("没有数据")
        return
    hits = {p: {t for t, d in by_tid.items() if d.get(p) == "hit"} for p in providers}
    errs = {p: sum(1 for d in by_tid.values() if d.get(p) == "err") for p in providers}
    union = set().union(*hits.values()) if hits else set()

    head = f"样本 {n} 部"
    if label:
        head = f"[{label}] " + head
    print("=" * 72)
    print(head + f"，providers = {providers}")
    print("=" * 72)

    print(f"\n{'provider':10s} {'hit':>5s} {'命中率':>8s} {'独占':>5s} {'独占率':>8s} {'err':>5s}")
    for p in providers:
        others = set().union(*[hits[q] for q in providers if q != p]) if len(providers) > 1 else set()
        only = hits[p] - others
        print(f"{p:10s} {len(hits[p]):5d} {len(hits[p])/n*100:7.1f}% "
              f"{len(only):5d} {len(only)/n*100:7.1f}% {errs[p]:5d}")
    print(f"{'ANY':10s} {len(union):5d} {len(union)/n*100:7.1f}%")

    print("\n-- 边际收益：逐个移除后 ANY 掉多少（接入与否的直接依据）--")
    for p in providers:
        rest = set().union(*[hits[q] for q in providers if q != p]) if len(providers) > 1 else set()
        lost = len(union) - len(rest)
        print(f"  移除 {p:10s} ANY {len(union):3d} → {len(rest):3d}   "
              f"损失 {lost:3d} 部 ({lost/n*100:.1f}pp)")

    print("\n-- 两两重叠（Jaccard 越接近 1 越像换皮）--")
    for a, b in combinations(providers, 2):
        inter, uni = hits[a] & hits[b], hits[a] | hits[b]
        j = len(inter) / len(uni) if uni else 0
        ca = len(inter) / len(hits[a]) if hits[a] else 0
        cb = len(inter) / len(hits[b]) if hits[b] else 0
        print(f"  {a:9s} vs {b:9s} 交集 {len(inter):3d}  J={j:.2f}  "
              f"{b} 覆盖 {a} {ca:.0%}  {a} 覆盖 {b} {cb:.0%}")

    print("\n-- 同片 url 是否逐字相同（真换皮的铁证）--")
    pair = defaultdict(lambda: [0, 0])
    for t, d in urls_by.items():
        for a, b in combinations(sorted(d), 2):
            if d[a] and d[b]:
                pair[(a, b)][1] += 1
                if d[a] == d[b]:
                    pair[(a, b)][0] += 1
    for (a, b), (same, both) in sorted(pair.items()):
        print(f"  {a:9s} vs {b:9s} 共同命中 {both:3d}，url 完全相同 {same:3d} "
              f"({same/both*100 if both else 0:.0f}%)")

    print("\n-- url 域名分布（判断后端归属）--")
    dom = defaultdict(lambda: defaultdict(int))
    for t, d in urls_by.items():
        for p, us in d.items():
            for u in us:
                dom[p][urlparse(u).netloc] += 1
    for p in providers:
        tops = sorted(dom[p].items(), key=lambda kv: -kv[1])[:4]
        print(f"  {p:10s} " + (", ".join(f"{k}×{v}" for k, v in tops) or "-"))
    print()


def main():
    args = sys.argv[1:]
    compare = "--compare" in args
    paths = [a for a in args if not a.startswith("--")]
    if not paths:
        sys.exit(__doc__)

    if compare and len(paths) > 1:
        # 分批各出一份，用于确认结论在不同样本上是否稳定
        for path in paths:
            by_tid, urls_by, provs = load([path])
            report(by_tid, urls_by, provs, label=path.split("/")[-1])
        print("#" * 72)
        print("# 合并后")
        print("#" * 72)
    by_tid, urls_by, provs = load(paths)
    report(by_tid, urls_by, provs, label="合并" if len(paths) > 1 else "")


if __name__ == "__main__":
    main()
