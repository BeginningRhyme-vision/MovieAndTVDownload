#!/usr/bin/env python3
"""多节点 fallback 与取流侧边界情况的真实网络验证。

download_movies 的节点 fallback、跨形态切换（m3u8↔mp4），以及 tmdb_ids_to_links
的判死语义，此前都只有假 Session 的离线用例覆盖。本脚本用**真实取流结果**跑这些
路径，并通过注入可控失败来覆盖那些"等真实失败自然发生"不可靠的分支。

用法（必须在有 ffmpeg 的机器上跑）：
    python verify_fallback.py fetch          # 取流侧边界（判死语义、多源汇总）
    python verify_fallback.py fallback       # 下载侧多节点 fallback（含跨形态）
    python verify_fallback.py all
"""
import os
import random
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tmdb_ids_to_links as f  # noqa: E402
import download_movies as d  # noqa: E402


def hr(t):
    print("\n" + "=" * 72)
    print(t)
    print("=" * 72)


# ============================================================ 取流侧边界情况
def verify_fetch():
    hr("一、取流侧：多源汇总的顺序与去重（真实网络）")
    # 用已知有源片，确认四家的 url 被正确汇总、按 providers 顺序排列、按 url 去重
    st, r = f.process_tmdb_id("27205")
    assert st == "ok", f"预期 ok，实际 {st}"
    urls = r["urls"]
    provs = [u["provider"] for u in urls]
    seen_order = list(dict.fromkeys(provs))
    print(f"  节点数 {len(urls)}，provider 顺序 {seen_order}")
    print(f"  配置顺序 {f.ACTIVE_PROVIDERS}")
    # 汇总顺序必须与 providers 配置顺序一致（下载侧据此决定尝试次序）
    expect = [p for p in f.ACTIVE_PROVIDERS if p in seen_order]
    assert seen_order == expect, f"顺序不符：{seen_order} != {expect}"
    print("  ✓ 汇总顺序与 providers 配置一致")
    all_urls = [u["url"] for u in urls]
    assert len(all_urls) == len(set(all_urls)), "存在重复 url，去重失效"
    print(f"  ✓ url 全局去重（{len(all_urls)} 条互不相同）")
    types = Counter(u["type"] for u in urls)
    print(f"  ✓ 形态分布 {dict(types)}"
          + ("  ← 含 m3u8+mp4 混装，可测跨形态 fallback" if len(types) > 1 else ""))
    assert r["tmdbId"] == "27205", "身份字段被污染"
    assert isinstance(r.get("fetched_at"), int), "缺少 fetched_at"
    print("  ✓ 身份字段（tmdbId/fetched_at）正确")

    hr("二、取流侧：判死语义（NoSource 是唯一判死证据）")
    # 从 fail.txt 取真实 dead 片，确认返回 dead 而非 retry
    fail_path = f.Path(f._CFG.get("fail_file", "fail.txt"))
    if not fail_path.is_absolute():
        fail_path = f.Path(__file__).with_name(str(fail_path))
    if not fail_path.exists():
        print(f"  ⚠️ 缺少 {fail_path}，跳过")
    else:
        with open(fail_path, encoding="utf-8") as fh:
            dead = [ln.strip() for ln in fh if ln.strip()]
        sample = random.Random(7).sample(dead, 6)
        print(f"  取 {len(sample)} 部真实 dead 片，逐个跑 process_tmdb_id：")
        stat = Counter()
        t0 = time.time()
        for tid in sample:
            st, _ = f.process_tmdb_id(tid)
            stat[st] += 1
            print(f"    {tid:>9s} → {st}")
        print(f"  统计 {dict(stat)}（耗时 {time.time()-t0:.0f}s）")
        print("  说明：dead 表示四家都明确无源；retry 表示有瞬时错误，判死保守不误杀")

    hr("三、取流侧：dead_confirm 二次确认真实生效")
    # 计数 provider 被调用的次数：开启二次确认时，全家无源会完整重探一遍
    calls = Counter()
    orig = dict(f.PROVIDERS)

    def counting(name, fn):
        def wrapper(session, tid):
            calls[name] += 1
            return fn(session, tid)
        return wrapper

    for name, fn in orig.items():
        f.PROVIDERS[name] = counting(name, fn)
    try:
        tid = sample[0] if fail_path.exists() else "999999999"
        calls.clear()
        st, _ = f.process_tmdb_id(tid, providers=["vidup"])
        n_confirm = calls["vidup"]
        print(f"  DEAD_CONFIRM={f.DEAD_CONFIRM}  status={st}  vidup 被调用 {n_confirm} 次")
        if st == "dead" and f.DEAD_CONFIRM:
            assert n_confirm >= 2, f"二次确认未生效，只探了 {n_confirm} 次"
            print("  ✓ 判死前确实换 IP 重探（≥2 次）")
    finally:
        f.PROVIDERS.update(orig)


# ============================================================ 下载侧 fallback
def _fake_entry(urls, tid="27205", title="Inception"):
    return {"tmdbId": tid, "title": title, "year": 2010,
            "runtime_minutes": 148, "urls": urls}


def _pick_good(nodes, label):
    """挑一个真正可用的节点：逐个实跑 _attempt_download 的前置探测。

    直接取 nodes[0] / nodes[-1] 不可靠——同一部片的多个节点里常有失效的，
    用失效节点当"好节点"会把测试结果误判成 fallback 失败（本脚本踩过）。
    """
    for n in nodes:
        try:
            if n["type"] == "m3u8":
                variants = d.parse_master_playlist(n["url"], retries=2,
                                                   headers=n.get("headers") or None)
                if variants:
                    print(f"  选定可用 {label}：{n['provider']}/{n['type']}"
                          f"（{len(variants)} 个 variant）")
                    return n
            else:
                total, ok = d._mp4_probe_total_size(
                    n["url"], d._mp4_request_headers(n["headers"]), n["size"])
                if total > 0:
                    print(f"  选定可用 {label}：{n['provider']}/{n['type']}"
                          f"（{total/1024/1024:.0f} MB）")
                    return n
        except Exception as exc:
            print(f"  跳过不可用 {n['provider']}/{n['type']}: {str(exc)[:70]}")
    return None


def verify_fallback():
    st, r = f.process_tmdb_id("27205")
    if st != "ok":
        sys.exit(f"取流失败：{st}")
    nodes = [d._normalize_url_entry(u) for u in r["urls"]]
    nodes = [n for n in nodes if n]
    m3u8s = [n for n in nodes if n["type"] == "m3u8"]
    mp4s = [n for n in nodes if n["type"] == "mp4"]
    print(f"真实节点：m3u8 × {len(m3u8s)}，mp4 × {len(mp4s)}")
    if not m3u8s or not mp4s:
        sys.exit("需要同时有 m3u8 与 mp4 节点才能测跨形态切换")

    print("\n预筛可用节点（避免拿失效节点当好节点）：")
    good_m3u8 = _pick_good(m3u8s, "m3u8")
    good_mp4 = _pick_good(mp4s, "mp4")
    if not good_m3u8 or not good_mp4:
        sys.exit("找不到可用的 m3u8 或 mp4 节点，无法构造用例")

    bad_m3u8 = {**m3u8s[0], "url": "https://moon.peakstorm.top/vd/DOES-NOT-EXIST/master.m3u8"}
    bad_mp4 = {**mp4s[0], "url": "https://bcdn.hakunaymatata.com/resource/DOES-NOT-EXIST.mp4"}

    cases = [
        ("坏 m3u8 → 好 m3u8（同形态）", [bad_m3u8, good_m3u8]),
        ("坏 m3u8 → 好 mp4（跨形态 m3u8→mp4）", [bad_m3u8, good_mp4]),
        ("坏 mp4 → 好 m3u8（跨形态 mp4→m3u8）", [bad_mp4, good_m3u8]),
        ("坏 mp4 → 好 mp4（同形态）", [bad_mp4, good_mp4]),
        ("全坏（应判失败）", [bad_m3u8, bad_mp4]),
    ]

    for label, urls in cases:
        hr(f"fallback：{label}")
        # 每个用例用独立 tmdbId，避免 processed/processing 去重互相干扰
        tid = f"9{abs(hash(label)) % 10**6}"
        entry = _fake_entry(urls, tid=tid)
        final_ts = os.path.join(d.TEMP_DIR, f"temp_{d.safe_file_token(tid)}.ts")
        d.remove_file(final_ts)
        t0 = time.time()
        rid, ok, info = d.process_one_entry(entry, set())
        dt = time.time() - t0
        if ok:
            job = info
            used = job["url"]
            which = next((n["provider"] + "/" + n["type"]
                          for n in urls if n["url"] == used), "?")
            print(f"  ✓ 成功，实际用的是节点 {urls.index(next(n for n in urls if n['url'] == used)) + 1}"
                  f" ({which})  {job['resolution']} {job['bitrate_kbps']}kbps  耗时 {dt:.0f}s")
            assert used != urls[0]["url"], "居然用了本该失败的首节点"
            # 成功路径把临时文件交给转封装，此处手动清理
            for p in job.get("cleanup_paths", []):
                d.remove_file(p)
        else:
            print(f"  ✗ 失败：{info.get('error', '')[:110]}")
            print(f"     retriable={info.get('retriable')}  耗时 {dt:.0f}s")
            if label.startswith("全坏"):
                print("     （符合预期）")
            else:
                print("     ⚠️ 本用例预期应当 fallback 成功")
        # 无论成败，节点切换/失败后都不应留下 ts 残留
        leftover = os.path.exists(final_ts)
        print(f"  临时文件残留: {'⚠️ 有' if leftover else '无（已清理）'}")
        if leftover:
            d.remove_file(final_ts)


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what in ("fetch", "all"):
        verify_fetch()
    if what in ("fallback", "all"):
        verify_fallback()
    print("\n完成")
