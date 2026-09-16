#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""端到端漏斗统计：把 ids → 季集展开 → 取流 → 下载 → 上传 每一级的数量与
流失原因摊开，回答"应下多少集、实际到手多少、每级丢在哪"。

跑完一轮全量后执行：
    python pipeline_report.py

只读不写，不依赖网络与外部工具。任何数据文件缺失都会明确标注而非崩溃——
中途查看进度时上游文件往往还没齐。

各级数据来源：
    ids.txt              人工/上游给定的剧 ID 清单
    seasons_cache.jsonl  TMDB 展开的季集结构（每剧一行，含 air_dates/ended）
    fail.txt             取流侧失败：剧级 404 / 集级真无源 / retry-exhausted
    results.jsonl        取流成功的集（download_tv 的输入）
    download_ok.jsonl    下载成功的集（不含转封装/上传）
    success.jsonl        转封装落地的集，uploaded 字段标记是否已传 R2
    failed.jsonl         各阶段失败明细，按 stage 分组
    upload_pending.jsonl 待补传（上传失败或反压降级留本地）
"""

import json
import os
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

_SCRIPT_DIR = Path(__file__).resolve().parent


def _cfg():
    path = _SCRIPT_DIR / "config.yaml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _resolve(value, default_name):
    raw = value.strip() if isinstance(value, str) else value
    return str((_SCRIPT_DIR / (raw or default_name)).resolve())


def _iter_jsonl(path):
    """逐行读 JSONL，跳过空行与坏行。文件不存在时产出空序列。"""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _ep_key(record):
    """从含 tmdbId/season/episode 的记录生成集级 key，与 download_tv 口径一致。"""
    if not isinstance(record, dict):
        return ""
    tid = record.get("tmdbId")
    season = record.get("season")
    episode = record.get("episode")
    if tid is None or season is None or episode is None:
        return ""
    try:
        return f"{str(tid).strip()}_S{int(season):02d}E{int(episode):02d}"
    except (TypeError, ValueError):
        return ""


def _missing(path, label):
    print(f"  ⚠️ 缺少 {label}：{path}")


def load_expected(ids_file, cache_file, air_grace_days):
    """按 TMDB 季集结构算出"本应下载"的集合，并扣除尚未播出的集。

    未播判定与 tv_ids_to_links._is_unaired 保持一致：已完结剧缺 air_date 视为
    已播出，在播剧缺 air_date 视为 TBA 未播；有日期时加 air_grace_days 宽限。
    """
    ids = []
    if os.path.exists(ids_file):
        with open(ids_file, "r", encoding="utf-8") as fh:
            seen = set()
            for line in fh:
                tid = line.strip()
                if tid and tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
    else:
        _missing(ids_file, "ids.txt")

    cache = {}
    for entry in _iter_jsonl(cache_file):
        tid = entry.get("tmdbId")
        if tid:
            cache[str(tid)] = entry
    if not cache:
        _missing(cache_file, "seasons_cache.jsonl")

    today = date.today()
    expected = set()
    unaired = 0
    not_expanded = []
    for tid in ids:
        entry = cache.get(str(tid))
        if not entry:
            not_expanded.append(tid)
            continue
        ended = bool(entry.get("ended"))
        for season in entry.get("seasons", []):
            air_dates = season.get("air_dates")
            for episode in season.get("episodes", []):
                if air_dates is not None:
                    raw = air_dates.get(str(episode))
                    if raw:
                        try:
                            aired = date.fromisoformat(str(raw)[:10])
                            if aired + timedelta(days=air_grace_days) > today:
                                unaired += 1
                                continue
                        except ValueError:
                            pass
                    elif not ended:
                        unaired += 1
                        continue
                try:
                    expected.add(
                        f"{tid}_S{int(season['season']):02d}E{int(episode):02d}"
                    )
                except (TypeError, ValueError, KeyError):
                    continue
    return ids, expected, unaired, not_expanded


def load_fail_txt(path):
    """解析 fail.txt 的三种行格式。

    返回 (剧级死, 集级死, 重试耗尽集合, 重试耗尽原始行数)。原始行数与集合大小
    的差值反映"同一集被多少次运行判为耗尽"——上游写入前已去重，若两者仍不等，
    说明存在历史遗留的重复行。
    """
    dead_shows, dead_eps, exhausted = set(), set(), set()
    exhausted_rows = 0
    if not os.path.exists(path):
        _missing(path, "fail.txt")
        return dead_shows, dead_eps, exhausted, exhausted_rows
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 4 and parts[3] == "retry-exhausted":
                exhausted_rows += 1
                try:
                    exhausted.add(
                        f"{parts[0]}_S{int(parts[1]):02d}E{int(parts[2]):02d}"
                    )
                except ValueError:
                    pass
                continue
            if len(parts) != 3:
                continue
            tid, season, episode = parts
            if season == "-" and episode == "-":
                dead_shows.add(tid)
                continue
            try:
                dead_eps.add(f"{tid}_S{int(season):02d}E{int(episode):02d}")
            except ValueError:
                continue
    return dead_shows, dead_eps, exhausted, exhausted_rows


def _pct(part, whole):
    return f"{part / whole:.1%}" if whole else "n/a"


def _human_bytes(num):
    """字节数转人类可读（二进制单位）。非法输入返回 "n/a"。

    带到 PB：全量跑的预估在百 TB 量级，长期累计可能跨过 1024 TB。
    """
    try:
        value = float(num)
    except (TypeError, ValueError):
        return "n/a"
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(value) < 1024 or unit == "PB":
            return f"{int(value)} B" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} PB"


def _print_counter(title, counter, total):
    if not counter:
        return
    print(f"\n  {title}")
    for reason, count in counter.most_common():
        print(f"    {reason}: {count} 次（{_pct(count, total)}）")


def _print_provider_funnel(offered_by, won_by, fallback_wins,
                           attributed, downloaded_total):
    """各取流源的「提供 → 下成」转化率，外加独占贡献与 fallback 救回数。

    这是判断「某个源该不该留」的唯一依据：一个源如果命中的集别家也都命中，
    它就是冗余的——只有**独占**（只有它提供了节点的集）和**靠 fallback 才
    下成的集**才是接入它的理由。光看"给出多少条 url"会严重高估同后端换皮的源。

    ⚠️ 分母是「该源为多少集提供过节点」而非「多少集下成了」：同一集往往有
    多家提供节点，各家分母会重叠，所以各行的百分比**不该相加**。
    """
    if not offered_by:
        return

    missing = downloaded_total - attributed
    print("\n  各源转化率（集级；分母=该源为多少集提供过节点，各行分母有重叠）：")
    for name in sorted(offered_by, key=lambda n: -len(offered_by[n])):
        offered = offered_by[name]
        # 独占 = 只有这一家给这集提供了节点，别家一条都没有。
        others = set()
        for other_name, keys in offered_by.items():
            if other_name != name:
                others |= keys
        exclusive = offered - others
        won = won_by.get(name, 0)
        saved = fallback_wins.get(name, 0)
        extra = []
        if exclusive:
            extra.append(f"独占 {len(exclusive)} 集")
        if saved:
            extra.append(f"其中 {saved} 集靠 fallback 救回")
        suffix = f"；{'、'.join(extra)}" if extra else ""
        print(
            f"    {name}: 提供 {len(offered)} 集 → 下成 {won} 集"
            f"（{_pct(won, len(offered))}）{suffix}"
        )

    if missing > 0:
        print(
            f"    ⚠️ {missing} 集下载成功但无 provider 归因"
            f"（{_pct(missing, downloaded_total)}）——"
            "这些是加归因字段之前跑出来的旧记录，不计入上表"
        )


def _print_provider_failures(fail_by_provider, fail_rows_without_attr):
    """各取流源的失败原因拆分（来自 failed.jsonl 的 node_failures）。

    与上面的转化率表互补：那张表回答"哪家下成了"，这张回答"哪家为什么没成"。

    🔑 没有它就只能靠"直链类目 ≈ vidlink"这种代理指标硬猜，而且**无法区分
    同为 m3u8 的 vidup 与 vidfast**——`error` 字段只保留末节点的文案，
    前面节点的失败原因此前根本不落盘。

    ⚠️ 计数单位是「节点失败次数」不是「集数」：同一集的多个节点各记一次，
    且跨轮重投会重复计数。用于看**构成比例**，不要与集数横向相加。
    """
    if not fail_by_provider:
        return
    print("\n  各源失败原因（单位=节点失败次数，同集多节点各记一次、跨轮会重复计）：")
    for name in sorted(fail_by_provider, key=lambda n: -sum(fail_by_provider[n].values())):
        reasons = fail_by_provider[name]
        total = sum(reasons.values())
        top = "；".join(
            f"{reason} {count}（{_pct(count, total)}）"
            for reason, count in reasons.most_common(4)
        )
        print(f"    {name}: 共 {total} 次 —— {top}")
    if fail_rows_without_attr:
        print(
            f"    ⚠️ {fail_rows_without_attr} 条下载失败记录没有逐节点归因——"
            "加该字段之前的旧记录，或异常抛在节点循环之外（磁盘闸门等）"
        )


def main():
    cfg = _cfg()
    links_cfg = cfg.get("tv_ids_to_links", {}) or {}
    dl_cfg = cfg.get("download_tv", {}) or {}

    ids_file = _resolve(links_cfg.get("input"), "ids.txt")
    cache_file = _resolve(links_cfg.get("seasons_cache"), "seasons_cache.jsonl")
    fail_file = _resolve(links_cfg.get("fail_file"), "fail.txt")
    results_file = _resolve(links_cfg.get("output"), "results.jsonl")
    ok_log = _resolve(dl_cfg.get("download_ok_log"), "download_ok.jsonl")
    success_log = _resolve(dl_cfg.get("success_log"), "success.jsonl")
    failed_log = _resolve(dl_cfg.get("failed_log"), "failed.jsonl")
    pending_log = _resolve(
        (dl_cfg.get("s3", {}) or {}).get("upload_pending_log")
        or dl_cfg.get("upload_pending_log"),
        "upload_pending.jsonl",
    )
    air_grace_days = int(links_cfg.get("air_grace_days", 5))

    print("=" * 64)
    print("端到端漏斗统计")
    print("=" * 64)

    ids, expected, unaired, not_expanded = load_expected(
        ids_file, cache_file, air_grace_days
    )
    dead_shows, dead_eps, exhausted, exhausted_rows = load_fail_txt(fail_file)

    # 取流侧：results.jsonl 按集去重（同集多行只算一集）
    fetched, providers, types = set(), Counter(), Counter()
    # provider 归因用的集级视图（上面的 providers 是"url 条数"，粒度不同）：
    #   offered_by[家] = 该家为哪些集提供过节点
    # 这是算转化率的分母，也是判断某家该不该留的基础。
    offered_by = {}
    result_rows = 0
    fetch_times = []
    for record in _iter_jsonl(results_file):
        key = _ep_key(record)
        if not key:
            continue
        result_rows += 1
        fetched.add(key)
        ts = record.get("fetched_at")
        if isinstance(ts, int) and ts > 0:
            fetch_times.append(ts)
        for node in record.get("urls") or []:
            if isinstance(node, dict):
                name = node.get("provider") or "unknown"
                providers[name] += 1
                types[node.get("type") or "m3u8"] += 1
            else:
                # 旧格式裸字符串：按当初的唯一来源 vidup 计。
                name = "vidup"
                providers["vidup"] += 1
                types["m3u8"] += 1
            offered_by.setdefault(name, set()).add(key)
    if not fetched:
        _missing(results_file, "results.jsonl")

    # 下载侧：带 provider 归因（新格式）。旧行没有这些字段，计入 unknown，
    # 这样"统计不出来"是显式可见的，而不是悄悄少算某一家。
    downloaded = set()
    won_by = Counter()
    fallback_wins = Counter()
    attributed = 0
    for record in _iter_jsonl(ok_log):
        key = _ep_key(record)
        if not key:
            continue
        downloaded.add(key)
        name = record.get("provider")
        if name:
            attributed += 1
            won_by[name] += 1
            # node_index > 1 = 前面的节点都挂了、靠 fallback 才救回来。
            # 这是"多源到底有没有用"最直接的证据。
            if isinstance(record.get("node_index"), int) and record["node_index"] > 1:
                fallback_wins[name] += 1

    # success.jsonl 每集一条，uploaded 标记是否已进 R2
    # 体积按**集级 key 去重**：SUCCESS_LOG 是 append-only，同一集可能先写
    # uploaded:false、补传后再写 true；而 R2 的 key 是幂等覆盖的，去重后的和
    # 才等于 R2 实际占用。后写的正值覆盖先写的（与 update_success_log 的覆盖
    # 语义一致），但**无效值不覆盖已记录的正值**——补传若 stat 失败会写回
    # None，不能让它抹掉首传时的实测值。
    # uploaded 一旦为真就不再撤销：对象 PUT 成功后就在 R2 上，后续记录里的
    # false 只反映那一刻的写入状态，不代表对象消失。
    finalized, uploaded = set(), set()
    size_by_key = {}
    for record in _iter_jsonl(success_log):
        key = _ep_key(record)
        if not key:
            continue
        finalized.add(key)
        size = record.get("file_size_bytes")
        # 只接受正整数；None/缺失/bool/非正数都算"无体积信息"（旧记录没有该字段）。
        if isinstance(size, int) and not isinstance(size, bool) and size > 0:
            size_by_key[key] = size
        if record.get("uploaded"):
            uploaded.add(key)

    pending = {k for k in (_ep_key(r) for r in _iter_jsonl(pending_log)) if k}

    stage_fail = Counter()
    dl_fail_reason = Counter()
    # 逐节点归因：provider -> 失败类目计数。来自 download 阶段的 node_failures。
    fail_by_provider = {}
    fail_rows_without_attr = 0
    for record in _iter_jsonl(failed_log):
        stage = record.get("stage") or "unknown"
        stage_fail[stage] += 1
        if stage == "download":
            dl_fail_reason[str(record.get("error", ""))[:60] or "未知"] += 1
            nodes = record.get("node_failures") or []
            if not nodes:
                # 旧记录（加该字段之前）或异常抛在节点循环之外。单独计数报出，
                # 不静默少算——否则各源占比会被悄悄扭曲。
                fail_rows_without_attr += 1
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                name = str(node.get("provider") or "unknown")
                reason = str(node.get("reason_class") or "其他")
                fail_by_provider.setdefault(name, Counter())[reason] += 1

    total = len(expected)
    print(f"\n剧集清单：{len(ids)} 部")
    if not_expanded:
        print(f"  未展开季集（TMDB 失败或缓存缺失）：{len(not_expanded)} 部")
    if dead_shows:
        print(f"  剧级判死（TMDB 404）：{len(dead_shows)} 部")
    print(f"  跳过未播/宽限期内的集：{unaired} 集")

    print(f"\n【应下载】已播出的集：{total} 集")

    print(f"\n【1·取流】成功 {len(fetched)} 集（{_pct(len(fetched), total)}）")
    if result_rows > len(fetched):
        print(
            f"  results.jsonl 原始有效行 {result_rows} 行 → 去重后 {len(fetched)} 集"
            f"（{result_rows - len(fetched)} 行为同集重复取流，下游按 fetched_at 取最新）"
        )
    if fetch_times:
        span_hours = (max(fetch_times) - min(fetch_times)) / 3600
        print(
            "  取流时间跨度："
            f"{datetime.fromtimestamp(min(fetch_times)):%Y-%m-%d %H:%M}"
            f" ~ {datetime.fromtimestamp(max(fetch_times)):%Y-%m-%d %H:%M}"
            f"（{span_hours:.1f} 小时）"
        )
        no_ts = result_rows - len(fetch_times)
        if no_ts > 0:
            print(f"  ⚠️ {no_ts} 行缺 fetched_at（旧格式），下游对其回退按文件位置取后出现的一条")
    elif result_rows:
        print("  ⚠️ 全部行缺 fetched_at（上游为旧版本产出），下游去重回退按文件位置")
    print(f"  真无源判死：{len(dead_eps)} 集（{_pct(len(dead_eps), total)}）")
    print(f"  重试耗尽：{len(exhausted)} 集（{_pct(len(exhausted), total)}）")
    if exhausted_rows > len(exhausted):
        print(
            f"    ⚠️ retry-exhausted 原始 {exhausted_rows} 行 → 去重后 {len(exhausted)} 集，"
            "存在历史重复行（新版写入前已去重）"
        )
    overlap = exhausted & fetched
    if overlap:
        print(f"    其中 {len(overlap)} 集已在后续轮次取流成功，下游不受影响")
    missed = expected - fetched - dead_eps - exhausted
    if missed:
        print(f"  ❗ 既未成功也无失败记录（可能未跑到）：{len(missed)} 集")
    if providers:
        print("\n  取流节点来源分布（按 url 条数）：")
        for name, count in providers.most_common():
            print(f"    {name}: {count} 条")
        print("  节点类型分布：")
        for name, count in types.most_common():
            print(f"    {name}: {count} 条")
        _print_provider_funnel(offered_by, won_by, fallback_wins,
                               attributed, len(downloaded))

    base = len(fetched)
    print(f"\n【2·下载】成功 {len(downloaded)} 集（占取流成功 {_pct(len(downloaded), base)}）")
    not_downloaded = fetched - downloaded
    if not_downloaded:
        print(f"  未下载成功：{len(not_downloaded)} 集")
    _print_counter(
        "下载失败原因 Top（截断至 60 字符）", dl_fail_reason,
        sum(dl_fail_reason.values()),
    )
    # 放在整集失败原因之后：先看"整体为什么失败"，再看"拆到每个源是什么构成"。
    _print_provider_failures(fail_by_provider, fail_rows_without_attr)

    print(f"\n【3·转封装落地】{len(finalized)} 集（占下载成功 {_pct(len(finalized), len(downloaded))}）")

    print(f"\n【4·上传 R2】成功 {len(uploaded)} 集（占落地 {_pct(len(uploaded), len(finalized))}）")
    print(f"  待补传（pending）：{len(pending)} 集")
    local_only = finalized - uploaded
    if local_only:
        print(f"  仅本地未上传：{len(local_only)} 集")

    # R2 总量：按集级 key 去重后累加**已上传**集的体积，等于 R2 实际占用。
    # 窗口是整个台账而非单次运行——SUCCESS_LOG 追加式记录，首轮/自愈轮/跨运行
    # 重试/断点续跑写进来的集都在里面，所以这就是"完整一次全量取流下载"的总和。
    uploaded_with_size = uploaded & size_by_key.keys()
    total_bytes = sum(size_by_key[k] for k in uploaded_with_size)
    if uploaded_with_size:
        print(f"  R2 总大小：{_human_bytes(total_bytes)}"
              f"（{len(uploaded_with_size)} 集，单集均值 "
              f"{_human_bytes(total_bytes / len(uploaded_with_size))}）")
        # 缺体积的集单独报出，避免把"少算的量"悄悄吞掉看不出来。
        lacking = len(uploaded) - len(uploaded_with_size)
        if lacking:
            print(f"  ⚠️ 另有 {lacking} 集无体积信息（早于该字段的旧记录），未计入上面的总大小")
    elif uploaded:
        print("  ⚠️ 已上传的集均无 file_size_bytes 字段（旧记录），无法统计 R2 总大小")

    _print_counter("各阶段失败记录条数", stage_fail, sum(stage_fail.values()))

    print("\n" + "=" * 64)
    print(
        f"端到端达成率：{len(uploaded)}/{total} = {_pct(len(uploaded), total)}"
        "（已上传 / 应下载）"
    )
    print("=" * 64)


if __name__ == "__main__":
    sys.exit(main())
