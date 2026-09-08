#!/usr/bin/env python3
"""多源增量探测（电影版）：从 fail.txt 的 dead 影片中分层抽样，去候选嵌入站探一遍，
统计各家对 vidup 无源影片的增量命中率。

仅做验证，不写 results.jsonl / fail.txt。用法（服务器 .venv 内）：
    python probe_sources.py --sample 300
    python probe_sources.py --sample 100 --providers vidfast,vidlink
    python probe_sources.py --input ids.txt            # 自带样本，每行一个 tmdb_id
    python probe_sources.py --sample 30 --no-proxy     # 本地直连试跑（可能被 CF 拦）

输出：probe_detail.jsonl（每片×每家明细）与终端汇总表。

与 TVDownloader/probe_sources.py 的差异仅在于「处理单位是一部电影而非一集」：
路由 /movie/{tid}、vidlink /api/b/movie/{enc}、videasy mediaType=movie。
"""
import argparse
import json
import os
import random
import re
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import yaml
from curl_cffi import requests

HERE = Path(__file__).resolve().parent


# ---------- 配置 / 凭据 ----------
def _load_dotenv(path):
    try:
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass


_load_dotenv(HERE / ".env")
_cfg_path = HERE / "config.yaml"
_CFG = {}
if _cfg_path.exists():
    with open(_cfg_path, encoding="utf-8") as f:
        _CFG = (yaml.safe_load(f) or {}).get("tmdb_ids_to_links", {}) or {}
_PROXY_CFG = _CFG.get("proxy", {}) or {}

API = _CFG.get("api", "https://enc-dec.app/api")
TIMEOUT = int(_CFG.get("timeout", 12))
PROXY_HOST = _PROXY_CFG.get("host", "unmetered.residential.proxyrack.net")
PROXY_USER = os.environ.get("PROXY_USER", "").strip() or (_PROXY_CFG.get("user") or "").strip()
PROXY_PASS = os.environ.get("PROXY_PASSWORD", "").strip() or (_PROXY_CFG.get("password") or "").strip()
PROXY_PORT_RANGE = tuple(_PROXY_CFG.get("port_range", (9000, 9050)))
USE_PROXY = bool(_PROXY_CFG.get("enabled", True))

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"


def build_proxy():
    if not USE_PROXY:
        return None
    port = random.randint(*PROXY_PORT_RANGE)
    url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{port}"
    return {"http": url, "https": url}


# ---------- 结果判定 ----------
class NoSource(Exception):
    """源站明确表示无此片（页面/接口 404 或返回空源）。"""


class Transient(Exception):
    """瞬时错误（超时/5xx/403/解析失败），换 IP 可重试。"""


def _validate(data, path):
    if not isinstance(data, dict) or data.get("status") != 200:
        raise Transient(f"enc-dec {path}: {data.get('error', data) if isinstance(data, dict) else data}")
    return data["result"]


_MEDIA_RE = re.compile(r"https?://[^\s\"']+\.(?:m3u8|mp4)(?:\?[^\s\"']*)?", re.I)


def _find_media_urls(obj):
    """递归找出结构中的 m3u8/mp4 链接（vidlink/videasy 返回结构不固定，宽松判定）。"""
    found = []
    if isinstance(obj, str):
        found.extend(_MEDIA_RE.findall(obj))
    elif isinstance(obj, dict):
        for v in obj.values():
            found.extend(_find_media_urls(v))
    elif isinstance(obj, list):
        for v in obj:
            found.extend(_find_media_urls(v))
    return found


# ---------- 各 provider ----------
def _vidup_like(session, site, api_name, tid):
    """vidup/vidfast/vidcore 同架构：页面正则 -> enc -> servers -> dec -> 逐 server stream -> dec。"""
    page_headers = {"User-Agent": UA, "Referer": f"https://{site}/"}
    # 页面请求不能带 X-Requested-With（会被 Cloudflare 403），后续接口要带
    api_headers = dict(page_headers, **{"X-Requested-With": "XMLHttpRequest"})
    resp = session.get(f"https://{site}/movie/{tid}/", headers=page_headers, timeout=TIMEOUT)
    if resp.status_code == 404:
        raise NoSource("page 404")
    if resp.status_code != 200:
        raise Transient(f"page HTTP {resp.status_code}")
    m = re.search(r'\\"(?:en|token)\\":\\"(.*?)\\"', resp.text)
    if not m:
        raise Transient("extract failed")
    enc = f"{API}/enc-{api_name}?text={quote(m.group(1), safe='')}"
    parts = _validate(session.get(enc, headers=api_headers, timeout=TIMEOUT).json(), f"enc-{api_name}")
    servers_url, stream_base, token = parts.get("servers"), parts.get("stream"), parts.get("token")
    if not (servers_url and stream_base):
        raise Transient("missing servers/stream")
    if token:
        api_headers["X-CSRF-Token"] = token
    r = session.post(servers_url, headers=api_headers, timeout=TIMEOUT)
    if r.status_code == 404:
        raise NoSource("servers 404")
    if r.status_code != 200:
        raise Transient(f"servers HTTP {r.status_code}")
    dec = f"{API}/dec-{api_name}"
    servers = _validate(session.post(dec, json={"text": r.text}, timeout=TIMEOUT).json(), f"dec-{api_name}")
    if not isinstance(servers, list) or not servers:
        raise NoSource("empty server list")
    urls, s404, errs = [], 0, []
    for sv in servers:
        try:
            r = session.post(f"{stream_base}/{sv['data']}", headers=api_headers, timeout=TIMEOUT)
            if r.status_code == 404:
                s404 += 1
                continue
            if r.status_code != 200:
                errs.append(f"stream HTTP {r.status_code}")
                continue
            d = _validate(session.post(dec, json={"text": r.text}, timeout=TIMEOUT).json(), f"dec-{api_name}")
            if isinstance(d, dict) and d.get("url"):
                urls.append(d["url"])
        except Exception as ex:  # noqa: BLE001
            errs.append(str(ex)[:80])
    if urls:
        return urls
    if s404 == len(servers):
        raise NoSource(f"all {s404} servers 404")
    raise Transient(f"all servers failed: {errs[:2]}")


def probe_vidup(session, ep):
    """当前主源，用于复探：判断其它站的命中是真增量还是 vidup 误判。"""
    return _vidup_like(session, "vidup.to", "vidup", ep["tid"])


def probe_vidfast(session, ep):
    return _vidup_like(session, "vidfast.vc", "vidfast", ep["tid"])


def probe_vidcore(session, ep):
    return _vidup_like(session, "vidcore.io", "vidcore", ep["tid"])


def probe_vidlink(session, ep):
    headers = {"User-Agent": UA, "Origin": "https://vidlink.pro", "Referer": "https://vidlink.pro/"}
    enc = _validate(session.get(f"{API}/enc-vidlink?text={ep['tid']}", timeout=TIMEOUT).json(), "enc-vidlink")
    r = session.get(f"https://vidlink.pro/api/b/movie/{quote(str(enc), safe='')}", headers=headers, timeout=TIMEOUT)
    if r.status_code == 404:
        raise NoSource("api 404")
    if r.status_code != 200:
        raise Transient(f"api HTTP {r.status_code}")
    try:
        data = r.json()
    except Exception:  # noqa: BLE001
        raise Transient("api non-json")
    if data is None:
        # 实测无源返回字面量 null
        raise NoSource("api null")
    urls = _find_media_urls(data)
    if urls:
        return list(dict.fromkeys(urls))
    raise NoSource("no media url in response")


# 与 TV 版一致：vsrc 路由已不存在（404 Route not found），只保留 cdn / m4uhd；
# m4uhd 需要 title/year（缺则 500 "Required parameters not found"），所以样本要带 movies.jsonl 元数据
VIDEASY_SERVERS = ("cdn", "m4uhd")


def probe_videasy(session, ep):
    headers = {"Accept": "*/*", "Origin": "https://player.videasy.to",
               "Referer": "https://player.videasy.to/", "User-Agent": UA}
    tid = ep["tid"]
    title = ep.get("title") or ""
    year = ep.get("year") or ""
    imdb = ep.get("imdb") or ""
    r = session.get(f"https://api.speedracelight.com/seed?mediaId={tid}", headers=headers, timeout=TIMEOUT)
    if r.status_code != 200:
        raise Transient(f"seed HTTP {r.status_code}")
    seed = r.json().get("seed")
    if not seed:
        raise Transient("no seed")
    enc_title = quote(quote(title, safe=""), safe="")
    # m4uhd 靠 title/year 检索，缺元数据时直接跳过，避免整家被记成 err
    servers = VIDEASY_SERVERS if title else ("cdn",)
    urls, miss, errs = [], 0, []
    for sv in servers:
        url = (f"https://api.speedracelight.com/{sv}/sources-with-title?title={enc_title}&mediaType=movie"
               f"&year={year}&tmdbId={tid}&imdbId={imdb}&enc=2&seed={seed}")
        try:
            r = session.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 404 or not r.text.strip():
                miss += 1
                continue
            if r.status_code == 500 and "No streams available" in r.text:
                # 源站明确回复该片无流，等价于 404
                miss += 1
                continue
            if r.status_code != 200:
                errs.append(f"{sv} HTTP {r.status_code} {r.text[:60]}")
                continue
            d = _validate(session.post(f"{API}/dec-videasy", json={"text": r.text, "id": str(tid), "seed": seed},
                                       timeout=TIMEOUT).json(), "dec-videasy")
            got = _find_media_urls(d)
            if got:
                urls.extend(got)
            else:
                miss += 1
        except Exception as ex:  # noqa: BLE001
            errs.append(f"{sv}: {str(ex)[:60]}")
    if urls:
        return list(dict.fromkeys(urls))
    if miss == len(servers):
        raise NoSource("all servers empty")
    raise Transient(f"servers failed: {errs[:2]}")


PROVIDERS = {
    "vidup": probe_vidup,
    "vidfast": probe_vidfast,
    "vidlink": probe_vidlink,
    "videasy": probe_videasy,
    # TV 侧实测：enc-vidcore 给出的 servers 路由在 vidcore.io 上 404（Route not found），
    # 协议已变、enc-dec 尚未跟进；保留实现但不进默认列表，需要时 --providers 显式指定
    "vidcore": probe_vidcore,
}
DEFAULT_PROVIDERS = ("vidfast", "vidlink", "videasy")


def probe_one(provider, ep, retries):
    """返回 (status, detail)：status ∈ hit/miss/err。miss 二次换 IP 确认，err 换 IP 重试。"""
    fn = PROVIDERS[provider]
    miss_hits, last, last_nosource = 0, "", ""
    for attempt in range(retries + 2):
        t0 = time.time()
        with requests.Session(impersonate="chrome") as session:
            p = build_proxy()
            if p:
                session.proxies = p
            try:
                urls = fn(session, ep)
                return "hit", {"urls": urls[:3], "n_urls": len(urls), "ms": int((time.time() - t0) * 1000)}
            except NoSource as ex:
                miss_hits += 1
                last_nosource = str(ex)
                if miss_hits >= 2:
                    return "miss", {"reason": last_nosource}
            except Exception as ex:  # noqa: BLE001
                last = f"{type(ex).__name__}: {str(ex)[:120]}"
                if attempt >= retries:
                    break
        # 429 是源站限流：多等一会再换 IP（直连时尤其明显）
        time.sleep(3 if "429" in last else 0.5)
    # 命中过一次 NoSource 但确认那跳是瞬时错误：按 miss 记（保守），reason 标注未二次确认
    if miss_hits:
        return "miss", {"reason": f"{last_nosource} (unconfirmed; {last})"}
    return "err", {"reason": last}


# ---------- 抽样 ----------
def load_meta(keep_ids=None):
    """从 movies.jsonl 读 title/year/imdb。movies.jsonl 约 1.4GB，
    传 keep_ids 只保留待抽样的 id，避免把全量元数据读进内存。"""
    path = HERE / (_CFG.get("metadata", "movies.jsonl"))
    table = {}
    if not path.exists():
        print(f"[meta] 未找到 {path}，将无年代分层、videasy m4uhd 不可用")
        return table
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                m = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = m.get("tmdb_id")
            if tid is None:
                continue
            tid = str(tid)
            if keep_ids is not None and tid not in keep_ids:
                continue
            table[tid] = {"title": m.get("primary_title") or m.get("original_title") or "",
                          "year": m.get("start_year"), "imdb": m.get("imdb_id") or ""}
    print(f"[meta] 已加载 {len(table)} 条影片元数据")
    return table


def load_dead(fail_path):
    """fail.txt 每行一个 tmdb_id（电影版无季集维度）。"""
    ids = []
    seen = set()
    with open(fail_path, encoding="utf-8") as f:
        for line in f:
            tid = line.strip()
            if tid and tid not in seen:
                seen.add(tid)
                ids.append(tid)
    return ids


def load_exclude(paths):
    """读出已经测过的 tmdb_id，抽样时排除，保证多批样本互不重叠。

    接受两种格式（按行自动识别）：
      - jsonl（如既往的 probe_detail.jsonl）：取每行的 tmdbId
      - 纯文本：每行一个 id
    """
    seen = set()
    for path in paths:
        p = Path(path)
        if not p.exists():
            print(f"[exclude] 跳过不存在的文件 {p}")
            continue
        n0 = len(seen)
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("{"):
                    try:
                        tid = json.loads(line).get("tmdbId")
                    except json.JSONDecodeError:
                        continue
                    if tid is not None:
                        seen.add(str(tid))
                else:
                    seen.add(line.split("\t")[0])
        print(f"[exclude] {p.name} → 新增 {len(seen) - n0} 个（累计 {len(seen)}）")
    return seen


def sample_movies(ids, n, seed, meta, exclude=None):
    """按年代分层轮转抽样，避免样本被某个年代段垄断。

    exclude 里的 id 先剔除，用于抽取与历史批次不重叠的新样本。
    """
    rnd = random.Random(seed)
    ids = [t for t in ids if not exclude or t not in exclude]
    rnd.shuffle(ids)
    buckets = defaultdict(list)
    for tid in ids:
        y = (meta.get(tid) or {}).get("year")
        buckets[(y // 10 * 10) if isinstance(y, int) else "?"].append(tid)
    order = []
    while len(order) < n and any(buckets.values()):
        for k in list(buckets):
            if buckets[k]:
                order.append(buckets[k].pop())
                if len(order) >= n:
                    break
    return [{"tid": tid, **(meta.get(tid) or {})} for tid in order]


def load_input(path, meta):
    eps = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            tid = line.strip().split("\t")[0]
            if tid:
                eps.append({"tid": tid, **(meta.get(tid) or {})})
    return eps


# ---------- 主流程 ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fail", default=str(HERE / (_CFG.get("fail_file", "fail.txt"))))
    ap.add_argument("--input", help="自带样本文件，每行一个 tmdb_id，优先于 --sample")
    ap.add_argument("--sample", type=int, default=300, help="抽多少部影片")
    ap.add_argument("--providers", default=",".join(DEFAULT_PROVIDERS),
                    help=f"逗号分隔，可选 {list(PROVIDERS)}")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--retries", type=int, default=2, help="瞬时错误换 IP 重试次数")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--exclude", action="append", default=[],
                    help="排除这些文件里出现过的 tmdb_id（可重复指定）。"
                         "支持 probe_detail.jsonl 或每行一个 id 的文本，"
                         "用于抽取与历史批次不重叠的新样本")
    ap.add_argument("--no-proxy", action="store_true")
    ap.add_argument("--out", default=str(HERE / "probe_detail.jsonl"))
    args = ap.parse_args()

    global USE_PROXY
    if args.no_proxy:
        USE_PROXY = False
    if USE_PROXY and not (PROXY_USER and PROXY_PASS):
        sys.exit("代理已启用但缺少 PROXY_USER / PROXY_PASSWORD（.env），或加 --no-proxy 直连")

    providers = [p.strip() for p in args.providers.split(",") if p.strip()]
    bad = [p for p in providers if p not in PROVIDERS]
    if bad:
        sys.exit(f"未知 provider: {bad}，可选 {list(PROVIDERS)}")

    if args.input:
        with open(args.input, encoding="utf-8") as f:
            want = {ln.strip().split("\t")[0] for ln in f if ln.strip()}
        meta = load_meta(want)
        eps = load_input(args.input, meta)
    else:
        dead = load_dead(args.fail)
        print(f"[dead] {len(dead)} 部影片可抽样")
        exclude = load_exclude(args.exclude) if args.exclude else set()
        if exclude:
            avail = sum(1 for t in dead if t not in exclude)
            print(f"[exclude] 排除 {len(exclude)} 个已测 id，剩余 {avail} 部可抽")
            if avail < args.sample:
                sys.exit(f"可抽样本不足：需要 {args.sample} 部，仅剩 {avail} 部")
        meta = load_meta(set(dead))
        eps = sample_movies(dead, args.sample, args.seed, meta, exclude)
        # 抽完再核一次，确保排除真的生效（多批对比的结论全靠这条保证）
        if exclude:
            overlap = {ep["tid"] for ep in eps} & exclude
            if overlap:
                sys.exit(f"内部错误：抽样结果与排除集重叠 {len(overlap)} 个")
            print(f"[exclude] ✓ 已核验：本批 {len(eps)} 部与历史批次零重叠")
    if not eps:
        sys.exit("没有样本")
    no_title = sum(1 for ep in eps if not ep.get("title"))
    print(f"[sample] {len(eps)} 部；providers={providers}；"
          f"proxy={'on' if USE_PROXY else 'off'}" + (f"；{no_title} 部缺元数据（videasy 命中率会偏低）" if no_title else ""))

    tasks = [(p, ep) for ep in eps for p in providers]
    results = {}  # tid -> {provider: (status, detail)}
    lock = threading.Lock()
    done = 0
    with open(args.out, "w", encoding="utf-8") as fo, ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(probe_one, p, ep, args.retries): (p, ep) for p, ep in tasks}
        for fut in as_completed(futs):
            p, ep = futs[fut]
            status, detail = fut.result()
            key = ep["tid"]
            with lock:
                results.setdefault(key, {})[p] = (status, detail)
                done += 1
                fo.write(json.dumps({"tmdbId": ep["tid"], "title": ep.get("title"), "year": ep.get("year"),
                                     "provider": p, "status": status, **detail}, ensure_ascii=False) + "\n")
                fo.flush()
                mark = {"hit": "+", "miss": "-", "err": "!"}[status]
                extra = detail.get("urls", [""])[0][:70] if status == "hit" else detail.get("reason", "")[:70]
                print(f"[{done}/{len(tasks)}] {mark} {p:8s} {ep['tid']:>8s} {extra}")

    # ---- 汇总 ----
    n = len(results)
    print("\n" + "=" * 70)
    print(f"样本 {n} 部（全部为 vidup 判死影片）")
    print(f"{'provider':10s} {'hit':>5s} {'miss':>5s} {'err':>5s}   {'增量命中率':>8s}")
    for p in providers:
        c = Counter(results[k][p][0] for k in results if p in results[k])
        print(f"{p:10s} {c['hit']:5d} {c['miss']:5d} {c['err']:5d}   {c['hit'] / n * 100:7.1f}%")
    union = sum(1 for k in results if any(v[0] == "hit" for v in results[k].values()))
    print(f"{'ANY':10s} {union:5d} {'':5s} {'':5s}   {union / n * 100:7.1f}%   <- 任一家命中")
    if len(providers) > 1:
        print("\n各家独占命中（只有它能救的片）：")
        for p in providers:
            only = sum(1 for k in results if results[k].get(p, ("",))[0] == "hit"
                       and not any(v[0] == "hit" for q, v in results[k].items() if q != p))
            print(f"  {p:10s} {only}")
    if meta:
        print("\n按年代（ANY 命中 / 样本）：")
        dec = defaultdict(lambda: [0, 0])
        for k in results:
            y = (meta.get(k) or {}).get("year")
            b = f"{y // 10 * 10}s" if isinstance(y, int) else "?"
            dec[b][1] += 1
            dec[b][0] += any(v[0] == "hit" for v in results[k].values())
        for b in sorted(dec):
            h, t = dec[b]
            print(f"  {b:6s} {h:3d}/{t:<3d} {h / t * 100:5.1f}%")
    print(f"\n明细已写入 {args.out}")


if __name__ == "__main__":
    main()
