"""
tv_ids_to_links.py —— 由剧集 tmdb_id 展开季集结构，并逐集解析出可播放的 m3u8 地址。

与电影版 tmdb_ids_to_links.py 的差异：
    - 处理单位从“一部电影”变为“一集”：(tmdb_id, season, episode) 三元组。
    - 取流前先调 TMDB /tv/{id}（含 append_to_response=season/N）拿准确的季集结构，
      结果缓存到 seasons_cache.jsonl，多轮/续跑不重复调 API。
    - vidup.to 的 TV 页面地址：https://vidup.to/tv/{tmdb_id}/{season}/{episode}/
    - fail.txt 一行一集：tmdb_id\\tseason\\tepisode；剧级失效（TMDB 查不到）记为 tmdb_id\\t-\\t-
    - results.jsonl 一行一集：{urls, tmdbId, season, episode, title, + tv_series.jsonl 静态元数据}

其余机制（住宅代理随机端口、enc-dec 加解密、三态 ok/dead/retry、进程内多轮捞回）
与电影版保持一致。
"""

import random
from curl_cffi import requests
import re
import json
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import yaml
import requests as std_requests


def _load_dotenv(path):
    """轻量解析同目录 .env（KEY=VALUE，支持 # 注释与引号），不覆盖已存在的环境变量。"""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass


_load_dotenv(str(Path(__file__).with_name(".env")))


def load_config():
    """读取同目录 config.yaml 中本脚本对应的配置段；缺失时返回空字典。"""
    cfg_path = Path(__file__).with_name("config.yaml")
    if not cfg_path.exists():
        return {}
    with open(cfg_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("tv_ids_to_links", {}) or {}


_CFG = load_config()
_PROXY_CFG = _CFG.get("proxy", {}) or {}


def _secret(cfg_key, env_key, source=None):
    """敏感项：优先环境变量（同目录 .env），缺省回退 config.yaml（便于本地调试）。"""
    env_val = os.environ.get(env_key, "").strip()
    if env_val:
        return env_val
    src = _CFG if source is None else source
    return (src.get(cfg_key, "") or "").strip()


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Referer": "https://vidup.to/",
    "X-Requested-With": "XMLHttpRequest"
}

# 抓取 vidup.to 页面时不能带 X-Requested-With，否则会被 Cloudflare 拦成 403
PAGE_HEADERS = {k: v for k, v in HEADERS.items() if k != "X-Requested-With"}

API = _CFG.get("api", "https://enc-dec.app/api")
MAX_RETRIES = _CFG.get("max_retries", 3)
RETRY_DELAY = _CFG.get("retry_delay", 1)  # 秒
TIMEOUT = _CFG.get("timeout", 12)  # 单个 HTTP 请求超时（秒）

# ---- TMDB 季集展开 ----
TMDB_API_KEY = _secret("tmdb_api_key", "TMDB_API_KEY")
TMDB_BASE = "https://api.themoviedb.org/3"
INCLUDE_SPECIALS = bool(_CFG.get("include_specials", True))
TMDB_WORKERS = int(_CFG.get("tmdb_workers", 8))
TMDB_SLEEP = float(_CFG.get("tmdb_sleep", 0.1))
TMDB_TIMEOUT = int(_CFG.get("tmdb_timeout", 15))
TMDB_RETRIES = int(_CFG.get("tmdb_retries", 3))
# TMDB append_to_response 单次最多附带 20 个子请求
_TMDB_APPEND_LIMIT = 20

if not TMDB_API_KEY:
    raise SystemExit(
        "缺少 TMDB API Key：请在同目录 .env 配置 TMDB_API_KEY"
        "（或在 config.yaml 的 tv_ids_to_links.tmdb_api_key 填写，仅限本地调试）。"
    )


# 代理开关：设为 True 时启用下方代理，False 则直连
# 注意：vidup.to 有 Cloudflare 机房 IP 拦截，直连会返回 403，必须走住宅代理
USE_PROXY = _PROXY_CFG.get("enabled", True)
PROXY_HOST = _PROXY_CFG.get("host", "unmetered.residential.proxyrack.net")
PROXY_USER = _secret("user", "PROXY_USER", _PROXY_CFG)
PROXY_PASS = _secret("password", "PROXY_PASSWORD", _PROXY_CFG)
PROXY_PORT_RANGE = tuple(_PROXY_CFG.get("port_range", (9000, 9050)))  # 每次随机取一个端口，换一个出口 IP

# 启动期校验：启用代理但凭据缺失时立刻报错退出，避免拼出畸形代理 URL 后静默跑成一堆 403
if USE_PROXY and (not PROXY_USER or not PROXY_PASS):
    _missing = [n for n, v in (("PROXY_USER", PROXY_USER), ("PROXY_PASSWORD", PROXY_PASS)) if not v]
    raise SystemExit(
        f"代理已启用（proxy.enabled=true）但缺少凭据：{', '.join(_missing)}。"
        "请在同目录 .env 配置 PROXY_USER / PROXY_PASSWORD"
        "（或将 config.yaml 的 tv_ids_to_links.proxy.enabled 设为 false 直连）。"
    )


def build_proxy():
    """返回 requests 用的 proxies；未启用代理时返回 None 表示直连。"""
    if not USE_PROXY:
        return None
    port = random.randint(*PROXY_PORT_RANGE)
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{port}"
    return {"http": proxy_url, "https": proxy_url}


def _resolve(p):
    """相对路径以脚本所在目录为根。"""
    p = Path(p)
    return p if p.is_absolute() else Path(__file__).with_name(str(p))


# ---------- 元数据补全 ----------
# 从 tv_series.jsonl 预加载 tmdb_id -> 静态元数据，供取流成功时一并写进 results.jsonl
# （下游 download_tv 据此拼 R2 路径 {year}/{tmdbId}/S..）。


def load_series_metadata():
    meta_path = _resolve(_CFG.get("metadata", "tv_series.jsonl"))
    table = {}
    if not meta_path.exists():
        print(f"[metadata] 未找到 {meta_path}，results.jsonl 将不含元数据字段")
        return table
    with open(meta_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                m = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = m.get("tmdb_id")
            if tid is None:
                continue
            table[str(tid)] = {
                "year": m.get("start_year"),
                "original_title": m.get("original_title"),
                "runtime_minutes": m.get("runtime_minutes"),
                "genres": m.get("genres", []),
                "title_type": m.get("title_type"),
                "imdb_id": m.get("imdb_id"),
            }
    print(f"[metadata] 已加载 {len(table)} 条剧集元数据")
    return table


_SERIES_META = load_series_metadata()


# ---------- TMDB 季集结构展开 ----------
_tmdb_session = std_requests.Session()
_tmdb_session.headers.update({"Accept": "application/json"})


class TmdbNotFound(Exception):
    """TMDB 查不到该剧（404）：剧级失效，整部剧永久跳过。"""


def _tmdb_get(path, params=None):
    """调 TMDB v3 接口；404 抛 TmdbNotFound，其它错误重试 TMDB_RETRIES 次后抛出。"""
    q = {"api_key": TMDB_API_KEY}
    if params:
        q.update(params)
    last = None
    for attempt in range(1, TMDB_RETRIES + 1):
        try:
            resp = _tmdb_session.get(f"{TMDB_BASE}{path}", params=q, timeout=TMDB_TIMEOUT)
            if resp.status_code == 404:
                raise TmdbNotFound(path)
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 2) or 2)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            if TMDB_SLEEP > 0:
                time.sleep(TMDB_SLEEP)
            return resp.json()
        except TmdbNotFound:
            raise
        except Exception as e:
            last = e
            if attempt < TMDB_RETRIES:
                time.sleep(1.5 * attempt)
    raise Exception(f"TMDB request failed for {path}: {last}")


def fetch_seasons_from_tmdb(tmdb_id):
    """
    展开一部剧的季集结构（以 TMDB 为准）。

    返回：
        {
          "tmdbId": "123",
          "name": "...",
          "year": 2011 或 None,           # first_air_date 年份，作 tv_series.jsonl 缺 start_year 时的回退
          "seasons": [{"season": 0, "episodes": [1,2,...]}, {"season": 1, "episodes": [...]}, ...]
        }
    先取 /tv/{id} 拿 seasons 列表，再用 append_to_response=season/N 分批拉各季的 episodes，
    以 episode_number 为准（可正确处理编号不连续的情况），而不是简单用 episode_count 数数。
    """
    info = _tmdb_get(f"/tv/{tmdb_id}")
    season_numbers = []
    for s in info.get("seasons") or []:
        n = s.get("season_number")
        if n is None:
            continue
        n = int(n)
        if n == 0 and not INCLUDE_SPECIALS:
            continue
        season_numbers.append(n)
    season_numbers = sorted(set(season_numbers))

    seasons = []
    for i in range(0, len(season_numbers), _TMDB_APPEND_LIMIT):
        chunk = season_numbers[i:i + _TMDB_APPEND_LIMIT]
        data = _tmdb_get(
            f"/tv/{tmdb_id}",
            {"append_to_response": ",".join(f"season/{n}" for n in chunk)},
        )
        for n in chunk:
            sd = data.get(f"season/{n}") or {}
            eps = sorted({
                int(e["episode_number"])
                for e in (sd.get("episodes") or [])
                if e.get("episode_number") is not None
            })
            if eps:
                seasons.append({"season": n, "episodes": eps})

    year = None
    fad = info.get("first_air_date") or ""
    if len(fad) >= 4 and fad[:4].isdigit():
        year = int(fad[:4])

    return {
        "tmdbId": str(tmdb_id),
        "name": info.get("name"),
        "year": year,
        "seasons": seasons,
    }


def load_seasons_cache(cache_file):
    cache = {}
    if not cache_file.exists():
        return cache
    with open(cache_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = obj.get("tmdbId")
            if tid is not None:
                cache[str(tid)] = obj
    return cache


def expand_seasons(ids, cache_file, fail_file, dead_shows):
    """
    对 ids 中尚未缓存的剧并发调 TMDB 展开季集，追加写入 cache_file。
    TMDB 404 的剧写 fail_file（tid\\t-\\t-）并加入 dead_shows。
    返回 {tid: cache_entry}。
    """
    cache = load_seasons_cache(cache_file)
    todo = [tid for tid in ids if tid not in cache and tid not in dead_shows]
    print(f"[tmdb] 季集缓存 {len(cache)} 部 | 需展开 {len(todo)} 部")
    if not todo:
        return cache

    lock = threading.Lock()
    done = 0

    def one(tid):
        try:
            return tid, fetch_seasons_from_tmdb(tid), None
        except TmdbNotFound:
            return tid, None, "not_found"
        except Exception as e:
            return tid, None, str(e)

    with ThreadPoolExecutor(max_workers=TMDB_WORKERS) as ex:
        futures = [ex.submit(one, tid) for tid in todo]
        for fut in as_completed(futures):
            tid, entry, err = fut.result()
            with lock:
                done += 1
                if entry is not None:
                    cache[tid] = entry
                    with open(cache_file, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    n_eps = sum(len(s["episodes"]) for s in entry["seasons"])
                    print(f"[tmdb] {done}/{len(todo)} {tid} {entry.get('name')} | {len(entry['seasons'])} 季 {n_eps} 集")
                elif err == "not_found":
                    dead_shows.add(tid)
                    with open(fail_file, "a", encoding="utf-8") as f:
                        f.write(f"{tid}\t-\t-\n")
                    print(f"[tmdb] {done}/{len(todo)} {tid} TMDB 404，整部剧跳过")
                else:
                    # 瞬时错误：本次不缓存也不判死，下次运行再试
                    print(f"[tmdb] {done}/{len(todo)} {tid} 展开失败（下次再试）: {err}")
    return cache


# ---------- 辅助函数 ----------
def validate(data, path):
    if data.get("status") != 200:
        error_msg = data.get("error", "unknown")
        raise Exception(f"API Error at {path}: status={data.get('status')}, error={error_msg}")
    return data["result"]


def _is_retriable(exc):
    """判断异常是否值得重试：只对瞬时错误（网络/SSL/超时/连接中断/5xx/403）重试。
    真无源的干净 404（All servers failed / stream 取流 404）不重试——重试也还是 404，纯浪费。
    """
    msg = str(exc)
    if "All servers failed" in msg:
        return "404" not in msg
    retriable_markers = (
        "curl: (35)", "SSL", "timed out", "Timeout", "timeout",
        "Connection", "connection", "Failed to perform", "Proxy",
        "Could not resolve", "Recv failure", "Send failure",
        "403",  # 偶发 Cloudflare 限流，换 IP 重试可能过
        "500", "502", "503", "504",
        "API Error",  # enc/dec 服务瞬时非200，换次重试可能过
        "No servers found",  # 偶发 server 列表空，重试可能拿到
        "Extract failed (retriable)",  # 页面200但提取不到加密文本：可能撞Cloudflare挑战页，换IP重试
    )
    return any(m in msg for m in retriable_markers)


def _ep_label(tid, season, episode):
    return f"{tid} S{int(season):02d}E{int(episode):02d}"


def process_episode(tid, season, episode):
    """处理单集，仅对瞬时错误重试。

    返回 (status, 结果字典或None)，status 三态供多轮捞回区分：
      - "ok"    成功，附结果字典 {urls, tmdbId, season, episode, title, + 静态元数据}
      - "dead"  真无源的干净404 → 这一集永久排除（不影响同剧其它集）
      - "retry" 瞬时错误换 IP 重试 MAX_RETRIES 次仍失败 → 下一轮重跑
    """
    label = _ep_label(tid, season, episode)
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        # 每集、每次重试用一个独立 Session：复用连接、绑定本次随机出口 IP
        with requests.Session(impersonate="chrome") as session:
            proxy = build_proxy()
            if proxy:
                session.proxies = proxy
            try:
                # 1. 获取页面，提取加密文本
                base_url = f"https://vidup.to/tv/{tid}/{season}/{episode}/"
                resp = session.get(base_url, timeout=TIMEOUT, headers=PAGE_HEADERS)
                resp.raise_for_status()
                html = resp.text

                match_1 = re.search(r'\\"en\\":\\"(.*?)\\"', html)
                match = re.search(r'\\"token\\":\\"(.*?)\\"', html)
                if match_1:
                    text = match_1.group(1)
                else:
                    if match:
                        text = match.group(1)
                    else:
                        raise Exception(f"Extract failed (retriable) for {label}")

                # 2. 调用 enc-vidup 获取 parts
                enc_vidup = f"{API}/enc-vidup?text={text}"
                resp = session.get(enc_vidup, timeout=TIMEOUT, headers=HEADERS)
                resp.raise_for_status()
                data = resp.json()
                parts = validate(data, enc_vidup)
                servers = parts['servers']
                stream = parts['stream']
                token = parts['token']

                headers_with_token = HEADERS.copy()
                headers_with_token["X-CSRF-Token"] = token

                # 3. 获取加密的服务器列表
                resp = session.post(servers, headers=headers_with_token, timeout=TIMEOUT)
                servers_encrypted = resp.text

                # 4. 解密服务器列表
                dec_vidup = f"{API}/dec-vidup"
                resp = session.post(dec_vidup, json={"text": servers_encrypted}, timeout=TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                servers_decrypted = validate(data, dec_vidup)

                if not servers_decrypted:
                    raise Exception("No servers found")

                # 遍历所有服务器，收集全部可用 url
                last_server_error = None
                urls = []
                result_tmdb_id = None
                result_title = None
                for server in servers_decrypted:
                    try:
                        data_val = server['data']
                        # 5. 获取加密的流数据
                        stream_url = f"{stream}/{data_val}"
                        resp = session.post(stream_url, headers=headers_with_token, timeout=TIMEOUT)
                        resp.raise_for_status()
                        stream_encrypted = resp.text

                        # 6. 解密流数据
                        resp = session.post(dec_vidup, json={"text": stream_encrypted}, timeout=TIMEOUT)
                        resp.raise_for_status()
                        data = resp.json()
                        stream_decrypted = validate(data, dec_vidup)

                        url = stream_decrypted.get("url")
                        r_tid = stream_decrypted.get("tmdbId")
                        if url and r_tid:
                            if url not in urls:
                                urls.append(url)
                            if result_tmdb_id is None:
                                result_tmdb_id = r_tid
                                result_title = stream_decrypted.get("title")
                        else:
                            last_server_error = "Missing url or tmdbId in decrypted data"
                    except Exception as e:
                        last_server_error = e
                        server_name = server.get('name', 'unknown')
                        print(f"  Server '{server_name}' failed for {label}: {e}")
                        continue

                if urls and result_tmdb_id:
                    # 恒用入参 tid/season/episode 作为 key，保证全链路一致：
                    # 续跑去重、元数据查表、下游 R2 路径与文件名都对得上。
                    result = {
                        "urls": urls,
                        "tmdbId": str(tid),
                        "season": int(season),
                        "episode": int(episode),
                        "title": result_title,
                    }
                    result.update(_SERIES_META.get(str(tid), {}))
                    return "ok", result

                raise Exception(f"All servers failed for {label}. Last error: {last_server_error}")

            except Exception as e:
                last_exception = e
                if not _is_retriable(e):
                    print(f"  [无源 404] {label}: {e}")
                    return "dead", None
                print(f"  [Attempt {attempt}/{MAX_RETRIES}] 瞬时错误 for {label}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"  All {MAX_RETRIES} attempts failed for {label}")

    return "retry", None


def load_processed(results_file, fail_file):
    """返回 (已处理的 (tid, season, episode) 集合, 剧级失效的 tid 集合)。"""
    processed = set()
    dead_shows = set()
    if results_file.exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    tid, s, e = obj.get('tmdbId'), obj.get('season'), obj.get('episode')
                    if tid is not None and s is not None and e is not None:
                        processed.add((str(tid), int(s), int(e)))
                except Exception:
                    pass
    if fail_file.exists():
        with open(fail_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) != 3:
                    continue
                tid, s, e = parts
                if s == "-" and e == "-":
                    dead_shows.add(tid)
                    continue
                try:
                    processed.add((tid, int(s), int(e)))
                except ValueError:
                    pass
    return processed, dead_shows


def run_batch(to_process, results_file, fail_file, max_workers):
    """并发处理一批集，实时落盘 ok/dead，返回本批“瞬时耗尽”待重跑的三元组列表。"""
    lock = threading.Lock()
    ok_count = 0
    dead_count = 0
    retry_items = []

    def process_one(item):
        tid, s, e = item
        status, result = process_episode(tid, s, e)
        label = _ep_label(tid, s, e)
        with lock:
            if status == "ok" and result:
                with open(results_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                print(f"✅ SUCCESS: {result.get('title')} ({label})")
            elif status == "retry":
                retry_items.append(item)
                print(f"🔁 RETRY-LATER: {label}")
            else:
                with open(fail_file, 'a', encoding='utf-8') as f:
                    f.write(f"{tid}\t{s}\t{e}\n")
                print(f"❌ DEAD: {label}")
        return status

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, item): item for item in to_process}
        for future in as_completed(futures):
            item = futures[future]
            try:
                status = future.result()
                if status == "ok":
                    ok_count += 1
                elif status == "retry":
                    pass
                else:
                    dead_count += 1
            except Exception as e:
                print(f"⚠️  Unexpected exception for {_ep_label(*item)}: {e}")
                with lock:
                    retry_items.append(item)

    print(f"\n本批完成 | 成功 {ok_count} | 真无源 {dead_count} | 待重跑 {len(retry_items)}")
    return retry_items


def main():
    ids_file = _resolve(_CFG.get("input", "ids.txt"))
    results_file = _resolve(_CFG.get("output", "results.jsonl"))
    fail_file = _resolve(_CFG.get("fail_file", "fail.txt"))
    cache_file = _resolve(_CFG.get("seasons_cache", "seasons_cache.jsonl"))

    if not ids_file.exists():
        print(f"{ids_file} not found!")
        return

    with open(ids_file, 'r', encoding='utf-8') as f:
        ids = []
        seen = set()
        for line in f:
            tid = line.strip()
            if tid and tid not in seen:
                seen.add(tid)
                ids.append(tid)

    processed, dead_shows = load_processed(results_file, fail_file)

    # ---- 第一步：TMDB 展开季集结构（带缓存）----
    cache = expand_seasons(ids, cache_file, fail_file, dead_shows)

    # ---- 第二步：按 ids.txt 顺序展开成 (tid, season, episode) 任务，剔除已处理 ----
    to_process = []
    total_eps = 0
    for tid in ids:
        if tid in dead_shows:
            continue
        entry = cache.get(tid)
        if not entry:
            continue  # 本次展开失败，下次运行再试
        # tv_series.jsonl 缺 start_year 时，用 TMDB first_air_date 年份回退
        meta = _SERIES_META.setdefault(tid, {})
        if meta.get("year") is None and entry.get("year") is not None:
            meta["year"] = entry["year"]
        for s in entry["seasons"]:
            for e in s["episodes"]:
                total_eps += 1
                key = (tid, int(s["season"]), int(e))
                if key not in processed:
                    to_process.append(key)

    print(f"Total shows: {len(ids)} | Total episodes: {total_eps} | "
          f"Already processed: {len(processed)} | To process: {len(to_process)}")

    if not to_process:
        print("All episodes processed.")
        return

    max_workers = _CFG.get("max_workers", 50)
    max_rounds = _CFG.get("max_rounds", 8)

    # ---- 内嵌多轮捞回：首轮跑全部，之后每轮只重跑上一轮“瞬时耗尽”的集 ----
    pending = to_process
    round_no = 0
    while pending:
        round_no += 1
        print(f"\n{'=' * 70}")
        print(f"==> 第 {round_no}/{max_rounds} 轮 | 待处理 {len(pending)} 集")
        print(f"{'=' * 70}")

        retry_items = run_batch(pending, results_file, fail_file, max_workers)

        if not retry_items:
            print("\n==> 瞬时失败已清零，所有有源集已捞干净，正常结束。")
            break
        if round_no >= max_rounds:
            with open(fail_file, 'a', encoding='utf-8') as f:
                for tid, s, e in retry_items:
                    f.write(f"{tid}\t{s}\t{e}\n")
            print(f"\n==> 已达最大轮数 {max_rounds}，剩余 {len(retry_items)} 集瞬时失败归入 fail_file。")
            break
        pending = retry_items

    print(f"\nAll done. 共跑 {round_no} 轮，结果已合并写入 {results_file}")


if __name__ == "__main__":
    main()
