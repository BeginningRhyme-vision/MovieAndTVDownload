"""
tv_ids_to_links.py —— 由剧集 tmdb_id 展开季集结构，并逐集从多个取流源解析出可下载地址。

与电影版 tmdb_ids_to_links.py 的差异：
    - 处理单位从“一部电影”变为“一集”：(tmdb_id, season, episode) 三元组。
    - 取流前先调 TMDB /tv/{id}（含 append_to_response=season/N）拿准确的季集结构，
      结果缓存到 seasons_cache.jsonl，多轮/续跑不重复调 API。
    - 多源：按 config.providers（默认 vidup → vidlink → vidfast）顺序逐家取流，首家命中即返回；
      全部真无源才判 dead。vidup.to / vidfast.vc 同构（页面 → enc → servers → dec → stream，出 m3u8），
      vidlink.pro 为 enc-vidlink(tmdb id) → /api/b/tv 直接返 JSON（出带时效签名的 mp4 直链）。
    - fail.txt 一行一集：tmdb_id\\tseason\\tepisode；剧级失效（TMDB 查不到）记为 tmdb_id\\t-\\t-
    - results.jsonl 一行一集：{urls, tmdbId, season, episode, title, + tv_series.jsonl 静态元数据}
      urls 每项为 {url, provider, type("m3u8"|"mp4"), headers, quality, size}；
      历史文件中 urls 元素可能仍是纯字符串（旧 vidup m3u8），下游需兼容。

其余机制（住宅代理随机端口、enc-dec 加解密、三态 ok/dead/retry、进程内多轮捞回）
与电影版保持一致。
"""

import argparse
import random
from curl_cffi import requests
import re
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from urllib.parse import quote

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


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"


def _site_headers(site):
    """vidup 同构站（vidup.to / vidfast.vc）的 (页面头, 接口头)。
    页面请求不能带 X-Requested-With，否则会被 Cloudflare 拦成 403；后续接口要带。"""
    api = {"User-Agent": UA, "Referer": f"https://{site}/", "X-Requested-With": "XMLHttpRequest"}
    page = {k: v for k, v in api.items() if k != "X-Requested-With"}
    return page, api


VIDLINK_API_HEADERS = {"User-Agent": UA, "Origin": "https://vidlink.pro", "Referer": "https://vidlink.pro/"}
# vidlink CDN（hakunaymatata.com）校验请求头：浏览器 UA → 428，带任何 Referer → 429，
# 只有 okhttp UA + 不带 Referer 才 206。随 url 一起写进 results.jsonl，供 download_tv.py 直接使用。
VIDLINK_DOWNLOAD_HEADERS = {"User-Agent": "okhttp/4.9.3"}

DEFAULT_PROVIDERS = ["vidup", "vidlink", "vidfast"]

API = _CFG.get("api", "https://enc-dec.app/api")
MAX_RETRIES = _CFG.get("max_retries", 3)
RETRY_DELAY = _CFG.get("retry_delay", 1)  # 秒
TIMEOUT = _CFG.get("timeout", 12)  # 单个 HTTP 请求超时（秒）

# ---- 多轮捞回的轮间退避 ----
# 每轮之间递增等待，给 enc-dec / 代理的短时故障留恢复窗口，避免 8 轮在几秒内烧光。
ROUND_BACKOFF_BASE = int(_CFG.get("round_backoff", 30))       # 第 n 轮后等待 base*n 秒
ROUND_BACKOFF_MAX = int(_CFG.get("round_backoff_max", 300))   # 单次等待上限
# 某轮“待重跑”占比 ≥ 该比例且数量 ≥ 下限，视为基础设施故障（而非个别集抖动），直接按上限等待
OUTAGE_RETRY_RATIO = 0.9
OUTAGE_MIN_ITEMS = 50
# 轮次耗尽仍是瞬时失败的集，写 fail.txt 时带此第 4 列标记；load_processed 不把它们当已处理，下次运行自动再试
RETRY_EXHAUSTED_TAG = "retry-exhausted"

# ---- TMDB 季集展开 ----
TMDB_API_KEY = _secret("tmdb_api_key", "TMDB_API_KEY")
TMDB_BASE = "https://api.themoviedb.org/3"
INCLUDE_SPECIALS = bool(_CFG.get("include_specials", True))
TMDB_WORKERS = int(_CFG.get("tmdb_workers", 8))
TMDB_SLEEP = float(_CFG.get("tmdb_sleep", 0.1))
TMDB_TIMEOUT = int(_CFG.get("tmdb_timeout", 15))
TMDB_RETRIES = int(_CFG.get("tmdb_retries", 3))
# 429 限流时单次按 Retry-After 等待的上限（秒），防止服务端给出离谱值导致线程长时间挂住
TMDB_RETRY_AFTER_MAX = float(_CFG.get("tmdb_retry_after_max", 30))
# TMDB append_to_response 单次最多附带 20 个子请求
_TMDB_APPEND_LIMIT = 20
_TMDB_MAX_429 = 5  # 单次请求最多容忍的 429 限流次数
# 上架宽限期（天）：播出日起 N 天内的集视同未播出，跳过不取流也不判死。
# 源站通常在播出后数天才上架，此窗口内的页面 404 不是“真无源”，若判死会永久丢集。
AIR_GRACE_DAYS = int(_CFG.get("air_grace_days", 5))
# NoSource 判死前换 IP 再探一次页面确认，两次都 404 才写 fail.txt（防 CDN/代理抖动误判永久丢集）
DEAD_CONFIRM = bool(_CFG.get("dead_confirm", True))
# ---- 批量误杀熔断 ----
# 连续 N 集结算为 dead（中间没有任何成功）时，先拿 CANARY_COUNT 个历史成功过的集复探：
# 金丝雀有成功 → 上游正常，只是这段剧真无源，清零计数继续跑；
# 金丝雀也全 dead / 无金丝雀可用 → 判定上游系统性变更，回滚本窗口写入的 fail 行并终止。0 关闭。
DEAD_STREAK_BREAKER = int(_CFG.get("dead_streak_breaker", 500))
CANARY_COUNT = 3


class DeadStreakBreaker(Exception):
    """连续 N 集判死且金丝雀复探失败：上游疑似系统性故障，已停止以免整批误杀。"""

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
# 每集结果里的"身份字段"：由 process_episode 用入参权威写入，合并剧级元数据时
# 必须保护、绝不允许被覆盖（下游据此做去重、拼文件名与 R2 对象键）。
_IDENTITY_KEYS = frozenset({
    "urls", "tmdbId", "season", "episode", "title", "fetched_at",
})
# tmdbId -> TMDB 剧名，由 main() 在季集展开后填充；源站不返回 title 时用它兜底
_TMDB_NAMES = {}


# ---------- TMDB 季集结构展开 ----------
_tmdb_session = std_requests.Session()
_tmdb_session.headers.update({"Accept": "application/json"})


class TmdbNotFound(Exception):
    """TMDB 查不到该剧（404）：剧级失效，整部剧永久跳过。"""


class TmdbAuthError(Exception):
    """TMDB 返回 401/403：API Key 无效或被封。全局性错误，继续跑每部剧都只会空转，
    expand_seasons 不吞它，直接抛回主线程终止进程。"""


_API_KEY_RE = re.compile(r"api_key=[^&\s]+")


def _redact(e):
    """requests 的 HTTPError 消息带完整 URL（含 api_key= 查询参数），打印/落盘前一律脱敏。"""
    return _API_KEY_RE.sub("api_key=***", f"{type(e).__name__}: {e}")


def _retry_after_seconds(resp, default=2.0, cap=None):
    """解析 Retry-After（秒数或 HTTP-date），非法/缺失用默认值，并设上限防止单次无限等待。
    cap 缺省取配置项 tmdb_retry_after_max（默认 30 秒）。"""
    if cap is None:
        cap = TMDB_RETRY_AFTER_MAX
    raw = resp.headers.get("Retry-After")
    wait = default
    if raw:
        try:
            wait = float(raw)
        except (TypeError, ValueError):
            try:
                from email.utils import parsedate_to_datetime
                wait = (parsedate_to_datetime(raw) - datetime.now(timezone.utc)).total_seconds()
            except Exception:
                wait = default
    return max(0.0, min(wait, cap))


def _tmdb_get(path, params=None):
    """调 TMDB v3 接口；404 抛 TmdbNotFound，401/403 抛 TmdbAuthError，其它错误重试 TMDB_RETRIES 次后抛出。"""
    q = {"api_key": TMDB_API_KEY}
    if params:
        q.update(params)
    last = None
    attempt = 0
    throttled = 0
    # 429 限流不计入 attempt（按 Retry-After 等待后原样重试），但设上限防止无限等待
    while attempt < TMDB_RETRIES:
        try:
            resp = _tmdb_session.get(f"{TMDB_BASE}{path}", params=q, timeout=TMDB_TIMEOUT)
            if resp.status_code == 404:
                raise TmdbNotFound(path)
            if resp.status_code in (401, 403):
                raise TmdbAuthError(f"TMDB HTTP {resp.status_code} for {path}: API Key 无效或被封")
            if resp.status_code == 429:
                throttled += 1
                last = Exception(f"HTTP 429 rate limited ({throttled}x)")
                if throttled > _TMDB_MAX_429:
                    break
                time.sleep(_retry_after_seconds(resp))
                continue
            resp.raise_for_status()
            if TMDB_SLEEP > 0:
                time.sleep(TMDB_SLEEP)
            return resp.json()
        except (TmdbNotFound, TmdbAuthError):
            raise
        except Exception as e:
            last = e
            attempt += 1
            if attempt < TMDB_RETRIES:
                time.sleep(1.5 * attempt)
    raise Exception(f"TMDB request failed for {path}: {_redact(last)}")


def fetch_seasons_from_tmdb(tmdb_id):
    """
    展开一部剧的季集结构（以 TMDB 为准）。

    返回：
        {
          "tmdbId": "123",
          "name": "...",
          "year": 2011 或 None,           # first_air_date 年份，作 tv_series.jsonl 缺 start_year 时的回退
          "seasons": [{"season": 0, "episodes": [1,2,...], "air_dates": {"1": "2011-04-17", ...}}, ...]
        }
    先取 /tv/{id} 拿 seasons 列表，再用 append_to_response=season/N 分批拉各季的 episodes，
    以 episode_number 为准（可正确处理编号不连续的情况），而不是简单用 episode_count 数数。
    air_dates 记录每集播出日期（缺失则不记），供 main() 跳过尚未播出的集（源站必然无源，
    不能因此判死）。
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
            eps = set()
            air_dates = {}
            for e in (sd.get("episodes") or []):
                num = e.get("episode_number")
                if num is None:
                    continue
                num = int(num)
                eps.add(num)
                ad = e.get("air_date")
                if ad:
                    air_dates[str(num)] = ad
            if eps:
                seasons.append({"season": n, "episodes": sorted(eps), "air_dates": air_dates})

    year = None
    fad = info.get("first_air_date") or ""
    if len(fad) >= 4 and fad[:4].isdigit():
        year = int(fad[:4])

    return {
        "tmdbId": str(tmdb_id),
        "name": info.get("name"),
        "year": year,
        # 已完结/取消：缺 air_date 的集视为已播出；在播/制作中：缺 air_date 视为 TBA 未播
        "ended": (info.get("status") or "") in ("Ended", "Canceled"),
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


def expand_seasons(ids, cache_file, fail_file, dead_shows, refresh_ongoing=False,
                   stop_event=None):
    """
    对 ids 中尚未缓存的剧并发调 TMDB 展开季集，追加写入 cache_file。
    TMDB 404 的剧写 fail_file（tid\\t-\\t-）并加入 dead_shows。
    refresh_ongoing=True 时，缓存中 ended 非 True 的剧（在播 / 旧格式缺 ended 字段）也重新展开，
    以追加新行覆盖旧行（load_seasons_cache 同 tmdbId 取最后一行），捞回新播出的集。
    stop_event（可选，pipeline 模式用）：置位后不再展开新的剧，已在跑的自然结束。
    全量首跑要展开近 10 万部（约 2.5 小时），没有它的话 Ctrl+C 要等到展开跑完。
    返回 {tid: cache_entry}。
    """
    cache = load_seasons_cache(cache_file)
    todo = []
    refreshed = 0
    for tid in ids:
        if tid in dead_shows:
            continue
        if tid not in cache:
            todo.append(tid)
        elif refresh_ongoing and cache[tid].get("ended") is not True:
            todo.append(tid)
            refreshed += 1
    print(f"[tmdb] 季集缓存 {len(cache)} 部 | 需展开 {len(todo)} 部"
          + (f"（其中刷新未完结 {refreshed} 部）" if refresh_ongoing else ""))
    if not todo:
        return cache

    lock = threading.Lock()
    done = 0

    def one(tid):
        # 停止信号已置位：不再打 TMDB。返回"未展开"，该剧下次运行再试
        # （不写缓存也不判死，与瞬时错误同口径）。
        if stop_event is not None and stop_event.is_set():
            return tid, None, "stopped"
        try:
            return tid, fetch_seasons_from_tmdb(tid), None
        except TmdbNotFound:
            return tid, None, "not_found"
        except TmdbAuthError:
            raise
        except Exception as e:
            return tid, None, _redact(e)

    ex = ThreadPoolExecutor(max_workers=TMDB_WORKERS)
    try:
        futures = [ex.submit(one, tid) for tid in todo]
        for fut in as_completed(futures):
            # TmdbAuthError 在此抛回主线程：API Key 失效时整个任务立即终止，不再空转
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
                elif err == "stopped":
                    pass   # 停止信号下的静默跳过，不刷屏
                else:
                    # 瞬时错误：本次不缓存也不判死，下次运行再试
                    print(f"[tmdb] {done}/{len(todo)} {tid} 展开失败（下次再试）: {err}")
    except BaseException:
        # TmdbAuthError / Ctrl+C：取消尚未开始的剧，不再用失效 Key 把剩余几千部都打一遍 401
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    ex.shutdown(wait=True)
    return cache


# ---------- 辅助函数 ----------
class HttpStatusError(Exception):
    """vidup / enc-dec 返回非 2xx。带结构化状态码，判死只看这里的 status，不做字符串匹配
    （tmdb id 本身可能含 "404"/"503" 等子串，字符串匹配会误判）。"""

    def __init__(self, status, where):
        super().__init__(f"HTTP {status} at {where}")
        self.status = status
        self.where = where


class NoSource(Exception):
    """已确认真无源：页面 404，或有 server 列表但所有 server 的 stream 请求都 404。
    这是唯一允许把一集判死（写 fail.txt 永久排除）的证据；其它任何异常都只算瞬时，交给多轮重试。
    误判死的代价是永久丢失一集，误重试的代价只是多几次请求，所以判死必须保守。"""


def _check(resp, where):
    if resp.status_code >= 400:
        raise HttpStatusError(resp.status_code, where)
    return resp


def _json(resp, where):
    """enc-dec 偶发返回 HTML（Cloudflare 页 / 网关错误页），json() 会抛 ValueError；
    包一层给出可读文案，并保持“默认瞬时”语义。"""
    try:
        return resp.json()
    except Exception as e:
        raise Exception(f"Non-JSON response at {where}: {e}")


def validate(data, path):
    if not isinstance(data, dict):
        raise Exception(f"API Error at {path}: unexpected payload type {type(data).__name__}")
    if data.get("status") != 200:
        error_msg = data.get("error", "unknown")
        raise Exception(f"API Error at {path}: status={data.get('status')}, error={error_msg}")
    return data["result"]


def _is_retriable(exc):
    """白名单判死：只有 NoSource（页面 404 / 所有 server 的 stream 都 404）不重试，其余一律重试。
    网络/SSL/超时/代理/5xx/403/enc-dec 抽风/JSON 解析失败/字段缺失……全部视为瞬时。"""
    return not isinstance(exc, NoSource)


def _ep_label(tid, season, episode):
    return f"{tid} S{int(season):02d}E{int(episode):02d}"


# ---------- 取流源（provider）----------
# 每个 provider 的签名：fn(session, tid, season, episode) -> (urls, title)
#   urls  非空列表，每项 {url, provider, type("m3u8"|"mp4"), headers, quality, size}
#   title 源站给的剧名，可为 None
# 语义：真无源抛 NoSource（唯一判死证据）；其余任何异常都视为瞬时错误。
def _url_entry(url, provider, type_, headers=None, quality=None, size=None):
    return {
        "url": url,
        "provider": provider,
        "type": type_,
        # 下载该 url 时必须携带的请求头（空表示用 download_tv 默认头）
        "headers": dict(headers or {}),
        # 源站声明的画质高度（int）与文件大小（bytes）；m3u8 源不声明，留 None 由下游探测
        "quality": quality,
        "size": size,
    }


def _fetch_vidup_like(session, site, api_name, tid, season, episode):
    """vidup.to / vidfast.vc 同构链路：
    页面正则 → enc-{api} → servers POST → dec-{api} → 逐 server stream POST → dec-{api} 取 url。"""
    label = _ep_label(tid, season, episode)
    page_headers, api_headers = _site_headers(site)

    # 1. 获取页面，提取加密文本
    resp = session.get(f"https://{site}/tv/{tid}/{season}/{episode}/", timeout=TIMEOUT, headers=page_headers)
    if resp.status_code == 404:
        raise NoSource(f"page 404 for {label}")
    _check(resp, "page")
    html = resp.text

    match_1 = re.search(r'\\"en\\":\\"(.*?)\\"', html)
    match = re.search(r'\\"token\\":\\"(.*?)\\"', html)
    if match_1:
        text = match_1.group(1)
    elif match:
        text = match.group(1)
    else:
        raise Exception(f"Extract failed (retriable) for {label}")

    # 2. 调用 enc-{api} 获取 parts（text 来自页面正则，可能含 +/= 等保留字符，必须编码）
    enc_url = f"{API}/enc-{api_name}?text={quote(text, safe='')}"
    resp = _check(session.get(enc_url, timeout=TIMEOUT, headers=api_headers), f"enc-{api_name}")
    parts = validate(_json(resp, f"enc-{api_name}"), enc_url)
    if not isinstance(parts, dict):
        raise Exception(f"API Error at {enc_url}: result is not an object")
    servers = parts.get('servers')
    stream = parts.get('stream')
    # 2026-09 起 enc-dec 返回 token 为空串，后续接口不带 X-CSRF-Token 也能正常取流；
    # token 仅在非空时携带，不再作为必需字段（否则整批 0 成功）
    token = parts.get('token')
    if not (servers and stream):
        raise Exception(f"API Error at {enc_url}: missing servers/stream in result")

    headers_with_token = dict(api_headers)
    if token:
        headers_with_token["X-CSRF-Token"] = token

    # 3. 获取加密的服务器列表
    resp = _check(session.post(servers, headers=headers_with_token, timeout=TIMEOUT), "servers")
    servers_encrypted = resp.text

    # 4. 解密服务器列表
    dec_url = f"{API}/dec-{api_name}"
    resp = _check(session.post(dec_url, json={"text": servers_encrypted}, timeout=TIMEOUT), f"dec-{api_name}(servers)")
    servers_decrypted = validate(_json(resp, f"dec-{api_name}(servers)"), dec_url)

    if not servers_decrypted or not isinstance(servers_decrypted, list):
        raise Exception("No servers found")

    # 遍历所有服务器，收集全部可用 url
    last_server_error = None
    urls = []
    result_title = None
    stream_404 = 0
    for server in servers_decrypted:
        server_name = server.get('name', 'unknown') if isinstance(server, dict) else 'unknown'
        try:
            data_val = server['data']
            # 5. 获取加密的流数据
            stream_url = f"{stream}/{data_val}"
            resp = _check(session.post(stream_url, headers=headers_with_token, timeout=TIMEOUT), "stream")
            stream_encrypted = resp.text

            # 6. 解密流数据
            resp = _check(session.post(dec_url, json={"text": stream_encrypted}, timeout=TIMEOUT), f"dec-{api_name}(stream)")
            stream_decrypted = validate(_json(resp, f"dec-{api_name}(stream)"), dec_url)
            if not isinstance(stream_decrypted, dict):
                raise Exception("decrypted stream is not an object")

            url = stream_decrypted.get("url")
            if not url:
                raise Exception("Missing url in decrypted data")
            # 只以 url 为成功条件；返回的 tmdbId 仅用于告警，不参与 key 也不作为成功条件
            r_tid = stream_decrypted.get("tmdbId")
            if r_tid is not None and str(r_tid) != str(tid):
                print(f"  [warn] {label} {api_name} server '{server_name}' 返回 tmdbId={r_tid}，与入参不一致，key 仍用入参")
            if all(u["url"] != url for u in urls):
                urls.append(_url_entry(url, api_name, "m3u8"))
            if result_title is None:
                result_title = stream_decrypted.get("title")
        except HttpStatusError as e:
            # 只有源站 stream 接口本身的 404 才算“该 server 无源”；enc-dec 的 404 是服务故障
            if e.status == 404 and e.where == "stream":
                stream_404 += 1
            last_server_error = e
            print(f"  {api_name} server '{server_name}' failed for {label}: {e}")
            continue
        except Exception as e:
            last_server_error = e
            print(f"  {api_name} server '{server_name}' failed for {label}: {e}")
            continue

    if urls:
        return urls, result_title
    if stream_404 == len(servers_decrypted):
        raise NoSource(f"all {stream_404} {api_name} servers returned 404 for {label}")
    raise Exception(f"All {api_name} servers failed for {label}. Last error: {last_server_error}")


def _fetch_vidup(session, tid, season, episode):
    return _fetch_vidup_like(session, "vidup.to", "vidup", tid, season, episode)


def _fetch_vidfast(session, tid, season, episode):
    return _fetch_vidup_like(session, "vidfast.vc", "vidfast", tid, season, episode)


def _fetch_vidlink(session, tid, season, episode):
    """vidlink.pro：enc-vidlink(tmdb id) → GET /api/b/tv/{enc}/{s}/{e} 直接返 JSON（不需再 dec）。
    stream.qualities.{360,480,720,1080}.{url,size,...}，url 为带时效签名的 mp4 直链。
    无源的唯一证据是 HTTP 200 + body null；404 可能是路由变更 / enc 值异常 / WAF 拦截，
    按瞬时处理（判死必须保守）。"""
    label = _ep_label(tid, season, episode)
    enc_url = f"{API}/enc-vidlink?text={quote(str(tid), safe='')}"
    resp = _check(session.get(enc_url, timeout=TIMEOUT, headers=VIDLINK_API_HEADERS), "enc-vidlink")
    enc = validate(_json(resp, "enc-vidlink"), enc_url)
    if not isinstance(enc, str) or not enc:
        raise Exception(f"API Error at {enc_url}: result is not a string")

    # enc 作为路径段拼接，含 / ? # 等字符会拼错 URL，必须编码
    resp = session.get(f"https://vidlink.pro/api/b/tv/{quote(enc, safe='')}/{season}/{episode}",
                       timeout=TIMEOUT, headers=VIDLINK_API_HEADERS)
    _check(resp, "vidlink-api")
    data = _json(resp, "vidlink-api")
    if data is None:
        raise NoSource(f"vidlink api null for {label}")
    if not isinstance(data, dict):
        raise Exception(f"vidlink api: unexpected payload type {type(data).__name__}")

    stream = data.get("stream")
    qualities = stream.get("qualities") if isinstance(stream, dict) else None
    if not isinstance(qualities, dict) or not qualities:
        # 有响应但没有画质表：不是 null，不能当无源证据，交给重试
        raise Exception(f"vidlink api: missing stream.qualities for {label}")

    entries = []
    seen_urls = set()
    for q, info in qualities.items():
        url = info.get("url") if isinstance(info, dict) else None
        # 多个画质可能指向同一 url，按 url 去重（与 vidup 分支一致）
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        try:
            quality = int(q)
        except (TypeError, ValueError):
            quality = None
        size = info.get("size")
        # bool 是 int 子类：size=true 会被 int() 成 1，必须排除
        if isinstance(size, bool) or not isinstance(size, (int, float)) or size <= 0:
            size = None
        else:
            size = int(size)
        entries.append(_url_entry(url, "vidlink", "mp4", VIDLINK_DOWNLOAD_HEADERS, quality, size))
    if not entries:
        raise Exception(f"vidlink api: qualities without url for {label}")
    # 画质从高到低，下游按顺序择优
    entries.sort(key=lambda u: (u["quality"] is None, -(u["quality"] or 0)))
    return entries, None


PROVIDERS = {
    "vidup": _fetch_vidup,
    "vidlink": _fetch_vidlink,
    "vidfast": _fetch_vidfast,
}


def _resolve_providers(names):
    """校验 provider 名单（config.yaml providers / --providers），去重保序；未知名字直接退出。
    允许传逗号分隔字符串（config 误写成 "vidup,vidlink" 时不至于逐字符报"未知取流源 'v'"）。"""
    if isinstance(names, str):
        names = names.split(",")
    out = []
    for n in names:
        n = str(n).strip()
        if not n:
            continue
        if n not in PROVIDERS:
            raise SystemExit(f"未知取流源 '{n}'，可选：{', '.join(PROVIDERS)}")
        if n not in out:
            out.append(n)
    if not out:
        raise SystemExit("providers 为空：至少配置一个取流源")
    return out


ACTIVE_PROVIDERS = _resolve_providers(_CFG.get("providers") or DEFAULT_PROVIDERS)


def process_episode(tid, season, episode, providers=None):
    """处理单集，仅对瞬时错误重试。

    返回 (status, 结果字典或None)，status 三态供多轮捞回区分：
      - "ok"    成功，附结果字典 {urls, tmdbId, season, episode, title, + 静态元数据}
      - "dead"  确认真无源（NoSource）→ 这一集永久排除（不影响同剧其它集）
      - "retry" 瞬时错误换 IP 重试 MAX_RETRIES 次仍失败 → 下一轮重跑

    多源：同一次尝试内按 providers 顺序逐家取流，把各家给出的 url 全部汇总
    （按 url 去重、保持 providers 顺序），下载侧按节点顺序逐个尝试，某家的流
    画质/码率不达标时还能落到下一家；有任一家给出 url 即 ok。
    全部跑完仍无 url 时，只要有任一家是瞬时错误就按瞬时处理（重试），
    全部 NoSource 才算一次“疑似无源”。

    判死二次确认（DEAD_CONFIRM）：首次全家 NoSource 不立即判死，换 IP 再完整跑一次，
    累计两次全家 NoSource 才返回 dead；确认过程最多多花 1 次尝试（总尝试数 ≤ MAX_RETRIES+1）。
    若确认那次是瞬时错误且尝试已耗尽，返回 retry 交给下一轮（宁可多试，不误判永久丢集）。
    """
    label = _ep_label(tid, season, episode)
    providers = list(ACTIVE_PROVIDERS if providers is None else providers)
    attempt = 0
    nosource_hits = 0
    while True:
        attempt += 1
        # 每集、每次重试用一个独立 Session：复用连接、绑定本次随机出口 IP
        with requests.Session(impersonate="chrome") as session:
            proxy = build_proxy()
            if proxy:
                session.proxies = proxy
            try:
                urls = []
                seen_urls = set()
                result_title = None
                nosource_errors = []
                transient_errors = []
                for name in providers:
                    try:
                        provider_urls, provider_title = PROVIDERS[name](session, tid, season, episode)
                    except NoSource as e:
                        nosource_errors.append(f"{name}: {e}")
                        continue
                    except Exception as e:
                        transient_errors.append(f"{name}: {e}")
                        print(f"  [{name} 瞬时错误] {label}: {e}")
                        continue
                    if not provider_urls:
                        transient_errors.append(f"{name}: empty urls")
                        continue
                    # 汇总各家 url：按 providers 顺序拼接、按 url 去重，
                    # 给下载侧尽可能多的候选节点（某家画质不达标时还能落到下一家）
                    for u in provider_urls:
                        if u["url"] not in seen_urls:
                            seen_urls.add(u["url"])
                            urls.append(u)
                    if result_title is None and provider_title:
                        result_title = provider_title

                if urls:
                    # 恒用入参 tid/season/episode 作为 key，保证全链路一致：
                    # 续跑去重、元数据查表、下游 R2 路径与文件名都对得上。
                    result = {
                        "urls": urls,
                        "tmdbId": str(tid),
                        "season": int(season),
                        "episode": int(episode),
                        # 源站 title 偶尔为空，回退 TMDB 剧名，再兜底空串（下游 download_tv/fetch_subtitles 按 str 使用）
                        "title": result_title or _TMDB_NAMES.get(str(tid)) or "",
                        # 取流时刻（UTC 秒级时间戳）。results.jsonl 是追加写，同一集
                        # 复扫/重跑会留下多行；下游据此挑真正最新的一条，而不是靠
                        # "文件里最后出现"这种会被手工编辑破坏的位置假设。
                        # 对 vidlink 这类带时效签名的直链尤为关键：拿到过期 url
                        # 等于白跑一次下载。
                        "fetched_at": int(time.time()),
                    }
                    # 合并剧级静态元数据，但**绝不覆盖上面的身份字段**：元数据来自
                    # tv_series.jsonl，是剧级的（一剧一条），若它哪天多出 season/
                    # episode/tmdbId 之类的键，直接 update 会把本集的真实身份改掉，
                    # 下游据此拼 R2 路径与文件名，成片会静默写到错误的位置且极难发现。
                    for meta_key, meta_value in _SERIES_META.get(str(tid), {}).items():
                        if meta_key not in _IDENTITY_KEYS:
                            result[meta_key] = meta_value
                    return "ok", result

                if transient_errors:
                    raise Exception(f"All providers failed for {label}: {transient_errors}")
                raise NoSource("; ".join(nosource_errors))

            except Exception as e:
                if not _is_retriable(e):
                    nosource_hits += 1
                    if not DEAD_CONFIRM or nosource_hits >= 2:
                        print(f"  [无源 404] {label}: {e}")
                        return "dead", None
                    # 首次 404 只算“疑似无源”：换出口 IP 再完整探一次，防 CDN/代理抖动误判永久丢集
                    print(f"  [疑似无源，换 IP 确认] {label}: {e}")
                    time.sleep(RETRY_DELAY)
                    continue
                # 命中过 NoSource 时多给 1 次预算，让确认那一跳不挤占常规重试次数
                budget = MAX_RETRIES + (1 if (DEAD_CONFIRM and nosource_hits) else 0)
                print(f"  [Attempt {attempt}/{budget}] 瞬时错误 for {label}: {e}")
                if attempt < budget:
                    time.sleep(RETRY_DELAY)
                    continue
                print(f"  All {budget} attempts failed for {label}")
                return "retry", None


def _load_ok_keys(results_file):
    """results.jsonl 中已成功的 (tid, season, episode) 集合。"""
    ok = set()
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
                        ok.add((str(tid), int(s), int(e)))
                except Exception:
                    pass
    return ok


def _load_fail(fail_file):
    """解析 fail.txt，返回 (集级真无源集合, 剧级失效 tid 集合)；retry-exhausted 行忽略。"""
    dead_eps = set()
    dead_shows = set()
    if fail_file.exists():
        with open(fail_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) == 4 and parts[3] == RETRY_EXHAUSTED_TAG:
                    continue
                if len(parts) != 3:
                    continue
                tid, s, e = parts
                if s == "-" and e == "-":
                    dead_shows.add(tid)
                    continue
                try:
                    dead_eps.add((tid, int(s), int(e)))
                except ValueError:
                    pass
    return dead_eps, dead_shows


def _load_exhausted(fail_file):
    """fail.txt 中已标记 retry-exhausted 的 (tid, season, episode) 集合。

    仅用于写入前去重，避免同一集在多次运行中被反复追加、让文件无限膨胀。
    """
    exhausted = set()
    if fail_file.exists():
        with open(fail_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 4 and parts[3] == RETRY_EXHAUSTED_TAG:
                    try:
                        exhausted.add((parts[0], int(parts[1]), int(parts[2])))
                    except ValueError:
                        pass
    return exhausted


def load_processed(results_file, fail_file):
    """返回 (已处理的 (tid, season, episode) 集合, 剧级失效的 tid 集合)。

    fail.txt 行格式：
      - `tid\\ts\\te`                      集级真无源 → 已处理
      - `tid\\t-\\t-`                      剧级失效（TMDB 404）
      - `tid\\ts\\te\\tretry-exhausted`     上次运行轮次耗尽仍是瞬时失败 → 不算已处理，本次自动再试
    """
    dead_eps, dead_shows = _load_fail(fail_file)
    return _load_ok_keys(results_file) | dead_eps, dead_shows


def load_dead_episodes(results_file, fail_file):
    """--recheck-dead 用：fail.txt 里集级真无源、且至今未在 results.jsonl 成功过的集。
    （复查成功的集会追加进 results.jsonl，fail.txt 旧行保留不动；results 优先，故不会重复复查。）"""
    dead_eps, _ = _load_fail(fail_file)
    return dead_eps - _load_ok_keys(results_file)


def _is_unaired(air_date, today, ended, grace_days=None):
    """判断一集是否尚未播出 / 尚在上架宽限期（源站必然或大概率无源，本次跳过、不写 fail.txt，下次运行再纳入）。
    - air_date + grace_days 晚于今天 → 未播出（含刚播出但源站尚未上架的窗口）
    - air_date 缺失：已完结/取消的剧视为已播出（老剧 TMDB 缺日期很常见，不能因此永久跳过）；
      仍在播/制作中的剧视为 TBA 占位集 → 未播出
    grace_days 缺省取配置项 air_grace_days。
    """
    if grace_days is None:
        grace_days = AIR_GRACE_DAYS
    if not air_date:
        return not ended
    try:
        return date.fromisoformat(air_date[:10]) + timedelta(days=grace_days) > today
    except ValueError:
        return False  # 日期格式异常：不因此跳过，交给取流去判


def _pick_canaries(recent_ok, results_file, k):
    """挑 k 个历史成功集作金丝雀：优先本轮刚成功的集（最能代表当前代码 + 当前上游），
    本轮尚无成功时才回退解析 results.jsonl。"""
    pool = list(recent_ok) or list(_load_ok_keys(results_file))
    return random.sample(pool, min(k, len(pool)))


def _canaries_alive(recent_ok, results_file, k=None):
    """复探 k 个历史成功集，返回三态：
    True  任一仍能取到流 → 上游正常；
    False 无一成功且至少一个明确 dead → 上游疑似 404 化；
    None  无金丝雀可用 / 全是瞬时错误 → 无法判断（调用方放行，不熔断）。"""
    k = CANARY_COUNT if k is None else k
    canaries = _pick_canaries(recent_ok, results_file, k)
    if not canaries:
        print("  [熔断] 无历史成功集可作金丝雀，无法判断上游状态，放行。")
        return None
    saw_dead = False
    for tid, s, e in canaries:
        try:
            status, _ = process_episode(tid, s, e)
        except Exception as e_:
            status = f"exception: {e_}"
        print(f"  [熔断] 金丝雀 {_ep_label(tid, s, e)} → {status}")
        if status == "ok":
            return True
        if status == "dead":
            saw_dead = True
    return False if saw_dead else None


def _rollback_fail_tail(fail_file, items):
    """撤销本次连败窗口写入 fail.txt 的末尾 len(items) 行（写入受 lock 串行化，必然位于文件尾）。
    只做字节级 truncate，不重写整个文件，中途被 kill 也不会损坏已有内容。
    尾部与预期不一致时不动文件，返回 False 由调用方提示人工处理。"""
    if not items:
        return True
    tail = "".join(f"{tid}\t{s}\t{e}\n" for tid, s, e in items).encode("utf-8")
    with open(fail_file, "rb+") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size < len(tail):
            return False
        f.seek(size - len(tail))
        if f.read() != tail:
            return False
        f.truncate(size - len(tail))
    return True


def run_batch(to_process, results_file, fail_file, max_workers, write_dead=True,
              on_result=None, stop_event=None):
    """并发处理一批集，实时落盘 ok/dead，返回本批“瞬时耗尽”待重跑的三元组列表。
    write_dead=False（--recheck-dead 模式）：仍无源的集不再重复写 fail.txt（旧行已在），且不启用熔断。
    熔断：连续 DEAD_STREAK_BREAKER 集 dead（无任何成功间隔）→ 持锁暂停全员，复探金丝雀；
    金丝雀存活或无法判断则清零继续，明确失败则回滚窗口内 fail 行并抛 DeadStreakBreaker。

    on_result（可选，pipeline 模式用）：取到一条 ok 结果时回调一次，让下游
    （下载侧）立刻拿到这一集，而不必等整轮跑完再读文件。
      - 落盘照旧：回调只是"额外的快车道"，results_file 一行不少，
        故断点续跑 / fetched_at 择新 / --recheck-dead 全部不受影响；
      - **在写盘锁之外调用**，故允许阻塞（pipeline 的队列满时正是靠它形成反压）；
        放在锁内会让一条的等待堵死其余所有取流线程，反压就变成了冻结；
      - 回调抛异常不得影响取流：已落盘的结果不能因为下游出问题而白费。

    stop_event（可选，pipeline 模式用）：协作式停止信号。置位后：
      - 尚未开跑的集直接跳过（不再消耗代理流量），按 retry 收集；
      - 已在跑的那一集仍会跑完（HTTP 请求本身无法中途取消），结果照常落盘。
    没有它的话，pipeline 被 Ctrl+C 时取流线程会把整批几百万集跑完才罢休。
    """
    lock = threading.Lock()
    ok_count = 0
    dead_count = 0
    retry_items = []
    breaker_on = write_dead and DEAD_STREAK_BREAKER > 0
    dead_streak = []   # 当前连败窗口内已写入 fail.txt 的集（按写入顺序）
    recent_ok = deque(maxlen=50)   # 本轮最近成功的集，作金丝雀候选
    tripped = False

    def process_one(item):
        nonlocal tripped
        tid, s, e = item
        # 停止信号已置位：不再开新的取流请求。按 retry 收集而非 dead——
        # 这些集从未被判过无源，绝不能写进 fail.txt 被永久排除。
        if stop_event is not None and stop_event.is_set():
            with lock:
                retry_items.append(item)
            return "skipped"
        status, result = process_episode(tid, s, e)
        label = _ep_label(tid, s, e)
        with lock:
            if status == "ok" and result:
                # 熔断后到达的成功仍照常落盘（真成功没理由丢）
                with open(results_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                print(f"✅ SUCCESS: {result.get('title')} ({label})")
                dead_streak.clear()
                recent_ok.append((str(tid), int(s), int(e)))
            elif tripped:
                return "retry"   # 熔断后到达的 dead/retry 一律不落盘，留给下次运行
            elif status == "retry":
                retry_items.append(item)
                print(f"🔁 RETRY-LATER: {label}")
            else:
                if write_dead:
                    with open(fail_file, 'a', encoding='utf-8') as f:
                        f.write(f"{tid}\t{s}\t{e}\n")
                print(f"❌ DEAD: {label}")
                if breaker_on:
                    dead_streak.append(item)
                    if len(dead_streak) >= DEAD_STREAK_BREAKER:
                        print(f"\n⚠️  [熔断] 连续 {len(dead_streak)} 集判死且无一成功，暂停并复探金丝雀…")
                        verdict = _canaries_alive(recent_ok, results_file)
                        if verdict is False:
                            tripped = True
                            rolled = _rollback_fail_tail(fail_file, dead_streak)
                            shows = sorted({t for t, _, _ in dead_streak})
                            print(f"  [熔断] 已回滚 fail.txt 末尾 {len(dead_streak)} 行。" if rolled else
                                  "  [熔断] fail.txt 尾部与预期不符，未回滚，请人工核对以下剧：")
                            print(f"  [熔断] 涉及剧 tmdb_id：{' '.join(shows)}")
                            raise DeadStreakBreaker(
                                f"连续 {len(dead_streak)} 集判死且 {CANARY_COUNT} 个历史成功集复探全部失败，"
                                f"疑似上游系统性变更，已停止以免整批误杀")
                        print("  [熔断] 金丝雀存活，上游正常，这段剧为真无源，继续。\n" if verdict else
                              "  [熔断] 金丝雀结果无法判断，放行继续。\n")
                        dead_streak.clear()
        # ⚠️ 回调必须在**锁外**执行：它可能阻塞（pipeline 模式下队列满时要等
        # 下载侧腾出空位，最长可达数分钟）。若放在临界区内，这一条的等待会把
        # 其余 max_workers-1 个取流线程全堵在 lock 上，整批取流吞吐直接归零——
        # 反压本意是"降速"，绝不该变成"冻结"。
        # 落盘已在锁内完成，故此处失败也不丢数据。
        if status == "ok" and result and on_result is not None:
            try:
                on_result(result)
            except Exception as exc:  # noqa: BLE001
                print(f"⚠️  on_result 回调失败（结果已落盘，不影响取流）: {exc}")
        return status

    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {executor.submit(process_one, item): item for item in to_process}
        for future in as_completed(futures):
            item = futures[future]
            try:
                status = future.result()
                if status == "ok":
                    ok_count += 1
                elif status in ("retry", "skipped"):
                    # skipped = 停止信号置位后未开跑的集，已按 retry 收集，
                    # 既不算成功也不算无源。
                    pass
                else:
                    dead_count += 1
            except DeadStreakBreaker:
                raise
            except Exception as e:
                print(f"⚠️  Unexpected exception for {_ep_label(*item)}: {e}")
                with lock:
                    retry_items.append(item)
    except BaseException:
        # Ctrl+C / 致命错误：取消尚未开始的集，不等排队任务跑完（否则数万集要跑到底才能退出）；
        # 正在跑的集让它自然结束，避免 kill -9 截断正在写的 results.jsonl 行
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    executor.shutdown(wait=True)

    print(f"\n本批完成 | 成功 {ok_count} | 真无源 {dead_count} | 待重跑 {len(retry_items)}")
    return retry_items


def _parse_args(argv):
    parser = argparse.ArgumentParser(description="TMDB 季集展开 + 多源（vidup/vidlink/vidfast）逐集取流")
    parser.add_argument(
        "--refresh-ongoing", action="store_true",
        help="重新展开缓存中未完结（ended 非 true，含旧格式缓存行）的剧，捞回新播出的集；默认只展开未缓存的剧",
    )
    parser.add_argument(
        "--recheck-dead", action="store_true",
        help="只复查 fail.txt 中集级真无源、且至今未成功的集（源站后补上架时捞回）；"
             "成功追加 results.jsonl，仍无源不重复写 fail.txt",
    )
    parser.add_argument(
        "--providers", default=None,
        help=f"取流源顺序，逗号分隔，覆盖 config.yaml providers；可选：{','.join(PROVIDERS)}",
    )
    return parser.parse_args(argv)


def main(argv=None, on_result=None, stop_event=None):
    """取流主流程。

    on_result（可选）：pipeline 模式下把每条 ok 结果实时推给下游（下载侧），
    而不必等整轮跑完再读 results.jsonl。落盘行为完全不变，回调只是快车道。

    stop_event（可选）：pipeline 模式下的协作式停止信号，让取流能在下载侧
    收工 / 用户 Ctrl+C 后及时停下，而不是把整批几百万集跑完才罢休。
    置位后：不开新一轮、不发起新请求、季集展开与轮间退避立即醒来。
    """
    global ACTIVE_PROVIDERS
    args = _parse_args([] if argv is None else argv)
    if args.providers:
        ACTIVE_PROVIDERS = _resolve_providers(args.providers)
    print(f"取流源顺序：{' → '.join(ACTIVE_PROVIDERS)}")
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
    # ⚠️ pipeline 模式下这一步**不产出任何取流结果**：全新部署、缓存为空时，
    # 9.7 万部剧要展开约 2.5 小时，期间下载侧会一直拿到 "wait"（它会按
    # stream_idle_poll_seconds 让出 CPU，不空烧）。续跑时缓存已在，只需几十秒。
    cache = expand_seasons(ids, cache_file, fail_file, dead_shows,
                           refresh_ongoing=args.refresh_ongoing,
                           stop_event=stop_event)
    if stop_event is not None and stop_event.is_set():
        print("\n==> 收到停止信号，取流在季集展开阶段退出（缓存已落盘，下次续跑）。")
        return

    # ---- 第二步：按 ids.txt 顺序展开成 (tid, season, episode) 任务，剔除已处理 ----
    to_process = []
    total_eps = 0
    skipped_unaired = 0
    today = date.today()
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
        if entry.get("name"):
            _TMDB_NAMES[tid] = entry["name"]
        # 旧版缓存行没有 ended / air_dates 字段：air_dates 缺失时不做未播判断（全部纳入），
        # 避免把老缓存里的集全当成 TBA 跳过
        ended = bool(entry.get("ended"))
        for s in entry["seasons"]:
            air_dates = s.get("air_dates")
            for e in s["episodes"]:
                total_eps += 1
                key = (tid, int(s["season"]), int(e))
                if key in processed:
                    continue
                # 未播集不发请求、也不写 fail.txt，下次运行到播出日期后自动纳入
                if air_dates is not None and _is_unaired(air_dates.get(str(e)), today, ended):
                    skipped_unaired += 1
                    continue
                to_process.append(key)

    if args.recheck_dead:
        # 复查模式：只跑 fail.txt 里集级真无源且至今未成功的集（源站后补上架 / 当初误判），
        # 仍限定在 ids.txt 内、剔除剧级失效；上面的循环仍需跑一遍以填充 _SERIES_META/_TMDB_NAMES
        order = {tid: i for i, tid in enumerate(ids)}
        to_process = sorted(
            (k for k in load_dead_episodes(results_file, fail_file)
             if k[0] in order and k[0] not in dead_shows),
            key=lambda k: (order[k[0]], k[1], k[2]),
        )
        print(f"[recheck-dead] 待复查真无源集: {len(to_process)}")

    print(f"Total shows: {len(ids)} | Total episodes: {total_eps} | "
          f"Already processed: {len(processed)} | Unaired skipped: {skipped_unaired} | "
          f"To process: {len(to_process)}")

    if not to_process:
        print("All episodes processed.")
        return

    max_workers = _CFG.get("max_workers", 50)
    max_rounds = _CFG.get("max_rounds", 8)

    # ---- 内嵌多轮捞回：首轮跑全部，之后每轮只重跑上一轮“瞬时耗尽”的集 ----
    pending = to_process
    round_no = 0
    while pending:
        # 停止信号：不再开新一轮。未结算的集没写任何文件，下次运行自动续跑。
        if stop_event is not None and stop_event.is_set():
            print(f"\n==> 收到停止信号，取流不再开新一轮（剩余 {len(pending)} 集留待下次运行）。")
            break
        round_no += 1
        print(f"\n{'=' * 70}")
        print(f"==> 第 {round_no}/{max_rounds} 轮 | 待处理 {len(pending)} 集")
        print(f"{'=' * 70}")

        try:
            retry_items = run_batch(pending, results_file, fail_file, max_workers,
                                    write_dead=not args.recheck_dead,
                                    on_result=on_result, stop_event=stop_event)
        except DeadStreakBreaker as e:
            print(f"\n{'!' * 70}\n==> 熔断退出：{e}\n"
                  f"    本窗口的 fail 行已回滚，其余未结算的集下次运行自动续跑；"
                  f"请先人工核实 {' / '.join(ACTIVE_PROVIDERS)} 与 enc-dec 链路是否变更再重启。\n{'!' * 70}")
            raise SystemExit(2)

        if not retry_items:
            print("\n==> 瞬时失败已清零，所有有源集已捞干净，正常结束。")
            break
        if stop_event is not None and stop_event.is_set():
            # 本轮是被停止信号提前截断的：retry_items 里多半是"未开跑"的集，
            # 它们从未被真正尝试过，绝不能当成"重试耗尽"写进 fail.txt。
            print(f"\n==> 收到停止信号，本轮剩余 {len(retry_items)} 集留待下次运行。")
            break
        if round_no >= max_rounds:
            # 第 4 列标记：这些集只是“重试耗尽”而非真无源，load_processed 不会把它们当作已处理，
            # 下次运行会自动重跑；只是留痕方便排查本次运行的瞬时失败规模。
            # 按 (tid, s, e) 去重后再追加：同一集连续多次运行都耗尽时，若无脑
            # 追加会让 fail.txt 无限膨胀（每轮一条），拖慢每次启动的 _load_fail，
            # 也让漏斗统计把同一集重复计数。
            existing_exhausted = _load_exhausted(fail_file)
            fresh = [item for item in retry_items
                     if (str(item[0]), int(item[1]), int(item[2]))
                     not in existing_exhausted]
            if fresh:
                with open(fail_file, 'a', encoding='utf-8') as f:
                    for tid, s, e in fresh:
                        f.write(f"{tid}\t{s}\t{e}\t{RETRY_EXHAUSTED_TAG}\n")
            print(f"\n==> 已达最大轮数 {max_rounds}，剩余 {len(retry_items)} 集瞬时失败记入 fail_file"
                  f"（标记 {RETRY_EXHAUSTED_TAG}，新增 {len(fresh)} 行，下次运行自动重试）。")
            break

        # 轮间退避：瞬时失败多半是代理/enc-dec/源站抖动，立刻重跑大概率撞同一堵墙。
        # 若本轮几乎全部 retry（≥90% 且 ≥50 集），判定为基础设施故障，直接等上限。
        ratio = len(retry_items) / max(len(pending), 1)
        outage = ratio >= OUTAGE_RETRY_RATIO and len(retry_items) >= OUTAGE_MIN_ITEMS
        wait = ROUND_BACKOFF_MAX if outage else min(ROUND_BACKOFF_BASE * round_no, ROUND_BACKOFF_MAX)
        if outage:
            print(f"\n==> 本轮 {len(retry_items)}/{len(pending)} 集瞬时失败（{ratio:.0%}），"
                  f"疑似代理/源站故障，等待 {wait}s 后再试。")
        else:
            print(f"\n==> 本轮剩余 {len(retry_items)} 集瞬时失败，等待 {wait}s 后进入下一轮。")
        # 用 stop_event.wait 代替 sleep：退避最长 300s，Ctrl+C 后干等这么久
        # 会让 pipeline 的 shutdown 白白超时。置位即立刻醒来。
        if stop_event is not None:
            if stop_event.wait(wait):
                print("\n==> 退避期间收到停止信号，取流提前收尾。")
                break
        else:
            time.sleep(wait)
        pending = retry_items

    print(f"\nAll done. 共跑 {round_no} 轮，结果已合并写入 {results_file}")


if __name__ == "__main__":
    main(sys.argv[1:])
