import argparse
import random
from curl_cffi import requests
import re
import json
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
import threading

import yaml


def _load_dotenv(path):
    """轻量解析同目录 .env（KEY=VALUE，支持 # 注释与引号），不覆盖已存在的环境变量。
    不引入 python-dotenv 依赖，保持最小改动。"""
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


def load_config(section="tmdb_ids_to_links"):
    """读取同目录 config.yaml 中指定脚本的配置段；缺失时返回空字典。

    section 参数是为 --refetch-failed 准备的：它要读下载侧的 failed_log 路径，
    那个键属于 download_movies 段。只读一个路径，不值得为此 import 整个
    download_movies（那会连带触发 .env 解析、S3 配置、目录创建等副作用）。
    """
    cfg_path = Path(__file__).with_name("config.yaml")
    if not cfg_path.exists():
        return {}
    with open(cfg_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get(section, {}) or {}


_CFG = load_config()
_DOWNLOAD_CFG = load_config("download_movies")
_PROXY_CFG = _CFG.get("proxy", {}) or {}

# 脚本所在目录（MovieDownloader/），作为所有相对路径的根。
_SCRIPT_DIR = Path(__file__).resolve().parent


def resolve_file(value, default_name):
    """解析输入/输出/元数据等文件路径，统一锚定到脚本目录：
    - 为空 -> 脚本目录下的 default_name；
    - 相对路径 -> 以脚本目录为根拼接（不随进程当前工作目录漂移）；
    - 绝对路径 -> 直接使用。

    必须与 download_movies.py 的同名函数保持一致：两侧若基准不同，从非脚本目录
    启动取流会把 results.jsonl 写到 CWD，而下载侧仍去脚本目录读——取流明明成功
    却"没片可下"，且元数据照常加载、毫无报错，极难排查。
    """
    raw = value.strip() if isinstance(value, str) else value
    if not raw:
        raw = default_name
    return _SCRIPT_DIR / raw


def _proxy_secret(cfg_key, env_key):
    """代理凭据：优先取环境变量（同目录 .env），缺省时回退 config.yaml（便于本地调试）。"""
    env_val = os.environ.get(env_key, "").strip()
    if env_val:
        return env_val
    return (_PROXY_CFG.get(cfg_key, "") or "").strip()


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"


def _site_headers(site):
    """vidup 同构站（vidup.to / vidfast.vc）的 (页面头, 接口头)。
    页面请求不能带 X-Requested-With，否则会被 Cloudflare 拦成 403；后续接口要带。"""
    api = {"User-Agent": UA, "Referer": f"https://{site}/", "X-Requested-With": "XMLHttpRequest"}
    page = {k: v for k, v in api.items() if k != "X-Requested-With"}
    return page, api


# 兼容保留：部分旧调用/测试直接引用这两个常量（vidup 主源的头）
HEADERS = {"User-Agent": UA, "Referer": "https://vidup.to/", "X-Requested-With": "XMLHttpRequest"}
PAGE_HEADERS = {k: v for k, v in HEADERS.items() if k != "X-Requested-With"}

VIDLINK_API_HEADERS = {"User-Agent": UA, "Origin": "https://vidlink.pro", "Referer": "https://vidlink.pro/"}
# vidlink CDN（hakunaymatata.com）校验请求头：浏览器 UA → 428，带任何 Referer → 429，
# 只有 okhttp UA + 不带 Referer 才 206。随 url 一起写进 results.jsonl，供 download_movies.py 直接使用。
VIDLINK_DOWNLOAD_HEADERS = {"User-Agent": "okhttp/4.9.3"}

VIDEASY_API_HEADERS = {"Accept": "*/*", "Origin": "https://player.videasy.to",
                       "Referer": "https://player.videasy.to/", "User-Agent": UA}
# videasy 的 vsrc 路由已下线（404 Route not found）；m4uhd 需要 title/year 才能检索，
# 缺元数据时只走 cdn（见 _fetch_videasy）
VIDEASY_SERVERS = ("cdn", "m4uhd")

# 2026-09-08 电影版 300 部 dead 抽样实测（详见 AGENTS.md §10）：
#   videasy 11.7%（独占 11）、vidfast 8.0%（独占 0）、vidlink 6.7%（独占 5），ANY 13.3%
# 与电视剧版结论相反（那边 vidlink 是主力、videasy 与 vidfast 完全重合），故电影版默认全开。
DEFAULT_PROVIDERS = ["vidup", "videasy", "vidlink", "vidfast"]

API = _CFG.get("api", "https://enc-dec.app/api")
MAX_RETRIES = _CFG.get("max_retries", 3)
RETRY_DELAY = _CFG.get("retry_delay", 1)  # 秒
TIMEOUT = _CFG.get("timeout", 12)  # 单个 HTTP 请求超时（秒）

# ---- 多轮捞回的轮间退避 ----
# 每轮之间递增等待，给 enc-dec / 代理的短时故障留恢复窗口，避免多轮在几秒内烧光。
ROUND_BACKOFF_BASE = int(_CFG.get("round_backoff", 30))       # 第 n 轮后等待 base*n 秒
ROUND_BACKOFF_MAX = int(_CFG.get("round_backoff_max", 300))   # 单次等待上限
# 某轮"待重跑"占比 ≥ 该比例且数量 ≥ 下限，视为基础设施故障（而非个别片抖动），直接按上限等待
OUTAGE_RETRY_RATIO = 0.9
OUTAGE_MIN_ITEMS = 50
# NoSource 判死前换 IP 再完整探一次，两次都无源才写 fail.txt（防 CDN/代理抖动误判永久丢片）
DEAD_CONFIRM = bool(_CFG.get("dead_confirm", True))

# ---- 最终捞回（跑满 max_rounds 后的加时赛）----
# 常规多轮的退避最长 round_backoff_max（默认 300s），扛得住几分钟级抖动；但代理
# 套餐额度耗尽、enc-dec 长时间维护这类故障要更久才恢复，此时整批会被打成
# unresolved 等下次运行——而"下次运行"要人来发起。故跑满轮次后再做一轮长冷却重试，
# 把这类"只是恢复得慢"的片捞回来。关闭时行为与旧版一致（直接写 unresolved.txt）。
_FINAL_CFG = _CFG.get("final_retry", {}) or {}
FINAL_RETRY_ENABLED = bool(_FINAL_CFG.get("enabled", True))
# 加时赛轮数。与常规轮次分开计数，语义也不同：常规轮打的是瞬时抖动，
# 这里打的是"需要几十分钟才恢复"的基础设施故障。
FINAL_RETRY_ROUNDS = max(1, int(_FINAL_CFG.get("rounds", 2)))
# 加时赛的冷却（秒）。默认 30 分钟——短于此基本等于再烧一次常规轮，没有意义。
FINAL_RETRY_COOLDOWN = max(0, int(_FINAL_CFG.get("cooldown_seconds", 1800)))

# ⚠️ 必须与 download_movies.py 的 `_NEEDS_REFETCH_MARKER` **逐字一致**。
# 下载侧遇 vidlink 签名直链过期（403/410）时把这段文案写进 failed.jsonl 的 error 字段，
# 本脚本的 --refetch-failed 据此挑出要重取的 id。两边是靠字符串约定耦合的（跨进程、
# 跨文件，没有共享常量），改一边必须同步改另一边，否则闭环会静默断开——
# 表现是 --refetch-failed 永远挑不出任何 id，且不报任何错。有回归用例锁死这一点。
NEEDS_REFETCH_MARKER = "需重新取流"


# 代理开关：设为 True 时启用下方代理，False 则直连
# 注意：vidup.to 有 Cloudflare 机房 IP 拦截，直连会返回 403，必须走住宅代理
USE_PROXY = _PROXY_CFG.get("enabled", True)
PROXY_HOST = _PROXY_CFG.get("host", "unmetered.residential.proxyrack.net")
# 代理账号密码为敏感项：优先环境变量 PROXY_USER / PROXY_PASSWORD（同目录 .env），
# config.yaml 里对应字段应留空，仅作本地调试回退。
PROXY_USER = _proxy_secret("user", "PROXY_USER")
PROXY_PASS = _proxy_secret("password", "PROXY_PASSWORD")
PROXY_PORT_RANGE = tuple(_PROXY_CFG.get("port_range", (9000, 9050)))  # 每次随机取一个端口，换一个出口 IP

# 启动期校验：启用代理但凭据缺失时立刻报错退出，避免拼出畸形代理 URL
# （http://:@host:port）后每个 ID 静默跑成一堆 403/失败。凭据须落在同目录 .env。
if USE_PROXY and (not PROXY_USER or not PROXY_PASS):
    _missing = [n for n, v in (("PROXY_USER", PROXY_USER), ("PROXY_PASSWORD", PROXY_PASS)) if not v]
    raise SystemExit(
        f"代理已启用（proxy.enabled=true）但缺少凭据：{', '.join(_missing)}。"
        "请在同目录 .env 配置 PROXY_USER / PROXY_PASSWORD"
        "（或将 config.yaml 的 tmdb_ids_to_links.proxy.enabled 设为 false 直连）。"
    )


def build_proxy():
    """返回 requests 用的 proxies；未启用代理时返回 None 表示直连。"""
    if not USE_PROXY:
        return None
    port = random.randint(*PROXY_PORT_RANGE)
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{port}"
    return {"http": proxy_url, "https": proxy_url}


# ---------- 元数据补全 ----------
# 从 movies.jsonl 预加载 tmdb_id -> 元数据 映射，供取流成功时把静态元数据
# 一并写进 results.jsonl（下游 download_movies 据此拼 R2 路径 {year}/... 等）。
# 纯内存字典查表，无运行时探测；文件缺失时返回空表（各字段自然缺省）。


def load_movie_metadata():
    """返回 (元数据表, 检索表)：
      元数据表 tmdb_id -> 写进 results.jsonl 的静态字段；
      检索表   tmdb_id -> {title, year, imdb}，仅供 videasy 的 m4uhd 服务器按片名检索用，
               不写进结果（primary_title 不是 results.jsonl 契约的一部分）。"""
    meta_path = resolve_file(_CFG.get("metadata"), "movies.jsonl")
    table = {}
    search = {}
    if not meta_path.exists():
        print(f"[metadata] 未找到 {meta_path}，results.jsonl 将不含元数据字段")
        return table, search
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
            tid = str(tid)
            table[tid] = {
                "year": m.get("start_year"),
                "original_title": m.get("original_title"),
                "runtime_minutes": m.get("runtime_minutes"),
                "genres": m.get("genres", []),
                "title_type": m.get("title_type"),
                "imdb_id": m.get("imdb_id"),
            }
            search[tid] = {
                "title": m.get("primary_title") or m.get("original_title") or "",
                "year": m.get("start_year"),
                "imdb": m.get("imdb_id") or "",
            }
    print(f"[metadata] 已加载 {len(table)} 条影片元数据")
    return table, search


_MOVIE_META, _MOVIE_SEARCH_META = load_movie_metadata()
# 结果里的"身份字段"：由 process_tmdb_id 用入参权威写入，合并静态元数据时必须保护、
# 绝不允许被覆盖（下游据此去重、拼文件名与 R2 对象键）。
_IDENTITY_KEYS = frozenset({"urls", "tmdbId", "title", "fetched_at"})


# ---------- 辅助函数 ----------
class HttpStatusError(Exception):
    """携带结构化状态码的 HTTP 错误。where 标明出错环节（page/enc/servers/dec/stream/...），
    供判死逻辑精确区分"源站说没有"与"中间服务坏了"，不再靠字符串里是否含 "404" 猜。"""

    def __init__(self, status, where, detail=""):
        super().__init__(f"HTTP {status} at {where}{(': ' + detail) if detail else ''}")
        self.status = status
        self.where = where


class NoSource(Exception):
    """源站明确表示无此片：唯一的判死证据。其余任何异常一律按瞬时错误重试。"""


def _check(resp, where):
    """非 2xx 一律抛 HttpStatusError（带状态码与环节），成功则原样返回。"""
    if resp.status_code >= 400:
        raise HttpStatusError(resp.status_code, where, resp.text[:120])
    return resp


def _json(resp, where):
    try:
        return resp.json()
    except Exception as e:  # noqa: BLE001
        raise Exception(f"{where}: 响应不是合法 JSON（{type(e).__name__}）: {resp.text[:120]}")


def validate(data, path):
    if not isinstance(data, dict):
        raise Exception(f"API Error at {path}: 响应不是对象，实际为 {type(data).__name__}")
    if data.get("status") != 200:
        error_msg = data.get("error", "unknown")
        raise Exception(f"API Error at {path}: status={data.get('status')}, error={error_msg}")
    return data["result"]


def _is_retriable(exc):
    """白名单式判死：只有 NoSource（源站明确无此片）才不重试，其余一律重试。

    旧版是黑名单字符串匹配（`"404" in msg` 等），tt0404xxx 之类的 id、源站 5xx 页面
    文案、enc-dec 服务自身 404 都会被误判成"真无源"而永久写进 fail.txt。多源轮询下
    这种误判会成倍放大，故改为白名单。
    """
    return not isinstance(exc, NoSource)


# ---------- 取流源（provider）----------
# 每个 provider 的签名：fn(session, tmdb_id) -> (urls, title)
#   urls  非空列表，每项 {url, provider, type("m3u8"|"mp4"), headers, quality, size}
#   title 源站给的片名，可为 None
# 语义：真无源抛 NoSource（唯一判死证据）；其余任何异常都视为瞬时错误。
def _url_entry(url, provider, type_, headers=None, quality=None, size=None):
    return {
        "url": url,
        "provider": provider,
        "type": type_,
        # 下载该 url 时必须携带的请求头（空表示用 download_movies 默认头）
        "headers": dict(headers or {}),
        # 源站声明的画质高度（int）与文件大小（bytes）；m3u8 源不声明，留 None 由下游探测
        "quality": quality,
        "size": size,
    }


def _fetch_vidup_like(session, site, api_name, tmdb_id):
    """vidup.to / vidfast.vc 同构链路：
    页面正则 → enc-{api} → servers POST → dec-{api} → 逐 server stream POST → dec-{api} 取 url。"""
    page_headers, api_headers = _site_headers(site)

    # 1. 获取页面，提取加密文本
    resp = session.get(f"https://{site}/movie/{tmdb_id}/", timeout=TIMEOUT, headers=page_headers)
    if resp.status_code == 404:
        raise NoSource(f"{api_name} page 404 for {tmdb_id}")
    _check(resp, "page")
    html = resp.text

    match_1 = re.search(r'\\"en\\":\\"(.*?)\\"', html)
    match = re.search(r'\\"token\\":\\"(.*?)\\"', html)
    if match_1:
        text = match_1.group(1)
    elif match:
        text = match.group(1)
    else:
        # 页面 200 但提取不到加密文本：可能撞上 Cloudflare 挑战页/半截 HTML，换 IP 重试
        raise Exception(f"Extract failed (retriable) for {tmdb_id}")

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
    resp = _check(session.post(dec_url, json={"text": servers_encrypted}, timeout=TIMEOUT),
                  f"dec-{api_name}(servers)")
    servers_decrypted = validate(_json(resp, f"dec-{api_name}(servers)"), dec_url)

    if not servers_decrypted or not isinstance(servers_decrypted, list):
        raise Exception("No servers found")

    # 5-6. 遍历所有服务器，收集全部可用 url（方案C 的多节点 fallback 基础）
    last_server_error = None
    urls = []
    result_title = None
    stream_404 = 0
    for server in servers_decrypted:
        server_name = server.get('name', 'unknown') if isinstance(server, dict) else 'unknown'
        try:
            data_val = server['data']
            stream_url = f"{stream}/{data_val}"
            resp = _check(session.post(stream_url, headers=headers_with_token, timeout=TIMEOUT), "stream")
            stream_encrypted = resp.text

            resp = _check(session.post(dec_url, json={"text": stream_encrypted}, timeout=TIMEOUT),
                          f"dec-{api_name}(stream)")
            stream_decrypted = validate(_json(resp, f"dec-{api_name}(stream)"), dec_url)
            if not isinstance(stream_decrypted, dict):
                raise Exception("decrypted stream is not an object")

            url = stream_decrypted.get("url")
            if not url:
                raise Exception("Missing url in decrypted data")
            # 只以 url 为成功条件；返回的 tmdbId 仅用于告警，不参与 key 也不作为成功条件
            r_tid = stream_decrypted.get("tmdbId")
            if r_tid is not None and str(r_tid) != str(tmdb_id):
                print(f"  [warn] {tmdb_id} {api_name} server '{server_name}' 返回 tmdbId={r_tid}，"
                      f"与入参不一致，key 仍用入参")
            if all(u["url"] != url for u in urls):
                urls.append(_url_entry(url, api_name, "m3u8"))
            if result_title is None:
                result_title = stream_decrypted.get("title")
        except HttpStatusError as e:
            # 只有源站 stream 接口本身的 404 才算"该 server 无源"；enc-dec 的 404 是服务故障
            if e.status == 404 and e.where == "stream":
                stream_404 += 1
            last_server_error = e
            print(f"  {api_name} server '{server_name}' failed for {tmdb_id}: {e}")
            continue
        except Exception as e:  # noqa: BLE001
            last_server_error = e
            print(f"  {api_name} server '{server_name}' failed for {tmdb_id}: {e}")
            continue

    if urls:
        return urls, result_title
    if stream_404 == len(servers_decrypted):
        raise NoSource(f"all {stream_404} {api_name} servers returned 404 for {tmdb_id}")
    raise Exception(f"All {api_name} servers failed for {tmdb_id}. Last error: {last_server_error}")


def _fetch_vidup(session, tmdb_id):
    return _fetch_vidup_like(session, "vidup.to", "vidup", tmdb_id)


def _fetch_vidfast(session, tmdb_id):
    return _fetch_vidup_like(session, "vidfast.vc", "vidfast", tmdb_id)


def _fetch_vidlink(session, tmdb_id):
    """vidlink.pro：enc-vidlink(tmdb id) → GET /api/b/movie/{enc} 直接返 JSON（不需再 dec）。
    stream.qualities.{360,480,720,1080}.{url,size,...}，url 为带时效签名的 mp4 直链。
    无源的唯一证据是 HTTP 200 + body null；404 可能是路由变更 / enc 值异常 / WAF 拦截，
    按瞬时处理（判死必须保守）。"""
    enc_url = f"{API}/enc-vidlink?text={quote(str(tmdb_id), safe='')}"
    resp = _check(session.get(enc_url, timeout=TIMEOUT, headers=VIDLINK_API_HEADERS), "enc-vidlink")
    enc = validate(_json(resp, "enc-vidlink"), enc_url)
    if not isinstance(enc, str) or not enc:
        raise Exception(f"API Error at {enc_url}: result is not a string")

    # enc 作为路径段拼接，含 / ? # 等字符会拼错 URL，必须编码
    resp = session.get(f"https://vidlink.pro/api/b/movie/{quote(enc, safe='')}",
                       timeout=TIMEOUT, headers=VIDLINK_API_HEADERS)
    _check(resp, "vidlink-api")
    data = _json(resp, "vidlink-api")
    if data is None:
        raise NoSource(f"vidlink api null for {tmdb_id}")
    if not isinstance(data, dict):
        raise Exception(f"vidlink api: unexpected payload type {type(data).__name__}")

    stream = data.get("stream")
    qualities = stream.get("qualities") if isinstance(stream, dict) else None
    if not isinstance(qualities, dict) or not qualities:
        # 有响应但没有画质表：不是 null，不能当无源证据，交给重试
        raise Exception(f"vidlink api: missing stream.qualities for {tmdb_id}")

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
        # 非正画质（源站偶发 0/-1）不能留：下游 meets_resolution_redline 会据此
        # 把节点判定性淘汰，白丢一个本来可用的流。None 表示未声明、交由实测。
        if quality is not None and quality <= 0:
            quality = None
        size = info.get("size")
        # bool 是 int 子类：size=true 会被 int() 成 1，必须排除
        if isinstance(size, bool) or not isinstance(size, (int, float)) or size <= 0:
            size = None
        else:
            size = int(size)
        entries.append(_url_entry(url, "vidlink", "mp4", VIDLINK_DOWNLOAD_HEADERS, quality, size))
    if not entries:
        raise Exception(f"vidlink api: qualities without url for {tmdb_id}")
    # 画质从高到低，下游按顺序择优
    entries.sort(key=lambda u: (u["quality"] is None, -(u["quality"] or 0)))
    return entries, None


def _fetch_videasy(session, tmdb_id):
    """videasy（player.videasy.to）：seed → /{cdn|m4uhd}/sources-with-title → dec-videasy。
    2026-09-08 电影版实测命中率最高（11.7%）、独占最多（11 部）。
    m4uhd 靠 title/year 检索，缺元数据时只走 cdn（否则必然 500 Required parameters not found）。"""
    meta = _MOVIE_SEARCH_META.get(str(tmdb_id), {})
    title = meta.get("title") or ""
    year = meta.get("year") or ""
    imdb = meta.get("imdb") or ""

    resp = _check(session.get(f"https://api.speedracelight.com/seed?mediaId={quote(str(tmdb_id), safe='')}",
                              headers=VIDEASY_API_HEADERS, timeout=TIMEOUT), "videasy-seed")
    seed = _json(resp, "videasy-seed").get("seed")
    if not seed:
        raise Exception(f"videasy: 未拿到 seed for {tmdb_id}")

    enc_title = quote(quote(title, safe=""), safe="")
    servers = VIDEASY_SERVERS if title else ("cdn",)
    urls = []
    seen_urls = set()
    empty = 0
    errors = []
    for sv in servers:
        try:
            url = (f"https://api.speedracelight.com/{sv}/sources-with-title?title={enc_title}"
                   f"&mediaType=movie&year={year}&tmdbId={quote(str(tmdb_id), safe='')}"
                   f"&imdbId={quote(str(imdb), safe='')}&enc=2&seed={quote(str(seed), safe='')}")
            resp = session.get(url, headers=VIDEASY_API_HEADERS, timeout=TIMEOUT)
            # 源站对"无此片"的表达：404 / 空响应 / 500 + "No streams available"
            if resp.status_code == 404 or not resp.text.strip():
                empty += 1
                continue
            if resp.status_code == 500 and "No streams available" in resp.text:
                empty += 1
                continue
            _check(resp, f"videasy-{sv}")
            dec_url = f"{API}/dec-videasy"
            resp = _check(session.post(dec_url, json={"text": resp.text, "id": str(tmdb_id), "seed": seed},
                                       timeout=TIMEOUT), f"dec-videasy({sv})")
            decoded = validate(_json(resp, f"dec-videasy({sv})"), dec_url)
            found = _find_media_urls(decoded)
            if not found:
                empty += 1
                continue
            for u in found:
                if u not in seen_urls:
                    seen_urls.add(u)
                    # _find_media_urls 的正则同时匹配 .m3u8 与 .mp4，必须按后缀分别
                    # 标注 type：标错会让下游拿 mp4 直链去解析 HLS playlist，必然失败。
                    urls.append(_url_entry(u, "videasy", _media_type_of(u)))
        except Exception as e:  # noqa: BLE001
            errors.append(f"{sv}: {e}")
            print(f"  videasy server '{sv}' failed for {tmdb_id}: {e}")
            continue

    if urls:
        return urls, None
    if empty == len(servers):
        raise NoSource(f"all {empty} videasy servers empty for {tmdb_id}")
    raise Exception(f"All videasy servers failed for {tmdb_id}: {errors}")


# videasy 的解密结果结构不固定（有时嵌在 sources[].file，有时在别处），按正则递归捞直链
_MEDIA_URL_RE = re.compile(r"https?://[^\s\"']+\.(?:m3u8|mp4)(?:\?[^\s\"']*)?", re.I)


def _media_type_of(url):
    """按 url 路径后缀判断是 HLS 播放列表还是 mp4 直链。

    只看查询串之前的路径部分——`.mp4?sign=...` 这类带参数的直链若按整串判断会漏。
    无法识别时保守当 m3u8（HLS 是各源的主要形态，且 m3u8 分支对非播放列表内容
    有 validate_segment_content 兜底，比反过来更安全）。
    """
    path = str(url).split("?", 1)[0].split("#", 1)[0].lower()
    return "mp4" if path.endswith(".mp4") else "m3u8"


def _find_media_urls(obj):
    """递归找出结构中的 m3u8/mp4 链接，保持出现顺序并去重。"""
    found = []
    if isinstance(obj, str):
        found.extend(_MEDIA_URL_RE.findall(obj))
    elif isinstance(obj, dict):
        for v in obj.values():
            found.extend(_find_media_urls(v))
    elif isinstance(obj, list):
        for v in obj:
            found.extend(_find_media_urls(v))
    return list(dict.fromkeys(found))


PROVIDERS = {
    "vidup": _fetch_vidup,
    "videasy": _fetch_videasy,
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


def process_tmdb_id(tmdb_id, providers=None):
    """处理单个 TMDB ID，仅对瞬时错误重试。

    返回 (status, 结果字典或None)，status 三态供多轮捞回区分：
      - "ok"    成功，附结果字典 {urls, tmdbId, title, fetched_at, + movies.jsonl 静态元数据}
      - "dead"  确认真无源（NoSource）→ 永久排除，绝不再抓
      - "retry" 瞬时错误换 IP 重试 MAX_RETRIES 次仍失败 → 下一轮重跑

    多源：同一次尝试内按 providers 顺序逐家取流，把各家给出的 url 全部汇总
    （按 url 去重、保持 providers 顺序），下载侧按节点顺序逐个尝试，某家的流
    画质/码率不达标时还能落到下一家；有任一家给出 url 即 ok。
    全部跑完仍无 url 时，只要有任一家是瞬时错误就按瞬时处理（重试），
    全部 NoSource 才算一次"疑似无源"。

    判死二次确认（DEAD_CONFIRM）：首次全家 NoSource 不立即判死，换 IP 再完整跑一次，
    累计两次全家 NoSource 才返回 dead；确认过程最多多花 1 次尝试（总尝试数 ≤ MAX_RETRIES+1）。
    若确认那次是瞬时错误且尝试已耗尽，返回 retry 交给下一轮（宁可多试，不误判永久丢片）。
    """
    providers = list(ACTIVE_PROVIDERS if providers is None else providers)
    attempt = 0
    nosource_hits = 0
    while True:
        attempt += 1
        # 每个 id、每次重试用一个独立 Session：
        # - session 内部复用 TCP/TLS 连接，减少同一 id 内多次请求的握手开销；
        # - session 绑定本次 build_proxy() 的随机出口 IP，作用域仅限本次尝试。
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
                        provider_urls, provider_title = PROVIDERS[name](session, tmdb_id)
                    except NoSource as e:
                        nosource_errors.append(f"{name}: {e}")
                        continue
                    except Exception as e:  # noqa: BLE001
                        transient_errors.append(f"{name}: {e}")
                        print(f"  [{name} 瞬时错误] {tmdb_id}: {e}")
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
                    # 恒用入参 tmdb_id 作为 key（ids.txt / movies.jsonl 的 key），
                    # 保证全链路一致：续跑去重、元数据查表、下游 R2 路径与文件名都对得上。
                    result = {
                        "urls": urls,
                        "tmdbId": tmdb_id,
                        # 只有 vidlink/videasy 命中时源站不返回片名，result_title
                        # 会是 None。兜底成空串，保证契约里 title 恒为 str——
                        # 否则 None 会一路透传到 success.jsonl 与日志里打成 "None"。
                        "title": result_title or "",
                        # 取流时刻（秒级时间戳）。results.jsonl 是追加写，同一片多轮重试会留下
                        # 多行；下游据此挑真正最新的一条。对 vidlink 这类带时效签名的直链尤为
                        # 关键：拿到过期 url 等于白跑一次下载。
                        "fetched_at": int(time.time()),
                    }
                    # 合并静态元数据，但绝不覆盖上面的身份字段
                    for meta_key, meta_value in _MOVIE_META.get(str(tmdb_id), {}).items():
                        if meta_key not in _IDENTITY_KEYS:
                            result[meta_key] = meta_value
                    return "ok", result

                if transient_errors:
                    raise Exception(f"All providers failed for {tmdb_id}: {transient_errors}")
                raise NoSource("; ".join(nosource_errors))

            except Exception as e:  # noqa: BLE001
                if not _is_retriable(e):
                    nosource_hits += 1
                    if not DEAD_CONFIRM or nosource_hits >= 2:
                        print(f"  [无源] {tmdb_id}: {e}")
                        return "dead", None
                    # 首次无源只算"疑似"：换出口 IP 再完整探一次，防 CDN/代理抖动误判永久丢片
                    print(f"  [疑似无源，换 IP 确认] {tmdb_id}: {e}")
                    time.sleep(RETRY_DELAY)
                    continue
                # 命中过 NoSource 时多给 1 次预算，让确认那一跳不挤占常规重试次数
                budget = MAX_RETRIES + (1 if (DEAD_CONFIRM and nosource_hits) else 0)
                print(f"  [Attempt {attempt}/{budget}] 瞬时错误 for {tmdb_id}: {e}")
                if attempt < budget:
                    time.sleep(RETRY_DELAY)
                    continue
                print(f"  All {budget} attempts failed for ID {tmdb_id}")
                return "retry", None


def load_refetch_ids(failed_log, fail_file, success_log=None):
    """--refetch-failed 用：从下载侧的 failed.jsonl 里挑出"需重新取流"的 tmdb_id。

    背景（闭环缺口）：vidlink 出的是带时效签名的 mp4 直链，过期后下载侧拿到
    403/410，抛带"需重新取流"标记的错误并判死，注释里写着"交由上游重跑取流修复"。
    但本脚本的 load_processed_ids 把 results.jsonl 里的 id 都算已处理——这片当初
    取流是成功的、躺在 results.jsonl 里，重跑时会被直接跳过，那条过期 url 永远
    不会被刷新。两侧各自都合理，合在一起链就断了，谁都没在负责修。

    本函数就是把这条链接上：只认下载侧写的 _NEEDS_REFETCH_MARKER 文案，
    绕过 processed 判断强制重取。因为 results.jsonl 是追加写、下载侧按
    fetched_at 择新（见 §10.16 C），**这里不需要删改任何历史行**——
    重新取一条更新的追加进去，下载侧自然会挑到新的那条。

    两个排除条件，缺一都会造成无效重取或错误重取：

    - **fail_file（确认真无源）**：那是走完白名单判死 + dead_confirm 二次确认的
      结论，不该被一条下载失败记录推翻。
    - **success_log（已经下成功了）**：failed.jsonl 是**纯追加、永不清理**的
      （全程无 truncate，唯一的删除是 remove_upload_failure_from_log，且只删
      stage=="upload" 的行）。所以某片被本命令修复、下载成功后，它那条旧的
      "需重新取流"记录**仍留在 failed.jsonl 里**，下次再跑还会被挑出来重取一遍。
      不排除的话，无效重取会随运行次数持续累积。
    """
    dead = set()
    if fail_file.exists():
        with open(fail_file, 'r', encoding='utf-8') as f:
            for line in f:
                tid = line.strip()
                if tid:
                    dead.add(tid)

    done = set()
    if success_log is not None and success_log.exists():
        with open(success_log, 'r', encoding='utf-8') as f:
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
                    done.add(str(tid).strip())

    ids = []
    seen = set()
    if not failed_log.exists():
        return ids
    with open(failed_log, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if NEEDS_REFETCH_MARKER not in str(obj.get("error", "")):
                continue
            tid = obj.get("tmdbId")
            if tid is None:
                continue
            tid = str(tid).strip()
            if not tid or tid in seen or tid in dead or tid in done:
                continue
            seen.add(tid)
            ids.append(tid)
    return ids


def load_processed_ids(results_file, fail_file):
    """已处理集合 = 取流成功过的（results）+ 确认真无源的（fail）。

    刻意**不含** unresolved_file：那里面是"多轮跑满仍是瞬时错误"的 ID，
    从未被判定为 NoSource，下次重跑必须自动重试（见 run_all 的写入处注释）。
    """
    processed = set()
    if results_file.exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        tid = obj.get('tmdbId')
                        if tid is not None:
                            processed.add(str(tid))
                    except:
                        pass
    if fail_file.exists():
        with open(fail_file, 'r', encoding='utf-8') as f:
            for line in f:
                tid = line.strip()
                if tid:
                    processed.add(tid)
    return processed


def run_batch(to_process, results_file, fail_file, max_workers, providers=None):
    """并发处理一批 ID，实时落盘 ok/dead，返回本批"瞬时耗尽"待重跑的 ID 列表。

      - "ok"    → 追加写 results_file（output）
      - "dead"  → 追加写 fail_file，永久排除
      - "retry" → 不落盘，收集返回，交给上层多轮循环下一轮重跑
    """
    lock = threading.Lock()
    ok_count = 0
    dead_count = 0
    retry_ids = []

    def process_one(tid):
        status, result = process_tmdb_id(tid, providers=providers)
        with lock:
            if status == "ok" and result:
                with open(results_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                srcs = ",".join(dict.fromkeys(u.get("provider", "?") for u in result.get("urls", [])))
                print(f"✅ SUCCESS: {result.get('title')} ({result.get('tmdbId')}) [{srcs}]")
            elif status == "retry":
                retry_ids.append(tid)
                print(f"🔁 RETRY-LATER: {tid}")
            else:  # "dead" 或异常兜底
                with open(fail_file, 'a', encoding='utf-8') as f:
                    f.write(f"{tid}\n")
                print(f"❌ DEAD: {tid}")
        return status

    # 线程池手动管理：Ctrl+C 时取消排队任务立即退出，不等几十万个 future 跑完
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {executor.submit(process_one, tid): tid for tid in to_process}
        for future in as_completed(futures):
            tid = futures[future]
            try:
                status = future.result()
                if status == "ok":
                    ok_count += 1
                elif status == "retry":
                    pass  # 已收集进 retry_ids
                else:
                    dead_count += 1
            except Exception as e:  # noqa: BLE001
                print(f"⚠️  Unexpected exception for {tid}: {e}")
                with lock:
                    retry_ids.append(tid)
    except BaseException:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    executor.shutdown(wait=True)

    print(f"\n本批完成 | 成功 {ok_count} | 真无源 {dead_count} | 待重跑 {len(retry_ids)}")
    return retry_ids


def _parse_args(argv):
    ap = argparse.ArgumentParser(description="由 tmdb_id 多源解析出可播放地址")
    ap.add_argument("--providers",
                    help=f"逗号分隔的取流源，覆盖 config.yaml；可选：{', '.join(PROVIDERS)}"
                         f"（默认 {', '.join(ACTIVE_PROVIDERS)}）")
    ap.add_argument("--refetch-failed", action="store_true",
                    help="只重取下载侧标记为“需重新取流”的影片（vidlink 签名直链过期）。"
                         "绕过“已在 results.jsonl 即跳过”的判断，结果追加写入，"
                         "下载侧按 fetched_at 自动选用新的那条。")
    return ap.parse_args(argv)


def main(argv=None):
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    providers = _resolve_providers(args.providers) if args.providers else list(ACTIVE_PROVIDERS)

    ids_file = resolve_file(_CFG.get("input"), "ids.txt")
    results_file = resolve_file(_CFG.get("output"), "results.jsonl")
    fail_file = resolve_file(_CFG.get("fail_file"), "fail.txt")
    unresolved_file = resolve_file(_CFG.get("unresolved_file"), "unresolved.txt")

    print(f"取流源: {', '.join(providers)}")

    if args.refetch_failed:
        # 闭环修复模式：只处理下载侧判定"直链已失效、需重新取流"的片。
        # 这些 id 必然已在 results.jsonl 里（当初取流成功过），所以**刻意不做
        # processed 过滤**——否则会被全部跳过，正是这个缺口本身。
        #
        # 注：本模式下若某片这次探出真无源，仍会照常写进 fail.txt。这是**对的**——
        # 它同样走完了白名单判死 + dead_confirm 二次确认，与全量模式下的判死
        # 同等可信（源站确实可能在两次取流之间下架某片）。
        failed_log = resolve_file(_DOWNLOAD_CFG.get("failed_log"), "failed.jsonl")
        success_log = resolve_file(_DOWNLOAD_CFG.get("success_log"), "success.jsonl")
        to_process = load_refetch_ids(failed_log, fail_file, success_log)
        print(f"[refetch-failed] 从 {failed_log.name} 挑出 "
              f"{len(to_process)} 个待重新取流的 ID")
        if not to_process:
            print("没有需要重新取流的影片。")
            return
    else:
        if not ids_file.exists():
            print(f"ids.txt not found: {ids_file}")
            return

        with open(ids_file, 'r', encoding='utf-8') as f:
            ids = [line.strip() for line in f if line.strip()]

        processed = load_processed_ids(results_file, fail_file)
        to_process = [tid for tid in ids if tid not in processed]
        print(f"Total IDs: {len(ids)}, Already processed: {len(processed)}, "
              f"To process: {len(to_process)}")

        if not to_process:
            print("All IDs processed.")
            return

    max_workers = _CFG.get("max_workers", 50)
    max_rounds = _CFG.get("max_rounds", 8)

    # ---- 内嵌多轮捞回：首轮跑全部，之后每轮只重跑上一轮"瞬时耗尽"的 ID ----
    # ok 累加进 output、dead 累加进 fail_file 均在 run_batch 内实时落盘，
    # 故 output 执行完即为"原结果 + 捞回结果"的合并（原 total_results.jsonl）。
    pending = to_process
    round_no = 0
    unresolved = []
    while pending:
        round_no += 1
        print(f"\n{'=' * 70}")
        print(f"==> 第 {round_no}/{max_rounds} 轮 | 待处理 {len(pending)} 个 ID")
        print(f"{'=' * 70}")

        batch_size = len(pending)
        retry_ids = run_batch(pending, results_file, fail_file, max_workers, providers=providers)

        if not retry_ids:
            print("\n==> 瞬时失败已清零，所有有源 ID 已捞干净，正常结束。")
            break
        if round_no >= max_rounds:
            # 常规轮次已跑满。这些 ID 从未被判过 NoSource，是被超时/5xx/代理故障
            # 打下来的。常规轮的退避最长 round_backoff_max（默认 300s），若故障源
            # 是"代理额度耗尽"或"enc-dec 维护"这类几十分钟级的，整批会在这里被放弃、
            # 等人发起下次运行。加时赛用长冷却再试几轮，把它们自动捞回来。
            unresolved, extra_rounds = _final_retry(
                retry_ids, results_file, fail_file, max_workers, providers
            )
            round_no += extra_rounds
            if not unresolved:
                print("\n==> 最终捞回成功清零，无残留未解决 ID。")
                break
            print(f"\n==> 已达最大轮数 {max_rounds}，剩余 {len(unresolved)} 个瞬时失败 ID "
                  f"写入 {unresolved_file.name}（未判死，下次运行会自动重试）。")
            break
        pending = retry_ids

        # 轮间退避：给 enc-dec / 代理 / 源站的短时故障留恢复窗口。
        # 整批几乎全是"待重跑"时判定为基础设施故障（而非个别片抖动），直接等满上限。
        outage = (len(retry_ids) >= OUTAGE_MIN_ITEMS
                  and len(retry_ids) / max(batch_size, 1) >= OUTAGE_RETRY_RATIO)
        wait = ROUND_BACKOFF_MAX if outage else min(ROUND_BACKOFF_BASE * round_no, ROUND_BACKOFF_MAX)
        if outage:
            print(f"\n==> 本轮 {len(retry_ids)}/{batch_size} 待重跑，疑似代理/源站故障，等待 {wait}s 后重试")
        else:
            print(f"\n==> 等待 {wait}s 后开始下一轮")
        time.sleep(wait)

    # 多轮跑满仍未捞回的 ID **绝不能写进 fail_file**：fail_file 的语义是"源站明确
    # 说没有这片，永久排除"，为此判死链路做了白名单（只有 NoSource）加 dead_confirm
    # 换 IP 二次确认。而这批 ID 一次都没被判过 NoSource，它们是被超时/5xx/代理故障
    # 打下来的——一次持续 max_rounds 轮的 enc-dec 或代理故障就能把整批有源片永久
    # 判死，且两类记录混在同一个文件里事后无法区分，还会污染"拿 fail.txt 当真 dead
    # 样本"的分析（§10.15 的复验正是这么取样的）。
    #
    # 故单独落 unresolved_file，且不计入 load_processed_ids —— 下次运行自动重试。
    # 无条件覆盖写（包括清空）：该文件描述的是"最近一次运行结束时仍未解决的 ID"，
    # 若只在非空时才写，上次的残留会一直骗人说它们还没解决。
    #
    # ⚠️ --refetch-failed 模式下**跳过写入**：该模式只处理 failed.jsonl 里的一小撮
    # 直链过期片，跑完就覆盖 unresolved.txt 会把全量运行留下的残留清单冲掉，
    # 那些 ID 就此失去"下次自动重试"的线索。两种模式的 unresolved 语义不通用。
    if args.refetch_failed:
        if unresolved:
            print(f"\n==> 本次重取仍有 {len(unresolved)} 个瞬时失败，"
                  f"未写入 {unresolved_file.name}（避免覆盖全量运行的残留清单）；"
                  f"再跑一次 --refetch-failed 即可继续重试。")
    else:
        write_unresolved(unresolved_file, unresolved)

    print(f"\nAll done. 共跑 {round_no} 轮，结果已合并写入 {results_file}")


def _final_retry(retry_ids, results_file, fail_file, max_workers, providers):
    """常规轮次跑满后的加时赛：长冷却再试几轮。

    返回 (仍未解决的 ID 列表, 实际跑了几轮)。轮数要回传给调用方，否则收尾打印的
    "共跑 N 轮"会漏掉加时赛，看日志会以为跑完 max_rounds 就结束了。

    与常规多轮的区别只在**冷却时长**：常规轮退避最长 round_backoff_max（默认
    300s），打的是分钟级抖动；加时赛默认冷却 30 分钟，打的是"代理额度耗尽 /
    enc-dec 维护"这类需要更久才恢复的基础设施故障。

    不开启（FINAL_RETRY_ENABLED=False）时原样返回入参、轮数记 0，行为与旧版一致。

    注意：加时赛里判死的 ID 照常写 fail_file——它同样走完白名单判死 +
    dead_confirm 二次确认，与常规轮的判死同等可信。
    """
    if not FINAL_RETRY_ENABLED or not retry_ids:
        return retry_ids, 0

    pending = retry_ids
    for extra_round in range(1, FINAL_RETRY_ROUNDS + 1):
        print(f"\n{'=' * 70}")
        print(
            f"==> 最终捞回 {extra_round}/{FINAL_RETRY_ROUNDS} | "
            f"待处理 {len(pending)} 个 ID | 先冷却 {FINAL_RETRY_COOLDOWN}s"
        )
        print(f"{'=' * 70}")
        # 冷却放在跑之前：常规轮刚跑完，故障源大概率还没恢复，立刻重跑只是白烧配额。
        if FINAL_RETRY_COOLDOWN > 0:
            time.sleep(FINAL_RETRY_COOLDOWN)

        pending = run_batch(
            pending, results_file, fail_file, max_workers, providers=providers
        )
        if not pending:
            return [], extra_round
    return pending, FINAL_RETRY_ROUNDS


def write_unresolved(unresolved_file, ids):
    """覆盖写 unresolved.txt；ids 为空时写出空文件（清掉上次运行的残留）。"""
    try:
        with open(unresolved_file, 'w', encoding='utf-8') as f:
            for tid in ids:
                f.write(f"{tid}\n")
    except OSError as exc:
        # 这只是给人看的残留清单，写不进去不该让整轮取流的成果白费。
        print(f"⚠️ 写 {unresolved_file} 失败（不影响已落盘的结果）: {exc}")


if __name__ == "__main__":
    main()
