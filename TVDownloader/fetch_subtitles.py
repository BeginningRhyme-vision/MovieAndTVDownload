"""
按 tmdbId + season + episode 从 SubDL 抓取剧集外挂字幕（英文 / 简体中文）。

输入：success.jsonl（download_tv.py 的成功日志，一行一集，含 tmdbId/season/episode/final_path）
输出：SUBTITLE_DIR/tv_00000X/{tmdbId}_S01E03.en.srt、{tmdbId}_S01E03.zh.srt

字幕与视频同名（集级 key `{tmdbId}_S{季:02d}E{集:02d}`），播放器和前端可以按同名规则自动配对。
脚本可重复执行，已存在的字幕文件会跳过，因此新下载的剧集直接再跑一次即可。

配置读取同目录 config.yaml 的 fetch_subtitles 段；SUBDL_API_KEY 优先取环境变量 / .env。
"""

import io
import json
import os
import re
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yaml


_SCRIPT_DIR = Path(__file__).resolve().parent


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


_load_dotenv(str(_SCRIPT_DIR / ".env"))


def load_config():
    """读取与本脚本同目录的 config.yaml 中 fetch_subtitles 段。"""
    config_path = _SCRIPT_DIR / "config.yaml"
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("fetch_subtitles", {}) or {}


_CFG = load_config()


def _resolve(value, default_name):
    """相对路径以脚本目录为根；为空取默认名；绝对路径直接使用。"""
    raw = value.strip() if isinstance(value, str) else value
    if not raw:
        raw = default_name
    return str((_SCRIPT_DIR / raw).resolve())


# ========== 配置 ==========
# SubDL API Key：敏感项，优先环境变量 SUBDL_API_KEY（同目录 .env），config 留空回退。
# 在 https://subdl.com/panel/api 免费申请后填入 .env。
SUBDL_API_KEY = (
    os.environ.get("SUBDL_API_KEY", "").strip()
    or str(_CFG.get("subdl_api_key", "") or "").strip()
)

SUCCESS_LOG = _resolve(_CFG.get("success_log"), "success.jsonl")
SUBTITLE_DIR = _resolve(_CFG.get("subtitle_dir"), "tv_subtitles")
STATE_LOG = _resolve(_CFG.get("state_log"), "subtitles.jsonl")

# SubDL 语言代码 -> 输出文件后缀
LANGUAGES = {
    str(k).upper(): str(v)
    for k, v in (_CFG.get("languages") or {"EN": "en", "ZH": "zh"}).items()
}

SEARCH_API = "https://api.subdl.com/api/v1/subtitles"
DOWNLOAD_BASE = "https://dl.subdl.com"

MAX_WORKERS = int(_CFG.get("max_workers", 4))
REQUEST_TIMEOUT = int(_CFG.get("request_timeout", 30))
RETRY_MAX = int(_CFG.get("retry_max", 3))
RETRY_DELAY = int(_CFG.get("retry_delay", 3))

DEFAULT_FOLDER = "tv_000001"

state_lock = threading.Lock()


def parse_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def episode_key(tmdb_id, season, episode):
    tid = str(tmdb_id or "").strip()
    s = parse_int(season)
    e = parse_int(episode)
    if not tid or s is None or e is None or s < 0 or e < 0:
        return ""
    return f"{tid}_S{s:02d}E{e:02d}"


def load_entries():
    """从 success.jsonl 读出去重后的集级条目列表（同一集取最后一条）。"""
    entries = {}
    if not os.path.exists(SUCCESS_LOG):
        raise SystemExit(f"找不到 {SUCCESS_LOG}")

    with open(SUCCESS_LOG, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            tmdb_id = str(data.get("tmdbId") or "").strip()
            season = parse_int(data.get("season"))
            episode = parse_int(data.get("episode"))
            key = episode_key(tmdb_id, season, episode)
            if not key:
                continue

            # final_path 形如 .../downloads/tv_000001/346_S01E03.mp4，取它的目录名
            final_path = data.get("final_path") or ""
            folder = os.path.basename(os.path.dirname(final_path)) or DEFAULT_FOLDER

            entries[key] = {
                "key": key,
                "tmdbId": tmdb_id,
                "season": season,
                "episode": episode,
                "title": data.get("title"),
                "folder": folder,
            }
    return list(entries.values())


def request_with_retry(method, url, **kwargs):
    last_error = None
    for attempt in range(RETRY_MAX):
        try:
            response = requests.request(
                method, url, timeout=REQUEST_TIMEOUT, **kwargs
            )
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    raise last_error


def search_subtitles(tmdb_id, season, episode):
    """按 tmdb_id + 季 + 集查询该集的所有候选字幕。"""
    params = {
        "api_key": SUBDL_API_KEY,
        "tmdb_id": tmdb_id,
        "type": "tv",
        "season_number": season,
        "episode_number": episode,
        "languages": ",".join(LANGUAGES),
        "subs_per_page": 30,
        "client": "custom_integration",
    }
    response = request_with_retry("GET", SEARCH_API, params=params)
    data = response.json()
    if not data.get("status"):
        raise RuntimeError(data.get("error") or "SubDL 返回 status=false")
    return data.get("subtitles") or []


def pick_best(subtitles, language, season, episode):
    """
    同一语言可能有多个版本，挑第一个季/集匹配、非整季包的普通字幕。
    SubDL 的结果本身按相关度排序，第一个通常就是下载量最高的。
    条目自带 season/episode 时校验必须一致，缺失则信任服务端按参数过滤的结果。
    """
    for item in subtitles:
        if (item.get("language") or "").upper() != language:
            continue
        if item.get("full_season"):
            continue
        item_season = parse_int(item.get("season"))
        item_episode = parse_int(item.get("episode"))
        if item_season is not None and item_season != season:
            continue
        if item_episode is not None and item_episode != episode:
            continue
        if item.get("url"):
            return item
    return None


def extract_srt(zip_bytes):
    """SubDL 下载回来的是 zip，从里面取出体积最大的字幕文件。"""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        candidates = [
            info
            for info in archive.infolist()
            if not info.is_dir()
            and info.filename.lower().endswith((".srt", ".ass", ".ssa", ".vtt"))
        ]
        if not candidates:
            return None, None
        # 体积最大的通常是完整正片字幕，而不是片头片尾之类的碎片
        best = max(candidates, key=lambda info: info.file_size)
        suffix = os.path.splitext(best.filename)[1].lower()
        return archive.read(best), suffix


def decode_subtitle(raw):
    """字幕编码很杂，逐个尝试常见编码，最终统一输出 UTF-8。"""
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "big5", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def download_one(entry):
    key = entry["key"]
    tmdb_id = entry["tmdbId"]
    season = entry["season"]
    episode = entry["episode"]
    target_dir = os.path.join(SUBTITLE_DIR, entry["folder"])
    os.makedirs(target_dir, exist_ok=True)

    # 已经全部抓过就直接跳过，支持增量重跑
    pending = {}
    existing_names = os.listdir(target_dir)
    for language, suffix in LANGUAGES.items():
        existing = [
            name
            for name in existing_names
            if re.fullmatch(rf"{re.escape(key)}\.{re.escape(suffix)}\.\w+", name)
        ]
        if not existing:
            pending[language] = suffix

    if not pending:
        return key, {"status": "skipped"}

    try:
        subtitles = search_subtitles(tmdb_id, season, episode)
    except Exception as exc:
        return key, {"status": "search_failed", "error": str(exc)}

    result = {"status": "ok", "saved": [], "missing": []}

    for language, suffix in pending.items():
        picked = pick_best(subtitles, language, season, episode)
        if not picked:
            result["missing"].append(language)
            continue

        try:
            response = request_with_retry(
                "GET", DOWNLOAD_BASE + picked["url"],
                params={"api_key": SUBDL_API_KEY},
            )
            content, extension = extract_srt(response.content)
            if content is None:
                result["missing"].append(language)
                continue

            path = os.path.join(target_dir, f"{key}.{suffix}{extension}")
            with open(path, "w", encoding="utf-8", newline="") as file:
                file.write(decode_subtitle(content))
            result["saved"].append(os.path.basename(path))
        except Exception as exc:
            result["missing"].append(language)
            result.setdefault("errors", []).append(f"{language}: {exc}")

    return key, result


def write_state(record):
    with state_lock:
        with open(STATE_LOG, "a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    if not SUBDL_API_KEY:
        raise SystemExit("请先在 .env 中填写 SUBDL_API_KEY")

    entries = load_entries()
    print(f"待处理剧集: {len(entries)}", flush=True)

    stats = {"ok": 0, "skipped": 0, "search_failed": 0}
    saved_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_one, entry): entry for entry in entries}
        for index, future in enumerate(as_completed(futures), 1):
            entry = futures[future]
            try:
                key, result = future.result()
            except Exception as exc:
                key, result = entry["key"], {"status": "search_failed", "error": str(exc)}
            stats[result["status"]] = stats.get(result["status"], 0) + 1
            saved_count += len(result.get("saved", []))

            record = {
                "tmdbId": entry["tmdbId"],
                "season": entry["season"],
                "episode": entry["episode"],
                "title": entry["title"],
                **result,
            }
            write_state(record)

            if result["status"] == "ok":
                print(
                    f"[{index}/{len(entries)}] {key} {entry['title']} "
                    f"-> 已保存 {result['saved']} 缺失 {result['missing']}",
                    flush=True,
                )
            elif result["status"] == "search_failed":
                print(
                    f"[{index}/{len(entries)}] {key} 查询失败: {result['error']}",
                    flush=True,
                )

    print(f"\n完成。字幕文件 {saved_count} 个，统计: {stats}", flush=True)


if __name__ == "__main__":
    main()
