"""
按 tmdbId 从 SubDL 抓取外挂字幕（英文 / 简体中文）。

输入：success.jsonl（download_movies.py 的成功日志，含 tmdbId 和 final_path）
输出：SUBTITLE_DIR/movie_00000X/{tmdbId}.en.srt、{tmdbId}.zh.srt

字幕与视频同名，播放器和前端可以按同名规则自动配对。
脚本可重复执行，已存在的字幕文件会跳过，因此新下载的电影直接再跑一次即可。
"""

import io
import json
import os
import re
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ========== 配置 ==========
SUBDL_API_KEY = ""  # 在 https://subdl.com/panel/api 免费申请后填入

SUCCESS_LOG = "success.jsonl"
SUBTITLE_DIR = "/mnt/MovieAndTVDownload/subtitles"
STATE_LOG = "subtitles.jsonl"

# SubDL 语言代码 -> 输出文件后缀
LANGUAGES = {"EN": "en", "ZH": "zh"}

SEARCH_API = "https://api.subdl.com/api/v1/subtitles"
DOWNLOAD_BASE = "https://dl.subdl.com"

MAX_WORKERS = 4
REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_DELAY = 3

state_lock = threading.Lock()


def load_entries():
    """从 success.jsonl 读出去重后的 (tmdbId, title, folder) 列表。"""
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
            if not tmdb_id:
                continue

            # final_path 形如 .../downloads/movie_000001/346.mp4，取它的目录名
            final_path = data.get("final_path") or ""
            folder = os.path.basename(os.path.dirname(final_path)) or "movie_000001"

            entries[tmdb_id] = {
                "tmdbId": tmdb_id,
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


def search_subtitles(tmdb_id):
    """按 tmdb_id 查询该电影的所有候选字幕。"""
    params = {
        "api_key": SUBDL_API_KEY,
        "tmdb_id": tmdb_id,
        "type": "movie",
        "languages": ",".join(LANGUAGES),
        "subs_per_page": 30,
        "client": "custom_integration",
    }
    response = request_with_retry("GET", SEARCH_API, params=params)
    data = response.json()
    if not data.get("status"):
        raise RuntimeError(data.get("error") or "SubDL 返回 status=false")
    return data.get("subtitles") or []


def pick_best(subtitles, language):
    """
    同一语言可能有多个版本，挑第一个非整季包的普通字幕。
    SubDL 的结果本身按相关度排序，第一个通常就是下载量最高的。
    """
    for item in subtitles:
        if (item.get("language") or "").upper() != language:
            continue
        if item.get("full_season"):
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
    tmdb_id = entry["tmdbId"]
    target_dir = os.path.join(SUBTITLE_DIR, entry["folder"])
    os.makedirs(target_dir, exist_ok=True)

    # 已经全部抓过就直接跳过，支持增量重跑
    pending = {}
    for language, suffix in LANGUAGES.items():
        existing = [
            name
            for name in os.listdir(target_dir)
            if re.fullmatch(rf"{re.escape(tmdb_id)}\.{suffix}\.\w+", name)
        ]
        if not existing:
            pending[language] = suffix

    if not pending:
        return tmdb_id, {"status": "skipped"}

    try:
        subtitles = search_subtitles(tmdb_id)
    except Exception as exc:
        return tmdb_id, {"status": "search_failed", "error": str(exc)}

    result = {"status": "ok", "saved": [], "missing": []}

    for language, suffix in pending.items():
        picked = pick_best(subtitles, language)
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

            path = os.path.join(target_dir, f"{tmdb_id}.{suffix}{extension}")
            with open(path, "w", encoding="utf-8", newline="") as file:
                file.write(decode_subtitle(content))
            result["saved"].append(os.path.basename(path))
        except Exception as exc:
            result["missing"].append(language)
            result.setdefault("errors", []).append(f"{language}: {exc}")

    return tmdb_id, result


def write_state(record):
    with state_lock:
        with open(STATE_LOG, "a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    if not SUBDL_API_KEY:
        raise SystemExit("请先填写 SUBDL_API_KEY")

    entries = load_entries()
    print(f"待处理电影: {len(entries)}", flush=True)

    stats = {"ok": 0, "skipped": 0, "search_failed": 0}
    saved_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_one, entry): entry for entry in entries}
        for index, future in enumerate(as_completed(futures), 1):
            entry = futures[future]
            tmdb_id, result = future.result()
            stats[result["status"]] = stats.get(result["status"], 0) + 1
            saved_count += len(result.get("saved", []))

            record = {"tmdbId": tmdb_id, "title": entry["title"], **result}
            write_state(record)

            if result["status"] == "ok":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} {entry['title']} "
                    f"-> 已保存 {result['saved']} 缺失 {result['missing']}",
                    flush=True,
                )
            elif result["status"] == "search_failed":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} 查询失败: {result['error']}",
                    flush=True,
                )

    print(f"\n完成。字幕文件 {saved_count} 个，统计: {stats}", flush=True)


if __name__ == "__main__":
    main()
