"""
按 tmdbId 从 SubDL 抓取外挂字幕，为下载侧没取到的片补漏。

输入：success.jsonl（download_movies.py 的成功日志，含 tmdbId 与 year）
输出：{base_dir}/{folder_prefix}/{year}/{tmdbId}/subs/{lang}.vtt + .srt

字幕与视频落在**同一个影片目录**下，与 download_movies.py 的
`{folder_prefix}/{year}/{tmdbId}/` 结构、以及 R2 对象键完全同构，
前端按同前缀一次列举即可拿到视频 + 元信息 + 字幕。

🔑 设计原则：**字幕是"可有可无"的附属物**。
   取到最好，取不到就算了——绝不因为字幕而让任何片被判失败、被重下、被重传。
   故本脚本：
     - 全程不抛异常到调用方（单片失败只记录，继续下一片）；
     - 无 API Key / 无 success.jsonl 时安静退出（退出码 0），不报错；
     - 下载侧（取流时白捡的 vidup tracks / videasy subtitles）已拿到的语种
       直接跳过，本脚本只补缺口。

脚本可重复执行，已存在的字幕文件会跳过，因此新下载的电影直接再跑一次即可。
"""

import io
import json
import os
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from pathlib import Path

# 复用下载侧的配置与字幕转换逻辑：语种白名单、输出格式、目录结构、
# srt<->vtt 互转全部同源，避免两套实现各改一半导致字幕落到不同地方/格式不一致。
# download_movies 模块级只读 config、不建连接、不起线程，import 是安全的（~0.2s）。
sys.path.insert(0, str(Path(__file__).resolve().parent))
import download_movies as dm  # noqa: E402


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

# 脚本所在目录（MovieDownloader/），作为相对路径与默认目录的根。
_SCRIPT_DIR = Path(__file__).resolve().parent

# ========== 配置 ==========
# SubDL API Key：敏感项，优先环境变量 SUBDL_API_KEY（同目录 .env）。
# 在 https://subdl.com/panel/api 免费申请后填入 .env。
SUBDL_API_KEY = os.environ.get("SUBDL_API_KEY", "").strip()

SUCCESS_LOG = dm.SUCCESS_LOG
STATE_LOG = str(_SCRIPT_DIR / "subtitles.jsonl")

# 目录结构、语种白名单、输出格式全部复用 download_movies（单一事实来源）。
# 语种白名单来自 config.yaml 的 assets.subtitle_languages。
SUBTITLE_LANGUAGES = dm.SUBTITLE_LANGUAGES
SUBTITLE_FORMATS = dm.SUBTITLE_FORMATS

# SubDL 的 language 字段用大写语言代码（"EN"/"ZH"），查询参数也要大写。
# 由 config 的语种白名单推导，改配置即可，不必改代码。
SUBDL_LANGUAGES = [lang.upper() for lang in SUBTITLE_LANGUAGES]

SEARCH_API = "https://api.subdl.com/api/v1/subtitles"
DOWNLOAD_BASE = "https://dl.subdl.com"

MAX_WORKERS = 4
REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_DELAY = 3
# 防御性上限：字幕 zip 正常只有几十 KB，源站返回异常内容时不能整个读进内存。
MAX_ZIP_BYTES = dm.SUBTITLE_MAX_BYTES

state_lock = threading.Lock()


def subs_dir(tmdb_id, year):
    """字幕目录：{影片目录}/subs —— 与视频、meta.json 同级。"""
    return os.path.join(dm.movie_dir(tmdb_id, year), dm.SUBS_SUBDIR)


def load_entries():
    """从 success.jsonl 读出去重后的 (tmdbId, title, year) 列表。

    文件不存在时返回空列表而非退出：字幕是可有可无的附属步骤，
    "还没下过片"是完全正常的状态，不该当成错误。
    """
    entries = {}
    if not os.path.exists(SUCCESS_LOG):
        print(f"未找到 {SUCCESS_LOG}（还没有下载成功的影片），无需补字幕",
              flush=True)
        return []

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

            # year 直接取记录里的发布年份：它就是视频目录那一层的来源，
            # 不再从 final_path 反解目录名（上传成功后本地视频已被删除，
            # 但目录结构由 (tmdbId, year) 唯一决定，重建即可）。
            entries[tmdb_id] = {
                "tmdbId": tmdb_id,
                "title": data.get("title"),
                "year": data.get("year"),
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
        "languages": ",".join(SUBDL_LANGUAGES),
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

    language 是小写语言代码（en/zh），与 SubDL 的大写字段做不区分大小写比较。
    """
    want = str(language or "").upper()
    for item in subtitles:
        if (item.get("language") or "").upper() != want:
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
    """字幕编码很杂，逐个尝试常见编码，最终统一输出 UTF-8。

    直接复用下载侧的实现，保证两条路径的解码行为一致。
    """
    return dm._decode_subtitle(raw)


def _write_variants(target_dir, language, text, source_format):
    """把一份字幕按 SUBTITLE_FORMATS 落盘（vtt / srt），返回已写文件名。

    与下载侧同一套转换逻辑（dm.srt_to_vtt / dm.vtt_to_srt），保证无论字幕来自
    源站还是 SubDL，最终落盘的格式与命名完全一致。
    source_format 是**不带点**的扩展名（srt / vtt / ass ...）。
    ass/ssa 是带样式的富文本格式，转换规则与 srt/vtt 完全不同，不做转换，
    按原扩展名原样保存（前端可自行决定是否使用）。
    """
    fmt = str(source_format or "").strip().lower().lstrip(".")
    if fmt not in ("srt", "vtt"):
        # 未知格式原样存。必须显式补点号：调用方传进来的是已去点的扩展名，
        # 直接拼会得到 "enass" 这种无扩展名的文件——它既不能被播放器识别，
        # 也匹配不上"已存在语种"的正则，导致每次运行都重新抓一遍。
        path = os.path.join(target_dir, f"{language}.{fmt}" if fmt else language)
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        return [os.path.basename(path)]

    variants = {}
    if fmt == "srt":
        variants["srt"] = text
        variants["vtt"] = dm.srt_to_vtt(text)
    else:
        variants["vtt"] = text
        variants["srt"] = dm.vtt_to_srt(text)

    saved = []
    for out_fmt in SUBTITLE_FORMATS:
        content = variants.get(out_fmt)
        if not content or not content.strip():
            continue
        path = os.path.join(target_dir, f"{language}.{out_fmt}")
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(content)
        saved.append(os.path.basename(path))
    return saved


def download_one(entry):
    """为一部片补齐缺失语种的字幕。

    绝不抛异常：任何失败都归到返回值里，让主循环继续跑下一部。
    """
    tmdb_id = entry["tmdbId"]
    try:
        target_dir = subs_dir(tmdb_id, entry.get("year"))
        os.makedirs(target_dir, exist_ok=True)
    except OSError as exc:
        return tmdb_id, {"status": "failed", "error": f"建目录失败: {exc}"}

    # 只补缺口：下载侧取流时白捡的字幕（vidup tracks / videasy subtitles）已经
    # 落在同一目录下，这里按语种跳过，不重复请求 SubDL、不覆盖已有文件。
    try:
        existing = set(os.listdir(target_dir))
    except OSError:
        existing = set()

    pending = [
        lang for lang in SUBTITLE_LANGUAGES
        if not any(re.fullmatch(rf"{re.escape(lang)}\.\w+", name)
                   for name in existing)
    ]
    if not pending:
        return tmdb_id, {"status": "skipped"}

    try:
        subtitles = search_subtitles(tmdb_id)
    except Exception as exc:  # noqa: BLE001 - 查询失败只跳过这一部
        return tmdb_id, {"status": "search_failed", "error": str(exc)}

    result = {"status": "ok", "saved": [], "missing": []}

    for language in pending:
        picked = pick_best(subtitles, language)
        if not picked:
            # 源站没有该语种：这是常态，不是错误
            result["missing"].append(language)
            continue

        try:
            response = request_with_retry(
                "GET", DOWNLOAD_BASE + picked["url"],
                params={"api_key": SUBDL_API_KEY},
                stream=True,
            )
            # 流式累加 + 超限即断：字幕 zip 正常几十 KB，若源站给回一个大文件，
            # 一次性 .content 会在检查之前就把它整个读进内存。
            chunks = []
            total = 0
            for chunk in response.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_ZIP_BYTES:
                    raise ValueError(
                        f"zip 体积超过上限 {MAX_ZIP_BYTES} 字节，疑似非字幕内容"
                    )
                chunks.append(chunk)
            response.close()

            content, extension = extract_srt(b"".join(chunks))
            if content is None:
                result["missing"].append(language)
                continue

            text = decode_subtitle(content)
            saved = _write_variants(
                target_dir, language, text, extension.lstrip(".").lower()
            )
            if saved:
                result["saved"].extend(saved)
            else:
                result["missing"].append(language)
        except Exception as exc:  # noqa: BLE001 - 单语种失败不影响其它语种
            result["missing"].append(language)
            result.setdefault("errors", []).append(f"{language}: {exc}")

    return tmdb_id, result


def write_state(record):
    """追加一条状态记录。写失败不影响主流程——状态日志本身也是可有可无的。"""
    try:
        with state_lock:
            with open(STATE_LOG, "a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 状态日志写入失败（已忽略）: {exc}", flush=True)


def main():
    # 没配 API Key 就安静跳过：字幕是可有可无的附属步骤，不该让整条流水线
    # 因为缺一个可选凭证而以非零码退出（那会让 cron / && 串联的后续步骤中断）。
    if not SUBDL_API_KEY:
        print("未配置 SUBDL_API_KEY，跳过字幕补全（不影响已下载的影片）",
              flush=True)
        return

    entries = load_entries()
    if not entries:
        return
    print(f"待处理电影: {len(entries)}（语种: {', '.join(SUBTITLE_LANGUAGES)}）",
          flush=True)

    stats = {}
    saved_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_one, entry): entry for entry in entries}
        for index, future in enumerate(as_completed(futures), 1):
            entry = futures[future]
            try:
                tmdb_id, result = future.result()
            except Exception as exc:  # noqa: BLE001 - 单片异常不能带塌整批
                tmdb_id = entry.get("tmdbId")
                result = {"status": "failed", "error": str(exc)}
                print(f"[{index}/{len(entries)}] {tmdb_id} 异常（已跳过）: {exc}",
                      flush=True)

            stats[result["status"]] = stats.get(result["status"], 0) + 1
            saved_count += len(result.get("saved", []))
            write_state({"tmdbId": tmdb_id, "title": entry.get("title"),
                         **result})

            if result["status"] == "ok":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} {entry.get('title')} "
                    f"-> 已保存 {result['saved']} 缺失 {result['missing']}",
                    flush=True,
                )
            elif result["status"] == "search_failed":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} 查询失败（已跳过）: "
                    f"{result['error']}",
                    flush=True,
                )

    print(f"\n完成。字幕文件 {saved_count} 个，统计: {stats}", flush=True)


if __name__ == "__main__":
    # 字幕补全永远以成功码退出：它取不到字幕是常态，不该让调用方（cron /
    # pipeline 脚本）以为整条流水线出了问题。Ctrl+C 仍按惯例返回 130。
    try:
        main()
    except KeyboardInterrupt:
        print("\n已中断", flush=True)
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"字幕补全异常退出（不影响已下载的影片）: {exc}", flush=True)
