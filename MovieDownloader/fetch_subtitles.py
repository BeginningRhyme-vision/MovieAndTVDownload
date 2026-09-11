"""
按 tmdbId 从 SubDL 抓取外挂字幕，为下载侧没取到的片补漏。

输入：success.jsonl（download_movies.py 的成功日志，含 tmdbId 与 year）
输出：R2 上 {folder_prefix}/{year}/{tmdbId}/subs/{lang}.vtt + .srt
     （s3.enabled=false 时退化为写本地同构目录）

字幕与视频落在**同一个影片目录**下，与 download_movies.py 的
`{folder_prefix}/{year}/{tmdbId}/` 结构、以及 R2 对象键完全同构，
前端按同前缀一次列举即可拿到视频 + 元信息 + 字幕。

🔑 为什么以 R2 为准而不是扫本地目录：
   下载侧上传成功后会**连旁车资产带影片目录一起删掉**（几十万部规模下，
   本地留副本会把 inode 吃干净）。所以本地目录不存在 ≠ 没有字幕 ——
   恰恰相反，那是最正常的状态。若按本地判重，会把每一部已有字幕的片
   都当成缺口重抓一遍，白烧 SubDL 配额，且抓来的字幕只落本地、永远进不了 R2。
   故：**已有语种问 R2，新字幕传 R2，meta.json 也就地更新**。

🔑 设计原则：**字幕是"可有可无"的附属物**。
   取到最好，取不到就算了——绝不因为字幕而让任何片被判失败、被重下、被重传。
   故本脚本：
     - 全程不抛异常到调用方（单片失败只记录，继续下一片）；
     - 无 API Key / 无 success.jsonl 时安静退出（退出码 0），不报错；
     - 下载侧（取流时白捡的 vidup tracks / videasy subtitles）已拿到的语种
       直接跳过，本脚本只补缺口。

脚本可重复执行，已存在的字幕会跳过，因此新下载的电影直接再跑一次即可。

⚠️ 请在下载侧（pipeline.py / download_movies.py）跑完之后再执行：本脚本会
   "读-改-回传" R2 上的 meta.json，而下载侧上传成品时也会写同一个 key。
   两者同时跑存在互相覆盖的窗口（非原子更新）。错开跑即可，无需加锁。
"""

import io
import json
import os
import re
import shutil
import sys
import tempfile
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

# 是否把字幕写进 R2。下载侧开了上传就必须走 R2 —— 本地影片目录早被删了。
REMOTE_MODE = dm.S3_ENABLED

# SRT/VTT 的时间轴特征：两个时间戳夹一个 -->。秒与毫秒之间 SRT 用逗号、
# VTT 用点号，两者都认；小时段可有可无（源站两种写法都出现过）。
# 用于 _sniff_format 在没有 WEBVTT 头、也没有 ASS 节标题时确认这是字幕正文。
_SRT_CUE_RE = re.compile(
    r"\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3}\s*-->\s*\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3}"
)


def subs_dir(tmdb_id, year):
    """字幕目录：{影片目录}/subs —— 与视频、meta.json 同级。

    仅在 s3.enabled=false 的纯本地模式下作为最终落点；R2 模式下字幕写临时
    目录、传完即删，不走这里。
    """
    return os.path.join(dm.movie_dir(tmdb_id, year), dm.SUBS_SUBDIR)


def _language_of(filename):
    """从字幕文件名提取语种码：en.srt -> en、zh-CN.vtt -> zh-cn。

    R2 与本地两侧共用，保证"哪些语种已有"在两种模式下判据完全一致。
    没有扩展名的文件（畸形残留）返回 None，由调用方忽略。
    """
    name = str(filename).rsplit("/", 1)[-1]
    if "." not in name:
        return None
    return name.rsplit(".", 1)[0].strip().lower() or None


def _existing_languages_remote(tmdb_id, year):
    """列举 R2 上该片 subs/ 下已有的语种集合。

    返回 (语种集合, 是否查询成功)。查询失败时返回 (空集, False) —— 调用方
    据此**跳过这一部**，而不是当作"没有字幕"去重抓：列举失败多半是网络抖动，
    此时贸然重抓会把已有字幕再传一遍（虽然幂等覆盖无害，但白烧配额）。
    """
    prefix = dm.build_s3_key(tmdb_id, year, dm.SUBS_SUBDIR + "/")
    try:
        client = dm.get_s3_client()
        found = set()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=dm.S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                language = _language_of(obj["Key"])
                if language:
                    found.add(language)
        return found, True
    except Exception as exc:  # noqa: BLE001 - 列举失败只跳过这一部
        print(f"  [{tmdb_id}] ⚠️ 列举 R2 字幕失败（已跳过）: {exc}", flush=True)
        return set(), False


def _existing_languages_local(target_dir):
    """纯本地模式下扫目录得到已有语种集合（判据与 R2 侧同源）。"""
    try:
        names = os.listdir(target_dir)
    except OSError:
        return set()
    return {lang for lang in map(_language_of, names) if lang}


def _update_remote_meta(tmdb_id, year, new_files):
    """把新补的字幕并进 R2 上 meta.json 的 subtitles[]。

    前端按 meta.json 索引字幕时，光传上去文件是不够的——meta 里没有记录就等于
    不存在。meta.json 只有几百字节，下载-改-回传的代价可忽略。

    整个过程尽力而为：meta 不存在或格式坏掉都只打印，绝不影响字幕本身已经
    成功上传的事实。
    """
    key = dm.build_s3_key(tmdb_id, year, "meta.json")
    try:
        client = dm.get_s3_client()
        body = client.get_object(Bucket=dm.S3_BUCKET, Key=key)["Body"].read()
        meta = json.loads(body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - 没有 meta 就不更新，不是错误
        print(f"  [{tmdb_id}] ⚠️ 读取 meta.json 失败（字幕已上传，跳过更新）: {exc}",
              flush=True)
        return False

    entries = meta.get("subtitles")
    if not isinstance(entries, list):
        entries = []
    # 按 path 去重：重复跑本脚本、或同一语种被覆盖重传时不该堆出重复条目。
    known = {e.get("path") for e in entries if isinstance(e, dict)}
    added = 0
    for name in new_files:
        rel = f"{dm.SUBS_SUBDIR}/{name}"
        if rel in known:
            continue
        language, _, fmt = name.rpartition(".")
        entries.append({"language": language, "format": fmt, "path": rel})
        known.add(rel)
        added += 1
    if not added:
        return False

    meta["subtitles"] = entries
    # 留痕：标明这份 meta 被字幕补漏流程改过，便于日后排查字幕来源。
    meta["subtitlesUpdatedAt"] = int(time.time())
    try:
        client.put_object(
            Bucket=dm.S3_BUCKET, Key=key,
            Body=json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  [{tmdb_id}] ⚠️ 回写 meta.json 失败（字幕已上传）: {exc}",
              flush=True)
        return False



def load_entries():
    """从 success.jsonl 读出去重后的 (tmdbId, title, year) 列表。

    文件不存在时返回空列表而非退出：字幕是可有可无的附属步骤，
    "还没下过片"是完全正常的状态，不该当成错误。

    🔑 R2 模式下**只要 uploaded=true 的片**：下载侧对上传失败的片也会写一条
    uploaded=false（语义是"下载成功但还没进 R2"，等 reupload 补传）。给这种片
    传字幕会在 R2 上造出「有 subs/ 却没有视频」的畸形目录——前端按前缀列举
    会拿到一部放不了的片。等它补传成功后再跑一次本脚本即可补上字幕。
    """
    entries = {}
    if not os.path.exists(SUCCESS_LOG):
        print(f"未找到 {SUCCESS_LOG}（还没有下载成功的影片），无需补字幕",
              flush=True)
        return []

    skipped_not_uploaded = 0
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

            if REMOTE_MODE and not data.get("uploaded"):
                # 同一 tmdbId 的旧记录可能已进 entries（success.jsonl 按片去重
                # 覆盖写，理论上不会，但坏数据下要保证"未上传"是终态判定）。
                entries.pop(tmdb_id, None)
                skipped_not_uploaded += 1
                continue

            # year 直接取记录里的发布年份：它就是视频目录那一层的来源，
            # 不再从 final_path 反解目录名（上传成功后本地视频已被删除，
            # 但目录结构由 (tmdbId, year) 唯一决定，重建即可）。
            entries[tmdb_id] = {
                "tmdbId": tmdb_id,
                "title": data.get("title"),
                "year": data.get("year"),
            }

    if skipped_not_uploaded:
        print(
            f"跳过 {skipped_not_uploaded} 部尚未上传 R2 的影片"
            f"（补传成功后再跑一次本脚本即可补字幕）",
            flush=True,
        )
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


def _sniff_format(text, declared):
    """按**正文**判定字幕格式，扩展名只在正文认不出时兜底。

    返回 "vtt" / "srt" / 其它（原样保存的富文本格式，如 ass/ssa）。

    为什么不能信扩展名（2026-09-11 实测，两个方向都出过事）：
      - 源站把 SRT 正文塞进声明 type="vtt" 的条目 —— 按 vtt 原样存下会得到
        一个缺 WEBVTT 头的 .vtt，浏览器 <track> 直接拒绝加载（R2 上 42 个
        vtt 里 8 个中招）；
      - SubDL 的 zip 里也有文件名与内容对不上的情况 —— ASS 正文若被当成 srt
        处理，会连 .srt 带 .vtt 一起写废。
    正文特征是事实，文件名只是传闻。
    """
    head = (text or "").lstrip()
    if head.upper().startswith("WEBVTT"):
        return "vtt"
    # ASS/SSA 的节标题是硬特征，任何合法文件都必有 [Script Info] 或 [V4+ Styles]
    upper = head[:400].upper()
    if "[SCRIPT INFO]" in upper or "[V4+ STYLES]" in upper or "[V4 STYLES]" in upper:
        fallback = str(declared or "").strip().lower().lstrip(".")
        # 保留原扩展名以便区分 ass/ssa；认不出就统一叫 ass。
        return fallback if fallback in ("ass", "ssa") else "ass"
    # 有 "digits --> digits" 时间轴的就是 SRT（VTT 已在上面被 WEBVTT 头拦下）。
    if _SRT_CUE_RE.search(text or ""):
        return "srt"
    # 正文认不出：退回声明值，至少保住原有行为。
    return str(declared or "").strip().lower().lstrip(".")


def _write_variants(target_dir, language, text, source_format):
    """把一份字幕按 SUBTITLE_FORMATS 落盘（vtt / srt），返回已写文件名。

    与下载侧同一套转换逻辑（dm.srt_to_vtt / dm.vtt_to_srt），保证无论字幕来自
    源站还是 SubDL，最终落盘的格式与命名完全一致。
    source_format 是**不带点**的扩展名（srt / vtt / ass ...），仅作兜底提示 ——
    真正的判据是正文（见 _sniff_format）。
    ass/ssa 是带样式的富文本格式，转换规则与 srt/vtt 完全不同，不做转换，
    按原扩展名原样保存（前端可自行决定是否使用）。
    """
    fmt = _sniff_format(text, source_format)
    if fmt not in ("srt", "vtt"):
        # 未知格式原样存。必须显式补点号：调用方传进来的是已去点的扩展名，
        # 直接拼会得到 "enass" 这种无扩展名的文件——它既不能被播放器识别，
        # 也匹配不上"已存在语种"的判据，导致每次运行都重新抓一遍。
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

    R2 模式下的流程：问 R2 要已有语种 -> 只补缺口 -> 字幕写临时目录 ->
    上传 R2 -> 删临时目录 -> 更新 R2 上的 meta.json。
    本地不留任何残留，与下载侧"上传成功即删本地"的口径一致。
    """
    tmdb_id = entry["tmdbId"]
    year = entry.get("year")

    # 第一步：确定还缺哪些语种。R2 模式下**必须问 R2** —— 本地影片目录在
    # 视频上传成功那一刻就被删了，扫本地只会得到空集，把每部片都当成缺口。
    if REMOTE_MODE:
        existing, ok = _existing_languages_remote(tmdb_id, year)
        if not ok:
            return tmdb_id, {"status": "list_failed"}
    else:
        existing = _existing_languages_local(subs_dir(tmdb_id, year))

    pending = [
        lang for lang in SUBTITLE_LANGUAGES if lang.lower() not in existing
    ]
    if not pending:
        return tmdb_id, {"status": "skipped"}

    # 第二步：准备落盘目录。R2 模式用临时目录（传完即删），本地模式直接用
    # 影片目录。两种模式下后续写盘逻辑完全一致。
    if REMOTE_MODE:
        try:
            target_dir = tempfile.mkdtemp(prefix=f"subs_{tmdb_id}_")
        except OSError as exc:
            return tmdb_id, {"status": "failed", "error": f"建临时目录失败: {exc}"}
    else:
        target_dir = subs_dir(tmdb_id, year)
        try:
            os.makedirs(target_dir, exist_ok=True)
        except OSError as exc:
            return tmdb_id, {"status": "failed", "error": f"建目录失败: {exc}"}

    try:
        return tmdb_id, _fetch_into(tmdb_id, year, pending, target_dir)
    finally:
        if REMOTE_MODE:
            # 无论成败都要清临时目录，否则每跑一次就漏一批临时文件。
            shutil.rmtree(target_dir, ignore_errors=True)


def _fetch_into(tmdb_id, year, pending, target_dir):
    """把 pending 里各语种的字幕抓进 target_dir；R2 模式下再上传并更新 meta。"""
    try:
        subtitles = search_subtitles(tmdb_id)
    except Exception as exc:  # noqa: BLE001 - 查询失败只跳过这一部
        return {"status": "search_failed", "error": str(exc)}

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

    if REMOTE_MODE and result["saved"]:
        uploaded = _upload_subtitles(tmdb_id, year, target_dir, result["saved"])
        # 上传失败的不算数：本地临时目录马上就删，没进 R2 等于这份字幕不存在，
        # 报成功会让人以为补上了，而下次运行仍会（正确地）再抓一遍。
        result["saved"] = uploaded
        if uploaded:
            result["metaUpdated"] = _update_remote_meta(tmdb_id, year, uploaded)
        else:
            result["status"] = "upload_failed"

    return result


def _upload_subtitles(tmdb_id, year, target_dir, names):
    """把刚落盘的字幕传到 R2，返回真正上传成功的文件名列表。"""
    uploaded = []
    for name in names:
        local = os.path.join(target_dir, name)
        if not os.path.isfile(local):
            continue
        try:
            key = dm.build_s3_key(tmdb_id, year, f"{dm.SUBS_SUBDIR}/{name}")
            ok, reason = dm.upload_to_r2(local, key)
            if ok:
                uploaded.append(name)
            else:
                print(f"  [{tmdb_id}] ⚠️ 字幕上传失败（已跳过）{name}: {reason}",
                      flush=True)
        except Exception as exc:  # noqa: BLE001 - 单个字幕失败不影响其它
            print(f"  [{tmdb_id}] ⚠️ 字幕上传异常（已跳过）{name}: {exc}",
                  flush=True)
    return uploaded


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
    print(
        f"待处理电影: {len(entries)}（语种: {', '.join(SUBTITLE_LANGUAGES)}；"
        f"落点: {'R2 ' + dm.S3_BUCKET if REMOTE_MODE else '本地 ' + dm.BASE_DIR}）",
        flush=True,
    )

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
