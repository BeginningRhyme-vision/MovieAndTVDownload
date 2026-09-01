#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import math
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from pathlib import Path
from urllib.parse import urljoin

import requests
import urllib3
import yaml
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry


# ---------- 配置 ----------
def load_config():
    """读取与本脚本同目录的 config.yaml 中 download_movies 段。"""
    config_path = Path(__file__).with_name("config.yaml")
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("download_movies", {}) or {}


_CFG = load_config()

# 脚本所在目录（MovieDownloader/），作为相对路径与默认目录的根。
_SCRIPT_DIR = Path(__file__).resolve().parent


def resolve_dir(value, default_name):
    """解析下载/临时目录配置：
    - 为空 -> 脚本目录下的 default_name 子目录（随项目位置自动跟随）；
    - 相对路径 -> 以脚本目录为根拼接；
    - 绝对路径 -> 直接使用。
    """
    raw = value.strip() if isinstance(value, str) else value
    if not raw:
        raw = default_name
    return str((_SCRIPT_DIR / raw).resolve())


def resolve_file(value, default_name):
    """解析输入/日志/pending 等文件路径，统一锚定到脚本目录：
    - 为空 -> 脚本目录下的 default_name；
    - 相对路径 -> 以脚本目录为根拼接（不随进程当前工作目录漂移）；
    - 绝对路径 -> 直接使用。
    这样无论从哪个工作目录启动脚本，去重记录/pending 都指向同一份文件。
    """
    raw = value.strip() if isinstance(value, str) else value
    if not raw:
        raw = default_name
    return str((_SCRIPT_DIR / raw).resolve())


INPUT_JSONL = resolve_file(_CFG.get("input"), "results.jsonl")
SUCCESS_LOG = resolve_file(_CFG.get("success_log"), "success.jsonl")
FAILED_LOG = resolve_file(_CFG.get("failed_log"), "failed.jsonl")

# ---- 多轮下载配置 ----
_MULTI_ROUND_CFG = _CFG.get("multi_round", {}) or {}
MULTI_ROUND_ENABLED = _MULTI_ROUND_CFG.get("enabled", False)
# 最大轮次至少为 1（含第一轮）；关闭多轮时强制 1 轮。
MAX_ROUNDS = max(1, int(_MULTI_ROUND_CFG.get("max_rounds", 1))) if MULTI_ROUND_ENABLED else 1
ROUND_COOLDOWN_SECONDS = max(0, int(_MULTI_ROUND_CFG.get("cooldown_seconds", 300)))
# 两个独立的下载态状态文件（区别于 SUCCESS_LOG/FAILED_LOG）。
DOWNLOAD_OK_LOG = resolve_file(_CFG.get("download_ok_log"), "download_ok.jsonl")
DOWNLOAD_FAIL_LOG = resolve_file(_CFG.get("download_fail_log"), "download_fail.jsonl")
BASE_DIR = resolve_dir(_CFG.get("base_dir"), "downloads")
FOLDER_PREFIX = _CFG.get("folder_prefix", "movie_")
MAX_VIDEOS_PER_FOLDER = _CFG.get("max_videos_per_folder", 1000)
START_FOLDER_INDEX = _CFG.get("start_folder_index", 1)

# 下载线程池固定保持的影片下载数。
MAX_WORKERS = _CFG.get("max_workers", 32)
# 独立的 FFmpeg 转封装/移动线程数，不占用上面的下载槽位。
CONVERT_WORKERS = _CFG.get("convert_workers", 16)
# 单部影片同时下载的分片数。
SEGMENT_CONCURRENCY = _CFG.get("segment_concurrency", 64)
TEMP_DIR = resolve_dir(_CFG.get("temp_dir"), "temp")
SAMPLE_COUNT = _CFG.get("sample_count", 30)
SEG_RETRY_MAX = _CFG.get("seg_retry_max", 20)
SEG_RETRY_DELAY = _CFG.get("seg_retry_delay", 1)
# playlist（master/media）解析阶段的请求重试：源站临时 5xx 抽风时，这一层
# 若过早放弃会直接判整部影片失败。故给足重试次数与退避上限，扛过几十秒级故障。
PLAYLIST_RETRY_MAX = _CFG.get("playlist_retry_max", 10)
PLAYLIST_RETRY_BACKOFF = _CFG.get("playlist_retry_backoff", 1.0)
PLAYLIST_RETRY_BACKOFF_MAX = _CFG.get("playlist_retry_backoff_max", 60.0)
# 方案C 分阶重试：多节点 fallback 时，非末节点用更小的 playlist 重试次数，
# 坏节点快速判定并换下一个备用节点；末节点/单节点仍用 PLAYLIST_RETRY_MAX 死磕。
PLAYLIST_RETRY_FALLBACK = _CFG.get("playlist_retry_fallback", 3)
MIN_BITRATE_KBPS = _CFG.get("min_bitrate_kbps", 1850)
# 缺片保护阈值：缺片率超过 MAX_MISSING_RATIO 且缺片数超过豁免量，判失败可重下。
MAX_MISSING_RATIO = _CFG.get("max_missing_ratio", 0.02)
# 小样本豁免：允许至少丢这么多片而不触发阈值（与比例阈值取较大者）。
MIN_MISSING_ALLOWANCE = _CFG.get("min_missing_allowance", 1)

# ---- R2 上传配置 ----
UPLOAD_PENDING_LOG = resolve_file(_CFG.get("upload_pending_log"), "upload_pending.jsonl")
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


_load_dotenv(os.path.join(str(_SCRIPT_DIR), ".env"))


def _s3_secret(cfg_key, env_key):
    """敏感字段优先取环境变量；环境变量缺省时回退 config.yaml（便于本地调试）。"""
    env_val = os.environ.get(env_key, "").strip()
    if env_val:
        return env_val
    return (_S3_CFG.get(cfg_key, "") or "").strip()


_S3_CFG = _CFG.get("s3", {}) or {}
S3_ENABLED = bool(_S3_CFG.get("enabled", False))
S3_ENDPOINT_URL = _s3_secret("endpoint_url", "R2_ENDPOINT_URL")
S3_REGION = _S3_CFG.get("region", "auto") or "auto"
S3_BUCKET = _s3_secret("bucket", "R2_BUCKET")
S3_PREFIX = (_S3_CFG.get("prefix", "movies") or "").strip("/")
S3_ACCESS_KEY = _s3_secret("access_key", "R2_ACCESS_KEY")
S3_SECRET_KEY = _s3_secret("secret_key", "R2_SECRET_KEY")
UPLOAD_WORKERS = _S3_CFG.get("upload_workers", 16)
MAX_PENDING_UPLOADS = _S3_CFG.get("max_pending_uploads", 64)
UPLOAD_RETRY_MAX = _S3_CFG.get("upload_retry_max", 5)
UPLOAD_RETRY_DELAY = _S3_CFG.get("upload_retry_delay", 3)
DELETE_LOCAL_AFTER_UPLOAD = bool(_S3_CFG.get("delete_local_after_upload", True))

# ---- 磁盘水位监控（兜底）配置 ----
_DISK_CFG = _CFG.get("disk_guard", {}) or {}
DISK_GUARD_ENABLED = bool(_DISK_CFG.get("enabled", True))
DISK_HIGH_WATERMARK = float(_DISK_CFG.get("high_watermark", 0.85))
DISK_LOW_WATERMARK = float(_DISK_CFG.get("low_watermark", 0.80))
DISK_CHECK_INTERVAL = float(_DISK_CFG.get("check_interval", 5))
# 防误配：低水位必须严格小于高水位，否则清闸后永远无法恢复放行（下载卡死）。
if DISK_LOW_WATERMARK >= DISK_HIGH_WATERMARK:
    DISK_LOW_WATERMARK = max(0.0, DISK_HIGH_WATERMARK - 0.05)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    ),
    "Referer": "https://vidup.to/",
    "X-Requested-With": "XMLHttpRequest",
}


log_lock = threading.Lock()
folder_lock = threading.Lock()
processing_lock = threading.Lock()
processing_ids = set()
_thread_local = threading.local()

# 上传相关：pending 日志写入锁 + 反压信号量 + boto3 客户端单例（线程安全懒加载）。
pending_lock = threading.Lock()
# 反压：限制"在途+排队"的上传总量，达到上限时提交上传的线程阻塞，
# 阻塞回传到转封装、再回传到下载，从而钳制本地磁盘占用上限。
upload_semaphore = threading.BoundedSemaphore(MAX_PENDING_UPLOADS)
_s3_client_lock = threading.Lock()
_s3_client = None


def get_s3_client():
    """懒加载并复用 boto3 S3 客户端（R2 兼容）。多线程共享同一 client 是安全的。"""
    global _s3_client
    if _s3_client is not None:
        return _s3_client
    with _s3_client_lock:
        if _s3_client is None:
            import boto3
            from botocore.config import Config as BotoConfig

            _s3_client = boto3.client(
                "s3",
                endpoint_url=S3_ENDPOINT_URL,
                region_name=S3_REGION,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                config=BotoConfig(
                    signature_version="s3v4",
                    retries={"max_attempts": 1, "mode": "standard"},
                ),
            )
    return _s3_client

# 因为本脚本明确关闭了 TLS 证书校验，所以关闭对应警告。
urllib3.disable_warnings(InsecureRequestWarning)


# ---- 磁盘水位监控（兜底）----
# disk_gate 为“放行”闸门：置位=允许开新片下载；清位=磁盘吃紧，阻塞开新片。
# 初始置位（放行）。只闸“未开始的新片下载”，绝不打断已在跑的下载/转封装/上传。
disk_gate = threading.Event()
disk_gate.set()
# 监控线程停止信号：主流程退出时置位，让线程尽快收尾。
disk_monitor_stop = threading.Event()


def _disk_used_ratio(path):
    """返回 path 所在磁盘的占用比例（0~1）。取不到时返回 0（视为不吃紧）。"""
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return 0.0
    if usage.total <= 0:
        return 0.0
    return usage.used / usage.total


def disk_monitor_loop():
    """后台监控线程：按占用百分比做熔断。
    占用 >= high_watermark 清闸（阻塞新下载）；回落到 <= low_watermark 恢复放行。
    采用高/低双水位滞回，避免在阈值附近反复抖动。
    """
    os.makedirs(BASE_DIR, exist_ok=True)
    while not disk_monitor_stop.is_set():
        ratio = _disk_used_ratio(BASE_DIR)
        if disk_gate.is_set():
            if ratio >= DISK_HIGH_WATERMARK:
                disk_gate.clear()
                print(
                    f"[磁盘水位] 占用 {ratio:.1%} 达到高水位 "
                    f"{DISK_HIGH_WATERMARK:.0%}，暂停开启新片下载，"
                    f"等待上传腾出空间...",
                    flush=True,
                )
        else:
            if ratio <= DISK_LOW_WATERMARK:
                disk_gate.set()
                print(
                    f"[磁盘水位] 占用回落到 {ratio:.1%}（<= 低水位 "
                    f"{DISK_LOW_WATERMARK:.0%}），恢复新片下载。",
                    flush=True,
                )
        disk_monitor_stop.wait(DISK_CHECK_INTERVAL)


def wait_for_disk_gate():
    """开新片前调用：磁盘吃紧时在此阻塞，直到放行或监控线程停止。
    只阻塞尚未开始的下载，不影响已在跑的任务。未开启兜底时立即返回。
    """
    if not DISK_GUARD_ENABLED:
        return
    while not disk_gate.wait(timeout=DISK_CHECK_INTERVAL):
        # 若监控线程已停止（主流程退出中），不再苦等，放行让任务自然收尾。
        if disk_monitor_stop.is_set():
            return


class UnsupportedPlaylistError(RuntimeError):
    """播放列表使用了当前手工分片下载器不支持的 HLS 功能。"""


# 并发开大后用于观察是否被源站风控：统计 403/429/503 的出现次数。
block_status_lock = threading.Lock()
block_status_counter = {}


def record_block_status(status):
    with block_status_lock:
        block_status_counter[status] = block_status_counter.get(status, 0) + 1
        count = block_status_counter[status]
    if count in (1, 10, 50) or count % 200 == 0:
        print(f"  [风控监控] HTTP {status} 累计出现 {count} 次")


# ---------- HTTP ----------
def get_session():
    """每个线程复用自己的 requests.Session。"""
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(("GET", "HEAD")),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            pool_connections=SEGMENT_CONCURRENCY,
            pool_maxsize=SEGMENT_CONCURRENCY * 2,
            max_retries=retry,
            pool_block=True,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)
        session.verify = False
        _thread_local.session = session
    return _thread_local.session


def request_with_retry(
    method, url, retries=3, backoff=0.5, backoff_max=45.0, as_text=False,
    **kwargs
):
    """发起 HTTP 请求；成功时返回 str 或 bytes。

    退避采用指数增长并封顶到 backoff_max，附加少量抖动，避免多线程同时重试；
    这样 playlist 解析等关键请求能扛过源站几十秒级的临时 5xx 抽风。
    """
    session = get_session()
    kwargs.setdefault("timeout", 30)

    # 不修改 Session 的全局 headers，避免一次请求的临时头污染后续请求。
    request_headers = dict(HEADERS)
    request_headers.update(kwargs.pop("headers", {}) or {})

    last_error = None
    for attempt in range(retries):
        try:
            with session.request(
                method, url, headers=request_headers, **kwargs
            ) as response:
                response.raise_for_status()
                if method.upper() == "HEAD":
                    return None
                if as_text:
                    response.encoding = response.encoding or "utf-8"
                    return response.text
                return response.content
        except (requests.RequestException, ConnectionError, TimeoutError) as exc:
            last_error = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (403, 429, 503):
                record_block_status(status)
            if attempt == retries - 1:
                break
            wait = min(backoff * (2**attempt), backoff_max)
            wait += random.uniform(0, min(1.0, wait * 0.2))
            status_hint = f"HTTP {status}" if status else type(exc).__name__
            print(
                f"  请求重试 {attempt + 1}/{retries - 1} ({status_hint})，"
                f"{wait:.1f}s 后重试: {url}"
            )
            time.sleep(wait)

    raise RuntimeError(f"请求失败: {url}; {last_error}") from last_error


# ---------- 通用辅助 ----------
def normalize_tmdb_id(value):
    """统一使用字符串比较 ID，避免 JSON 数字和文件名字符串无法匹配。"""
    if value is None:
        return ""
    return str(value).strip()


def load_success_log_ids():
    processed = set()
    if not os.path.exists(SUCCESS_LOG):
        return processed

    with open(SUCCESS_LOG, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "tmdbId" in data:
                normalized_id = normalize_tmdb_id(data["tmdbId"])
                if normalized_id:
                    processed.add(normalized_id)
    return processed


def scan_downloaded_mp4_ids():
    """
    扫描目标目录下已经落盘的非空 MP4。

    返回 (ID 集合, 重复文件字典)。同一 ID 出现在多个目录时只报告，
    不自动删除已有文件。
    """
    downloaded_ids = set()
    locations = {}

    if not os.path.isdir(BASE_DIR):
        return downloaded_ids, {}

    try:
        folder_entries = list(os.scandir(BASE_DIR))
    except OSError as exc:
        print(f"警告: 无法扫描目标目录 {BASE_DIR}: {exc}")
        return downloaded_ids, {}

    for folder_entry in folder_entries:
        if not folder_entry.is_dir(follow_symlinks=False):
            continue
        if not folder_entry.name.startswith(FOLDER_PREFIX):
            continue

        try:
            file_entries = os.scandir(folder_entry.path)
        except OSError as exc:
            print(f"警告: 无法扫描目录 {folder_entry.path}: {exc}")
            continue

        with file_entries:
            for file_entry in file_entries:
                if not file_entry.is_file(follow_symlinks=False):
                    continue
                if not file_entry.name.lower().endswith(".mp4"):
                    continue
                try:
                    if file_entry.stat(follow_symlinks=False).st_size <= 0:
                        continue
                except OSError:
                    continue

                tmdb_id = normalize_tmdb_id(
                    os.path.splitext(file_entry.name)[0]
                )
                if not tmdb_id:
                    continue
                downloaded_ids.add(tmdb_id)
                locations.setdefault(tmdb_id, []).append(file_entry.path)

    duplicates = {
        tmdb_id: paths for tmdb_id, paths in locations.items() if len(paths) > 1
    }
    return downloaded_ids, duplicates


def write_log(log_file, data):
    with log_lock:
        with open(log_file, "a", encoding="utf-8") as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")


def truncate_log(log_file):
    """清空（重建）状态文件。用于每轮开头重置 download_fail 状态。"""
    with log_lock:
        with open(log_file, "w", encoding="utf-8") as file:
            file.write("")


# 确定性失败关键字：命中即判为“重试也没用”，绝不进入下一轮下载。
# 这些错误来自 process_one_entry 抛出的 RuntimeError 文案或早返回 error。
_PERMANENT_FAILURE_MARKERS = (
    "缺少 tmdbId 或 urls",
    "没有找到媒体播放列表",       # master 解析出来是空
    "没有找到 1080p",             # 声明分辨率全部不达标
    "低于 1080p",                 # 实测分辨率不达标
    "无法探测该流的分辨率",
    "服务器返回的不是视频分片",   # 源返回 HTML/m3u8，通常是无效源
)


def _classify_failure(error_msg):
    """判断一次下载失败是否值得下一轮重试。

    返回 True 表示“可重试”（瞬时错误：5xx/超时/SSL/连接/缺片率过高等），
    返回 False 表示“确定性失败”（画质不达标、无源、缺字段等，重下同样结果）。
    策略：默认可重试（瞬时问题更常见且重试成本可控），仅当命中确定性关键字时判不可重试。
    """
    if not error_msg:
        return True
    for marker in _PERMANENT_FAILURE_MARKERS:
        if marker in error_msg:
            return False
    return True


def update_success_log(tmdb_id, new_record):
    """按 tmdbId 去重地写 SUCCESS_LOG：同一 ID 覆盖旧记录，否则追加。

    用于 reupload 补传成功后，避免同一影片在 SUCCESS_LOG 中残留
    uploaded:false / uploaded:true 两条记录。理想状态：每个下载成功的
    影片只有一条记录（不管上传成功与否）。
    全程 log_lock 保护，读全量 -> 覆盖/追加 -> 写临时文件 -> os.replace 原子替换。
    """
    tmdb_id = str(tmdb_id)
    with log_lock:
        records = []
        replaced = False
        if os.path.exists(SUCCESS_LOG):
            with open(SUCCESS_LOG, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        records.append(line)  # 无法解析的行原样保留，避免丢数据
                        continue
                    if str(record.get("tmdbId")) == tmdb_id:
                        if not replaced:
                            records.append(new_record)
                            replaced = True
                        # 后续同 ID 记录直接丢弃（去重）
                        continue
                    records.append(record)
        if not replaced:
            records.append(new_record)

        tmp_path = SUCCESS_LOG + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as file:
            for record in records:
                if isinstance(record, str):
                    file.write(record + "\n")
                else:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
        os.replace(tmp_path, SUCCESS_LOG)


def remove_upload_failure_from_log(tmdb_id):
    """从 FAILED_LOG 中删除指定 tmdbId 的「上传阶段」失败记录（stage=="upload"）。

    用于 reupload 补传成功后清算：这样 FAILED_LOG 里若不再有 upload 阶段的行，
    即可判定所有下载成功的影片都已上传成功。
    只删 stage=="upload" 的行，保留 download/conversion/preflight 等其它阶段
    的失败记录（那些不是上传问题，不应被补传成功抹掉）。
    全程 log_lock 保护，读全量 -> 过滤 -> 写临时文件 -> os.replace 原子替换。
    """
    tmdb_id = str(tmdb_id)
    with log_lock:
        if not os.path.exists(FAILED_LOG):
            return
        kept = []
        changed = False
        with open(FAILED_LOG, "r", encoding="utf-8") as file:
            for line in file:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError:
                    kept.append(stripped)  # 无法解析的行原样保留
                    continue
                if (str(record.get("tmdbId")) == tmdb_id
                        and record.get("stage") == "upload"):
                    changed = True
                    continue  # 丢弃这条上传失败记录
                kept.append(record)
        if not changed:
            return
        tmp_path = FAILED_LOG + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as file:
            for record in kept:
                if isinstance(record, str):
                    file.write(record + "\n")
                else:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
        os.replace(tmp_path, FAILED_LOG)


def build_s3_key(local_path, year=None):
    """把本地成品映射为 R2 对象键，按「发布年份/上传日期」分层。

    规则：{S3_PREFIX}/{发布年份}/{上传日期YYYYMMDD}/{文件名}
    如成品 12345.mp4、发布年份 2000、上传日 20260901 ->
        {S3_PREFIX}/2000/20260901/12345.mp4
    year 缺失时用 unknown_year 兜底，避免拼出畸形 key。
    上传日期取上传发生当天的本地系统日期。
    """
    filename = os.path.basename(local_path)
    year_seg = str(year).strip() if year not in (None, "") else "unknown_year"
    date_seg = time.strftime("%Y%m%d")
    parts = [S3_PREFIX, year_seg, date_seg, filename] if S3_PREFIX \
        else [year_seg, date_seg, filename]
    return "/".join(parts)


def upload_to_r2(local_path, s3_key):
    """带指数退避重试地上传单个文件到 R2。成功返回 True，耗尽重试返回 (False, 原因)。"""
    client = get_s3_client()
    last_exc = None
    for attempt in range(1, UPLOAD_RETRY_MAX + 1):
        try:
            client.upload_file(local_path, S3_BUCKET, s3_key)
            return True, None
        except Exception as exc:  # noqa: BLE001 - 网络/凭证/服务端多种异常统一重试
            last_exc = exc
            if attempt < UPLOAD_RETRY_MAX:
                time.sleep(UPLOAD_RETRY_DELAY * attempt)
    return False, str(last_exc)


def write_pending(record):
    """线程安全地向 upload_pending_log 追加一条待补传记录。"""
    with pending_lock:
        with open(UPLOAD_PENDING_LOG, "a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


# 主流程运行标记文件：用于让手动 reupload 检测主流程是否在跑，
# 避免二者并发操作 pending 文件导致记录被覆盖丢失。
MAIN_LOCK_FILE = str((_SCRIPT_DIR / "download_movies.main.lock").resolve())


def _pid_alive(pid):
    """判断给定 PID 的进程是否存活（不发送真正的信号）。"""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # 进程存在但无权限——仍视为存活。
        return True
    except OSError:
        return False
    return True


def acquire_main_lock():
    """主流程启动时写入 PID 锁文件。"""
    with open(MAIN_LOCK_FILE, "w", encoding="utf-8") as file:
        file.write(str(os.getpid()))


def release_main_lock():
    """主流程退出时清理锁文件（仅当锁属于本进程时才删）。"""
    try:
        with open(MAIN_LOCK_FILE, "r", encoding="utf-8") as file:
            pid = int((file.read() or "0").strip() or 0)
    except (OSError, ValueError):
        pid = 0
    if pid == os.getpid():
        remove_file(MAIN_LOCK_FILE)


def is_main_running():
    """检测主流程是否正在运行：锁文件存在且其中 PID 仍存活。

    若锁文件存在但 PID 已死（上次异常退出留下的陈旧锁），清理后返回 False。
    """
    if not os.path.exists(MAIN_LOCK_FILE):
        return False
    try:
        with open(MAIN_LOCK_FILE, "r", encoding="utf-8") as file:
            pid = int((file.read() or "0").strip() or 0)
    except (OSError, ValueError):
        return False
    if pid > 0 and _pid_alive(pid):
        return True
    # 陈旧锁：进程已不在，清理掉。
    remove_file(MAIN_LOCK_FILE)
    return False


def safe_file_token(value):
    value = str(value or "unknown")
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", value)


def remove_file(path):
    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except OSError:
            pass


def clean_temp_directory():
    os.makedirs(TEMP_DIR, exist_ok=True)
    for name in os.listdir(TEMP_DIR):
        path = os.path.join(TEMP_DIR, name)
        if not os.path.isfile(path):
            continue
        if name.startswith(("sample_", "temp_")) and name.endswith(
            (".ts", ".mp4")
        ):
            remove_file(path)


def move_to_target_folder(temp_mp4, tmdb_id):
    """
    在同一把锁内选择目录并移动文件，防止高并发时目录容量超限。
    shutil.move 同时支持跨文件系统移动。
    """
    with folder_lock:
        index = START_FOLDER_INDEX
        while True:
            folder_name = f"{FOLDER_PREFIX}{index:06d}"
            folder_path = os.path.join(BASE_DIR, folder_name)
            os.makedirs(folder_path, exist_ok=True)

            mp4_count = sum(
                1 for name in os.listdir(folder_path) if name.endswith(".mp4")
            )
            final_path = os.path.join(folder_path, f"{tmdb_id}.mp4")

            # 同一个 tmdbId 覆盖旧文件不额外占用目录名额。
            if mp4_count < MAX_VIDEOS_PER_FOLDER or os.path.exists(final_path):
                remove_file(final_path)
                print(f"  [{tmdb_id}] 正在移动到: {final_path}", flush=True)
                shutil.move(temp_mp4, final_path)
                return final_path
            index += 1


# ---------- M3U8 解析 ----------
def parse_master_playlist(master_url, retries=None):
    """返回 [(resolution, media_playlist_url, declared_bandwidth_kbps), ...]。

    retries 为 None 时用默认强度 PLAYLIST_RETRY_MAX；方案C fallback 里对
    非末节点传更小的值，以便坏节点快速判定并换下一个备用节点。
    """
    text = request_with_retry(
        "GET", master_url, as_text=True,
        retries=PLAYLIST_RETRY_MAX if retries is None else retries,
        backoff=PLAYLIST_RETRY_BACKOFF,
        backoff_max=PLAYLIST_RETRY_BACKOFF_MAX,
    )
    lines = [line.strip() for line in text.splitlines()]
    variants = []

    for index, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue

        resolution_match = re.search(
            r"(?:^|,)RESOLUTION=(\d+x\d+)(?:,|$)", line, re.IGNORECASE
        )
        resolution = resolution_match.group(1) if resolution_match else "unknown"

        # BANDWIDTH 是 master 里声明的码率（bps），用于同分辨率下的初步排序，
        # 可以少下载几个采样片段。缺失时记为 0，后续仍以实测采样为准。
        bandwidth_match = re.search(
            r"(?:^|,)BANDWIDTH=(\d+)(?:,|$)", line, re.IGNORECASE
        )
        bandwidth_kbps = (
            int(bandwidth_match.group(1)) / 1000 if bandwidth_match else 0.0
        )

        # URI 通常在下一行；跳过中间可能存在的空行或标签行。
        for following in lines[index + 1 :]:
            if not following or following.startswith("#"):
                continue
            variants.append(
                (resolution, urljoin(master_url, following), bandwidth_kbps)
            )
            break

    if not variants and any(line.startswith("#EXTINF:") for line in lines):
        variants.append(("unknown", master_url, 0.0))

    return variants


def parse_media_playlist(playlist_url):
    """
    解析媒体播放列表，返回 (分片 URL 列表, 时长列表, init 段 URL)。

    同时支持 MPEG-TS 和 fMP4：fMP4 会带 #EXT-X-MAP 声明一个 init 段，
    该段必须写在所有媒体分片之前，否则产出的文件无法解码。TS 没有
    init 段，返回 None。
    """
    text = request_with_retry(
        "GET", playlist_url, as_text=True,
        retries=PLAYLIST_RETRY_MAX,
        backoff=PLAYLIST_RETRY_BACKOFF,
        backoff_max=PLAYLIST_RETRY_BACKOFF_MAX,
    )
    lines = [line.strip() for line in text.splitlines()]
    init_url = None

    for line in lines:
        upper = line.upper()
        if upper.startswith("#EXT-X-KEY:") and "METHOD=NONE" not in upper:
            raise UnsupportedPlaylistError(
                "播放列表含加密分片（#EXT-X-KEY）；请改用 FFmpeg 直接读取 m3u8"
            )
        if upper.startswith("#EXT-X-BYTERANGE"):
            raise UnsupportedPlaylistError(
                "播放列表使用 #EXT-X-BYTERANGE，不能按普通独立分片拼接"
            )
        if upper.startswith("#EXT-X-MAP"):
            # 形如：#EXT-X-MAP:URI="init.mp4"
            if "BYTERANGE" in upper:
                raise UnsupportedPlaylistError(
                    "#EXT-X-MAP 带 BYTERANGE，不能按独立分片拼接"
                )
            uri_match = re.search(r'URI="([^"]+)"', line, re.IGNORECASE)
            if not uri_match:
                raise UnsupportedPlaylistError("#EXT-X-MAP 缺少 URI 属性")
            init_url = urljoin(playlist_url, uri_match.group(1))

    segment_urls = []
    durations = []
    pending_duration = None

    for line in lines:
        if line.startswith("#EXTINF:"):
            match = re.match(r"#EXTINF:([0-9.]+)", line)
            pending_duration = float(match.group(1)) if match else None
            continue

        if not line or line.startswith("#"):
            continue

        if pending_duration is not None:
            segment_urls.append(urljoin(playlist_url, line))
            durations.append(pending_duration)
            pending_duration = None

    if not segment_urls:
        raise RuntimeError("媒体播放列表中没有找到任何 EXTINF 分片")

    return segment_urls, durations, init_url


def parse_resolution(resolution):
    """把 "1920x1080" 解析为 (width, height)；无法解析时返回 None。"""
    if not resolution or resolution == "unknown":
        return None
    try:
        width, height = resolution.lower().split("x", 1)
        return int(width), int(height)
    except (TypeError, ValueError):
        return None


def probe_resolution(sample_path):
    """用 ffprobe 读取采样文件的真实分辨率，失败返回 None。"""
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height",
        "-of",
        "csv=s=x:p=0",
        sample_path,
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=120,
        )
    except (subprocess.SubprocessError, OSError):
        return None

    return parse_resolution(result.stdout.strip())


def resolution_tier(size):
    """
    按高度归类画质档位，数值越大画质越高。

    用高度而不是宽度判断，因为宽银幕片源（如 3600x2160、2972x2160）宽度
    差异很大，但高度能稳定反映实际清晰度档位。
    """
    if not size:
        return -1

    height = size[1]
    if height >= 2160:
        return 3
    if height >= 1440:
        return 2
    if height >= 1080:
        return 1
    return 0


# ---------- 分片下载 ----------
def validate_segment_content(content, url):
    if not content:
        raise RuntimeError(f"服务器返回空分片: {url}")

    prefix = content[:256].lstrip().lower()
    if prefix.startswith((b"<!doctype html", b"<html", b"#extm3u")):
        raise RuntimeError(f"服务器返回的不是视频分片: {url}")


def download_single_segment(url, index, retry_max, delay):
    last_error = None
    for attempt in range(1, retry_max + 1):
        try:
            # 外层已经负责精确重试次数，因此这里关闭额外应用层重试。
            content = request_with_retry(
                "GET", url, retries=1, as_text=False, timeout=60
            )
            validate_segment_content(content, url)
            return content
        except Exception as exc:
            last_error = exc
            if attempt == retry_max:
                break

            wait = min(delay * (2 ** (attempt - 1)), 60)
            wait += random.uniform(0, min(1.0, wait * 0.2))
            print(
                f"    分片 {index + 1} 下载失败 "
                f"({attempt}/{retry_max}): {exc}; {wait:.1f}s 后重试"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"分片 {index + 1} 重试 {retry_max} 次后仍失败: {last_error}"
    ) from last_error


def download_segments(
    segment_urls,
    output_path,
    start_idx=0,
    end_idx=None,
    concurrency=SEGMENT_CONCURRENCY,
    init_url=None,
):
    """
    并发下载、按索引顺序写入分片。

    使用滑动窗口：始终保持 concurrency 个分片在途，任一分片完成就立刻补进
    下一个，避免"整批等最慢分片"的木桶效应。写盘仍严格按索引顺序进行。

    关键点：输出文件在整个下载过程中只打开一次，不能每批用 wb 重开；
    否则前面已经写入的批次会被清空。

    单个分片耗尽重试次数后会记录并跳过，不中止整部影片。返回值为：
    (成功写入的字节数, 失败分片索引列表)。

    init_url 用于 fMP4：该 init 段必须写在文件最前面，只在新建文件
    （start_idx == 0）时写入一次。
    """
    if end_idx is None:
        end_idx = len(segment_urls)

    indices = list(range(start_idx, min(end_idx, len(segment_urls))))
    if not indices:
        return 0, []

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    mode = "ab" if os.path.exists(output_path) and start_idx > 0 else "wb"
    total_bytes = 0
    failed_indices = []

    # 只打开一次：这是修复 PPS/SPS 丢失问题的核心。
    with open(output_path, mode) as output_file:
        # fMP4 的 init 段携带 moov（编解码参数），必须位于所有媒体分片之前。
        if init_url and mode == "wb":
            init_data = download_single_segment(
                init_url, -1, SEG_RETRY_MAX, SEG_RETRY_DELAY
            )
            output_file.write(init_data)
            total_bytes += len(init_data)

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            next_submit = 0
            write_cursor = 0
            done_buffer = {}
            future_to_index = {}
            # 已下完但因前面分片未到、还不能落盘的分片会暂存在内存里。
            # 限制暂存量，避免某个慢分片导致缓冲无限膨胀吃光内存。
            max_buffered = concurrency * 2

            def submit_next():
                nonlocal next_submit
                if next_submit >= len(indices):
                    return
                index = indices[next_submit]
                future = executor.submit(
                    download_single_segment,
                    segment_urls[index],
                    index,
                    SEG_RETRY_MAX,
                    SEG_RETRY_DELAY,
                )
                future_to_index[future] = index
                next_submit += 1

            def refill():
                # 缓冲过大时暂缓投递新分片，等落盘追上来再继续。
                while (
                    len(future_to_index) < concurrency
                    and next_submit < len(indices)
                    and (len(done_buffer) < max_buffered or not future_to_index)
                ):
                    submit_next()

            refill()

            while future_to_index:
                done, _ = wait(
                    future_to_index.keys(), return_when=FIRST_COMPLETED
                )
                for future in done:
                    index = future_to_index.pop(future)
                    try:
                        done_buffer[index] = future.result()
                    except Exception as exc:
                        done_buffer[index] = None
                        failed_indices.append(index)
                        print(
                            f"    警告: 分片 {index + 1} 耗尽重试次数，"
                            f"将跳过并继续；{exc}"
                        )

                # 按索引顺序把已就绪的分片落盘，保证输出严格有序。
                while write_cursor < len(indices):
                    index = indices[write_cursor]
                    if index not in done_buffer:
                        break
                    data = done_buffer.pop(index)
                    if data is not None:
                        output_file.write(data)
                        total_bytes += len(data)
                    write_cursor += 1

                # 落盘后再补满窗口，保持 concurrency 个分片始终在途。
                refill()

    return total_bytes, sorted(failed_indices)


# ---------- FFmpeg ----------
def convert_ts_to_mp4(ts_path, mp4_path):
    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-probesize",
        "100M",
        "-analyzeduration",
        "100M",
        "-i",
        ts_path,
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        mp4_path,
    ]

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            errors="replace",
        )
        if result.stderr.strip():
            print(f"  FFmpeg 警告:\n{result.stderr.strip()}")
        return True
    except subprocess.CalledProcessError as exc:
        print(f"  FFmpeg 转换失败:\n{exc.stderr}")
        return False
    except FileNotFoundError:
        print("  未找到 ffmpeg，请安装并加入 PATH")
        return False


# ---------- 单条影片处理 ----------
def process_one_entry(entry, processed_ids):
    tmdb_id = entry.get("tmdbId")
    normalized_id = normalize_tmdb_id(tmdb_id)
    title = entry.get("title", "")
    urls = entry.get("urls", [])
    year = entry.get("year")

    if not normalized_id or not urls:
        return tmdb_id, False, {"error": "缺少 tmdbId 或 urls", "retriable": False}

    # 原子地检查“历史已完成”和“当前正在处理”，防止并发重复下载。
    with processing_lock:
        if normalized_id in processed_ids:
            print(f"跳过已成功处理: {tmdb_id}")
            return tmdb_id, False, {"error": "already processed successfully"}
        if normalized_id in processing_ids:
            print(f"跳过当前运行中的重复条目: {tmdb_id}")
            return tmdb_id, False, {"error": "duplicate entry currently processing"}
        processing_ids.add(normalized_id)

    # 磁盘水位兜底：仅在此处（尚未开始任何下载动作前）阻塞。磁盘吃紧时新片
    # 在闸门前等待，不会占用 temp/带宽；已在跑的下载不受影响。
    wait_for_disk_gate()

    print(f"\n开始处理: {tmdb_id} - {title}")
    os.makedirs(TEMP_DIR, exist_ok=True)

    handed_off_to_conversion = False
    cleanup_paths = set()
    final_ts = os.path.join(TEMP_DIR, f"temp_{safe_file_token(tmdb_id)}.ts")
    temp_mp4 = os.path.join(TEMP_DIR, f"temp_{safe_file_token(tmdb_id)}.mp4")
    cleanup_paths.update((final_ts, temp_mp4))

    def _attempt_download(url, is_last_node):
        """对单个取流节点(url)尝试完整下载，成功返回 conversion_job，失败抛异常。

        is_last_node=False（还有备用节点）时，master playlist 解析用短重试
        PLAYLIST_RETRY_FALLBACK，坏节点快速判定即换下一个；末节点/单节点用
        默认 PLAYLIST_RETRY_MAX 死磕，不放过最后的机会。
        """
        variants = parse_master_playlist(
            url, retries=None if is_last_node else PLAYLIST_RETRY_FALLBACK
        )
        if not variants:
            raise RuntimeError("没有找到媒体播放列表或清晰度变体")

        # 分辨率优先：先按档位从高到低排，档位相同时优先试 BANDWIDTH 高的。
        # 分辨率未知的流排在最后，等采样后用 ffprobe 探测真实分辨率。
        annotated = [
            (resolution, playlist_url, bandwidth, parse_resolution(resolution))
            for resolution, playlist_url, bandwidth in variants
        ]
        # 已声明分辨率且低于 1080p 的流直接排除，不必浪费采样流量。
        candidates = [
            item
            for item in annotated
            if item[3] is None or resolution_tier(item[3]) >= 1
        ]
        if not candidates:
            raise RuntimeError("没有找到 1080p 或更高分辨率的流")

        candidates.sort(
            key=lambda item: (resolution_tier(item[3]), item[2]), reverse=True
        )

        best_tier = -1
        best_bitrate = 0.0
        best_resolution = None
        best_segment_urls = None
        best_durations = None
        best_init_url = None
        best_sample_bytes = 0
        best_sample_count = 0
        best_sample_path = None
        best_sample_failed_indices = []

        for resolution, playlist_url, _declared_bandwidth, size in candidates:
            print(f"  检测流 {resolution}: {playlist_url}")
            sample_path = os.path.join(
                TEMP_DIR,
                f"sample_{safe_file_token(tmdb_id)}_"
                f"{safe_file_token(resolution)}.ts",
            )
            cleanup_paths.add(sample_path)
            remove_file(sample_path)

            try:
                segment_urls, durations, init_url = parse_media_playlist(
                    playlist_url
                )
                sample_count = min(SAMPLE_COUNT, len(segment_urls))
                sample_bytes, sample_failed_indices = download_segments(
                    segment_urls,
                    sample_path,
                    start_idx=0,
                    end_idx=sample_count,
                    concurrency=min(4, SEGMENT_CONCURRENCY),
                    init_url=init_url,
                )
                sample_failed_set = set(sample_failed_indices)
                sample_duration = sum(
                    duration
                    for index, duration in enumerate(durations[:sample_count])
                    if index not in sample_failed_set
                )
                if sample_duration <= 0 or sample_bytes <= 0:
                    raise RuntimeError("采样数据或采样时长为 0")

                # master 没声明 RESOLUTION 时，用采样文件探测真实分辨率。
                actual_size = size
                actual_resolution = resolution
                if actual_size is None:
                    actual_size = probe_resolution(sample_path)
                    if actual_size is None:
                        raise RuntimeError("无法探测该流的分辨率")
                    actual_resolution = f"{actual_size[0]}x{actual_size[1]}"
                    print(f"  流 {resolution} 实测分辨率: {actual_resolution}")

                tier = resolution_tier(actual_size)
                if tier < 1:
                    raise RuntimeError(
                        f"分辨率 {actual_resolution} 低于 1080p，跳过"
                    )

                bitrate = sample_bytes * 8 / sample_duration / 1000
                print(f"  流 {actual_resolution} 采样码率: {bitrate:.0f} kbps")

                # 恰好 1080p 档要求码率达标；更高分辨率不再设码率门槛。
                if tier == 1 and bitrate <= MIN_BITRATE_KBPS:
                    raise RuntimeError(
                        f"1080p 流码率 {bitrate:.0f} kbps "
                        f"未达到 {MIN_BITRATE_KBPS} kbps"
                    )

                # 分辨率优先，同档位下再比码率。
                better = tier > best_tier or (
                    tier == best_tier and bitrate > best_bitrate
                )
                if better:
                    if best_sample_path and best_sample_path != sample_path:
                        remove_file(best_sample_path)
                    best_tier = tier
                    best_bitrate = bitrate
                    best_resolution = actual_resolution
                    best_segment_urls = segment_urls
                    best_durations = durations
                    best_init_url = init_url
                    best_sample_bytes = sample_bytes
                    best_sample_count = sample_count
                    best_sample_path = sample_path
                    best_sample_failed_indices = sample_failed_indices
                else:
                    remove_file(sample_path)
            except Exception as exc:
                remove_file(sample_path)
                print(f"  处理流 {resolution} 失败: {exc}")

        if not best_sample_path:
            raise RuntimeError(
                "没有可用的流：候选流全部低于 1080p、采样失败，"
                f"或 1080p 流码率未达到 {MIN_BITRATE_KBPS} kbps"
            )

        print(
            f"  选中流: 分辨率 {best_resolution}, "
            f"采样码率 {best_bitrate:.0f} kbps"
        )

        # sample 文件现在确实包含第 0～best_sample_count-1 段。
        remove_file(final_ts)
        os.replace(best_sample_path, final_ts)

        remaining_count = len(best_segment_urls) - best_sample_count
        extra_bytes = 0
        remaining_failed_indices = []
        if remaining_count > 0:
            print(
                f"  并发下载剩余 {remaining_count} 个分片 "
                f"(并发数 {SEGMENT_CONCURRENCY})..."
            )
            extra_bytes, remaining_failed_indices = download_segments(
                best_segment_urls,
                final_ts,
                start_idx=best_sample_count,
                end_idx=None,
                concurrency=SEGMENT_CONCURRENCY,
                init_url=best_init_url,
            )

        failed_segment_indices = sorted(
            set(best_sample_failed_indices + remaining_failed_indices)
        )
        failed_segment_set = set(failed_segment_indices)
        total_bytes = best_sample_bytes + extra_bytes
        total_duration = sum(
            duration
            for index, duration in enumerate(best_durations)
            if index not in failed_segment_set
        )
        total_bitrate = (
            total_bytes * 8 / total_duration / 1000
            if total_duration > 0
            else best_bitrate
        )
        print(f"  下载完成，总码率约 {total_bitrate:.0f} kbps")
        if failed_segment_indices:
            print(
                f"  警告: 本片共跳过 {len(failed_segment_indices)} 个失败分片；"
                f"索引: {failed_segment_indices}"
            )

        # 缺片保护：缺太多会明显影响观看，判失败写 failed 以便二次重下，
        # 避免"残片也判成功后被去重逻辑永久跳过"。
        # 触发条件：缺片数同时超过比例阈值和小样本豁免量。
        total_segment_count = len(best_segment_urls)
        missing_count = len(failed_segment_indices)
        # 比例阈值向上取整（从宽）：如 625 片 ×2%=12.5 允许丢 13 片而非 12；
        # 再与小样本豁免量取较大者，保证片数很少时也允许丢 MIN_MISSING_ALLOWANCE 片。
        allowed_missing = max(
            MIN_MISSING_ALLOWANCE,
            math.ceil(total_segment_count * MAX_MISSING_RATIO),
        )
        if missing_count > allowed_missing:
            missing_ratio = (
                missing_count / total_segment_count if total_segment_count else 0
            )
            raise RuntimeError(
                f"缺片率过高：缺 {missing_count}/{total_segment_count} 片 "
                f"({missing_ratio:.1%})，超过阈值 "
                f"{MAX_MISSING_RATIO:.1%}（豁免 {MIN_MISSING_ALLOWANCE} 片），"
                f"判失败以便二次重下"
            )

        conversion_job = {
            "tmdbId": tmdb_id,
            "normalized_id": normalized_id,
            "title": title,
            "year": year,
            "url": url,
            "final_ts": final_ts,
            "temp_mp4": temp_mp4,
            "cleanup_paths": list(cleanup_paths),
            "bitrate_kbps": round(total_bitrate),
            "resolution": best_resolution,
            "missing_segment_count": len(failed_segment_indices),
            "missing_segment_indices": failed_segment_indices,
        }
        print(
            f"  [{tmdb_id}] 分片下载完成，已释放下载槽位并进入转封装队列",
            flush=True,
        )
        return conversion_job

    try:
        # 方案C：依次尝试各取流节点，任一节点下完即成功；全部失败才判失败。
        conversion_job = None
        last_exc = None
        any_retriable = False  # 只要有任一节点是“可重试失败”，整片就值得下一轮重试
        for idx, url in enumerate(urls, start=1):
            is_last_node = idx == len(urls)
            try:
                if idx > 1:
                    print(f"  [{tmdb_id}] 切换备用节点 {idx}/{len(urls)}", flush=True)
                conversion_job = _attempt_download(url, is_last_node)
                break
            except Exception as exc:
                last_exc = exc
                # 记录本节点失败是否可重试：任一可重试即让整片进入外层多轮，
                # 避免末节点恰为确定性失败时“连坐”误伤前面本可恢复的瞬时节点。
                if _classify_failure(str(exc)):
                    any_retriable = True
                # 本节点失败：清掉本轮残留的 ts，避免污染下一个节点。
                remove_file(final_ts)
                if idx < len(urls):
                    print(f"  [{tmdb_id}] 节点 {idx} 失败，尝试下一个: {exc}")
        if conversion_job is None:
            raise last_exc if last_exc else RuntimeError("所有取流节点均失败")

        handed_off_to_conversion = True
        return tmdb_id, True, conversion_job

    except Exception as exc:
        msg = str(exc)
        # 整片可否重试：全节点失败时以“任一节点可重试”为准（乐观，首要目标是下全）；
        # 其它异常路径（单次抛出）回退到按该异常本身分类。
        retriable = any_retriable or _classify_failure(msg)
        return tmdb_id, False, {"error": msg, "retriable": retriable}
    finally:
        # 下载成功后临时文件和 ID 锁交给转封装阶段管理。
        if not handed_off_to_conversion:
            for path in cleanup_paths:
                remove_file(path)
            with processing_lock:
                processing_ids.discard(normalized_id)


def finalize_one_entry(conversion_job, processed_ids):
    """转封装 + 移动到目标目录。成功后登记去重，把成品交给上传阶段。

    注意：SUCCESS_LOG 的写入推迟到上传阶段统一处理（以便标记 uploaded 字段），
    但 processed_ids 在此登记——成品已落地，无论后续上传成败都不应再重新下载。
    """
    tmdb_id = conversion_job["tmdbId"]
    normalized_id = conversion_job["normalized_id"]
    final_ts = conversion_job["final_ts"]
    temp_mp4 = conversion_job["temp_mp4"]
    cleanup_paths = conversion_job["cleanup_paths"]
    completed = False

    try:
        print(f"  [{tmdb_id}] 开始转封装为 MP4...", flush=True)
        remove_file(temp_mp4)
        if not convert_ts_to_mp4(final_ts, temp_mp4):
            raise RuntimeError("FFmpeg 转换失败")

        print(f"  [{tmdb_id}] 转封装完成，准备移动文件", flush=True)
        final_path = move_to_target_folder(temp_mp4, tmdb_id)
        success_info = {
            "tmdbId": tmdb_id,
            "title": conversion_job["title"],
            "year": conversion_job.get("year"),
            "url": conversion_job["url"],
            "final_path": final_path,
            "bitrate_kbps": conversion_job["bitrate_kbps"],
            "resolution": conversion_job["resolution"],
            "missing_segment_count": conversion_job["missing_segment_count"],
            "missing_segment_indices": conversion_job[
                "missing_segment_indices"
            ],
        }
        completed = True
        print(f"  [{tmdb_id}] 转封装完成: {final_path}", flush=True)
        return tmdb_id, True, success_info

    except Exception as exc:
        return tmdb_id, False, {"error": str(exc)}
    finally:
        # 只清理中间产物（TS/采样），成品 mp4 交给上传阶段处理，不在此删。
        for path in cleanup_paths:
            remove_file(path)
        # 成品已落地即登记去重（成败上传都不再重下）；失败则释放 ID 锁允许重试。
        with processing_lock:
            processing_ids.discard(normalized_id)
            if completed:
                processed_ids.add(normalized_id)


def upload_one_entry(success_info):
    """上传阶段：把成品 mp4 传到 R2，成功删本地；失败留本地并写 pending。

    S3_ENABLED=False 时走老逻辑：不传不删，成品留本地，仅写 SUCCESS_LOG。
    无论上传成败都会写 SUCCESS_LOG（标记 uploaded 字段），避免下次重新下载。
    反压信号量由提交方在 future 完成回调中释放，本函数不负责 release。
    """
    tmdb_id = success_info["tmdbId"]
    local_path = success_info["final_path"]
    if not S3_ENABLED:
        success_info["uploaded"] = False
        write_log(SUCCESS_LOG, success_info)
        print(f"  [{tmdb_id}] 成功（未上传，本地保留）: {local_path}",
              flush=True)
        return tmdb_id, True, success_info

    s3_key = build_s3_key(local_path, success_info.get("year"))
    ok, reason = upload_to_r2(local_path, s3_key)
    if ok:
        success_info["uploaded"] = True
        success_info["s3_key"] = s3_key
        if DELETE_LOCAL_AFTER_UPLOAD:
            remove_file(local_path)
        write_log(SUCCESS_LOG, success_info)
        print(f"  [{tmdb_id}] 上传成功: {s3_key}", flush=True)
        return tmdb_id, True, success_info

    # 上传失败：保留本地文件，写 SUCCESS_LOG(uploaded=false) 防重下 + 写 pending。
    success_info["uploaded"] = False
    success_info["s3_key"] = s3_key
    write_log(SUCCESS_LOG, success_info)
    write_pending({
        "tmdbId": tmdb_id,
        "title": success_info.get("title", ""),
        "year": success_info.get("year"),
        "local_path": local_path,
        "s3_key": s3_key,
        "fail_reason": reason,
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    print(f"  [{tmdb_id}] 上传失败，已留本地待补传: {reason}", flush=True)
    return tmdb_id, False, {"error": f"上传失败: {reason}"}





# ---------- 主函数 ----------
def preflight_check_s3():
    """开启 s3 上传时的启动前预检：任一不过则写日志并退出，避免每部片静默失败进 pending。

    检查顺序：
      1. import boto3/botocore（未安装 -> 报错退出，提示装依赖）
      2. 必填配置字段（endpoint_url/bucket/access_key/secret_key）非空
      3. head_bucket 真连一次：凭证错(403)/桶不存在(404) 当场暴露
    """
    def _fail(reason):
        print(f"[S3 预检失败] {reason}", flush=True)
        write_log(FAILED_LOG, {
            "stage": "preflight",
            "error": reason,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        release_main_lock()
        sys.exit(1)

    try:
        import boto3  # noqa: F401
        from botocore.exceptions import ClientError, BotoCoreError  # noqa: F401
    except ImportError as exc:
        _fail(f"未安装 boto3/botocore，无法上传 R2：{exc}。请先在虚拟环境中安装依赖。")

    missing = [
        name for name, value in (
            ("endpoint_url", S3_ENDPOINT_URL),
            ("bucket", S3_BUCKET),
            ("access_key", S3_ACCESS_KEY),
            ("secret_key", S3_SECRET_KEY),
        ) if not value
    ]
    if missing:
        _fail(f"config.yaml 的 s3 段缺少必填字段：{', '.join(missing)}")

    try:
        client = get_s3_client()
        client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("403", "401") or status in (401, 403):
            _fail(f"连接 R2 存储桶 '{S3_BUCKET}' 被拒绝（凭证错误或无访问权限，HTTP {status}）：{exc}")
        elif code == "404" or status == 404:
            _fail(f"R2 存储桶 '{S3_BUCKET}' 不存在（HTTP 404）：{exc}")
        else:
            _fail(f"连接 R2 存储桶 '{S3_BUCKET}' 失败（HTTP {status}, Code={code}）：{exc}")
    except BotoCoreError as exc:
        _fail(f"连接 R2 失败（网络/endpoint 配置错误）：{exc}")
    except Exception as exc:  # noqa: BLE001 - 兜底拦截其余未知异常
        _fail(f"连接 R2 存储桶 '{S3_BUCKET}' 失败：{exc}")

    print(
        f"[S3 预检通过] 已连通存储桶 '{S3_BUCKET}'，endpoint={S3_ENDPOINT_URL}",
        flush=True,
    )


def main():
    acquire_main_lock()
    if S3_ENABLED:
        preflight_check_s3()
    monitor_thread = None
    if DISK_GUARD_ENABLED:
        monitor_thread = threading.Thread(
            target=disk_monitor_loop, name="disk-monitor", daemon=True
        )
        monitor_thread.start()
        print(
            f"[磁盘水位] 兜底监控已启动：高水位 {DISK_HIGH_WATERMARK:.0%} 闸下载，"
            f"低水位 {DISK_LOW_WATERMARK:.0%} 恢复，每 {DISK_CHECK_INTERVAL:g}s 检查一次",
            flush=True,
        )
    try:
        _run_pipeline()
    finally:
        # 先停监控线程并放行闸门，避免仍有线程卡在 wait_for_disk_gate 上。
        disk_monitor_stop.set()
        disk_gate.set()
        if monitor_thread is not None:
            monitor_thread.join(timeout=DISK_CHECK_INTERVAL + 1)
        release_main_lock()


def _run_pipeline():
    clean_temp_directory()
    print("已清理 temp 目录中的旧临时文件")

    logged_ids = load_success_log_ids()
    disk_ids, duplicate_files = scan_downloaded_mp4_ids()
    processed_ids = logged_ids | disk_ids
    print(
        f"成功日志中有 {len(logged_ids)} 个 ID，"
        f"目标目录中有 {len(disk_ids)} 个已下载 ID；"
        f"合并去重后将跳过 {len(processed_ids)} 个 ID"
    )

    if duplicate_files:
        print(
            f"警告: 磁盘上发现 {len(duplicate_files)} 个 ID 存在重复 MP4；"
            "本脚本不会自动删除，以下最多显示 10 个:"
        )
        for duplicate_id, paths in list(sorted(duplicate_files.items()))[:10]:
            print(f"  {duplicate_id}: {' | '.join(paths)}")

    if not os.path.exists(INPUT_JSONL):
        print(f"错误: 找不到 {INPUT_JSONL}")
        return

    entries = []
    input_ids = set()
    duplicate_input_count = 0
    with open(INPUT_JSONL, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"跳过 JSONL 第 {line_number} 行: {exc}")
                continue

            normalized_id = normalize_tmdb_id(entry.get("tmdbId"))
            if normalized_id and normalized_id in input_ids:
                duplicate_input_count += 1
                continue
            if normalized_id:
                input_ids.add(normalized_id)
            entries.append(entry)

    print(
        f"共读取 {len(entries)} 个去重后的条目；"
        f"输入文件内跳过 {duplicate_input_count} 个重复 ID"
    )

    ignored_errors = {
        "already processed successfully",
        "duplicate entry currently processing",
    }
    conversion_future_to_entry = {}
    upload_future_to_entry = {}
    download_future_to_entry = {}

    print(
        f"启动三级流水线: {MAX_WORKERS} 个下载槽位，"
        f"{CONVERT_WORKERS} 个独立转封装槽位，"
        f"{UPLOAD_WORKERS} 个上传槽位"
        f"（反压上限 {MAX_PENDING_UPLOADS} 在途上传，S3 上传="
        f"{'开启' if S3_ENABLED else '关闭'}）"
    )
    if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
        print(
            f"多轮下载已启用：最多 {MAX_ROUNDS} 轮，"
            f"轮次间冷却 {ROUND_COOLDOWN_SECONDS}s；"
            f"仅“可重试”的下载失败会进入下一轮。"
        )

    # 单一事件循环驱动的真三级流水线：所有在途 future（下载/转封装/上传）
    # 放进同一个 pending 集合，用 wait(FIRST_COMPLETED) 取最先完成的任意一个，
    # 按其阶段就地推进到下一级。这样每部影片一完成当前阶段就立即流入下一阶段——
    # 下载完立刻转封装、转封装完立刻上传，三级真正并行流动，互不阻塞。
    #
    # 多轮（方案 A）：pending / stage_of / 三个 future 映射 / 三个线程池 全部建在
    # 多轮循环之外，跨轮存活。每轮只向 pending 注入“本轮待下载”的下载 future；
    # 上一轮遗留的转封装/上传 future 仍在同一 pending 里被顺带推进，与本轮下载
    # 真正并行——新一轮无需等上一轮排空（它们是已下载成功的片，与本轮要重下的
    # 失败片天然不相交）。仅在全部轮次结束后统一排空剩余在途任务。
    stage_of = {}  # future -> "download" | "conversion" | "upload"
    pending = set()
    stats = {"conversions": 0, "uploads": 0}

    def handle_done_future(future, round_failed_retriable):
        """处理一个已完成的 future，按其阶段推进流水线。

        round_failed_retriable 为本轮“可重试下载失败”的收集器（list）；
        末轮排空阶段传 None（此时 pending 里只会剩转封装/上传，不会命中下载分支）。
        """
        stage = stage_of.pop(future, None)

        if stage == "download":
            entry = download_future_to_entry.pop(future)
            try:
                tmdb_id, download_success, info = future.result()
            except Exception as exc:
                tmdb_id = entry.get("tmdbId")
                download_success = False
                info = {"error": str(exc), "retriable": _classify_failure(str(exc))}

            if download_success:
                # 下载成功：写独立的下载态状态文件（只记下载，不含转封装/上传）。
                write_log(DOWNLOAD_OK_LOG, {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title", ""),
                    "year": entry.get("year"),
                })
                conversion_future = conversion_executor.submit(
                    finalize_one_entry, info, processed_ids
                )
                conversion_future_to_entry[conversion_future] = entry
                stage_of[conversion_future] = "conversion"
                pending.add(conversion_future)
                stats["conversions"] += 1
                return

            error_msg = info.get("error", "未知错误")
            # “已处理/处理中”属跳过而非失败：不写任何失败记录、不进下一轮。
            if error_msg in ignored_errors:
                return

            retriable = bool(info.get("retriable", True))
            write_log(FAILED_LOG, {
                "tmdbId": tmdb_id,
                "title": entry.get("title", ""),
                "urls": entry.get("urls", []),
                "error": error_msg,
                "stage": "download",
            })
            # 下载态状态文件：本轮下载失败逐条记录（含可否重试）。
            write_log(DOWNLOAD_FAIL_LOG, {
                "tmdbId": tmdb_id,
                "title": entry.get("title", ""),
                "error": error_msg,
                "retriable": retriable,
            })
            print(
                f"下载失败: {tmdb_id}: {error_msg}"
                f"（{'可重试' if retriable else '确定性失败,不重试'}）"
            )
            # 仅“可重试”的失败进入下一轮；确定性失败绝不重下。
            if retriable and round_failed_retriable is not None:
                round_failed_retriable.append(entry)

        elif stage == "conversion":
            entry = conversion_future_to_entry.pop(future)
            try:
                tmdb_id, conversion_success, info = future.result()
            except Exception as exc:
                tmdb_id = entry.get("tmdbId")
                conversion_success = False
                info = {"error": str(exc)}

            if not conversion_success:
                write_log(FAILED_LOG, {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title", ""),
                    "urls": entry.get("urls", []),
                    "error": info.get("error", "未知错误"),
                    "stage": "conversion",
                })
                print(f"转封装失败: {tmdb_id}: {info.get('error', '未知错误')}")
                return

            # 转封装成功 -> 立即提交上传。反压：先 acquire 信号量（限制
            # 在途+排队的上传总量为 MAX_PENDING_UPLOADS），若上传慢于下载
            # 会在此阻塞主循环，从而钳制本地磁盘占用上限。release 由 future
            # 完成回调对称释放，保证无论上传成功/异常/取消都不泄漏信号量。
            upload_semaphore.acquire()
            upload_future = upload_executor.submit(upload_one_entry, info)
            upload_future.add_done_callback(
                lambda _f: upload_semaphore.release()
            )
            upload_future_to_entry[upload_future] = entry
            stage_of[upload_future] = "upload"
            pending.add(upload_future)
            stats["uploads"] += 1

        elif stage == "upload":
            entry = upload_future_to_entry.pop(future)
            try:
                tmdb_id, upload_success, info = future.result()
            except Exception as exc:
                tmdb_id = entry.get("tmdbId")
                upload_success = False
                info = {"error": str(exc)}

            if not upload_success:
                # upload_one_entry 内部已写 pending 与
                # SUCCESS_LOG(uploaded=false)，这里再落一条 FAILED_LOG
                # 便于统计上传阶段失败。
                write_log(FAILED_LOG, {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title", ""),
                    "urls": entry.get("urls", []),
                    "error": info.get("error", "未知错误"),
                    "stage": "upload",
                })
                print(f"上传失败: {tmdb_id}: {info.get('error', '未知错误')}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as download_executor, \
            ThreadPoolExecutor(max_workers=CONVERT_WORKERS) as conversion_executor, \
            ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as upload_executor:

        current_batch = entries
        round_no = 1
        while True:
            # 每轮开头清空 download_fail 状态文件，只记录本轮下载失败。
            truncate_log(DOWNLOAD_FAIL_LOG)
            if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
                print(
                    f"\n===== 下载轮次 {round_no}/{MAX_ROUNDS}："
                    f"本轮待下载 {len(current_batch)} 部 =====",
                    flush=True,
                )

            # 注入本轮下载 future，并单独跟踪“本轮下载 future”集合。
            round_download_futures = set()
            for entry in current_batch:
                f = download_executor.submit(
                    process_one_entry, entry, processed_ids
                )
                download_future_to_entry[f] = entry
                stage_of[f] = "download"
                pending.add(f)
                round_download_futures.add(f)

            round_failed_retriable = []

            # 关键：只等“本轮下载 future”全部离开 download 阶段即算本轮下载完成，
            # 不等 pending 全空。上一轮遗留的转封装/上传在同一循环里并行推进，
            # 但不阻塞本轮判定——这正是方案 A 的并行精髓。
            remaining_downloads = set(round_download_futures)
            while remaining_downloads:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    pending.discard(future)
                    if future in remaining_downloads:
                        remaining_downloads.discard(future)
                    handle_done_future(future, round_failed_retriable)

            # 本轮下载全部有结论，决定是否再来一轮。
            if not round_failed_retriable:
                if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
                    print("\n本轮无可重试的下载失败，多轮下载提前结束。", flush=True)
                break
            if round_no >= MAX_ROUNDS:
                print(
                    f"\n已达最大轮次 {MAX_ROUNDS}，仍有 "
                    f"{len(round_failed_retriable)} 部下载失败未成功，停止重试。",
                    flush=True,
                )
                break

            print(
                f"\n本轮有 {len(round_failed_retriable)} 部可重试下载失败，"
                f"冷却 {ROUND_COOLDOWN_SECONDS}s 后进入第 {round_no + 1} 轮...",
                flush=True,
            )
            if ROUND_COOLDOWN_SECONDS > 0:
                time.sleep(ROUND_COOLDOWN_SECONDS)
            current_batch = round_failed_retriable
            round_no += 1

        # 多轮下载结束，但 pending 里可能还有末轮的转封装/上传在途 -> 显式排空，
        # 确保所有失败/成功记录都在循环内被处理（而非交给 with 退出时静默等待）。
        if pending:
            print(
                f"\n下载轮次结束，等待剩余 {len(pending)} 个转封装/上传任务完成...",
                flush=True,
            )
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                pending.discard(future)
                handle_done_future(future, None)

        # 至此所有下载/转封装/上传 future 均已完成，数据完整性得到保证。
        print(
            f"三级流水线全部完成：转封装 {stats['conversions']} 部，"
            f"上传 {stats['uploads']} 部。"
        )


def reupload_pending():
    """手动补传：读 upload_pending.jsonl，逐条重传上传失败留在本地的成品。

    一致性铁律：
      1. 补传前检查 os.path.exists(local_path)，文件不在（已被补传/手动清理）则
         直接从 pending 移除，视为已消解，不再重复上传。
      2. 同一 tmdbId 只保留最新一条 pending 记录（去孤儿/去重复）。
      3. 补传成功 -> 删本地 + 从 pending 移除（重写整个文件）+ 更新 SUCCESS_LOG
         标 uploaded:true；仍失败则保留该条 pending。
    """
    if not S3_ENABLED:
        print("s3.enabled=false，未开启远端上传，无需补传。")
        return
    if is_main_running():
        print(
            "检测到主流程（download_movies.py）正在运行，"
            "此时手动补传会与主流程并发操作 pending 文件、可能导致记录丢失。"
            "请在主流程结束后再执行 reupload。本次补传已忽略。"
        )
        return
    if not os.path.exists(UPLOAD_PENDING_LOG):
        print(f"未找到 pending 日志 {UPLOAD_PENDING_LOG}，无待补传文件。")
        return

    # 读入全部记录，同 tmdbId 只保留最新一条。
    latest_by_id = {}
    order = []
    with open(UPLOAD_PENDING_LOG, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            tmdb_id = record.get("tmdbId")
            if tmdb_id is None:
                continue
            if tmdb_id not in latest_by_id:
                order.append(tmdb_id)
            latest_by_id[tmdb_id] = record

    if not latest_by_id:
        print(f"{UPLOAD_PENDING_LOG} 中无有效待补传记录。")
        return

    print(f"共 {len(latest_by_id)} 个待补传文件，开始逐条补传...")

    remaining = {}  # tmdbId -> record，仍失败保留
    success_count = 0
    orphan_count = 0
    fail_count = 0

    for tmdb_id in order:
        record = latest_by_id[tmdb_id]
        local_path = record.get("local_path", "")
        s3_key = record.get("s3_key") or build_s3_key(local_path, record.get("year"))

        if not local_path or not os.path.exists(local_path):
            # 文件已不在本地：视为已消解（可能此前已成功补传），从 pending 移除。
            orphan_count += 1
            print(f"  [{tmdb_id}] 本地文件不存在，跳过并移除 pending: {local_path}")
            continue

        print(f"  [{tmdb_id}] 补传中 -> {s3_key}")
        ok, reason = upload_to_r2(local_path, s3_key)
        if ok:
            if DELETE_LOCAL_AFTER_UPLOAD:
                remove_file(local_path)
            update_success_log(tmdb_id, {
                "tmdbId": tmdb_id,
                "title": record.get("title", ""),
                "final_path": local_path,
                "s3_key": s3_key,
                "uploaded": True,
                "reupload": True,
            })
            remove_upload_failure_from_log(tmdb_id)
            success_count += 1
            print(f"  [{tmdb_id}] 补传成功: {s3_key}")
        else:
            record["s3_key"] = s3_key
            record["fail_reason"] = reason
            record["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
            remaining[tmdb_id] = record
            fail_count += 1
            print(f"  [{tmdb_id}] 补传仍失败，保留 pending: {reason}")

    # 重写整个 pending 文件：仅保留仍失败的记录。用锁保证与在跑主流程互斥。
    with pending_lock:
        with open(UPLOAD_PENDING_LOG, "w", encoding="utf-8") as file:
            for tmdb_id in order:
                if tmdb_id in remaining:
                    file.write(
                        json.dumps(remaining[tmdb_id], ensure_ascii=False) + "\n"
                    )

    print(
        f"补传完成：成功 {success_count}，仍失败 {fail_count}，"
        f"孤儿(本地已无)清理 {orphan_count}；pending 剩余 {len(remaining)} 条。"
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reupload":
        reupload_pending()
    else:
        main()
