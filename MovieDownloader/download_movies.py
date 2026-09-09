#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import hashlib
import importlib
import os
import math
import random
import re
import shutil
import signal
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
# 轮次间冷却：只为源站的**短时**抖动留恢复窗口。原设的 300s 是按"等风控解除"
# 定的，但 200 部首跑实测（§12.11 E）403/429 零触发，502 才是主因，而 502 是
# 源站容量问题、冷却再久也不解决——三轮 10 分钟纯空转占了总时长 13%。
ROUND_COOLDOWN_SECONDS = max(0, int(_MULTI_ROUND_CFG.get("cooldown_seconds", 60)))

# ---- 轮次间就地重取流（直链过期自愈）----
# 直链过期是唯一一类"本脚本判死、但重新取一次流就能救回"的失败：vidlink 的签名
# url 带时效，重投同一条必然再挂，故 _classify_failure 判它确定性失败。开启本项后，
# 每轮结束时就地调用取流侧的 provider 拿新 url 替换 entry，让这批片能在**同一次
# 运行内**自动捞回，不必人工跑 --refetch-failed 再重跑本脚本。
# 关闭时行为与旧版完全一致（仅收尾打印提示，等人工处理）。
_REFETCH_CFG = _CFG.get("auto_refetch", {}) or {}
AUTO_REFETCH_ENABLED = bool(_REFETCH_CFG.get("enabled", True))
# 单部片在整次运行中最多被就地重取几次。重取拿到的新链接同样可能在下载排队期间
# 再次过期，故允许多次；但必须有上限，否则"取流-过期-重取"可能反复空转。
AUTO_REFETCH_MAX_PER_MOVIE = max(1, int(_REFETCH_CFG.get("max_per_movie", 2)))
# 重取的并发数。取流是纯网络 IO 且走代理，与下载争带宽，故默认远小于取流侧独立运行时。
AUTO_REFETCH_WORKERS = max(1, int(_REFETCH_CFG.get("workers", 8)))
# 单轮重取的总耗时上限（秒）。refetch_entries 由**主事件循环线程**同步调用，
# 期间 wait(pending) 不再被执行：已下载完的片无人提交转封装、成品在 temp 里
# 持续堆积（已交接给转封装阶段，不受任何 finally 清理）、上传反压链条僵住。
# 这与 UPLOAD_SLOT_WAIT_TIMEOUT 防的是同一类问题。
# 全量重跑时一轮几百部过期很正常，而单片取流要跑 4 个 provider × 多 server ×
# 12s 超时 × max_retries，几百部足以拖住主循环几十分钟。超时即收下已完成的部分、
# 放弃仍在跑的，未救回的片留给下次运行（它们不是真淘汰）。
AUTO_REFETCH_TIMEOUT = max(
    30, int(_REFETCH_CFG.get("round_timeout_seconds", 600))
)

# ---- 收尾自动补传 ----
# 上传槽位等待超时后会降级为"留本地 + 写 upload_pending.jsonl"，这些成品不会被
# 任何后续轮次处理，只能靠人跑 `download_movies.py reupload`。主流程跑完时 R2
# 往往已恢复，自动补一次能省掉这次人工介入；手动 reupload 子命令始终保留。
AUTO_REUPLOAD_ENABLED = bool(
    (_CFG.get("auto_reupload", {}) or {}).get("enabled", True)
)
# 两个独立的下载态状态文件（区别于 SUCCESS_LOG/FAILED_LOG）。
DOWNLOAD_OK_LOG = resolve_file(_CFG.get("download_ok_log"), "download_ok.jsonl")
DOWNLOAD_FAIL_LOG = resolve_file(_CFG.get("download_fail_log"), "download_fail.jsonl")
BASE_DIR = resolve_dir(_CFG.get("base_dir"), "downloads")
FOLDER_PREFIX = _CFG.get("folder_prefix", "movie_")
MAX_VIDEOS_PER_FOLDER = _CFG.get("max_videos_per_folder", 1000)
START_FOLDER_INDEX = _CFG.get("start_folder_index", 1)

# 下载线程池固定保持的影片下载数。
MAX_WORKERS = _CFG.get("max_workers", 32)
# 主循环同时持有的"下载 future"上限（分批投递深度）。
# 一次性把整轮几十万部全 submit 进 pending，会让 wait(FIRST_COMPLETED) 每次都对
# 全部未完成 future 挂/摘 waiter，主循环退化成 O(N²)。分批后 wait 规模恒定在
# 槽位量级。必须 > max_workers，否则下载池喂不满、并发上不去。
DOWNLOAD_QUEUE_DEPTH = max(
    int(MAX_WORKERS) + 1,
    int(_CFG.get("download_queue_depth", int(MAX_WORKERS) * 2)),
)
# 流式来源（§12）下"在途任务已排空、生产者仍未产出新片"时的轮询间隔（秒）。
# 只在这一种情况下才会 sleep：此刻 pending 为空，wait() 会立即返回、退化成
# 100% CPU 空转，故必须让出 CPU。取值无需精细——取流侧产出约 4.6 部/分钟
# （§12.3），2s 的粒度不会成为瓶颈；用 list 来源时这段逻辑永不触发。
STREAM_IDLE_POLL_SECONDS = max(
    0.1, float(_CFG.get("stream_idle_poll_seconds", 2))
)
# 独立的 FFmpeg 转封装/移动线程数，不占用上面的下载槽位。
# 转封装是 `ffmpeg -c copy` 纯 IO 拷贝，并发过高只会在同一块盘上互抢 IO，
# 吞吐不升反降，故取值明显低于 max_workers。
CONVERT_WORKERS = _CFG.get("convert_workers", 8)
# 单部影片同时下载的分片数。
SEGMENT_CONCURRENCY = _CFG.get("segment_concurrency", 64)
TEMP_DIR = resolve_dir(_CFG.get("temp_dir"), "temp")
SAMPLE_COUNT = int(_CFG.get("sample_count", 10))
SEG_RETRY_MAX = int(_CFG.get("seg_retry_max", 20))
SEG_RETRY_DELAY = float(_CFG.get("seg_retry_delay", 1))
# 采样阶段的单分片重试上限，**刻意远小于** seg_retry_max。
#
# 采样只是为了"测个码率决定这条流要不要下"，探不到就该立刻换下一条流/节点；
# 而正片下载死磕 20 次是值得的（已经投入了大量带宽，半途而废等于全白下）。
# 两者用同一个值是把"判断成本"抬到了"执行成本"的量级。
#
# 🔴 服务器实跑教训：源站持续吐 400/502（都不在 _NO_RETRY_HTTP_STATUS 白名单里，
# 故每片必须走满重试），一部片 = N 条候选流 × 10 个采样分片 × 20 次重试，
# 退避第 7 次起封顶 60s —— 10 部片在采样阶段空转了 90 分钟仍无结论。
# 按 3 次算，同样场景下单分片最长约 7s（1+2+4），整体缩短两个数量级。
SAMPLE_SEG_RETRY_MAX = max(1, int(_CFG.get("sample_seg_retry_max", 3)))
# 转封装(ffmpeg -c copy)单片超时(秒)：纯拷贝通常几十秒内完成，给足冗余防坏 TS
# 让 ffmpeg 无限阻塞占死 convert worker。超时判失败(可重试)，不拖垮转封装池。
CONVERT_TIMEOUT = int(_CFG.get("convert_timeout", 1800))
# playlist（master/media）解析阶段的请求重试：源站临时 5xx 抽风时，这一层
# 若过早放弃会直接判整部影片失败。故给足重试次数与退避上限，扛过几十秒级故障。
PLAYLIST_RETRY_MAX = int(_CFG.get("playlist_retry_max", 10))
PLAYLIST_RETRY_BACKOFF = float(_CFG.get("playlist_retry_backoff", 1.0))
PLAYLIST_RETRY_BACKOFF_MAX = float(_CFG.get("playlist_retry_backoff_max", 60.0))
# 方案C 分阶重试：多节点 fallback 时，非末节点用更小的 playlist 重试次数，
# 坏节点快速判定并换下一个备用节点；末节点/单节点仍用 PLAYLIST_RETRY_MAX 死磕。
PLAYLIST_RETRY_FALLBACK = int(_CFG.get("playlist_retry_fallback", 3))
# mp4 直链（vidlink 等）按 Range 分块并发下载：每块字节数与并发块数。
# 块太小会放大请求次数（CDN 限速/风控），太大则单块失败重传代价高；8MB 是折中。
MP4_CHUNK_SIZE = int(_CFG.get("mp4_chunk_size", 8 * 1024 * 1024))
MP4_CONCURRENCY = int(_CFG.get("mp4_concurrency", 8))
# mp4 直链画质预检的头部样本大小（字节）。mp4 的分辨率/编码/时长都在 moov box
# 里，整片码率 = Range 探测到的 total_size × 8 / duration，所以只要样本能被
# ffprobe 解析，判定结果与下完整片完全一致，却只花几 MB。8MB 足以覆盖绝大多数
# faststart mp4 的 moov；moov 在尾部时探测失败，放行走整片下载后再验。
MP4_SAMPLE_SIZE = int(_CFG.get("mp4_sample_size", 8 * 1024 * 1024))
# 样本探测出的时长低于此值（秒）时视为"疑似样本自身时长"而非整片时长，预检放弃、
# 放行整片下载后再验。正片通常 60 分钟以上，600s 是个宽松的下界。
# 仅在上游 runtime_minutes 缺失、不得不用样本时长时才参与判断。
MP4_MIN_TRUSTED_DURATION = float(_CFG.get("mp4_min_trusted_duration", 600))
MIN_RESOLUTION_HEIGHT = int(_CFG.get("min_resolution_height", 1080))
# 【分辨率是否参与画质判定】码率任何时候都参与判定，分辨率则可整关摘除。
#
# false（默认，2026-09-09 起）：只看码率。红线关整关放行，码率门槛是**与分辨率
#   无关的绝对线**（基准[codec] × leniency，不乘 (h/1080)²），择优纯比码率。
# true（旧口径）：分辨率红线 + 码率门槛两关都卡，且分辨率绝对优先——红线用
#   MIN_RESOLUTION_HEIGHT，码率门槛按 (h/1080)² 随流高度缩放，择优也按高度优先。
#
# ⚠️ 为什么 false 时必须同时去掉 (h/1080)² 缩放：那个因子本身就是分辨率在参与
# 判定。若只摘掉红线关却保留缩放，480p 的门槛会被缩到 2000×(480/1080)²≈316
# kbps —— 低分辨率片反而更容易过关，等于把分辨率以更隐蔽的方式又请了回来，
# 与"只按码率判断"的意图正好相反。
RESOLUTION_CHECK_ENABLED = bool(_CFG.get("resolution_check_enabled", False))
# 唯一宽松系数：同时放宽“分辨率红线”与“码率门槛”两关（合并原来的两个容差系数）。
LENIENCY = float(_CFG.get("leniency", 0.8))
# 【整片画质判死阈值】本轮尝试过的节点中，被判"画质确定性淘汰"的比例达到该值，
# 且无一节点成功时，整片判死（不进下一轮），而非按默认的乐观口径重投。
#
# 为什么可以这么判（用概率代替求证）：画质声明与实际码率是**源站侧的固有属性**，
# 不是随机变量——同一条 url 下一轮拿到的还是 480p。既然过半节点都实测/声明不达标，
# 就有充分理由推断其余节点大概率同样不达标，不必再花两轮去"求证"。
# 实测依据（§12.11 D）：29 部失败片各白跑 3 轮共约 28 分钟（占总时长 37%），
# 只救回 1 部；其中约 17 部是画质注定不达标。全量几十万部时这个浪费会线性放大。
#
# 阈值语义：0.5 = 过半即判死；1.0 = 退回最保守档（全部节点都画质淘汰才判死）；
# 设 >1.0 等于永不判死（回到改动前的行为）。
QUALITY_KILL_RATIO = float(_CFG.get("quality_kill_ratio", 0.5))
# 各编码在 1080p 基准下的最低码率门槛（kbps）。实际门槛按该流自身高度平方缩放：
#   门槛 = 基准[codec] × (h/1080)² × LENIENCY
# 分辨率判定关闭时不做缩放，直接是 基准[codec] × LENIENCY（绝对线）。
# HEVC/AV1/VP9 同主观画质更省码率，单独设等效基准；探测不到编码回退 H.264 基准（最严）。
BITRATE_BASELINE = {
    "h264": float(_CFG.get("bitrate_h264", 2000)),
    "hevc": float(_CFG.get("bitrate_hevc", 1189)),
    "av1": float(_CFG.get("bitrate_av1", 1000)),
    "vp9": float(_CFG.get("bitrate_vp9", 1514)),
}
# 1080p 码率基准高度：门槛随 (实测高度/此值)² 缩放（码率需求 ∝ 像素数 ∝ 高度²）。
_BITRATE_BASELINE_HEIGHT = 1080
# 缺片保护阈值：缺片率超过 MAX_MISSING_RATIO 且缺片数超过豁免量，判失败可重下。
MAX_MISSING_RATIO = float(_CFG.get("max_missing_ratio", 0.02))
# 小样本豁免：允许至少丢这么多片而不触发阈值（与比例阈值取较大者）。
MIN_MISSING_ALLOWANCE = int(_CFG.get("min_missing_allowance", 1))

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
S3_PREFIX = (_S3_CFG.get("prefix", "") or "").strip("/")
S3_ACCESS_KEY = _s3_secret("access_key", "R2_ACCESS_KEY")
S3_SECRET_KEY = _s3_secret("secret_key", "R2_SECRET_KEY")
UPLOAD_WORKERS = _S3_CFG.get("upload_workers", 16)
MAX_PENDING_UPLOADS = _S3_CFG.get("max_pending_uploads", 64)
UPLOAD_RETRY_MAX = _S3_CFG.get("upload_retry_max", 5)
UPLOAD_RETRY_DELAY = _S3_CFG.get("upload_retry_delay", 3)
# boto3 连接/读取超时（秒）。botocore 默认 60s 连接超时太长：R2 抖动时上传
# worker 全被顶住，反压信号量迅速耗尽，进而触发主循环的 300s 槽位等待降级。
S3_CONNECT_TIMEOUT = float(_S3_CFG.get("connect_timeout", 15))
S3_READ_TIMEOUT = float(_S3_CFG.get("read_timeout", 120))
# 等待上传槽位的上限（秒）。超时即降级为"留本地 + 写 pending"，绝不无限期等：
# acquire 由主事件循环线程调用，一旦挂住，下载完成的 future 无人处理、转封装
# 不再提交、已下好的 final_ts 在 temp 里持续堆积（它们已交接给转封装阶段，
# 不会被任何 finally 清理），整条流水线连同磁盘一起被远端故障拖垮。
UPLOAD_SLOT_WAIT_TIMEOUT = float(_S3_CFG.get("upload_slot_wait_timeout", 300))
DELETE_LOCAL_AFTER_UPLOAD = bool(_S3_CFG.get("delete_local_after_upload", True))

# ---- 磁盘水位监控（兜底）配置 ----
_DISK_CFG = _CFG.get("disk_guard", {}) or {}
DISK_GUARD_ENABLED = bool(_DISK_CFG.get("enabled", True))
DISK_HIGH_WATERMARK = float(_DISK_CFG.get("high_watermark", 0.85))
DISK_LOW_WATERMARK = float(_DISK_CFG.get("low_watermark", 0.75))
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
# 目标目录填充游标（folder_lock 保护）：缓存"当前正在填的目录号"，避免每次移动
# 都从 START_FOLDER_INDEX 起对每个已满目录 os.listdir 计数（大批量时 O(N²)）。
# 单调递增：当前目录填满即前进、不回头扫。重启后重置为 START，首次移动一次性
# 定位到第一个未满目录再缓存住（一次 O(N)，之后 O(1) 起步）。
_current_folder_index = START_FOLDER_INDEX
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
                    connect_timeout=S3_CONNECT_TIMEOUT,
                    read_timeout=S3_READ_TIMEOUT,
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
        try:
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
        except Exception as exc:
            # 监控线程绝不能因意外异常静默死亡：否则 gate 一旦停在 clear 态，
            # 所有等在 wait_for_disk_gate 上的 download worker 会永久阻塞。
            # 兜底放行 gate（宁可暂时不熔断，也不卡死主流程），下轮继续尝试。
            disk_gate.set()
            print(f"[磁盘水位] 监控异常，已放行闸门以防卡死: {exc}", flush=True)
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


class QualityRejectedError(RuntimeError):
    """本条流 / 本个节点因画质不达标被淘汰——同一条 url 重下必然复现。

    🔑 为什么要用异常类型而不是继续认错误文案：
    "画质不达标"这个**语义**是稳定的，但它的**判据是会变的**。当前是
    "分辨率红线 + 码率门槛、分辨率绝对优先"，日后可能改成"只看码率"。
    若上层靠 `"低于红线" in msg` 这类字符串识别，判据一变就要同步改判定表、
    统计表、内层聚合三处，必漏。改成认类型后，判据怎么演进上层都零改动：
    增删画质关卡时，只要新关卡照样 raise 本异常即可。

    文案仍保留既有 marker（低于红线 / 码率未达到 / 没有找到高度达标），
    因为 `_PERMANENT_FAILURE_MARKERS`、`_REJECT_REASON_RULES` 与历史
    failed.jsonl 都按文案工作，换类型不该破坏它们的兼容性。
    """


# 全局中断信号：Ctrl+C / SIGTERM 后置位，所有分片重试循环见状立刻放弃退避、
# 归还线程。
#
# ⚠️ 为什么必须有它（服务器实跑踩到的坑）：分片重试是 `time.sleep(退避)` 的
# 长循环，退避第 7 次起就封顶 60s，单分片最多 20 次。Ctrl+C 只会中断**主线程**，
# 线程池里的 worker 察觉不到，会各自把 20 次重试跑完才罢休。
# 实测表现是：中断统计都打印完了（"[pipeline] 已中断"），进程却还挂着 31 个
# 线程继续刷失败日志，`kill -INT` 形同虚设，只能 kill -9。
#
# mp4 直链层早有同款机制（_download_mp4_chunk 的 abort_event），但那是**每部片
# 一个**的局部信号，只能在"这部片判失败"时打断自己；进程级中断需要这个全局的。
interrupted = threading.Event()


# 并发开大后用于观察是否被源站风控：统计 403/429/503 的出现次数。
block_status_lock = threading.Lock()
block_status_counter = {}


def record_block_status(status):
    with block_status_lock:
        block_status_counter[status] = block_status_counter.get(status, 0) + 1
        count = block_status_counter[status]
    if count in (1, 10, 50) or count % 200 == 0:
        print(f"  [风控监控] HTTP {status} 累计出现 {count} 次")


# 确定性 HTTP 状态码：同一条 url 重试必然复现同样结果，重试纯属浪费时间与槽位。
#   401/403 鉴权失败或签名过期（源站不认这个请求，退避多久都一样）
#   404/410  资源不存在/已删除
#   416      Range 越界（探测到的总长与实际不符）
# 注意 429/503 不在此列——它们是限流/临时不可用，退避后有很大概率成功，
# 属于"必须重试"的一类，与本集合语义相反。
_NO_RETRY_HTTP_STATUS = frozenset({401, 403, 404, 410, 416})
# 上述状态码抛出的错误统一带此标记，供各重试层快速短路（不必解析 HTTP 文案，
# 也不依赖 requests/urllib3 的具体措辞，跨层稳定）。
_HTTP_PERMANENT_MARKER = "确定性HTTP失败"


def _status_of(exc):
    """从异常里取 HTTP 状态码；取不到返回 None。"""
    return getattr(getattr(exc, "response", None), "status_code", None)


def is_permanent_http_failure(exc_or_msg):
    """判断一次失败是否为"重试也没用"的确定性 HTTP 失败。

    用于 mp4 直链块重试层快速短路，避免同一类 404/403 被白重试 SEG_RETRY_MAX
    次（累计十几分钟退避），拖死整片下载窗口、延误换下一个取流节点。
    """
    status = _status_of(exc_or_msg)
    if status is not None:
        return status in _NO_RETRY_HTTP_STATUS
    return _HTTP_PERMANENT_MARKER in str(exc_or_msg)


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

    确定性 HTTP 失败（401/403/404/410/416）不重试：这类结果重试必然复现，
    白等十几分钟退避只会拖慢换下一个取流节点。抛出的错误带确定性标记，
    供上层继续短路。
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
            status = _status_of(exc)
            if status in (403, 429, 503):
                record_block_status(status)
            if status in _NO_RETRY_HTTP_STATUS:
                raise RuntimeError(
                    f"请求失败({_HTTP_PERMANENT_MARKER} HTTP {status}): {url}; {exc}"
                ) from exc
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


def parse_int(value):
    """解析整数字段（quality/size/Content-Length 等）：接受 int 或纯数字字符串，
    其余返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


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

    顺带清理 0 字节 mp4：那是 move_to_target_folder 落了占位文件后、移动完成前
    进程被杀留下的孤儿，既不是有效成品也不该白占目录名额。只删大小为 0 的，
    有内容的文件一律不动。
    """
    downloaded_ids = set()
    locations = {}
    orphan_count = 0

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
                        # 0 字节孤儿：占位后进程被杀留下的残骸，直接清掉。
                        remove_file(file_entry.path)
                        orphan_count += 1
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

    if orphan_count:
        print(f"已清理 {orphan_count} 个 0 字节 mp4 孤儿（移动中断留下的占位文件）")

    duplicates = {
        tmdb_id: paths for tmdb_id, paths in locations.items() if len(paths) > 1
    }
    return downloaded_ids, duplicates


def write_log(log_file, data):
    # 日志写盘绝不能抛异常逃逸：磁盘满/inode 耗尽/权限变更时，
    # 只打印告警并放弃这条记录，避免崩掉整条流水线（丢一条记录
    # 好过丢整批在途任务）。
    try:
        with log_lock:
            with open(log_file, "a", encoding="utf-8") as file:
                file.write(json.dumps(data, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"⚠️ 写日志失败({log_file}): {exc}", flush=True)


def truncate_log(log_file):
    """清空（重建）状态文件。用于每轮开头重置 download_fail 状态。"""
    try:
        with log_lock:
            with open(log_file, "w", encoding="utf-8") as file:
                file.write("")
    except Exception as exc:
        print(f"⚠️ 清空日志失败({log_file}): {exc}", flush=True)


# 确定性失败关键字：命中即判为“重试也没用”，绝不进入下一轮下载。
# 这些错误来自 process_one_entry 抛出的 RuntimeError 文案或早返回 error。
_PERMANENT_FAILURE_MARKERS = (
    "缺少 tmdbId 或 urls",
    "没有找到媒体播放列表",       # master 解析出来是空
    "不支持的播放列表结构",       # 加密/BYTERANGE/MAP 等手工分片器永久不支持的结构
    "没有找到高度达标",           # 声明分辨率全部低于红线（含容差）
    "低于红线",                   # 实测分辨率低于红线
    "码率未达到",                 # 采样码率未达到按高度平方缩放的门槛
    # 画质淘汰的两条**汇总**文案（内层"全部候选流画质淘汰"、外层"过半节点画质
    # 淘汰"）。它们不含上面的单因关键字，故必须单列，否则 _classify_failure 会
    # 把判死结论又翻回"可重试"。与 QualityRejectedError 类型互为双保险：类型管
    # 进程内的判定，文案管落盘后（failed.jsonl 重新载入时只剩字符串）的判定。
    "因画质不达标",
    "服务器返回的不是视频分片",   # 源返回 HTML/m3u8，通常是无效源
    # ---- mp4 直链：同一条 url 重下必然复现的确定性失败 ----
    # 本脚本读的是固化的 results.jsonl，没有重新取流的能力，多轮重投拿到的
    # 还是同一条 url，白烧带宽与下载槽位。这类失败要靠重跑 tmdb_ids_to_links.py
    # 换一条新直链来修复，故在此判死、只留 failed.jsonl 供上游重新取流。
    #
    # 闭环怎么走：`python tmdb_ids_to_links.py --refetch-failed` 会扫本文件写的
    # failed.jsonl，挑出带 _NEEDS_REFETCH_MARKER 的 tmdbId 强制重取（绕过
    # "已在 results.jsonl 即跳过"），新结果追加落盘，本脚本按 fetched_at 择新
    # 自动选用。注意光重跑主命令没用——那些 id 已在 results.jsonl 里会被跳过。
    "需重新取流",                 # 403/410 签名直链已过期
    "直链块不可用",               # 404 直链不存在 / 416 Range 越界
    "直链不支持 Range",           # 服务端不支持 Range 分块
    "服务器未按 Range 响应",      # 块请求被 200 全量响应
    "直链总长异常",               # 探测出的总长 <= 0
    "直链下载长度不符",           # 各块均成功但总长对不上，探测总长本身有误
)

# 注意：_HTTP_PERMANENT_MARKER（401/403/404/410/416）有意**不**列入上面的
# 整片判死表。它只用于"层内短路"——让分片/块/playlist 请求不再空等退避、
# 尽快换下一个取流节点。但整片是否重投要更乐观：403 很多时候是源站的临时
# 风控（record_block_status 正是把 403 当风控信号在统计），冷却一轮后往往
# 就能恢复；若在此判死会把可救回的片永久淘汰，与"尽可能提高成功率"相悖。
# 真正需要判死的 mp4 直链场景已由上面的专用文案（需重新取流/直链块不可用）覆盖。

# mp4 直链（vidlink 签名 url 带 sign&t 时效）返回 403/410 时的文案标记。
# 注意：它同时也在 _PERMANENT_FAILURE_MARKERS 中——本脚本无法重新取流，
# 重试同一条过期 url 必然再挂，判死后交由上游重跑取流修复。
# ⚠️ 该字符串是**跨文件契约**：tmdb_ids_to_links.py 的 NEEDS_REFETCH_MARKER 必须
# 与它逐字相同，`--refetch-failed` 靠匹配这段文案从 failed.jsonl 里挑重取对象。
# 改这里必须同步改那边，否则闭环静默断开（重取永远挑不出 id 且不报错）。
_NEEDS_REFETCH_MARKER = "需重新取流"

# mp4 直链单块下载中“重试也没用”的文案：命中即不再走块级退避重试，直接上抛。
_MP4_CHUNK_NO_RETRY_MARKERS = (
    _NEEDS_REFETCH_MARKER,        # 403/410 直链过期
    "服务器未按 Range 响应",      # 200 全量响应，服务端不支持 Range
    "服务器返回的不是视频分片",   # 首块是 HTML/m3u8
    "直链块不可用",               # 404 直链不存在 / 416 Range 越界
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


# 被拒/失败原因归类规则：(类别名, 命中关键字元组)，按顺序首个命中者胜出。
# 仅用于收尾聚合统计（观测性），量化各类误杀/失败占比，指导码率门槛校准。
# 不参与任何判定逻辑，改动零风险。
#
# 例外：`_REFETCH_REASON_LABEL` 这一类**会被收尾提示读取**（提示用户跑
# --refetch-failed），故抽成常量而非裸字符串——改类别名时不会漏掉那处。
_REFETCH_REASON_LABEL = "直链失效需重新取流"
_REJECT_REASON_RULES = (
    ("缺少字段/无媒体列表", ("缺少 tmdbId 或 urls", "没有找到媒体播放列表")),
    # 画质汇总判死（内层全流淘汰 / 外层过半节点淘汰）：单列类目，便于在收尾统计
    # 里直接看到"被概率口径判死"的片有多少，是评估该口径是否过激的一手数据。
    # ⚠️ 必须排在"候选流无一入选"之前：汇总文案里附带了**末节点**的原始错误，
    # 而末节点常常正是"本轮候选流无一入选"。首个命中者胜出，排在后面就会被抢走，
    # 判死片全被记到"无一入选"类目下，正好污染要用来评估本口径的那份数据。
    ("画质整体不达标(判死)", ("因画质不达标",)),
    # “候选流无一入选”是汇总文案（不含单因 marker），须先于单因规则匹配。
    ("候选流无一入选", ("候选流无一入选",)),
    ("不支持的播放列表结构", ("不支持的播放列表结构",)),
    ("分辨率低于红线", ("低于红线", "没有找到高度达标")),
    ("码率未达门槛", ("码率未达到",)),
    ("采样探测分辨率失败", ("采样探测分辨率失败",)),
    ("采样数据异常", ("采样数据或采样时长",)),
    ("正片缺片率过高", ("缺片率过高",)),
    ("源返回非视频分片", ("服务器返回的不是视频分片",)),
    (_REFETCH_REASON_LABEL, (_NEEDS_REFETCH_MARKER,)),
    # mp4 直链专属类目：与上面的 m3u8 类目并列，便于在收尾统计里单独看
    # vidlink 直链的淘汰构成（多源接入后校准门槛/判断源质量的关键数据）。
    # 放在“超时/SSL”之前：这几条是确定性结论，不该被通用网络类目抢先命中。
    ("直链块不可用(404/416)", ("直链块不可用",)),
    ("直链不支持Range", ("直链不支持 Range", "服务器未按 Range 响应")),
    ("直链总长异常", ("直链总长异常", "直链下载长度不符")),
    ("直链块重试耗尽", ("直链块",)),
    ("直链探测失败", ("直链探测失败",)),
    ("源站5xx", ("HTTP Error 5", "500 Server Error", "502", "503", "504")),
    # 确定性 4xx（401/403/404/410/416）：层内已短路不重试，统计上单列一类，
    # 便于跑完后判断是源站风控（403 居多）还是链接真失效（404/410 居多）。
    ("确定性4xx", (_HTTP_PERMANENT_MARKER,)),
    ("超时", ("timed out", "timeout", "超时")),
    ("SSL/连接错误", ("SSL", "Connection", "ConnectionError")),
)


def classify_reject_reason(error_msg):
    """把失败 error 文案归入 _REJECT_REASON_RULES 的类别；无命中归“其他”。"""
    if not error_msg:
        return "其他"
    for category, markers in _REJECT_REASON_RULES:
        for marker in markers:
            if marker in error_msg:
                return category
    return "其他"


# ---------- 轮次间就地重取流（直链过期自愈） ----------
def needs_refetch(error_msg):
    """该失败是否属于"换一条新直链就能救回"。

    只认 _NEEDS_REFETCH_MARKER（403/410 签名过期）。其余确定性失败——画质不达标、
    不支持的播放列表结构、404 直链不存在——重取流也是同样结果或本就不该救，
    放进来只会白烧取流配额。
    """
    return bool(error_msg) and _NEEDS_REFETCH_MARKER in error_msg


def plan_retry_buckets(retriable, error_msg, refetch_flag=None):
    """决定一次下载失败要进哪些重投桶，返回 (要重投, 要重新取流)。

    两者**不互斥**，这是本函数存在的全部理由：
      - retriable 是乐观口径——任一节点可重试，整片就值得下一轮重投；
      - needs_refetch 说明至少有一个 mp4 节点的签名 url 已失效，重投拿到的
        还是同一条、必然再挂。
    多源下"vidup m3u8 挂 5xx + vidlink mp4 签名过期"是常态（5xx 约占可重试
    失败八成）。若写成互斥分支，这类片只会被重投而永远不换新直链，那个 mp4
    节点在剩余所有轮次里都是废的，白白损失一个可用源。

    refetch_flag：调用方逐节点统计出的显式结论。传 None 表示"没有该信息"，
    此时回退到按 error_msg 文案判断。之所以要这个参数——error_msg 只保留
    **最后一个**节点的错误，过期节点排在非末位时文案里根本没有过期 marker。
    """
    if refetch_flag is None:
        refetch_flag = needs_refetch(error_msg)
    return (
        bool(retriable),
        AUTO_REFETCH_ENABLED and bool(refetch_flag),
    )


# ---- 首轮待下载条目的来源抽象（§12 流式化改造，第 1 步）----
# 背景：首轮原本是一个已读全的固定 list（`current_batch[next_submit]`）。要让取流
# 与下载重叠（§12.1），首轮必须能"边产边下"。这里把"下一部要下载的片从哪来"抽象成
# 一个统一接口，主循环只依赖该接口，不关心背后是 list 还是队列。
#
# 🔑 poll() 必须**非阻塞且三态**，这是整个设计的关键约束：
#   ("item", entry) 拿到一部片
#   ("wait", None)  暂时没货，但生产者还活着 —— 主循环应去推进在途任务，稍后再问
#   ("done", None)  生产者已收工且存货取尽 —— 首轮投递到此为止
#
# 为什么不能设计成"没货就阻塞等"：主事件循环若卡在 poll() 里，`wait(pending)` 就
# 停摆——已下载完的片无人提交转封装、成品堆在 temp、上传信号量不释放。这正是
# §10.21 B-6 踩过的坑（refetch 阻塞主循环数十分钟），不能再踩第二次。
class ListEntrySource:
    """把既有的固定 list 包装成来源接口：行为与改造前逐个索引取数完全一致。

    第二轮起的重试批次仍用它，故多轮语义零改动；首轮在 download_movies.py 单独
    运行时也用它（此时 results.jsonl 是取流跑完后的静态文件，一次读全最简单）。
    永不返回 "wait" —— list 的存货是确定的，不存在"暂时没货"。
    """

    def __init__(self, entries):
        self._entries = list(entries)
        self._next = 0

    def poll(self):
        if self._next < len(self._entries):
            entry = self._entries[self._next]
            self._next += 1
            return "item", entry
        return "done", None

    def __len__(self):
        """已知总量，仅供日志显示；队列来源无此方法，故打印处需容错。"""
        return len(self._entries)


# 原始 list 来源的固定引用。pipeline.py 会把模块级的 ListEntrySource 换成自己的
# 工厂，_run_pipeline 靠 `ListEntrySource is not _ListEntrySource` 判断当前是不是
# 流式模式——两者语义差别很大（如 results.jsonl 缺失时该不该退出）。
_ListEntrySource = ListEntrySource


def merge_next_batch(round_failed_retriable, revived):
    """合并两条重投路径，按 tmdbId 去重，重取后的新 entry 优先。

    两个桶可能含同一部片（既 retriable 又有过期直链，多源下是常态）：
      - 用旧 entry 会让那个 mp4 节点在整轮里继续是废的；
      - 投两份会让同一部片被并发下载两次，第二份在 process_one_entry 的
        processing_ids 检查里被判"重复条目"直接丢弃，白占一个下载槽位。
    revived 排在后面，dict 的值取最新者胜出，正好覆盖成新 urls 的版本。
    """
    return list({
        str(entry.get("tmdbId")): entry
        for entry in (round_failed_retriable + revived)
    }.values())


# 异步重取流钩子（§12 pipeline 模式安装）。默认 None = 走同步的 refetch_entries。
#
# 为什么要这个钩子：refetch_entries 由**主事件循环线程同步调用**，期间
# wait(pending) 完全停摆——已下载完的片无人提交转封装、成品堆在 temp、
# 上传信号量不释放（§10.21 B-6，只好加 AUTO_REFETCH_TIMEOUT 硬兜）。
# pipeline 模式下取流线程本就常驻，把请求丢给它即可，主循环一步都不阻塞。
#
# 约定（三个方法，都**不得阻塞**）：
#   dispatch(entries)  -> 投递重取请求，立即返回
#   collect()          -> 取走目前已完成的重取结果（entry 列表），没有就返回空
#   pending_count()    -> 还有多少条在途（已投递但没出结果）
#
# ⚠️ 重取结果**不能走主队列回流**：主队列有哨兵语义，取流主任务一结束就被
# 标记 done，之后推进去的东西再也取不出来（实测验证过），异步重取会形同虚设。
# 故走这条独立通道，由轮次循环在每轮末尾 collect 后并入 next_batch——
# 与同步路径的 revived 走完全相同的合并逻辑，语义一致。
async_refetch_hook = None

# 本轮投出异步重取后，等待结果回来的上限（秒）。
# 为什么必须等：dispatch 是异步的，投完立刻判 next_batch 会发现它是空的，
# 轮次循环就此 break —— 重取明明成功了，新链接却没有任何轮次去消费它
# （端到端实测复现过：过期片只尝试 1 次就收尾）。
# 为什么有上限：等待发生在主事件循环线程，不能无限等（那就退回同步的老问题）。
# 期间**每秒 collect 一次**，一旦有结果回来就立刻继续，不会白等满。
ASYNC_REFETCH_WAIT_SECONDS = max(
    0, int(_REFETCH_CFG.get("async_wait_seconds", 120))
)


def refetch_entries(entries, refetch_counts):
    """就地重新取流：调用取流侧的 provider 拿新 url，返回可重投的 entry 列表。

    与人工跑 `tmdb_ids_to_links.py --refetch-failed` 等价，区别只是发生在本进程内、
    轮次之间，不必等整次运行结束再由人接力。

    - 新 urls 会**追加写入 INPUT_JSONL**：与取流侧的落盘行为一致，这样即使本次
      运行中途被中断，下次启动也能按 fetched_at 择新直接用上新链接，重取不白做。
    - refetch_counts 记录每部片已被重取几次，达 AUTO_REFETCH_MAX_PER_MOVIE 即
      不再重取（新链接同样可能在排队期间再过期，但必须有上限防空转）。
    - 取流侧任何异常都不得逃逸：重取是"锦上添花"的捞回，失败了退回原状即可，
      绝不能让它崩掉整条下载流水线。
    """
    # 延迟导入：取流侧模块 import 时会读 config、要求代理凭证并建 Session，
    # 放在模块级会让"只想跑下载"的场景平白多出这些依赖与副作用。
    #
    # ⚠️ 必须连 SystemExit 一起捕获：tmdb_ids_to_links 在**模块级**用
    # `raise SystemExit` 做配置校验（缺 PROXY_USER/PROXY_PASSWORD、providers
    # 非法等）。SystemExit 继承 BaseException，`except Exception` 拦不住它。
    # 只配了 R2 凭证、没配代理凭证的机器（只跑下载，完全合理）一旦遇到直链过期，
    # 整条流水线会被这个 SystemExit 直接杀掉：pending 里的转封装/上传全丢、
    # processing_ids 不释放、temp 里的成品变孤儿。
    # 但**不能笼统捕获 BaseException**——KeyboardInterrupt 必须原样逃逸，
    # Ctrl+C 就该中止整个流程。
    try:
        import tmdb_ids_to_links as fetcher
    except (Exception, SystemExit) as exc:
        print(f"⚠️ 无法加载取流模块，跳过就地重取流: {exc}", flush=True)
        return []

    pending = []
    for entry in entries:
        tmdb_id = entry.get("tmdbId")
        if tmdb_id is None:
            continue
        if refetch_counts.get(str(tmdb_id), 0) >= AUTO_REFETCH_MAX_PER_MOVIE:
            continue
        pending.append(entry)

    if not pending:
        return []

    print(
        f"\n[自动重取流] {len(pending)} 部因直链过期失败，"
        f"就地重新取流（并发 {AUTO_REFETCH_WORKERS}）...",
        flush=True,
    )

    revived = []
    executor = ThreadPoolExecutor(max_workers=AUTO_REFETCH_WORKERS)
    try:
        future_to_entry = {
            executor.submit(fetcher.process_tmdb_id, entry["tmdbId"]): entry
            for entry in pending
        }
        # 带总超时地收集结果：as_completed 的 timeout 是**整体**预算，超时会抛
        # TimeoutError 中断迭代。此时已完成的部分照常收下，仍在跑的直接放弃——
        # 主循环不能为了捞回而僵在这里几十分钟（见 AUTO_REFETCH_TIMEOUT）。
        try:
            for future in as_completed(
                future_to_entry, timeout=AUTO_REFETCH_TIMEOUT
            ):
                entry = future_to_entry[future]
                tmdb_id = str(entry["tmdbId"])
                refetch_counts[tmdb_id] = refetch_counts.get(tmdb_id, 0) + 1
                try:
                    status, result = future.result()
                except (Exception, SystemExit) as exc:
                    # 同 import 处：process_tmdb_id 内部也可能触发模块级的
                    # SystemExit 式校验。单片重取失败绝不能带塌整批。
                    print(f"  [重取失败] {tmdb_id}: {exc}", flush=True)
                    continue
                if status != "ok" or not result or not result.get("urls"):
                    # dead（源站确认无此片）与 retry（瞬时错误耗尽）都不再重投本轮：
                    # 前者救不回来，后者留给下次运行——本轮已无新链接可用。
                    print(f"  [重取无果] {tmdb_id}: {status}", flush=True)
                    continue
                # 落盘新结果，与取流侧行为一致（追加写，下游按 fetched_at 择新）。
                write_log(INPUT_JSONL, result)
                # 用新 urls 覆盖 entry 的取流字段，其余元数据（title/year/runtime）
                # 保留：entry 可能带有 result 没有的历史字段，故逐键覆盖而非整体替换。
                new_entry = dict(entry)
                new_entry["urls"] = result["urls"]
                new_entry["fetched_at"] = result.get("fetched_at")
                revived.append(new_entry)
                print(
                    f"  [重取成功] {tmdb_id}: {len(result['urls'])} 个新节点",
                    flush=True,
                )
        except TimeoutError:
            unfinished = sum(1 for f in future_to_entry if not f.done())
            print(
                f"⚠️ 重取已达 {AUTO_REFETCH_TIMEOUT}s 上限，放弃仍在跑的 "
                f"{unfinished} 部（不是真淘汰，下次运行会再试），"
                f"主循环继续推进下载。",
                flush=True,
            )
    except BaseException:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    # 超时放弃的 future 不等它跑完：wait=False 让主循环立刻回到 wait(pending)，
    # 已提交的取流请求在后台线程里自然收尾。
    executor.shutdown(wait=False, cancel_futures=True)

    print(f"[自动重取流] 完成：{len(revived)}/{len(pending)} 部拿到新直链", flush=True)
    return revived


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
    # year 段只保留数字：防脏数据（含 '/'、空格等）拼出畸形 key / 多层意外目录。
    # 提取失败或缺失时兜底 unknown_year。
    year_digits = re.sub(r"\D", "", str(year)) if year not in (None, "") else ""
    year_seg = year_digits if year_digits else "unknown_year"
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


def _read_main_lock_pid():
    """读锁文件里的 PID；文件缺失/损坏时返回 0。"""
    try:
        with open(MAIN_LOCK_FILE, "r", encoding="utf-8") as file:
            return int((file.read() or "0").strip() or 0)
    except (OSError, ValueError):
        return 0


def acquire_main_lock():
    """抢下载单实例锁；已被别的活进程持有时抛 SystemExit。

    ⚠️ 必须做互斥检查（早期版本只是覆盖写 PID，形同虚设）：
    两个下载进程并发跑会读同一份 results.jsonl、写同一个 downloads/ 目录，
    把同一部片下两遍。三层去重拦不住这种情况——success.jsonl 与磁盘扫描
    都只在**启动时**读一次，processing_ids 更是进程内的集合，跨进程无效。
    结果是双倍带宽、双倍代理流量，还可能两个进程同时写同一个临时文件。

    pipeline.py 不另设锁，直接靠这把锁互斥：第二个 pipeline 会在
    downloader.main() 这一步被拒（它的取流线程也会被取流锁独立拒掉）。
    """
    if is_main_running():
        raise SystemExit(
            f"已有下载进程在运行（PID {_read_main_lock_pid()}）。\n"
            f"两个下载同时跑会把同一部片下两遍，白烧一倍带宽与代理流量。\n"
            f"若确认那个进程已死，删掉 {MAIN_LOCK_FILE} 再试。"
        )
    with open(MAIN_LOCK_FILE, "w", encoding="utf-8") as file:
        file.write(str(os.getpid()))


def release_main_lock():
    """主流程退出时清理锁文件（仅当锁属于本进程时才删）。"""
    if _read_main_lock_pid() == os.getpid():
        remove_file(MAIN_LOCK_FILE)


def is_main_running():
    """检测主流程是否正在运行：锁文件存在且其中 PID 仍存活。

    若锁文件存在但 PID 已死（上次异常退出留下的陈旧锁），清理后返回 False。
    """
    if not os.path.exists(MAIN_LOCK_FILE):
        return False
    pid = _read_main_lock_pid()
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
        if name.startswith(("sample_", "temp_", "mp4sample_")) and name.endswith(
            (".ts", ".mp4")
        ):
            remove_file(path)


def move_to_target_folder(temp_mp4, tmdb_id):
    """
    先在锁内选定落点目录并占位，再在锁外执行移动，防止高并发时目录容量超限。
    shutil.move 同时支持跨文件系统移动。

    用模块级游标 _current_folder_index 缓存"当前正在填的目录号"，从它起找而非
    每次从 START 全量重扫已满目录，把大批量下的 O(N²) listdir 降为 ~O(N)。

    移动本身放在锁外：base_dir 与 temp_dir 跨盘时 shutil.move 是 copy+delete，
    一部片要几十秒；若在锁内做，所有转封装 worker 会被这把全局锁完全串行化。
    锁内已用 0 字节占位文件把目标名额定死，故锁外移动不会导致目录超容量。
    """
    global _current_folder_index
    with folder_lock:
        index = _current_folder_index
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
                # 缓存住当前落点目录：下次从这里起找，跳过前面已满目录。
                _current_folder_index = index
                # 占位：立即以空文件占住该名额，这样并发的其它 worker 在锁内
                # 计数时就能看到它，不会把同一目录算成未满而超容量。空文件的
                # 大小为 0，扫描去重（scan_downloaded_mp4_ids 只认非空 mp4）
                # 也不会把它误判为已下载成品。
                with open(final_path, "wb"):
                    pass
                break
            index += 1

    print(f"  [{tmdb_id}] 正在移动到: {final_path}", flush=True)
    # 锁外移动：失败时清掉占位/半成品，交由上层按转封装失败处理。
    # 跨文件系统时 shutil.move 是 copy+del，若 copy 中途失败（目标盘写满/IO
    # 错误）会在 final_path 留下半成品 mp4：它不在 cleanup_paths、去重表也无
    # 登记，会成孤儿并白占目录名额。
    try:
        shutil.move(temp_mp4, final_path)
    except Exception:
        remove_file(final_path)
        raise
    return final_path


# ---------- M3U8 解析 ----------
def parse_master_playlist(master_url, retries=None, headers=None):
    """返回 [(resolution, media_playlist_url, declared_bandwidth_kbps), ...]。

    retries 为 None 时用默认强度 PLAYLIST_RETRY_MAX；方案C fallback 里对
    非末节点传更小的值，以便坏节点快速判定并换下一个备用节点。
    headers 为取流阶段记录的节点专属请求头，为空时用全局 HEADERS。
    """
    text = request_with_retry(
        "GET", master_url, as_text=True,
        retries=PLAYLIST_RETRY_MAX if retries is None else retries,
        backoff=PLAYLIST_RETRY_BACKOFF,
        backoff_max=PLAYLIST_RETRY_BACKOFF_MAX,
        headers=headers,
    )
    lines = [line.strip() for line in text.splitlines()]
    variants = []

    for index, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue

        # 属性紧跟在 "#EXT-X-STREAM-INF:" 之后，排在首位的属性前导是 ":" 而非 ","，
        # 两种分隔符都要接受，否则首位属性会漏解析。
        resolution_match = re.search(
            r"(?:^|[:,])RESOLUTION=(\d+x\d+)(?:,|$)", line, re.IGNORECASE
        )
        resolution = resolution_match.group(1) if resolution_match else "unknown"

        # BANDWIDTH 是 master 里声明的码率（bps），用于同分辨率下的初步排序，
        # 可以少下载几个采样片段。缺失时记为 0，后续仍以实测采样为准。
        bandwidth_match = re.search(
            r"(?:^|[:,])BANDWIDTH=(\d+)(?:,|$)", line, re.IGNORECASE
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


def parse_media_playlist(playlist_url, headers=None):
    """
    解析媒体播放列表，返回 (分片 URL 列表, 时长列表, init 段 URL)。

    同时支持 MPEG-TS 和 fMP4：fMP4 会带 #EXT-X-MAP 声明一个 init 段，
    该段必须写在所有媒体分片之前，否则产出的文件无法解码。TS 没有
    init 段，返回 None。
    headers 同 parse_master_playlist：节点专属请求头，为空时用全局 HEADERS。
    """
    text = request_with_retry(
        "GET", playlist_url, as_text=True,
        retries=PLAYLIST_RETRY_MAX,
        backoff=PLAYLIST_RETRY_BACKOFF,
        backoff_max=PLAYLIST_RETRY_BACKOFF_MAX,
        headers=headers,
    )
    lines = [line.strip() for line in text.splitlines()]
    init_url = None

    for line in lines:
        upper = line.upper()
        if upper.startswith("#EXT-X-KEY:") and "METHOD=NONE" not in upper:
            raise UnsupportedPlaylistError(
                "不支持的播放列表结构：含加密分片（#EXT-X-KEY）"
            )
        if upper.startswith("#EXT-X-BYTERANGE"):
            raise UnsupportedPlaylistError(
                "不支持的播放列表结构：使用 #EXT-X-BYTERANGE，不能按普通独立分片拼接"
            )
        if upper.startswith("#EXT-X-MAP"):
            # 形如：#EXT-X-MAP:URI="init.mp4"
            if "BYTERANGE" in upper:
                raise UnsupportedPlaylistError(
                    "不支持的播放列表结构：#EXT-X-MAP 带 BYTERANGE，不能按独立分片拼接"
                )
            uri_match = re.search(r'URI="([^"]+)"', line, re.IGNORECASE)
            if not uri_match:
                raise UnsupportedPlaylistError(
                    "不支持的播放列表结构：#EXT-X-MAP 缺少 URI 属性"
                )
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


# 分辨率探测不出来时的占位文案。模式 B 下分辨率不参与判定，探不到也照常下载
# （§12.15），此时它只是**成品元数据**，缺了不影响正确性但影响日后检索。
# 抽成常量是因为 finalize_one_entry 要拿它做相等比较来决定补探——散成字面量
# 的话，哪天改了措辞而漏改比较处，补探就会静默失效。
UNKNOWN_RESOLUTION = "未知分辨率"


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


def probe_codec(sample_path):
    """用 ffprobe 读取采样文件的视频编码名（小写，如 h264/hevc/av1）；失败返回 None。"""
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name",
        "-of",
        "default=nokey=1:noprint_wrappers=1",
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

    codec = result.stdout.strip().lower()
    return codec or None


def normalize_codec(codec):
    """把 ffprobe 的 codec_name 归一成 BITRATE_BASELINE 的键；无法识别返回 None。"""
    if not codec:
        return None
    codec = codec.lower()
    if codec in ("h264", "avc"):
        return "h264"
    if codec in ("hevc", "h265"):
        return "hevc"
    if codec in ("av1", "av01"):
        return "av1"
    if codec == "vp9":
        return "vp9"
    return None


def bitrate_threshold(height, codec):
    """码率门槛（kbps）。分辨率判定开关决定要不要按高度缩放。

    RESOLUTION_CHECK_ENABLED=True（默认）——单一码率曲线：
        门槛 = 基准[codec] × (height / 1080)² × LENIENCY
      - height 用每个流自己的实测高度，不是红线（高分辨率流按自身高度算，
        调低红线时也不会集体免检进伪高清）。
      - 码率需求 ∝ 像素数 ∝ 高度²，故用平方缩放而非线性。

    RESOLUTION_CHECK_ENABLED=False——与分辨率无关的绝对线：
        门槛 = 基准[codec] × LENIENCY
      此时 height 参数被完全忽略（保留在签名里是为了两条分支调用点一致）。
      去掉缩放是"只按码率判断"的必要条件：留着 (h/1080)² 等于让分辨率继续
      隐式参与——480p 门槛会被缩到约五分之一，低清片反而更好过关。

    两分支共同点：codec 无法识别/探测失败时回退 H.264 基准（最严）；
    都乘 LENIENCY，故"整体调松/调严"始终只改 leniency 一处。
    """
    key = normalize_codec(codec)
    baseline = BITRATE_BASELINE.get(key, BITRATE_BASELINE["h264"])
    if not RESOLUTION_CHECK_ENABLED:
        return baseline * LENIENCY
    scale = (height / _BITRATE_BASELINE_HEIGHT) ** 2
    return baseline * scale * LENIENCY


def meets_resolution_redline(height):
    """分辨率红线（带 LENIENCY 容差）：实测高度 ≥ 红线×宽松系数 即过关。

    容差用于救回准红线片（如红线 1080 时的 1072/900），避免差几像素被一刀切。

    分辨率判定关闭时恒真——把开关收敛在这一个函数里，三处红线关卡
    （mp4 声明预检 / mp4 实测复检 / m3u8 流层）与 master 候选过滤都会自动
    放行，无需在每个调用点各写一次 if，也就不会漏掉某一处。
    """
    if not RESOLUTION_CHECK_ENABLED:
        return True
    return height >= MIN_RESOLUTION_HEIGHT * LENIENCY


def bitrate_reject_message(resolution, codec_label, bitrate, min_bitrate):
    """码率不达标的淘汰文案。分辨率在两种模式下的地位不同，措辞也要跟着变。

    模式 A：分辨率是判定标准之一，写在前面合理。
    模式 B：分辨率**根本没参与判定**，若仍以"分辨率 854x480 流…"开头，日后翻
    failed.jsonl 会误以为是分辨率把片子卡掉的，从而对着一个不生效的
    min_resolution_height 反复调参。故降级为括号里的附带信息。

    两种措辞都保留 `码率未达到` 这个 marker —— `_PERMANENT_FAILURE_MARKERS`
    与 `_REJECT_REASON_RULES` 都靠它工作，历史 failed.jsonl 也按它归类。
    """
    if RESOLUTION_CHECK_ENABLED:
        return (
            f"分辨率 {resolution} 流（{codec_label}）"
            f"码率未达到门槛：{bitrate:.0f} kbps < {min_bitrate:.0f} kbps"
        )
    return (
        f"码率未达到门槛：{bitrate:.0f} kbps < {min_bitrate:.0f} kbps"
        f"（{codec_label}，实测 {resolution}）"
    )


# ---------- 分片下载 ----------
def validate_segment_content(content, url):
    if not content:
        raise RuntimeError(f"服务器返回空分片: {url}")

    prefix = content[:256].lstrip().lower()
    if prefix.startswith((b"<!doctype html", b"<html", b"#extm3u")):
        raise RuntimeError(f"服务器返回的不是视频分片: {url}")


def download_single_segment(url, index, retry_max, delay, headers=None,
                            abort_event=None):
    """下载单个 HLS 分片，失败按指数退避重试 retry_max 次。

    确定性失败（401/403/404/410/416，或源站返回 HTML/m3u8 而非视频数据）立即
    上抛、不再退避重试：这类结果重下必然复现，白等十几分钟只会占死下载窗口、
    延误换下一个取流节点。与 mp4 直链块层（_download_mp4_chunk）语义对齐。

    abort_event（可选）：本部片的放弃信号，与全局 `interrupted` 一起决定是否
    提前收手。⚠️ 退避必须用 wait() 而不是 sleep()——sleep 期间信号叫不醒它，
    单分片最坏要干等 20×60s，Ctrl+C 之后进程还会挂着几十个线程刷日志
    （服务器实跑实测，见 `interrupted` 的注释）。
    """
    last_error = None
    for attempt in range(1, retry_max + 1):
        # 开跑之前先看一眼：中断后连第一次请求都不该再发。
        if interrupted.is_set() or (abort_event is not None
                                    and abort_event.is_set()):
            raise RuntimeError(f"分片 {index + 1} 已取消")
        try:
            # 外层已经负责精确重试次数，因此这里关闭额外应用层重试。
            content = request_with_retry(
                "GET", url, retries=1, as_text=False, timeout=60,
                headers=headers,
            )
            validate_segment_content(content, url)
            return content
        except Exception as exc:
            last_error = exc
            message = str(exc)
            if (
                attempt == retry_max
                or is_permanent_http_failure(exc)
                # 源站返回 HTML/m3u8：通常是无效源，重试无意义。
                or "服务器返回的不是视频分片" in message
            ):
                break

            wait = min(delay * (2 ** (attempt - 1)), 60)
            wait += random.uniform(0, min(1.0, wait * 0.2))
            print(
                f"    分片 {index + 1} 下载失败 "
                f"({attempt}/{retry_max}): {exc}; {wait:.1f}s 后重试"
            )
            # 可被打断的退避：任一信号置位就立刻醒来收手。
            if interrupted.wait(wait):
                raise RuntimeError(f"分片 {index + 1} 已取消") from exc
            if abort_event is not None and abort_event.is_set():
                raise RuntimeError(f"分片 {index + 1} 已取消") from exc

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
    force_init=False,
    headers=None,
    retry_max=None,
):
    """
    并发下载、按索引顺序写入分片。

    使用滑动窗口：始终保持 concurrency 个分片在途，任一分片完成就立刻补进
    下一个，避免"整批等最慢分片"的木桶效应。写盘仍严格按索引顺序进行。

    关键点：输出文件在整个下载过程中只打开一次，不能每批用 wb 重开；
    否则前面已经写入的批次会被清空。

    单个分片耗尽重试次数后会记录并跳过，不中止整部影片。返回值为：
    (成功写入的总字节数, 失败分片索引列表, init 段字节数)。
    total_bytes 含 init 段字节；单独返回 init_bytes 便于调用方在按时长算码率时
    扣除 init（init 段无时长，若不扣会使码率虚高，fMP4 采样场景尤甚）。

    init_url 用于 fMP4：该 init 段携带 moov（编解码参数），必须位于所有媒体
    分片之前。默认只在新建文件（start_idx == 0，wb 模式）时写入一次。
    force_init=True 时，即使 start_idx>0（如中间采样单独成文件）也强制写一次
    init 段——否则 fMP4 中间采样片缺 moov，ffprobe 无法探测分辨率/编码。
    headers 为取流阶段记录的节点专属请求头，透传给每个分片请求；为空时用全局
    HEADERS（与旧版行为一致）。

    retry_max 为单分片重试上限，默认 SEG_RETRY_MAX（正片下载用）。
    ⚠️ **采样阶段必须传更小的值**（SAMPLE_SEG_RETRY_MAX）：采样只是为了测个
    码率决定要不要下，探不到就该早点换下一条流/节点。用正片那套 20 次会把
    "判断要不要下"的成本放大到和"真下一部片"一个量级——服务器实跑时源站持续吐
    400/502，10 部片在采样里空转了 90 分钟仍无结论（见 SAMPLE_SEG_RETRY_MAX）。
    """
    if end_idx is None:
        end_idx = len(segment_urls)

    if retry_max is None:
        retry_max = SEG_RETRY_MAX

    indices = list(range(start_idx, min(end_idx, len(segment_urls))))
    if not indices:
        return 0, [], 0

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    mode = "ab" if os.path.exists(output_path) and start_idx > 0 else "wb"
    total_bytes = 0
    init_bytes = 0
    failed_indices = []

    # 只打开一次：这是修复 PPS/SPS 丢失问题的核心。
    with open(output_path, mode) as output_file:
        # fMP4 的 init 段携带 moov（编解码参数），必须位于所有媒体分片之前。
        # 正片拼接时只在 wb（start_idx==0）写一次；中间采样单独成文件时用
        # force_init 强制补写，保证该采样文件自身可被 ffprobe 探测。
        if init_url and (mode == "wb" or force_init):
            init_data = download_single_segment(
                init_url, -1, retry_max, SEG_RETRY_DELAY, headers
            )
            output_file.write(init_data)
            init_bytes = len(init_data)
            total_bytes += init_bytes

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
                    retry_max,
                    SEG_RETRY_DELAY,
                    headers,
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

    return total_bytes, sorted(failed_indices), init_bytes


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
            timeout=CONVERT_TIMEOUT,
        )
        if result.stderr.strip():
            print(f"  FFmpeg 警告:\n{result.stderr.strip()}")
        return True
    except subprocess.TimeoutExpired:
        # 坏 TS/fMP4 可能让 ffmpeg 无限阻塞；超时判失败(上层可重试)，
        # 避免单片卡死占用转封装线程、拖垮整个转封装池。
        print(f"  FFmpeg 转换超时（>{CONVERT_TIMEOUT}s），已放弃: {ts_path}")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"  FFmpeg 转换失败:\n{exc.stderr}")
        return False
    except FileNotFoundError:
        print("  未找到 ffmpeg，请安装并加入 PATH")
        return False


# ---------- 取流条目归一化 / mp4 直链下载 ----------
# 上游 tmdb_ids_to_links.py 写入的 urls 元素有两种形态：
#   - 纯 str：历史 results.jsonl（旧 vidup m3u8）；
#   - dict：{"url","provider","type":"m3u8"|"mp4","headers","quality","size"}。
# 这里统一归一成 dict，下游按 type 分支；非法条目返回 None（调用方跳过）。
def _positive_or_none(value):
    """把 quality/size 归一为正整数，非正数与非法值一律 None。

    results.jsonl 是跨进程的不可信输入。负数/0 会造成实质损害：
      - quality<=0 在**模式 A** 下走 meets_resolution_redline 会被判定性淘汰，
        白丢一个可用节点（模式 B 该关放行，但归一化仍要做——不能依赖当前
        默认模式，开关随时可能被切回 true）；
      - size<=0 作为 _mp4_probe_total_size 的 declared_size 兜底会算出空/负区间。
    归为 None 即"未声明"，交由下游实测，是安全的退化方向。
    """
    parsed = parse_int(value)
    return parsed if parsed and parsed > 0 else None


def _normalize_url_entry(item):
    if isinstance(item, str):
        url = item.strip()
        if not url:
            return None
        return {
            "url": url, "provider": "vidup", "type": "m3u8",
            "headers": {}, "quality": None, "size": None,
        }
    if not isinstance(item, dict):
        return None
    url = item.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    type_ = str(item.get("type") or "m3u8").lower()
    if type_ not in ("m3u8", "mp4"):
        return None
    headers = item.get("headers")
    if not isinstance(headers, dict):
        headers = {}
    return {
        "url": url.strip(),
        "provider": str(item.get("provider") or "unknown"),
        "type": type_,
        "headers": {str(k): str(v) for k, v in headers.items() if v is not None},
        "quality": _positive_or_none(item.get("quality")),
        "size": _positive_or_none(item.get("size")),
    }


def _mp4_request_headers(node_headers):
    """mp4 直链请求头：以条目自带 headers 为准，去掉默认的 vidup Referer 与 XHR 头。

    vidlink CDN 带任何 Referer 都会 429，浏览器 UA 无 Referer 会 428，
    只有取流阶段验证过的 headers（okhttp UA、无 Referer）能拿到 206。
    值为 None 的键会被 requests 在合并 Session 头时删除。
    """
    headers = {"Referer": None, "X-Requested-With": None}
    headers.update(node_headers or {})
    return headers


def _probe_duration(path):
    """ffprobe 读容器时长（秒）；失败返回 None。"""
    command = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=nokey=1:noprint_wrappers=1", path,
    ]
    try:
        result = subprocess.run(
            command, check=True, capture_output=True, text=True,
            errors="replace", timeout=120,
        )
        return float(result.stdout.strip())
    except (subprocess.SubprocessError, OSError, ValueError):
        return None


def _mp4_probe_total_size(url, headers, declared_size):
    """用 Range: bytes=0-0 探测直链总长并验证 Range 支持。

    403/410 视为签名直链过期，抛带 _NEEDS_REFETCH_MARKER 的错误（需上游重新
    取流）。返回 (total_size, range_ok)：range_ok 仅由状态码是否为 206 决定；
    206 但 Content-Range 总长为 * 时回退到条目声明的 size。
    """
    session = get_session()
    request_headers = dict(HEADERS)
    request_headers.update(headers)
    request_headers["Range"] = "bytes=0-0"
    range_ok = False
    try:
        with session.request(
            "GET", url, headers=request_headers, timeout=30, stream=True
        ) as response:
            status = response.status_code
            if status in (403, 410):
                raise RuntimeError(
                    f"直链已失效（HTTP {status}），{_NEEDS_REFETCH_MARKER}: {url}"
                )
            if status in (429, 503):
                record_block_status(status)
            response.raise_for_status()
            range_ok = status == 206
            if range_ok:
                content_range = response.headers.get("Content-Range", "")
                match = re.search(r"/(\d+)\s*$", content_range)
                if match:
                    return int(match.group(1)), True
            length = parse_int(response.headers.get("Content-Length"))
            if status == 200 and length:
                return length, False
    except (requests.RequestException, ConnectionError, TimeoutError) as exc:
        raise RuntimeError(f"直链探测失败: {url}; {exc}") from exc
    if declared_size:
        return declared_size, range_ok
    raise RuntimeError(f"直链探测失败：无法确定文件总长: {url}")


def _download_mp4_chunk(url, headers, start, end, index, abort_event=None):
    """下载 [start, end] 闭区间字节块；服务端不按 Range 响应（200）时视为失败。

    abort_event 置位表示"整片已判失败、无需再抢救本块"：此时不再进入下一次
    退避重试，立即抛错让工作线程尽快归还。否则同片其它块失败后，在跑的块仍会
    跑满 SEG_RETRY_MAX 次退避（最长可达十几分钟），占死下载槽位、拖慢换节点，
    直接损害整体下载成功率。
    """
    last_error = None
    request_headers = dict(headers)
    request_headers["Range"] = f"bytes={start}-{end}"
    expected = end - start + 1
    for attempt in range(1, SEG_RETRY_MAX + 1):
        if abort_event is not None and abort_event.is_set():
            raise RuntimeError(f"直链块 {index + 1} 已随整片失败取消")
        try:
            session = get_session()
            merged = dict(HEADERS)
            merged.update(request_headers)
            with session.request(
                "GET", url, headers=merged, timeout=60
            ) as response:
                status = response.status_code
                if status in (403, 410):
                    raise RuntimeError(
                        f"直链已失效（HTTP {status}），{_NEEDS_REFETCH_MARKER}: {url}"
                    )
                if status in (404, 416):
                    # 404 直链不存在 / 416 Range 越界（探测总长与实际不符）：同一 url 重试无意义
                    raise RuntimeError(f"直链块不可用（HTTP {status}）: {url}")
                if status in (429, 503):
                    record_block_status(status)
                response.raise_for_status()
                if status != 206:
                    raise RuntimeError(f"服务器未按 Range 响应（HTTP {status}）")
                content = response.content
            if len(content) != expected:
                raise RuntimeError(
                    f"块长度不符：期望 {expected} 实得 {len(content)}"
                )
            if index == 0:
                # 首块校验：源返回 HTML/m3u8 而非视频数据时尽早判无效源。
                validate_segment_content(content, url)
            return content
        except Exception as exc:
            last_error = exc
            message = str(exc)
            if (
                any(marker in message for marker in _MP4_CHUNK_NO_RETRY_MARKERS)
                # 兜底：上面显式判过的 403/410/404/416 之外，若 raise_for_status
                # 抛出其它确定性状态码（如 401），同样不必退避重试。
                or is_permanent_http_failure(exc)
                or attempt == SEG_RETRY_MAX
                or (abort_event is not None and abort_event.is_set())
            ):
                break
            wait = min(SEG_RETRY_DELAY * (2 ** (attempt - 1)), 60)
            wait += random.uniform(0, min(1.0, wait * 0.2))
            print(
                f"    直链块 {index + 1} 下载失败 "
                f"({attempt}/{SEG_RETRY_MAX}): {exc}; {wait:.1f}s 后重试"
            )
            # 用 Event.wait 代替 sleep：整片一旦判失败可立刻醒来，不必空等完退避。
            if abort_event is not None:
                if abort_event.wait(wait):
                    break
            else:
                time.sleep(wait)
    raise RuntimeError(
        f"直链块 {index + 1} 重试后仍失败: {last_error}"
    ) from last_error


def _mp4_probe_quality_by_sample(
    url, headers, total_size, sample_path, label, runtime_minutes
):
    """整片下载前先取头部样本验画质，避免整部影片（GB 级）白下白丢。

    原理：mp4 的分辨率/编码/时长都写在 moov box 里，而整片码率
    = total_size×8/duration —— total_size 已由 Range 探测拿到。故只要样本能被
    ffprobe 解析出这三项，得出的判定结果与下完整片后再判**完全一致**，
    却只花几 MB 流量。

    仅当 moov 在文件头部（faststart）时样本可解析；moov 在尾部的文件 ffprobe
    会失败，此时返回 None 表示"无法预判"，由调用方放行走整片下载后再验——
    宁可多下也不误杀，与"尽可能提高成功率"一致。

    时长取值顺序刻意把上游 runtime_minutes 排在样本探测之前：样本是被截断的
    文件，ffprobe 从残缺 moov 里读出的可能是"样本自身时长"而非整片时长，一旦
    如此，bitrate = total_size×8/duration 会虚高几十倍，让本该淘汰的低码率片
    通过预检、预检形同虚设。runtime_minutes 来自 TMDB 元数据，是可信的整片
    时长。样本探测仅作为 runtime_minutes 缺失时的回退，且必须通过合理性校验。

    返回 (resolution_str, height, bitrate_kbps, codec) 或 None。
    """
    sample_end = min(MP4_SAMPLE_SIZE, total_size) - 1
    try:
        content = _download_mp4_chunk(url, headers, 0, sample_end, 0)
    except Exception as exc:
        # 采样块自身失败（含确定性 4xx / 直链失效）直接上抛：整片下载必然同样失败，
        # 没必要再浪费一次整片尝试。
        raise RuntimeError(f"直链采样失败: {exc}") from exc

    try:
        with open(sample_path, "wb") as fh:
            fh.write(content)
        actual_size = probe_resolution(sample_path)
        if not actual_size and RESOLUTION_CHECK_ENABLED:
            # 模式 A：分辨率是判定标准之一，探不到就无法预判，放行整片后再验。
            print(
                f"  [{label}] 直链头部样本无法探测（moov 可能不在文件头），"
                f"跳过预检、下载整片后再验",
                flush=True,
            )
            return None
        # 模式 B（默认，只看码率）：分辨率不参与判定，探不到也**不该放弃预检**。
        # 码率 = total_size×8/duration，与分辨率毫无关系；在这里返回 None 会让
        # 本可 8MB 就淘汰的低码率片白下整片（GB 级），把预检的全部收益架空。
        # height 传 0 —— 模式 B 的 bitrate_threshold 本就忽略该参数。
        if actual_size:
            height = actual_size[1]
            resolution = f"{actual_size[0]}x{actual_size[1]}"
        else:
            height = 0
            resolution = UNKNOWN_RESOLUTION

        duration = None
        minutes = parse_int(runtime_minutes)
        if minutes and minutes > 0:
            duration = minutes * 60
        else:
            # 无上游时长时才退回样本探测，并做合理性校验：样本只占整片的
            # sample_ratio，若 ffprobe 返回的是样本自身时长，该值会与
            # "整片时长×sample_ratio" 同量级而远小于正常影片时长。这里用
            # 「样本时长必须显著大于按字节比例折算出的样本时长」来识别，
            # 识别为不可信就返回 None 放行整片下载，绝不用可疑值去淘汰片子。
            probed_duration = _probe_duration(sample_path)
            sample_ratio = len(content) / total_size if total_size else 1.0
            if probed_duration and probed_duration > 0:
                if sample_ratio < 0.5 and probed_duration < MP4_MIN_TRUSTED_DURATION:
                    print(
                        f"  [{label}] 样本时长 {probed_duration:.0f}s 疑为样本自身"
                        f"时长（样本仅占全片 {sample_ratio:.1%}），预检不可信，"
                        f"下载整片后再验",
                        flush=True,
                    )
                    return None
                duration = probed_duration
        if not duration:
            return None

        bitrate = total_size * 8 / duration / 1000
        codec = probe_codec(sample_path)
        print(
            f"  [{label}] 直链预检 {resolution} 编码 {codec or 'unknown'}，"
            f"码率 {bitrate:.0f} kbps（样本 {len(content)} 字节）",
            flush=True,
        )
        return resolution, height, bitrate, codec
    finally:
        remove_file(sample_path)


def _mp4_preflight(node, label, runtime_minutes=None):
    """对单个 mp4 节点做「探总长 + 头部样本测码率」，供跨节点择优排序用。

    返回 dict：
      {"node":…, "total_size":…, "probed": (resolution, height, bitrate, codec)|None,
       "bitrate": float|None, "estimated": bool}
    失败（探不到总长 / 不支持 Range / 采样块请求失败）返回 None —— 该节点这一轮
    多半是坏的，交给调用方排到最后，但**不判死**：仍会参与下载尝试。

    ⚠️ 本函数只负责"测"，不做任何判死。画质门槛一律留给 `_download_mp4_direct`
    统一判，避免同一套判定散成两处、日后改门槛时漏改一边。
    """
    url = node["url"]
    headers = _mp4_request_headers(node.get("headers"))
    try:
        total_size, range_ok = _mp4_probe_total_size(url, headers, node.get("size"))
    except Exception:
        return None
    if not range_ok or total_size <= 0:
        return None

    probed = None
    if total_size > MP4_SAMPLE_SIZE * 2:
        sample_path = os.path.join(
            TEMP_DIR,
            f"mp4sample_{safe_file_token(label)}_"
            f"{hashlib.sha1(url.encode('utf-8')).hexdigest()[:8]}.mp4",
        )
        try:
            probed = _mp4_probe_quality_by_sample(
                url, headers, total_size, sample_path, label, runtime_minutes
            )
        except Exception:
            # 采样块拿不到：整片下载多半也会失败，但仍留着这个节点当兜底。
            probed = None

    if probed is not None:
        return {
            "node": node, "total_size": total_size, "probed": probed,
            "bitrate": probed[2], "estimated": False,
        }

    # 降级 2：预检探不出码率（moov 不在头部 / 时长不可信 / 文件太小不值得预检），
    # 用「总长 ÷ 上游时长」估一个。不花任何网络请求。
    # ⚠️ 只用于**排序**，绝不用于判死——它没经过 ffprobe 校验，源站声明的 size
    # 也未必可信。真正的判死一律在 _download_mp4_direct 里按实测码率做。
    minutes = parse_int(runtime_minutes)
    estimated = (
        total_size * 8 / (minutes * 60) / 1000
        if minutes and minutes > 0 else None
    )
    return {
        "node": node, "total_size": total_size, "probed": None,
        "bitrate": estimated, "estimated": True,
    }


def _rank_mp4_nodes(nodes, label, runtime_minutes=None):
    """把多个 mp4 节点按「实测码率优先」排序，返回 [(node, preflight_or_None), …]。

    这解决的问题：mp4 是"试到第一个成功就 break"，不像 m3u8 会在候选流之间择优。
    取流侧又是按**声明分辨率**降序给的节点，于是「1080p/1700kbps 刚过线」会直接
    胜出，而「480p/8000kbps」根本不会被看到 —— 同一份画质标准在两条路径上力度
    不一致。分辨率移出判定标准后这个问题更突出。

    三级降级（顺序即优先级）：
      1. 预检拿到**实测**码率 → 按码率降序，最可信；
      2. 预检拿不到 → 用 size×8/时长 **估算**码率降序（零网络开销，仅供排序）；
      3. 连估算都做不了（无 size 或无时长）→ 保持上游给的原始顺序（按声明
         分辨率降序），排在最后。

    🔑 两条底线：
      - 排序**不淘汰任何节点**。预检失败不代表节点坏，只是探不出码率；
      - 全部节点都预检不出时，本函数退化为"原样返回"，行为等同改动前。
    """
    if len(nodes) < 2:
        # 单节点无所谓择优，别白花一次预检（它会多发 1~2 个网络请求）。
        return [(node, None) for node in nodes]

    ranked = []
    for index, node in enumerate(nodes):
        info = _mp4_preflight(node, label, runtime_minutes)
        bitrate = info["bitrate"] if info else None
        # 排序键：① 有码率的排前面；② 实测的排在估算的前面（同为有码率时）；
        # ③ 码率降序；④ 同码率按上游原始顺序稳定排列。
        ranked.append((
            (
                bitrate is None,
                bool(info["estimated"]) if info else True,
                -(bitrate or 0),
                index,
            ),
            node,
            info,
        ))
    ranked.sort(key=lambda item: item[0])

    measured = [r for r in ranked if r[2] and not r[2]["estimated"]]
    if measured:
        best = measured[0]
        print(
            f"  [{label}] mp4 择优：{len(nodes)} 个节点，"
            f"选中实测 {best[2]['probed'][2]:.0f} kbps "
            f"({best[1].get('provider')}/{best[2]['probed'][0]})",
            flush=True,
        )
    return [(node, info) for _key, node, info in ranked]


def _download_mp4_direct(node, output_path, label, runtime_minutes=None,
                         preflight=None):
    """mp4 直链：Range 分块并发下载到 output_path，并做画质筛选。

    流程：
      1. 【仅模式 A】quality 声明存在时先过分辨率红线（不达标直接确定性淘汰，
         省流量）。模式 B 下 meets_resolution_redline 恒真，本关整关放行——
         声明 480p 也可能是高码率清晰片，该由第 2/3 步的码率关来判；
      2. Range 探测总长 → 头部样本预检码率（不达标当场淘汰，省下整片流量）；
      3. 按 MP4_CHUNK_SIZE 分块、MP4_CONCURRENCY 并发下载，严格按块序落盘
         （复用分片下载的滑动窗口思路）；不做断点续传；
      4. 下载完成后 ffprobe 实测分辨率/编码/时长，码率 = 字节×8/时长 对齐
         bitrate_threshold（与 m3u8 路径同一套门槛）。
    返回 (resolution_str, bitrate_kbps)。任何块失败即整体失败（直链无“缺片豁免”）。

    preflight：跨节点择优阶段已经跑过的预检结果（见 `_mp4_preflight`），形如
    {"total_size":…, "probed":…}。传入即复用，避免同一节点被探两次总长/两次
    头部样本——那是纯粹的重复网络开销。为 None 时按老路自己现探。
    """
    url = node["url"]
    headers = _mp4_request_headers(node.get("headers"))
    quality = node.get("quality")
    if quality and not meets_resolution_redline(quality):
        raise QualityRejectedError(
            f"声明分辨率 {quality}p 低于红线 {MIN_RESOLUTION_HEIGHT}"
            f"（容差 {LENIENCY:.2f}），跳过"
        )

    total_size, range_ok = (
        (preflight["total_size"], True) if preflight is not None
        else _mp4_probe_total_size(url, headers, node.get("size"))
    )
    if not range_ok:
        raise RuntimeError(f"直链不支持 Range 分块下载: {url}")
    if total_size <= 0:
        raise RuntimeError(f"直链总长异常({total_size}): {url}")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    # 画质预检：先取头部样本判分辨率+码率，不达标立刻淘汰，省下整片（GB 级）
    # 的下载流量与下载槽位。判定口径与整片下完后的复检完全一致（同一套红线与
    # bitrate_threshold），所以预检通过的片复检必然也通过，不会重复淘汰。
    #
    # 仅对「显著大于样本」的文件预检：total_size <= MP4_SAMPLE_SIZE 时采样等于
    # 把整片下一遍，之后正片再下一遍 —— 双倍流量却零收益，不如直接走正片下载
    # 后的复检。阈值取样本的 2 倍，保证预检省下的流量至少是样本本身的一倍。
    if preflight is not None:
        # 择优阶段已经探过：直接复用，不再发第二次请求。
        probed = preflight["probed"]
    elif total_size > MP4_SAMPLE_SIZE * 2:
        # 样本文件名带 url 摘要：同一片的多个 mp4 节点虽是串行尝试，但摘要能
        # 保证任何调用姿势下都不会两个节点写同一个临时文件。
        sample_path = os.path.join(
            os.path.dirname(output_path) or ".",
            f"mp4sample_{safe_file_token(label)}_"
            f"{hashlib.sha1(url.encode('utf-8')).hexdigest()[:8]}.mp4",
        )
        probed = _mp4_probe_quality_by_sample(
            url, headers, total_size, sample_path, label, runtime_minutes
        )
    else:
        probed = None

    if probed is not None:
        pre_resolution, pre_height, pre_bitrate, pre_codec = probed
        if not meets_resolution_redline(pre_height):
            raise QualityRejectedError(
                f"分辨率 {pre_resolution} 低于红线 {MIN_RESOLUTION_HEIGHT}"
                f"（容差 {LENIENCY:.2f}），跳过"
            )
        pre_min_bitrate = bitrate_threshold(pre_height, pre_codec)
        if pre_bitrate < pre_min_bitrate:
            raise QualityRejectedError(
                bitrate_reject_message(
                    pre_resolution, pre_codec or "unknown",
                    pre_bitrate, pre_min_bitrate,
                )
            )

    # 不做断点续传：节点失败时上层会删掉残留 ts，启动时也会清理 temp_*，
    # 残留文件无法证明与本次直链一致，存在即视为脏数据重下。
    remove_file(output_path)

    chunks = [
        (start, min(start + MP4_CHUNK_SIZE, total_size) - 1)
        for start in range(0, total_size, MP4_CHUNK_SIZE)
    ]
    print(
        f"  [{label}] 直链分块下载 {total_size} 字节，"
        f"{len(chunks)} 块 × {MP4_CHUNK_SIZE // (1024 * 1024)}MB，"
        f"并发 {MP4_CONCURRENCY}"
    )
    with open(output_path, "wb") as output_file:
        # 整片失败信号：任一块判失败即置位，让在跑的块立刻放弃退避重试并归还
        # 线程，避免 ThreadPoolExecutor.__exit__ 的 shutdown(wait=True) 干等。
        abort_event = threading.Event()
        with ThreadPoolExecutor(max_workers=MP4_CONCURRENCY) as executor:
            next_submit = 0
            write_cursor = 0
            done_buffer = {}
            future_to_index = {}
            max_buffered = MP4_CONCURRENCY * 2
            failure = None

            def refill():
                nonlocal next_submit
                while (
                    failure is None
                    and len(future_to_index) < MP4_CONCURRENCY
                    and next_submit < len(chunks)
                    and (len(done_buffer) < max_buffered or not future_to_index)
                ):
                    start, end = chunks[next_submit]
                    future = executor.submit(
                        _download_mp4_chunk, url, headers, start, end,
                        next_submit, abort_event,
                    )
                    future_to_index[future] = next_submit
                    next_submit += 1

            refill()
            try:
                while future_to_index:
                    done, _ = wait(future_to_index.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        index = future_to_index.pop(future)
                        try:
                            done_buffer[index] = future.result()
                        except Exception as exc:
                            if failure is None:
                                failure = exc
                    while write_cursor < len(chunks) and write_cursor in done_buffer:
                        output_file.write(done_buffer.pop(write_cursor))
                        write_cursor += 1
                    if failure is not None:
                        # 置位中止信号 + 取消未启动块：在跑的块会在下次重试点立即
                        # 退出，整片失败能在秒级归还下载槽位并换下一个取流节点。
                        # 残留文件由上层节点循环清理。
                        abort_event.set()
                        for future in list(future_to_index):
                            future.cancel()
                        break
                    refill()
            except BaseException:
                # 写盘失败（磁盘满/IO 错误）或 Ctrl+C 时，异常会直接穿出本块。
                # 若不在这里置位中止信号，with ThreadPoolExecutor 退出时的
                # shutdown(wait=True) 会干等所有在途块跑满退避（每块最坏
                # SEG_RETRY_MAX 次 × 数十秒 ≈ 十几分钟）才肯放行。
                abort_event.set()
                for future in list(future_to_index):
                    future.cancel()
                raise
        output_file.flush()

    if failure is not None:
        raise failure
    written = os.path.getsize(output_path)
    if written != total_size:
        raise RuntimeError(f"直链下载长度不符：{written} != {total_size}")

    actual_size = probe_resolution(output_path)
    if not actual_size and RESOLUTION_CHECK_ENABLED:
        # 模式 A：分辨率是判定标准，探不到就无从判断，只能判失败重下。
        raise RuntimeError("采样探测分辨率失败（可重试）")
    # 模式 B（默认，只看码率）：整片**已经下完了**，此时仅因探不到分辨率就丢弃
    # 它是纯粹的误杀——分辨率根本不参与判定，height 只会被 bitrate_threshold
    # 忽略。继续用码率判定即可；分辨率降级为成品元数据，未知就如实标注。
    if actual_size:
        resolution = f"{actual_size[0]}x{actual_size[1]}"
        height = actual_size[1]
    else:
        resolution = UNKNOWN_RESOLUTION
        height = 0
    print(f"  [{label}] 直链实测分辨率: {resolution}")
    if not meets_resolution_redline(height):
        raise QualityRejectedError(
            f"分辨率 {resolution} 低于红线 {MIN_RESOLUTION_HEIGHT}"
            f"（容差 {LENIENCY:.2f}），跳过"
        )

    duration = _probe_duration(output_path)
    if not duration or duration <= 0:
        minutes = parse_int(runtime_minutes)
        duration = minutes * 60 if minutes else None
    if not duration:
        raise RuntimeError("采样数据或采样时长异常：无法确定直链时长")
    bitrate = total_size * 8 / duration / 1000
    codec = probe_codec(output_path)
    min_bitrate = bitrate_threshold(height, codec)
    codec_label = codec or "unknown"
    print(
        f"  [{label}] 直链 {resolution} 编码 {codec_label}，"
        f"码率 {bitrate:.0f} kbps，门槛 {min_bitrate:.0f} kbps"
    )
    if bitrate < min_bitrate:
        raise QualityRejectedError(
            bitrate_reject_message(resolution, codec_label, bitrate, min_bitrate)
        )
    return resolution, bitrate


# ---------- 单条影片处理 ----------
def process_one_entry(entry, processed_ids):
    tmdb_id = entry.get("tmdbId")
    normalized_id = normalize_tmdb_id(tmdb_id)
    # 旧 results.jsonl 里 title 可能是 null（键存在值为 None，此时 .get 的默认值
    # 不生效），会一路透传进 success.jsonl 并在日志里打成 "None"。统一兜底成 str。
    title = entry.get("title") or ""
    # urls 元素兼容 str（历史 vidup m3u8）与 dict（多源：m3u8/mp4 + headers）。
    urls = [
        node for node in (
            _normalize_url_entry(item) for item in (entry.get("urls") or [])
        ) if node
    ]
    year = entry.get("year")
    runtime_minutes = entry.get("runtime_minutes")

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

    handed_off_to_conversion = False
    cleanup_paths = set()
    final_ts = os.path.join(TEMP_DIR, f"temp_{safe_file_token(tmdb_id)}.ts")
    temp_mp4 = os.path.join(TEMP_DIR, f"temp_{safe_file_token(tmdb_id)}.mp4")
    cleanup_paths.update((final_ts, temp_mp4))

    def _attempt_download(node, is_last_node, preflight=None):
        """对单个取流节点尝试完整下载，成功返回 conversion_job，失败抛异常。

        node 为归一化后的 dict；按 type 分支：m3u8 走 playlist 解析 + 采样 +
        分片拼接；mp4 走 Range 分块直链下载（画质筛选在 _download_mp4_direct 内）。
        is_last_node=False（还有备用节点）时，master playlist 解析用短重试
        PLAYLIST_RETRY_FALLBACK，坏节点快速判定即换下一个；末节点/单节点用
        默认 PLAYLIST_RETRY_MAX 死磕，不放过最后的机会。

        preflight 仅对 mp4 有意义：跨节点择优时已探过的总长/头部样本，透传下去
        复用，避免重复发请求。
        """
        url = node["url"]
        # 取流阶段记录的节点专属请求头（如特定 Referer/UA）：m3u8 与 mp4 两条
        # 分支都必须携带，否则某些源站会 403/428 直接判死，白白损失可用源。
        node_headers = node.get("headers") or None
        if node["type"] == "mp4":
            # 注：直链预检的头部样本文件由 _mp4_probe_quality_by_sample 自身的
            # finally 删除，文件名含 url 摘要故此处无法预知；进程被杀等极端情况
            # 由启动时的 clean_temp_directory（前缀 mp4sample_）兜底清理。
            resolution, bitrate = _download_mp4_direct(
                node, final_ts, tmdb_id, runtime_minutes, preflight
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
                "bitrate_kbps": round(bitrate),
                "resolution": resolution,
                "missing_segment_count": 0,
                "missing_segment_indices": [],
            }
            print(
                f"  [{tmdb_id}] 直链下载完成，已释放下载槽位并进入转封装队列",
                flush=True,
            )
            return conversion_job

        variants = parse_master_playlist(
            url, retries=None if is_last_node else PLAYLIST_RETRY_FALLBACK,
            headers=node_headers,
        )
        if not variants:
            raise RuntimeError("没有找到媒体播放列表或清晰度变体")

        # 解析出每条流的声明分辨率（可能为 None）。排序与筛选口径见下方两段：
        # 两者都随 RESOLUTION_CHECK_ENABLED 切换。
        annotated = [
            (resolution, playlist_url, bandwidth, parse_resolution(resolution))
            for resolution, playlist_url, bandwidth in variants
        ]
        # 模式 A：已声明分辨率且低于红线（含容差）的流直接排除，不必浪费采样流量。
        #   未声明分辨率的流保留，等采样后用 ffprobe 探测真实高度再判。
        # 模式 B：meets_resolution_redline 恒真 → 全部保留，候选一条不筛。
        #   这是对的：低码率流要靠**实测采样码率**才能判，声明分辨率说明不了问题
        #   （480p 也可能是高码率的清晰片）。代价是多采样几条流，但不会误杀。
        candidates = [
            item
            for item in annotated
            if item[3] is None or meets_resolution_redline(item[3][1])
        ]
        if not candidates:
            # 仅模式 A 可能走到：模式 B 下上面的过滤恒为全保留，variants 非空则
            # candidates 必非空（variants 为空已在前面单独报错）。故文案只按
            # 模式 A 的语义写即可。
            raise QualityRejectedError(
                f"没有找到高度达标（≥ {MIN_RESOLUTION_HEIGHT}×{LENIENCY:.2f}）的流"
            )

        # 预排序：分辨率参与判定时先试声明高度更高的流，同高度试 BANDWIDTH 更高的；
        # 未声明分辨率（item[3] is None）用 -1 排最后，等采样后 ffprobe 探测再定夺。
        # 分辨率不参与判定时改为纯按声明 BANDWIDTH 降序——此时择优只比码率，
        # 高声明带宽的流最可能先胜出，先试它才能让下面的提前终止真正省下采样。
        if RESOLUTION_CHECK_ENABLED:
            candidates.sort(
                key=lambda item: (
                    item[3][1] if item[3] else -1,
                    item[2],
                ),
                reverse=True,
            )
        else:
            candidates.sort(key=lambda item: item[2], reverse=True)

        best_height = -1
        best_bitrate = 0.0
        best_resolution = None
        best_segment_urls = None
        best_durations = None
        best_init_url = None
        best_selected = False
        # 本节点各候选流的淘汰性质统计，用于在"无一入选"时区分两种截然不同的
        # 情形（改动前它们被混为一谈，一律判可重试）：
        #   - 全部流都因画质被淘汰 → 画质是源站固有属性，下一轮重采结果相同，
        #     该判死（抛 QualityRejectedError）；
        #   - 有任一流是网络/采样异常（502、采样探测失败等）→ 真抖动，值得重试。
        # 实测依据见 §12.11 D：唯一被三轮救回的那部片正是"前 4 个节点全部
        # 无一入选、第 5 个节点成功"，故这里绝不能把网络抖动也算成画质淘汰。
        quality_rejected_streams = 0
        other_failed_streams = 0

        for resolution, playlist_url, _declared_bandwidth, size in candidates:
            # 候选已按声明高度降序排列。走到"声明高度严格低于已选中流"的候选时，
            # 它即便采样也必然落选（择优是高度绝对优先），故这里跳过纯属浪费的采样。
            # 四个合取项缺一不可：
            #   RESOLUTION_CHECK_ENABLED —— 本剪枝的正确性完全建立在"择优按高度
            #     绝对优先"之上。分辨率不参与判定时择优改成纯比码率，而声明高度低
            #     不代表实测码率低，此时剪枝会真的丢掉更优的流。声明 BANDWIDTH 也
            #     不能拿来剪枝：它是源站声明的峰值带宽，与我们实测的采样码率口径
            #     不同，据此跳过同样会误剪。故该模式下老老实实全部采样。
            #   size is not None —— 未声明分辨率的流排在末尾，真实高度未知，
            #     必须采样后 ffprobe，跳过会丢画质；
            #   best_selected —— 只有真正选中过某流才生效，否则最高档瞬时抖动
            #     挂掉后整片会因"无一入选"白白失败，直接损失成功率；
            #   严格小于 —— 同高度的仍要采样比码率。
            if (
                RESOLUTION_CHECK_ENABLED
                and size is not None
                and best_selected
                and size[1] < best_height
            ):
                print(f"  跳过流 {resolution}：声明高度低于已选中的 {best_resolution}")
                continue
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
                    playlist_url, headers=node_headers
                )
                sample_count = min(SAMPLE_COUNT, len(segment_urls))
                # 从影片“正中间”连续取 sample_count 段测码率：片头常是 logo/黑场/
                # 字幕卡等低动态画面、码率系统性偏低，会误杀压线的合格片；中间为高动态
                # 正片，采样码率更代表全片。采样片单独成文件、测完即删、不复用为正片。
                sample_start = (len(segment_urls) - sample_count) // 2
                sample_end = sample_start + sample_count
                sample_bytes, sample_failed_indices, sample_init_bytes = (
                    download_segments(
                        segment_urls,
                        sample_path,
                        start_idx=sample_start,
                        end_idx=sample_end,
                        concurrency=min(4, SEGMENT_CONCURRENCY),
                        init_url=init_url,
                        # 中间采样 start_idx>0 走 ab 模式不会自动写 init，强制补一次
                        # moov，否则 fMP4 采样片缺编解码参数导致 ffprobe 探测失败。
                        force_init=True,
                        headers=node_headers,
                        # 采样探不到就换下一条流，别按正片那套死磕（见常量注释）。
                        retry_max=SAMPLE_SEG_RETRY_MAX,
                    )
                )
                sample_failed_set = set(sample_failed_indices)
                # 时长求和用真实分片索引区间 [sample_start, sample_end)，与
                # download_segments 返回的 sample_failed_indices 索引空间一致。
                sample_duration = sum(
                    duration
                    for index, duration in enumerate(
                        durations[sample_start:sample_end], start=sample_start
                    )
                    if index not in sample_failed_set
                )
                if sample_duration <= 0 or sample_bytes <= 0:
                    raise RuntimeError("采样数据或采样时长为 0")

                # master 没声明 RESOLUTION 时，用采样文件探测真实分辨率。
                actual_size = size
                actual_resolution = resolution
                if actual_size is None:
                    actual_size = probe_resolution(sample_path)
                    if actual_size is None and RESOLUTION_CHECK_ENABLED:
                        # 模式 A：探测失败常是采样片本次没下全/损坏（瞬时抖动），
                        # 不是真无高清流 → 判可重试，下一轮重采样有机会救回。
                        raise RuntimeError("采样探测分辨率失败（可重试）")
                    if actual_size is not None:
                        actual_resolution = f"{actual_size[0]}x{actual_size[1]}"
                        print(f"  流 {resolution} 实测分辨率: {actual_resolution}")
                    else:
                        # 模式 B 才会走到这里。master 未声明且探测失败时
                        # actual_resolution 仍是 None，会一路透传进成品元数据与
                        # success.jsonl，打印成 "None"。统一成可读文案。
                        actual_resolution = UNKNOWN_RESOLUTION

                # 模式 B（默认，只看码率）：分辨率探不到也能继续按码率判定与择优，
                # 不该白白放弃一条可能合格的流。height 仅在模式 A 有意义。
                height = actual_size[1] if actual_size else 0
                # 第 1 关 · 分辨率红线（带 LENIENCY 容差）。
                if not meets_resolution_redline(height):
                    raise QualityRejectedError(
                        f"分辨率 {actual_resolution} 低于红线 "
                        f"{MIN_RESOLUTION_HEIGHT}（容差 {LENIENCY:.2f}），跳过"
                    )

                # 扣除 init 段字节：init 无时长，计入分子会让码率虚高（fMP4 尤甚）。
                media_bytes = max(0, sample_bytes - sample_init_bytes)
                bitrate = media_bytes * 8 / sample_duration / 1000
                print(f"  流 {actual_resolution} 采样码率: {bitrate:.0f} kbps")

                # 第 2 关 · 码率曲线：门槛按“该流自身高度”平方缩放并乘 LENIENCY，
                # 每个流一律按自身高度档卡码率（无免码率线）。探测不到编码回退 H.264 基准。
                codec = probe_codec(sample_path)
                min_bitrate = bitrate_threshold(height, codec)
                codec_label = codec or "unknown"
                print(
                    f"  流 {actual_resolution} 编码 {codec_label}，"
                    f"码率门槛 {min_bitrate:.0f} kbps"
                )
                if bitrate < min_bitrate:
                    raise QualityRejectedError(
                        bitrate_reject_message(
                            actual_resolution, codec_label, bitrate, min_bitrate
                        )
                    )

                # 择优：分辨率参与判定时，实测高度绝对优先、高度完全相同再比采样
                # 码率（用实测高度而非粗档 tier，任意分辨率都能精确区分，降档也不
                # 退化）；不参与判定时纯比采样码率——既然高度不再是画质标准，就
                # 不该拿它决定"多条合格流选哪条"，否则等于分辨率仍在暗中主导。
                if RESOLUTION_CHECK_ENABLED:
                    better = height > best_height or (
                        height == best_height and bitrate > best_bitrate
                    )
                else:
                    better = bitrate > best_bitrate
                if better:
                    best_height = height
                    best_bitrate = bitrate
                    best_resolution = actual_resolution
                    best_segment_urls = segment_urls
                    best_durations = durations
                    best_init_url = init_url
                    best_selected = True
                # 采样片仅用于测画质，不复用为正片（中间采样无法接成连续前缀）。
                # 无论选中与否都立即删除，防止 temp 目录长期堆积采样文件。
                remove_file(sample_path)
            except UnsupportedPlaylistError:
                # 加密/BYTERANGE/MAP 等结构是整片级属性（同一片各清晰度同构），
                # 重下必然同样失败 → 直接向外抛（带确定性 marker），不再试其余流、
                # 不落入下方"可重试"汇总，避免永久不支持的结构被白重试多轮。
                remove_file(sample_path)
                raise
            except QualityRejectedError as exc:
                # 画质淘汰：本流确定性出局，但其余流可能达标，故继续试而不外抛。
                remove_file(sample_path)
                quality_rejected_streams += 1
                print(f"  处理流 {resolution} 失败: {exc}")
            except Exception as exc:
                remove_file(sample_path)
                other_failed_streams += 1
                print(f"  处理流 {resolution} 失败: {exc}")

        if not best_selected:
            # 全部候选流都是画质淘汰、且无一条是瞬时异常 —— 此时"重下会不会变好"
            # 已经有确定答案：不会。分辨率与码率是源站固有属性，下一轮重采拿到的
            # 还是同样的流。故抛确定性异常判死，不再白烧后续轮次的下载槽位。
            #
            # 注意 quality_rejected_streams > 0 这个合取项：candidates 非空时它必
            # 成立，但若未来筛选逻辑演进出"零候选流且零失败"的路径，没有它就会把
            # 空集误判成"全部画质淘汰"。
            if quality_rejected_streams > 0 and other_failed_streams == 0:
                raise QualityRejectedError(
                    f"全部 {quality_rejected_streams} 条候选流均因画质不达标被淘汰"
                    f"（各流原因见上方日志）"
                )
            # 混合情形（存在瞬时异常）：无法断定是"真不达标"还是"采样抖动全挂"，
            # 落默认「可重试」交由多轮重采兜底，契合"宁可多下不误杀"。
            raise RuntimeError(
                "本轮候选流无一入选（各流原因见上方日志），下一轮重采"
            )

        # 模式 B 下 best_resolution 可能是 "未知分辨率"（探测失败），此时码率才是
        # 选中依据，分辨率只是附带信息。两种模式共用这一行，不必分支。
        print(
            f"  选中流: 分辨率 {best_resolution}, "
            f"采样码率 {best_bitrate:.0f} kbps"
        )

        # 采样片已删、不复用；正片从第 0 段完整下载（含 fMP4 init 段）。
        remove_file(final_ts)
        print(
            f"  并发下载全部 {len(best_segment_urls)} 个分片 "
            f"(并发数 {SEGMENT_CONCURRENCY})..."
        )
        total_bytes, failed_segment_indices, total_init_bytes = download_segments(
            best_segment_urls,
            final_ts,
            start_idx=0,
            end_idx=None,
            concurrency=SEGMENT_CONCURRENCY,
            init_url=best_init_url,
            headers=node_headers,
        )
        failed_segment_indices = sorted(set(failed_segment_indices))
        failed_segment_set = set(failed_segment_indices)
        total_duration = sum(
            duration
            for index, duration in enumerate(best_durations)
            if index not in failed_segment_set
        )
        # 扣除 init 段字节（无时长）后再按整片时长算总码率，口径与采样一致。
        media_total_bytes = max(0, total_bytes - total_init_bytes)
        total_bitrate = (
            media_total_bytes * 8 / total_duration / 1000
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
        # ID 锁一旦持有，任何可能抛异常的动作都必须在 try 内，否则会绕过 finally
        # 的 discard，让该 ID 永久停在"处理中"。后果是静默丢片：下一轮重投会命中
        # "duplicate entry currently processing"，而该文案在 ignored_errors 里被
        # 直接跳过——既不写 FAILED_LOG 也不再重试，这部片就此消失。
        # os.makedirs 会因权限/ENOSPC 抛 OSError，print 会因 stdout 断管抛
        # BrokenPipeError，都不是理论风险。
        #
        # 磁盘水位兜底：仅在此处（尚未开始任何下载动作前）阻塞。磁盘吃紧时新片
        # 在闸门前等待，不会占用 temp/带宽；已在跑的下载不受影响。
        wait_for_disk_gate()

        print(f"\n开始处理: {tmdb_id} - {title}")
        os.makedirs(TEMP_DIR, exist_ok=True)

        # 方案C：依次尝试各取流节点，任一节点下完即成功；全部失败才判失败。
        conversion_job = None
        last_exc = None
        any_retriable = False  # 只要有任一节点是“可重试失败”，整片就值得下一轮重试
        # 有任一节点的 mp4 签名直链已过期。必须独立记录而不能事后从 last_exc 的
        # 文案里读——`msg` 取的是**最后一个**节点的错误，若过期节点排在前面
        # （如「节点1 vidlink 403 过期 → 节点2 502」），needs_refetch 就永远看不到
        # 那条 marker，该直链在剩余所有轮次里都是废的。这是 §10.21 B-2「两条重投
        # 路径不能互斥」在多节点场景下的漏网。
        any_needs_refetch = False
        # 画质确定性淘汰的节点数。分母用**全部**节点（len(urls)），不是"拿到画质
        # 信息的节点数"——后者会把「2 个画质淘汰 + 1 个 502」算成 2/2=100% 判死，
        # 那个 502 节点从没被真正看过画质，据此判死过于激进。
        quality_rejected_nodes = 0

        # mp4 跨节点码率择优：把 mp4 节点按实测码率重排，最优的先试。
        # 见 `_rank_mp4_nodes` —— 它只重排、不淘汰任何节点，故 fallback 能力
        # （§10.13 实测救回过片子）完全保留，urls 的总数与集合都不变。
        # m3u8 节点不参与：拿到它的码率要解析 master + 下载采样分片，成本高得多，
        # 属 §10.10 ④「跨 provider 按画质排序」的范畴，用户明确暂缓。
        mp4_nodes = [n for n in urls if n["type"] == "mp4"]
        preflight_of = {}
        if len(mp4_nodes) > 1:
            ordered_mp4 = _rank_mp4_nodes(mp4_nodes, tmdb_id, runtime_minutes)
            preflight_of = {
                id(node): info for node, info in ordered_mp4 if info is not None
            }
            # 原地重排：mp4 节点之间按新顺序，m3u8 节点保持在原来的位置上，
            # 这样两类节点的相对次序（进而 is_last_node 的语义）完全不变。
            mp4_iter = iter(node for node, _info in ordered_mp4)
            urls = [
                next(mp4_iter) if n["type"] == "mp4" else n for n in urls
            ]

        for idx, node in enumerate(urls, start=1):
            is_last_node = idx == len(urls)
            try:
                if idx > 1:
                    print(
                        f"  [{tmdb_id}] 切换备用节点 {idx}/{len(urls)} "
                        f"({node['provider']}/{node['type']})",
                        flush=True,
                    )
                conversion_job = _attempt_download(
                    node, is_last_node, preflight_of.get(id(node))
                )
                break
            except Exception as exc:
                last_exc = exc
                if isinstance(exc, QualityRejectedError):
                    quality_rejected_nodes += 1
                if needs_refetch(str(exc)):
                    any_needs_refetch = True
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
        # 【画质概率判死】过半节点都因画质确定性淘汰时，推翻上面的乐观口径。
        #
        # 依据：画质是源站固有属性而非随机变量——同一条 url 下一轮拿到的还是
        # 480p。既然过半节点都不达标，剩下那些节点大概率同样不达标，用概率代替
        # 逐轮求证。实测代价见 §12.11 D：29 部失败片白跑两轮共 28 分钟只救回 1 部。
        #
        # 只在"无一节点成功"的失败路径上生效（本就在 except 里），且要求
        # quality_rejected_nodes > 0，避免 urls 为空等边界被 0/0 蒙混。
        if (
            urls
            and quality_rejected_nodes > 0
            and quality_rejected_nodes / len(urls) >= QUALITY_KILL_RATIO
        ):
            retriable = False
            # 换成汇总文案：改动前这里留的是**最后一个**节点的错误，会写出
            # 「retriable=False 却写着 502」这类自相矛盾、且会让收尾统计归错类
            # 的记录（§12.11 D 现场就是「retriable=True | 480p 低于红线」）。
            msg = (
                f"{len(urls)} 个节点中 {quality_rejected_nodes} 个因画质不达标被"
                f"确定性淘汰（≥ 阈值 {QUALITY_KILL_RATIO:.2f}），判定整片画质不达标；"
                f"末节点错误：{msg}"
            )
        # 有节点直链过期、但 msg 里读不到那条 marker（过期节点不在末位，或被上面
        # 的画质汇总文案改写）时，补挂上去。理由：`--refetch-failed` 这条**人工**
        # 闭环是从落盘的 failed.jsonl 里按文案挑 id 的（见 NEEDS_REFETCH_MARKER
        # 的跨文件约定），进程内的 needs_refetch 标志它读不到。不补的话，人工重取
        # 会静默漏掉这些片，且不报任何错。
        if any_needs_refetch and _NEEDS_REFETCH_MARKER not in msg:
            msg = f"{msg}；另有节点直链已失效，{_NEEDS_REFETCH_MARKER}"
        return tmdb_id, False, {
            "error": msg,
            "retriable": retriable,
            # 与 error 文案解耦：过期节点排在非末位时，文案里读不到过期 marker。
            "needs_refetch": any_needs_refetch,
        }
    finally:
        # 下载成功后临时文件和 ID 锁交给转封装阶段管理。
        if not handed_off_to_conversion:
            for path in cleanup_paths:
                remove_file(path)
            with processing_lock:
                processing_ids.discard(normalized_id)


def _resolve_final_resolution(resolution, final_path, label):
    """成品元数据里的分辨率：下载阶段没探到的，在成品上补探一次。

    为什么会没探到（§12.15）：模式 B 下分辨率不参与画质判定，采样探测失败时
    我们**故意不再阻断**下载（阻断是纯误杀——码率与分辨率无关）。代价是
    `resolution` 落成 UNKNOWN_RESOLUTION。500 部实跑里这占了 **44%**。

    为什么能在这里补：此刻转封装已完成、成品 mp4 就在本地，是**完整文件**而非
    采样片段，moov 齐全，ffprobe 几乎必成——而下载阶段探的是中间几个分片拼成的
    采样文件，master 未声明 RESOLUTION 时探不出来是常态。

    🔑 本函数只补**元数据**，不做任何判定：
      - 探到就用真值，探不到就保持原样（绝不因此判失败）；
      - 已有真值的直接返回，不重复探（省一次 ffprobe）。
    """
    if resolution and resolution != UNKNOWN_RESOLUTION:
        return resolution
    size = probe_resolution(final_path)
    if not size:
        # 成品都探不出来就是真探不出来，保持占位文案。绝不能在此判失败——
        # 片子已经下完并转封装好了，为一个元数据丢掉它是本末倒置。
        return resolution
    probed = f"{size[0]}x{size[1]}"
    print(f"  [{label}] 成品补探分辨率: {probed}", flush=True)
    return probed


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
            "resolution": _resolve_final_resolution(
                conversion_job["resolution"], final_path, tmdb_id
            ),
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

    # 从 build_s3_key 起整段包 try：即使 build_s3_key/write_log/write_pending
    # 等抛异常，也在此兜底为“留本地 + 尽力写 pending + 返回失败”，绝不让异常逃逸
    # 到调用方——否则成品既不删也不进 pending，reupload 无从感知、磁盘永久泄漏。
    try:
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
            "title": success_info.get("title") or "",
            "year": success_info.get("year"),
            "local_path": local_path,
            "s3_key": s3_key,
            "fail_reason": reason,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        print(f"  [{tmdb_id}] 上传失败，已留本地待补传: {reason}", flush=True)
        return tmdb_id, False, {"error": f"上传失败: {reason}"}
    except Exception as exc:
        # 上传流程中任何未预期异常：尽力留本地并补写 pending（pending 写入若也
        # 抛异常则再兜一层，至少保证本地文件不被删、日志有痕迹），返回失败。
        reason = f"上传阶段异常: {exc}"
        try:
            write_pending({
                "tmdbId": tmdb_id,
                "title": success_info.get("title") or "",
                "year": success_info.get("year"),
                "local_path": local_path,
                "s3_key": success_info.get("s3_key", ""),
                "fail_reason": reason,
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as pend_exc:
            print(
                f"  [{tmdb_id}] ⚠️ 上传异常且 pending 写入失败，本地文件已保留："
                f"{local_path}（{pend_exc}）",
                flush=True,
            )
        print(f"  [{tmdb_id}] 上传阶段异常，已留本地待补传: {exc}", flush=True)
        return tmdb_id, False, {"error": reason}





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
        importlib.import_module("boto3")
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
        _fail(
            f"缺少 R2 必填字段：{', '.join(missing)}"
            f"（请在环境变量 R2_* 或 config.yaml 的 s3 段中配置）"
        )

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


def preflight_check_ffmpeg():
    """启动前预检 ffmpeg/ffprobe 是否在 PATH 中：缺失则 fail-fast 退出。

    二者是转封装（TS->MP4）与分辨率探测的硬依赖，若缺失会导致每部影片
    在转封装阶段静默失败进 failed.jsonl，白跑整轮下载。故在此提前拦截。
    """
    missing = [tool for tool in ("ffmpeg", "ffprobe") if shutil.which(tool) is None]
    if missing:
        reason = (
            f"未找到外部工具：{', '.join(missing)}。"
            "请先安装 ffmpeg（含 ffprobe）并加入 PATH，否则转封装会全部失败。"
        )
        print(f"[ffmpeg 预检失败] {reason}", flush=True)
        write_log(FAILED_LOG, {
            "stage": "preflight",
            "error": reason,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        release_main_lock()
        sys.exit(1)
    print("[ffmpeg 预检通过] ffmpeg 与 ffprobe 均已就绪", flush=True)


def install_interrupt_handler():
    """让 Ctrl+C / SIGTERM 置位全局 `interrupted`，把中断意图传达给工作线程。

    ⚠️ 只靠 Python 默认的 KeyboardInterrupt 是不够的：它只打断**主线程**，
    线程池里正在退避重试的分片 worker 毫不知情，会各自把 20 次重试跑完
    （最坏 20×60s）。服务器实跑实测——中断统计都打印完了，进程还挂着 31 个线程
    继续刷失败日志，只能 kill -9。

    首次收到信号：置位事件 + 恢复默认处理器，然后照常抛 KeyboardInterrupt 走
    正常收尾（落盘、打统计、释放锁）。
    再按一次 Ctrl+C 就是默认行为（立即终止），给"等不及了"留出硬退出的口子。
    """
    def _handler(signum, _frame):
        interrupted.set()
        print(
            f"\n⚠️ 收到信号 {signum}，正在停止所有下载线程"
            f"（再按一次 Ctrl+C 可强制退出）...",
            flush=True,
        )
        # 恢复默认：第二次信号直接杀进程，不再走优雅收尾。
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        raise KeyboardInterrupt()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except ValueError:
            # 非主线程注册会抛 ValueError（如被 import 进别的框架里跑），忽略即可。
            pass


def main():
    acquire_main_lock()
    install_interrupt_handler()
    preflight_check_ffmpeg()
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

    # 收尾自动补传：上传槽位超时降级留下的成品躺在 upload_pending.jsonl 里，
    # 不会被任何后续轮次处理，只能靠人跑 `download_movies.py reupload`。此时
    # R2 往往已经恢复，自动补一次能省掉这次人工介入。
    # 必须放在 release_main_lock() **之后**：reupload_pending 内部有
    # is_main_running() 守卫，锁未释放时会直接拒绝执行。
    # 放在 finally 之外：_run_pipeline 抛异常时不补传——此时状态未知，
    # 交给人工判断更稳妥（手动 reupload 入口始终可用）。
    if AUTO_REUPLOAD_ENABLED and S3_ENABLED:
        print("\n===== 收尾自动补传 =====", flush=True)
        try:
            reupload_pending()
        except (Exception, SystemExit) as exc:
            # 补传失败不影响主流程的成功结论：成品仍留在本地且 pending 记录还在，
            # 随时可以手动 reupload。SystemExit 一并兜住（避免收尾动作把已经跑完
            # 的整次运行判成失败退出），但放过 KeyboardInterrupt。
            print(f"⚠️ 自动补传异常，成品仍留本地待手动 reupload: {exc}", flush=True)


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

    # ⚠️ 输入文件缺失在两种模式下含义完全不同：
    #   - 单独跑下载：results.jsonl 是唯一片源，没有它就无事可做，照旧退出。
    #   - pipeline 模式：片子由**同进程的取流线程**实时产出，全新部署时这个
    #     文件本来就还不存在（取流线程要几十秒才写出第一条）。此时若照旧
    #     return，下载侧会在启动瞬间退出，整条流水线只剩取流在跑——
    #     首次部署必现，且表现为"跑完什么都没下"。
    # 故 pipeline 模式（来源由 ListEntrySource 钩子接管）下把它当空存量继续跑，
    # 后续的片全部从队列里来。
    streaming = ListEntrySource is not _ListEntrySource
    if not os.path.exists(INPUT_JSONL):
        if not streaming:
            print(f"错误: 找不到 {INPUT_JSONL}")
            return
        print(f"{INPUT_JSONL} 尚不存在（全新部署），等待取流侧实时产出", flush=True)

    entries = []
    entry_by_id = {}
    invalid_input_count = 0
    duplicate_input_count = 0
    input_lines = []
    if os.path.exists(INPUT_JSONL):
        with open(INPUT_JSONL, "r", encoding="utf-8") as file:
            input_lines = list(enumerate(file, 1))
    for line_number, line in input_lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            print(f"跳过 JSONL 第 {line_number} 行: {exc}")
            continue

        normalized_id = normalize_tmdb_id(entry.get("tmdbId"))
        if not normalized_id:
            # 缺身份字段的行读入即跳过：留着也只会在 process_one_entry 里
            # 判"缺少 tmdbId 或 urls"，把一条残缺输入放大成一条 FAILED_LOG。
            invalid_input_count += 1
            continue
        previous = entry_by_id.get(normalized_id)
        if previous is None:
            entry_by_id[normalized_id] = entry
            continue
        duplicate_input_count += 1
        # results.jsonl 是追加写：同一片经多轮重试/复扫会留下多行，且
        # **不保证越靠后越新**。必须按 fetched_at 取真正最新的一条——
        # vidlink 的 mp4 直链带时效签名，拿到旧的等于白跑一次下载。
        # 无戳时回退 -1（有戳的一定胜出；都无戳则保留先出现者，维持旧行为）。
        # 括号不能省：`a or -1 > b` 会按 `a or (-1 > b)` 结合，恒真。
        new_ts = parse_int(entry.get("fetched_at"))
        old_ts = parse_int(previous.get("fetched_at"))
        if (new_ts if new_ts is not None else -1) > (
            old_ts if old_ts is not None else -1
        ):
            entry_by_id[normalized_id] = entry

    entries = list(entry_by_id.values())

    print(
        f"共读取 {len(entries)} 个去重后的条目；"
        f"输入文件内跳过 {duplicate_input_count} 个重复 ID（按 fetched_at 取最新）"
        + (f"；跳过 {invalid_input_count} 个缺 tmdbId 的行" if invalid_input_count else "")
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
    # 被拒原因聚合（观测性，仅统计下载阶段失败）：按类别计数，分确定性/可重试两组。
    # 确定性失败每片计一次；可重试失败跨轮会重复计（同片多轮重投），打印时分块标注。
    reject_permanent = {}
    reject_retriable = {}

    def handle_done_future(future, round_failed_retriable,
                           round_failed_expired=None):
        """处理一个已完成的 future，按其阶段推进流水线。

        round_failed_retriable 为本轮“可重试下载失败”的收集器（list）；
        round_failed_expired 为本轮“直链过期、可靠重新取流救回”的收集器；
        末轮排空阶段两者都传 None（此时 pending 里只会剩转封装/上传，不会命中下载分支）。
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
                    "title": entry.get("title") or "",
                    "year": entry.get("year"),
                })
                # submit 若抛异常（如线程池已 shutdown），finalize 永不执行 →
                # processing_ids 锁与临时文件会永久泄漏。故兜底：失败即释放 ID 锁、
                # 清理已交接的临时文件，并当作转封装失败记录（与 upload submit 对称）。
                try:
                    conversion_future = conversion_executor.submit(
                        finalize_one_entry, info, processed_ids
                    )
                except Exception as exc:
                    normalized_id = normalize_tmdb_id(tmdb_id)
                    with processing_lock:
                        processing_ids.discard(normalized_id)
                    for path in info.get("cleanup_paths", []):
                        remove_file(path)
                    write_log(FAILED_LOG, {
                        "tmdbId": tmdb_id,
                        "title": entry.get("title") or "",
                        "urls": entry.get("urls", []),
                        "error": f"转封装提交失败: {exc}",
                        "stage": "conversion",
                    })
                    print(f"转封装提交失败: {tmdb_id}: {exc}")
                    return
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
            # 被拒原因聚合统计（观测性，不影响任何判定）。
            reason = classify_reject_reason(error_msg)
            bucket = reject_retriable if retriable else reject_permanent
            bucket[reason] = bucket.get(reason, 0) + 1
            write_log(FAILED_LOG, {
                "tmdbId": tmdb_id,
                "title": entry.get("title") or "",
                "urls": entry.get("urls", []),
                "error": error_msg,
                "stage": "download",
            })
            # 下载态状态文件：本轮下载失败逐条记录（含可否重试）。
            write_log(DOWNLOAD_FAIL_LOG, {
                "tmdbId": tmdb_id,
                "title": entry.get("title") or "",
                "error": error_msg,
                "retriable": retriable,
            })
            print(
                f"下载失败: {tmdb_id}: {error_msg}"
                f"（{'可重试' if retriable else '确定性失败,不重试'}）"
            )
            # 两条重投路径不互斥，判定集中在 plan_retry_buckets（见其文档）。
            # needs_refetch 优先取 info 里的显式标志（节点循环逐节点记录，不受
            # "error 只留末节点文案"的影响）；缺失时回退到按文案判断，兼容
            # process_one_entry 之外的异常路径与历史记录。
            should_retry, should_refetch = plan_retry_buckets(
                retriable, error_msg, info.get("needs_refetch")
            )
            if should_retry and round_failed_retriable is not None:
                round_failed_retriable.append(entry)
            if should_refetch and round_failed_expired is not None:
                round_failed_expired.append(entry)

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
                    "title": entry.get("title") or "",
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
            #
            # 但绝不能无限期等：本函数由主事件循环线程调用，等满
            # UPLOAD_SLOT_WAIT_TIMEOUT 仍拿不到槽位，说明 R2 长时间消化不动，
            # 此时降级——不提交上传、成品留本地并写 pending，主循环继续推进
            # 下载与转封装，事后用 reupload 子命令补传。宁可暂时不传，也不让
            # 远端故障拖垮本地下载产能。
            #
            # S3_ENABLED=False 时上传任务只写日志、秒回，不可能积压；万一
            # 仍走到这里也不能写 pending——纯本地模式下的成品无需补传，
            # 塞进 pending 只会污染 reupload 的输入。故降级只对开启上传生效。
            if not upload_semaphore.acquire(timeout=UPLOAD_SLOT_WAIT_TIMEOUT):
                degrade_reason = (
                    f"上传积压超过 {UPLOAD_SLOT_WAIT_TIMEOUT:g}s 未消化，"
                    f"本片降级为留本地待补传"
                )
                if S3_ENABLED:
                    try:
                        write_pending({
                            "tmdbId": tmdb_id,
                            "title": info.get("title") or "",
                            "year": info.get("year"),
                            "local_path": info.get("final_path"),
                            "s3_key": "",
                            "fail_reason": degrade_reason,
                            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    except Exception as exc:
                        print(f"⚠️ 降级写 pending 失败: {tmdb_id}: {exc}", flush=True)
                # 记 SUCCESS_LOG(uploaded=false) 防止下次运行重新下载。用
                # update_success_log 按 tmdbId 覆盖写（而非 write_log 追加）：
                # 后续 reupload 补传成功时也走同一函数覆盖同一条，保证
                # SUCCESS_LOG "每片一条" 的设计意图不被破坏。
                info["uploaded"] = False
                try:
                    update_success_log(tmdb_id, info)
                except Exception as exc:
                    print(f"⚠️ 降级写 success 日志失败: {tmdb_id}: {exc}", flush=True)
                write_log(FAILED_LOG, {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title") or "",
                    "urls": entry.get("urls", []),
                    "error": degrade_reason,
                    "stage": "upload",
                })
                print(f"⚠️ {tmdb_id}: {degrade_reason}", flush=True)
                return
            # acquire 与 submit 之间若 submit 抛异常（如线程池已 shutdown），
            # 已 acquire 的配额会永久泄漏、累积到上限致主循环死锁。故用 try 兜底：
            # submit 失败立即 release 保证信号量对称，并就地写 FAILED_LOG 后 return
            # （与转封装提交失败分支对称）——绝不 raise，否则异常逃逸出无 try 包裹的
            # 主循环，剩余 pending 任务记录全部丢失、并可能卡死磁盘 gate。
            # 成品 mp4 有意留本地（未删），待 reupload 阶段补传，不构成泄漏。
            try:
                upload_future = upload_executor.submit(upload_one_entry, info)
            except Exception as exc:
                upload_semaphore.release()
                write_log(FAILED_LOG, {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title") or "",
                    "urls": entry.get("urls", []),
                    "error": f"上传提交失败: {exc}",
                    "stage": "upload",
                })
                print(f"上传提交失败: {tmdb_id}: {exc}")
                return
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
                    "title": entry.get("title") or "",
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
        # 每部片已被就地重取几次（跨轮累计），防"取流-过期-重取"无限空转。
        refetch_counts = {}
        while True:
            # 每轮开头清空 download_fail 状态文件，只记录本轮下载失败。
            truncate_log(DOWNLOAD_FAIL_LOG)
            # 本轮待下载条目的来源（§12 第 1 步）。当前两轮都传 list，行为与改造前
            # 完全一致；第 2 步会在首轮改传队列来源，其余轮次仍是 list。
            batch_source = (
                current_batch if hasattr(current_batch, "poll")
                else ListEntrySource(current_batch)
            )
            # 队列来源没有确定总量，故取不到长度时显示"未知"而不是崩掉。
            try:
                batch_total = f"{len(batch_source)} 部"
            except TypeError:
                batch_total = "持续接收中"
            if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
                print(
                    f"\n===== 下载轮次 {round_no}/{MAX_ROUNDS}："
                    f"本轮待下载 {batch_total} =====",
                    flush=True,
                )

            # 分批投递：不再一次性把整轮全部影片 submit 进 pending。同时存在的
            # 下载 future 上限为 DOWNLOAD_QUEUE_DEPTH，wait() 每次挂/摘 waiter
            # 的规模从 O(整轮片数) 降到 O(槽位数)——全量重跑几十万部时，一次性
            # 全投会让主循环退化成 O(N²) 空转，把 CPU 耗在 waiter 管理上。
            # 语义完全不变：本轮每一部仍会被逐一投递，且全部有结论后才进下一轮；
            # 附带收益是磁盘占用更平滑（未投递的片不占 temp）。
            source_exhausted = False
            round_download_futures = set()

            def submit_downloads():
                """从来源取片填满下载槽位。返回 False 表示"来源暂时没货但未耗尽"。

                取到 "wait" 时立即停止本次投递（而不是原地等），把控制权交回主循环
                去推进在途的转封装/上传——绝不能在这里阻塞（见 ListEntrySource 注释）。
                """
                nonlocal source_exhausted
                while len(round_download_futures) < DOWNLOAD_QUEUE_DEPTH:
                    if source_exhausted:
                        return True
                    state, entry = batch_source.poll()
                    if state == "done":
                        source_exhausted = True
                        return True
                    if state == "wait":
                        return False
                    f = download_executor.submit(
                        process_one_entry, entry, processed_ids
                    )
                    download_future_to_entry[f] = entry
                    stage_of[f] = "download"
                    pending.add(f)
                    round_download_futures.add(f)
                return True

            round_failed_retriable = []
            round_failed_expired = []
            submit_downloads()

            # 关键：只等“本轮下载 future”全部离开 download 阶段即算本轮下载完成，
            # 不等 pending 全空。上一轮遗留的转封装/上传在同一循环里并行推进，
            # 但不阻塞本轮判定——这正是方案 A 的并行精髓。
            # 循环条件涵盖“还有在途下载”或“来源尚未耗尽”，二者皆空才收尾。
            while round_download_futures or not source_exhausted:
                if not pending:
                    # 仅流式来源会走到这里：`round_download_futures ⊆ pending`
                    # （两者的 add/discard 严格成对），故 pending 空即本轮无在途
                    # 下载，再结合循环条件可知来源必定尚未耗尽——也就是"在途任务
                    # 已排空，但生产者还没产出新片"。此时 wait(空集合) 会立刻返回、
                    # 退化成 100% CPU 空转，故让出 CPU 后重新问来源要货。
                    # list 来源永不返回 wait，故这段对单独跑下载的场景是死分支。
                    time.sleep(STREAM_IDLE_POLL_SECONDS)
                    submit_downloads()
                    continue
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    pending.discard(future)
                    round_download_futures.discard(future)
                    # 单个 future 处理若抛异常（如日志写盘 OSError/磁盘满、
                    # 状态字典错位 KeyError），只记录并跳过，绝不让异常逃逸出
                    # 主循环——否则同批其余 future 全丢、整条流水线崩溃、在途
                    # 信号量与 processing_ids 锁无从释放（铁律：宁可单片失败，
                    # 绝不崩主流程）。
                    try:
                        handle_done_future(
                            future, round_failed_retriable, round_failed_expired
                        )
                    except Exception as exc:
                        print(f"⚠️ future 处理异常，已跳过该条: {exc}", flush=True)
                # 腾出槽位后立即补投，保持下载池始终满载。
                submit_downloads()

            # 本轮下载全部有结论。先看还有没有下一轮可跑，再决定要不要重取流——
            # 末轮重取纯属浪费取流配额（拿到新链接也没有轮次去用了）。
            has_more_rounds = round_no < MAX_ROUNDS

            # 就地重取流：把"直链过期"这类判死失败换成新 urls，重新变得可下载。
            # 这是本脚本唯一能自行修复确定性失败的路径，等价于人工跑
            # `tmdb_ids_to_links.py --refetch-failed` 后再重跑本脚本。
            #
            # 两条路径（互斥，由运行模式决定）：
            #   - 异步（pipeline 模式）：dispatch 只投递、立即返回，主循环不阻塞；
            #     结果由**下一轮**开头的 collect() 取回并入 next_batch。
            #   - 同步（只跑下载，无取流线程）：走原有的 refetch_entries 老路。
            revived = []
            if async_refetch_hook is not None:
                # 先收上一轮（及更早）已完成的重取结果——它们是真正救回来的片，
                # 与同步路径的 revived 等价，走同一套 merge_next_batch 合并。
                try:
                    revived = async_refetch_hook.collect()
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️ 收取异步重取结果失败: {exc}", flush=True)
                    revived = []

            if AUTO_REFETCH_ENABLED and round_failed_expired and has_more_rounds:
                try:
                    if async_refetch_hook is not None:
                        # 次数上限仍由这里把关：钩子只负责投递，不认识 refetch_counts。
                        to_dispatch = []
                        for entry in round_failed_expired:
                            tid = entry.get("tmdbId")
                            if tid is None:
                                continue
                            key = str(tid)
                            if refetch_counts.get(key, 0) >= AUTO_REFETCH_MAX_PER_MOVIE:
                                continue
                            refetch_counts[key] = refetch_counts.get(key, 0) + 1
                            to_dispatch.append(entry)
                        if to_dispatch:
                            async_refetch_hook.dispatch(to_dispatch)
                            print(
                                f"\n[自动重取流] {len(to_dispatch)} 部因直链过期失败，"
                                f"已交给取流线程异步重取（不阻塞本轮下载）",
                                flush=True,
                            )
                            # 投完必须等一等再判 next_batch：dispatch 是异步的，
                            # 立刻判会发现 next_batch 为空而 break，重取成功的新
                            # 链接就没有任何轮次去消费（端到端实测复现过）。
                            # 有上限、且一有结果就提前退出，不会白等满。
                            deadline = time.time() + ASYNC_REFETCH_WAIT_SECONDS
                            while time.time() < deadline:
                                fresh = async_refetch_hook.collect()
                                if fresh:
                                    revived.extend(fresh)
                                # 在途清零即可收工，无需等满
                                if async_refetch_hook.pending_count() == 0:
                                    revived.extend(async_refetch_hook.collect())
                                    break
                                time.sleep(1)
                            if revived:
                                print(
                                    f"[自动重取流] 收回 {len(revived)} 部新直链，"
                                    f"并入下一轮",
                                    flush=True,
                                )
                    else:
                        revived = refetch_entries(round_failed_expired, refetch_counts)
                except (Exception, SystemExit) as exc:
                    # 重取是尽力而为的捞回，绝不能让它崩掉整条流水线：
                    # 失败就当作没救回，本轮其余结论照常生效。
                    # 连 SystemExit 一起兜（取流侧模块级校验用的就是它），
                    # 但放过 KeyboardInterrupt——Ctrl+C 该中止整个流程。
                    print(f"⚠️ 就地重取流异常，已跳过本轮重取: {exc}", flush=True)
                    revived = []

            # 合并两条重投路径（去重逻辑见 merge_next_batch）。
            next_batch = merge_next_batch(round_failed_retriable, revived)
            # 本轮仍未解决的过期直链：分两种情况，但都必须报出来。
            #   - 非末轮：本轮重取没成功（源站抽风/代理故障/次数用尽）。它们还有
            #     轮次预算，但如果 next_batch 恰好为空，循环会在下面提前结束——
            #     那就等于放弃了这批本可再试的片，至少要让日志说清楚。
            #   - 末轮：本就不重取（拿到新链接也没轮次可用）。
            # 无论哪种，它们都不是真淘汰：下次运行会被重新投递并再次触发重取。
            #
            # ⚠️ 异步路径下 revived 来自**更早轮次**的投递，与本轮的
            # round_failed_expired 不是同一批，相减没有意义（会算出负数或虚高）。
            # 故异步路径只报"本轮新发现的过期数"，不做差值。
            if async_refetch_hook is not None:
                expired_left = len(round_failed_expired)
                expired_note = (
                    f"，本轮另有 {expired_left} 部直链过期已投异步重取"
                    f"（结果下一轮回收；未赶上则由 results.jsonl 承接，下次运行再用）"
                    if expired_left > 0 else ""
                )
            else:
                expired_left = len(round_failed_expired) - len(revived)
                expired_note = (
                    f"，另有 {expired_left} 部直链过期未能换到新链接"
                    f"（不是真淘汰，下次运行会再试）"
                    if expired_left > 0 else ""
                )

            if not next_batch:
                if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
                    print(
                        f"\n本轮无可重试的下载失败{expired_note}，多轮下载提前结束。",
                        flush=True,
                    )
                break
            if not has_more_rounds:
                print(
                    f"\n已达最大轮次 {MAX_ROUNDS}，仍有 "
                    f"{len(round_failed_retriable)} 部下载失败未成功{expired_note}，"
                    f"停止重试。",
                    flush=True,
                )
                break

            revived_note = f"、{len(revived)} 部已换到新直链" if revived else ""
            print(
                f"\n本轮有 {len(round_failed_retriable)} 部可重试下载失败{revived_note}，"
                f"冷却 {ROUND_COOLDOWN_SECONDS}s 后进入第 {round_no + 1} 轮...",
                flush=True,
            )
            if ROUND_COOLDOWN_SECONDS > 0:
                time.sleep(ROUND_COOLDOWN_SECONDS)
            current_batch = next_batch
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
                # 同上：排空阶段单条异常也不得逃逸，否则末轮转封装/上传记录全丢。
                try:
                    handle_done_future(future, None)
                except Exception as exc:
                    print(f"⚠️ future 处理异常，已跳过该条: {exc}", flush=True)

        # 至此所有下载/转封装/上传 future 均已完成，数据完整性得到保证。
        print(
            f"三级流水线全部完成：转封装 {stats['conversions']} 部，"
            f"上传 {stats['uploads']} 部。"
        )

        # 被拒原因聚合统计（观测性）：量化各类失败占比，指导码率门槛校准。
        def _print_reject_stats(title, counter, note):
            total = sum(counter.values())
            if total == 0:
                return
            print(f"\n{title}（共 {total} 次，{note}）:")
            for reason, count in sorted(
                counter.items(), key=lambda kv: kv[1], reverse=True
            ):
                print(f"  - {reason}: {count} 次（{count / total:.1%}）")

        if reject_permanent or reject_retriable:
            print("\n===== 下载失败原因聚合统计 =====")
            _print_reject_stats(
                "确定性失败（真淘汰，绝不重试）", reject_permanent, "每片计一次"
            )
            _print_reject_stats(
                "可重试失败（瞬时错误）", reject_retriable, "跨轮重投会重复计数"
            )

        # 直链过期是**唯一一类"本脚本判死、但重新取一次流就能救回"**的失败：
        # vidlink 的签名 url 有时效，重投同一条必然再挂，所以按确定性失败处理。
        # 开启 auto_refetch 后已在轮次间就地重取（见 refetch_entries），这里只需
        # 报告最终仍未救回的量；关闭时则退回旧行为——提示用户手动跑闭环命令。
        expired_count = reject_permanent.get(_REFETCH_REASON_LABEL, 0)
        if expired_count:
            if AUTO_REFETCH_ENABLED:
                print(
                    f"\n⚠️  累计 {expired_count} 次直链失效（签名过期）。\n"
                    f"    已在轮次间自动重新取流"
                    f"（每部最多 {AUTO_REFETCH_MAX_PER_MOVIE} 次），"
                    f"本次共重取 {len(refetch_counts)} 部。\n"
                    f"    若仍有残留（末轮失效、或重取次数已用尽），"
                    f"直接重跑本脚本即可继续自愈；\n"
                    f"    也可手动跑 "
                    f"`python tmdb_ids_to_links.py --refetch-failed` 单独修复。"
                )
            else:
                print(
                    f"\n⚠️  有 {expired_count} 部因直链已失效（签名过期）而失败。\n"
                    f"    这类失败**重跑本脚本无效**（拿到的还是同一条过期 url），\n"
                    f"    需先让上游换一条新直链：\n"
                    f"        python tmdb_ids_to_links.py --refetch-failed\n"
                    f"    跑完再重新执行本脚本即可（新结果会按 fetched_at 自动选用）。\n"
                    f"    提示：把 config.yaml 的 auto_refetch.enabled 设为 true "
                    f"可让本脚本自动完成这一步。"
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
                "title": record.get("title") or "",
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
