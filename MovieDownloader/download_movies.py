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
# 【源站回源故障专用的长冷却】本轮存在"整节点采样全挂"的片时，改用本值。
#
# 为什么要分档（500 部实跑 §12.18 / 待办 I 的实测结论）：
#   - 30 部这类失败中 29 部是**单节点**片，没有备用源可切，只能等源站恢复；
#   - 实测确认是 **CDN 回源故障**而非 IP 拦截——同一条分片 url 直连与住宅代理
#     各试 3 次全是 502，且域名根路径返回 200（拦截会在根路径就 403）。
#     故换代理、换节点都无效，唯一变量是**时间**；
#   - 这类故障的恢复是**小时级**：故障后约 4 小时复测，3 部里 2 部恢复正常
#     （分片返回 200 + 2MB 数据）。而 60s × 3 轮只跨几分钟，必然全部撞墙——
#     实测代价约 5-7.5 小时无效重试，换回 0 部成功。
#
# 取 1800s 与取流侧「加时赛」冷却一致（见 config.yaml 的 final_retry），
# 那里的经验同样适用：短于此基本等于再烧一次常规轮，没有意义。
#
# ⚠️ 只在**存在**该类失败时才生效；其余失败（单流抖动、个别分片失败等）
# 仍走 60s 快速重试，不受影响——它们几十秒就恢复，拉长纯属浪费。
SOURCE_OUTAGE_COOLDOWN_SECONDS = max(
    0, int(_MULTI_ROUND_CFG.get("source_outage_cooldown_seconds", 1800))
)

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
# 【画质判死账本】跨运行持久化"因画质被确定性判死"的片，启动时并入跳过集。
#
# 为什么需要它（2026-09-11 实跑复盘，§12.26）：判死只是**进程内**状态
# （_PERMANENT_FAILURE_MARKERS 只决定"不进下一轮"，从不落盘成排除集），而下载侧
# 的跳过集只有 success.jsonl ∪ 磁盘 mp4。于是每次重跑都会把注定失败的片重新
# 投递、重新采样、重新判死一遍。run3 实测：74 部判死片 × 2~3 条候选流 × 10 段
# ≈ 2000 次分片下载，结论与上次完全一致。
#
# 🔑 三条边界（与既有账本严格分工，勿混用）：
#   - **只记画质判死**：其余确定性失败（不支持的结构、缺字段等）不进本账本——
#     它们不受门槛变更影响，也没有"日后放回"的语义，混进来只会让文件失去焦点；
#   - **与 fail.txt 语义不同**：fail.txt 是取流侧"源站明确说没这片"，本文件是
#     "有源但画质不达标"。两者绝不可互相替代（详见 tmdb_ids_to_links.py 里
#     load_processed_ids 对 unresolved.txt 的同类论述）；
#   - **可被放回**：存下判定依据（实测码率/门槛/编码/分辨率），门槛调松后由
#     `--retry-dead` 离线对比放回，无需重新采样。
DOWNLOAD_DEAD_LOG = resolve_file(_CFG.get("download_dead_log"), "download_dead.jsonl")
# `--retry-dead` 放回判死片时的余量系数（要求 新门槛 × 本值 <= 记录的实测码率）。
# 1.05 = 门槛要比记录值低 5% 以上才放回。见 dead_record_passes_now 的震荡说明。
DEAD_REVIVE_MARGIN = max(1.0, float(_CFG.get("dead_revive_margin", 1.05)))
# 运行模式标志，由 __main__ 入口按命令行参数覆写（默认都是 False = 正常全量运行）。
# 用模块级标志而非参数透传：_run_pipeline 到启动过滤点之间隔着好几层，
# 且 pipeline.py 会 import 本模块后直接调 main()，标志比改签名更不侵入。
#
#   RETRY_DEAD_MODE：对 DOWNLOAD_DEAD_LOG 里的片按当前门槛复判，够格的放回重试。
#   RETRY_ONLY_MODE：仅作语义标记与日志提示。**它不改变任何筛选逻辑**——
#     下载侧本来就是"输入 results.jsonl、跳过 success.jsonl ∪ 磁盘 ∪ 判死"，
#     这恰好就是"只重试未成功的片"。加这个开关是为了让意图在命令行里显式可见
#     （适合挂定时任务），并避免误以为要重跑 pipeline.py 才能重试。
RETRY_DEAD_MODE = False
RETRY_ONLY_MODE = False
BASE_DIR = resolve_dir(_CFG.get("base_dir"), "downloads")
# 媒体类型命名空间（movies / tvs），同时作为本地与 R2 对象键的第一层目录，
# 使两侧严格同构：
#   本地  {BASE_DIR}/movies/{year}/{tmdbId}/{tmdbId}.mp4
#   R2    {S3_PREFIX}/movies/{year}/{tmdbId}/{tmdbId}.mp4
# strip("/") 防止配置写成 "movies/" 或 "/movies" 时拼出双斜杠/前导斜杠。
FOLDER_PREFIX = (_CFG.get("folder_prefix") or "movies").strip().strip("/")

# ---- 元信息与字幕（旁车资产，与视频同目录）----
# 两者都是"锦上添花"：获取/写入失败只记日志，绝不影响整片成败。
_ASSETS_CFG = _CFG.get("assets", {}) or {}
META_ENABLED = bool(_ASSETS_CFG.get("meta_enabled", True))
SUBTITLES_ENABLED = bool(_ASSETS_CFG.get("subtitles_enabled", True))
# 语种白名单（小写语言代码）。取流侧可能给十几种语言，只留我们要的。
SUBTITLE_LANGUAGES = [
    str(lang).strip().lower()
    for lang in (_ASSETS_CFG.get("subtitle_languages") or ["en", "zh"])
    if str(lang).strip()
]
# 输出格式。vtt 供浏览器原生 <track>，srt 供本地播放器；两者由同一份源转换而来。
SUBTITLE_FORMATS = [
    fmt for fmt in (
        str(f).strip().lower().lstrip(".")
        for f in (_ASSETS_CFG.get("subtitle_formats") or ["vtt", "srt"])
    ) if fmt in ("vtt", "srt")
] or ["vtt"]
SUBTITLE_TIMEOUT = float(_ASSETS_CFG.get("subtitle_timeout", 30))
# 大小上限是防御性的：字幕正常几十 KB，若源站塞来视频/错误页，不设限会整个读进内存。
SUBTITLE_MAX_BYTES = int(_ASSETS_CFG.get("subtitle_max_bytes", 5 * 1024 * 1024))
# 影片目录下存放字幕的子目录名（与 fetch_subtitles.py 保持一致）。
SUBS_SUBDIR = "subs"

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
#
# 3 → 5（2026-09-10，与 L1 状态码重试移除同批）：原先取 3 是在 urllib3 还会
# 静默重试 2 次的前提下定的——那时单次循环底下实际有 3 个请求，3 次循环
# ≈ 9 个请求，已经够多了。现在 L1 的 status_forcelist 清空（见 get_session），
# 单次循环就是 1 个请求，3 次循环反而变成了**真的只试 3 次**，对源站几秒级
# 抖动的容错变弱、可能压低成功率。提到 5 后：
#   实际请求数 5 × 1 = 5 < 改动前的 9，**成本仍是降的**；
#   L3 退避 1+2+4+8 ≈ 15s，足以跨过短抖动，又远低于正片那套（封顶 60s）。
SAMPLE_SEG_RETRY_MAX = max(1, int(_CFG.get("sample_seg_retry_max", 5)))
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


# ---- mp4 直链的「CDN 主机级熔断」（2026-09-10，§12.21）----
# 实测确诊：hakunaymatata 的 bcdnxw 这台主机整体故障（对**任何** IP、任何签名、
# 冷却后都恒返回 429，Server 头是 nginx 而非正常的 Tengine），而同域的
# bcdn/hcdn3 一切正常。一次实跑里 341 条 vidlink url 有 153 条（45%）指向它。
#
# 没有熔断时，每部片都要把这台坏主机的节点重试一遍（还跨 3 轮），
# 白烧大量时间——实测 162 次无效 429 请求。
#
# 🔑 三条边界（避免把"省时间"做成"降成功率"）：
#   - **只作用于 mp4 直链**：m3u8 分片层的 429 仍按限流处理、照常退避重试
#     （_NO_RETRY_HTTP_STATUS 的注释明确写了 429/503 属"必须重试"一类）；
#   - **只跳过同一台主机的节点**，其余节点（含同域其它主机）照常尝试；
#   - **不判整片死**：熔断文案不进 _PERMANENT_FAILURE_MARKERS，整片仍可进
#     下一轮重投——万一主机恢复了还能救回来。
#   - 计数仅存活于**本次运行**（模块级字典，进程退出即清空），不落盘。
_mp4_host_lock = threading.Lock()
_mp4_host_429 = {}
_mp4_host_tripped = set()
# 单台主机累计多少次 429 后熔断。3 次足以区分"偶发限流"与"整机故障"：
# 真限流退避后会恢复，整机故障则次次复现。
MP4_HOST_CIRCUIT_THRESHOLD = max(
    1, int(_CFG.get("mp4_host_circuit_threshold", 3))
)
# 熔断文案。⚠️ 有意**不**加入 _PERMANENT_FAILURE_MARKERS（见上）。
_MP4_HOST_BLOCKED_MARKER = "直链主机疑似故障已熔断"


def _host_of(url):
    """取 url 的主机名；解析不出返回空串。"""
    try:
        return str(url).split("://", 1)[1].split("/", 1)[0].lower()
    except (IndexError, AttributeError):
        return ""


def _mp4_host_record_429(url):
    """记一次 mp4 直链 429；达到阈值则熔断该主机（本次运行内）。"""
    host = _host_of(url)
    if not host:
        return
    with _mp4_host_lock:
        _mp4_host_429[host] = _mp4_host_429.get(host, 0) + 1
        count = _mp4_host_429[host]
        newly_tripped = (
            count >= MP4_HOST_CIRCUIT_THRESHOLD and host not in _mp4_host_tripped
        )
        if newly_tripped:
            _mp4_host_tripped.add(host)
    if newly_tripped:
        print(
            f"  [主机熔断] {host} 累计 {count} 次 429，本次运行内跳过该主机的"
            f"全部 mp4 直链节点（其余节点不受影响）",
            flush=True,
        )


def _mp4_host_is_tripped(url):
    host = _host_of(url)
    if not host:
        return False
    with _mp4_host_lock:
        return host in _mp4_host_tripped


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
# 确定性失败里**属于"签名过期、换条新链接就能救"**的那部分（2026-09-11，§12.27）。
# request_with_retry 对这几个额外挂 _NEEDS_REFETCH_MARKER，使 m3u8 链路也能
# 进重取流闭环——此前该 marker 只在 mp4 直链的两个函数里挂，m3u8 的 token
# 过期后会被判成普通确定性失败，`--refetch-failed` 永远挑不到它。
#
# 为什么是这三个而不是全部五个：
#   401/403 鉴权失败、410 已删除 → 签名过期的典型表现，重取有意义；
#   404 资源真不存在、416 Range 越界 → 换新签名仍是同样结果，重取纯浪费配额。
_REFETCHABLE_HTTP_STATUS = frozenset({401, 403, 410})


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
        # ⚠️ 状态码重试**全部交给上层**（L3 分片层 / mp4 块层），这里只保留
        # 连接级与读取级重试。2026-09-10 实测发现的隐形叠乘：
        #
        # 原配置 status_forcelist=(429,500,502,503,504) + total=2 会让 urllib3
        # 对 5xx **静默重试 2 次**，且这层对上层完全透明——L3 日志里打印
        # "分片 X 下载失败 (1/20)" 时，底层其实已经发了 3 个请求。
        # 于是单个采样分片最坏 = 3(L3) × 3(L1) = 9 个请求，而日志只显示 3 次。
        # 上次 1000 部实跑 `502 Server Error` 出现 7038 次、采样耗尽 2720 次，
        # 两个数字对不上正是因为中间那批请求根本不可见。
        #
        # 交给 L3 的三个理由：
        #   1. L3 的退避更合理（1/2/4/8s 指数 + 抖动，封顶 60s），
        #      而 L1 的 backoff_factor=0.5 只有 0s、1s，重试过于密集；
        #   2. L3 可被 `interrupted` / `abort_event` 打断，L1 的退避叫不醒；
        #   3. L3 每次重试都有日志，L1 完全静默，排障时看不见真实请求量。
        #
        # 🔑 顺带修掉一个新引入的问题：429 留在 forcelist 里会让 mp4 主机熔断
        # （§12.21）延迟生效——我们想"立刻换节点"，urllib3 却会先自己重试 2 次。
        # 移除后熔断真正做到即时短路。
        #
        # connect/read 保留：连接重置、握手失败这类 socket 级抖动在同一条连接上
        # 立即重试很划算，且不像 5xx 那样会被上层重复覆盖。
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            status=0,               # 状态码一律不在本层重试
            backoff_factor=0.5,
            status_forcelist=(),    # 空：5xx/429 全部上抛给 L3 处理
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

    🔑 其中 401/403/410 额外挂 `_NEEDS_REFETCH_MARKER`（2026-09-11，§12.27）：
    本函数是 **m3u8 链路全部 HTTP 请求的唯一入口**（master playlist /
    media playlist / 分片下载三处），而 peakstorm 系的 m3u8 地址是
    `.../vd/<token>/master.m3u8` 这种带签名 token 的形式，**token 会过期**。
    过期后整条链路只会报"确定性失败"，`--refetch-failed` 挑不到它 ——
    跨运行重试间隔若是几天，大批片会因 token 过期而永久卡死，重试完全失去意义。
    挂上 marker 后，这些片能走既有的重取流闭环换到新链接。

    为什么只挑 401/403/410 而不含 404/416：
      - 401/403 鉴权失败、410 资源已删除 → 典型的签名过期表现，重取有意义；
      - 404 分片真不存在、416 Range 越界 → 换个新签名还是同样结果，
        重取纯属浪费取流配额（`needs_refetch` 的文档也是这个口径）。
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
                # 签名过期型（401/403/410）额外挂重取 marker，让 m3u8 链路
                # 也能进重取流闭环（见本函数文档）。404/416 有意不挂。
                suffix = (
                    f"，{_NEEDS_REFETCH_MARKER}"
                    if status in _REFETCHABLE_HTTP_STATUS else ""
                )
                raise RuntimeError(
                    f"请求失败({_HTTP_PERMANENT_MARKER} HTTP {status})"
                    f"{suffix}: {url}; {exc}"
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

    目录结构 {BASE_DIR}/{FOLDER_PREFIX}/{year}/{tmdbId}/{tmdbId}.mp4，故按
    「year 层 -> tmdbId 层」两级 scandir，**ID 取自目录名**（不再从文件名反解），
    这样同目录下的 meta.json / subs/ 等非视频资产天然不参与去重判定。

    返回 (ID 集合, 重复文件字典)。同一 ID 出现在多个 year 目录时只报告，
    不自动删除已有文件。

    顺带清理 0 字节 mp4：那是移动中断留下的残骸，既不是有效成品也不该被
    误判为"已下载"而永久跳过该片。只删大小为 0 的，有内容的一律不动。
    """
    downloaded_ids = set()
    locations = {}
    orphan_count = 0

    root = os.path.join(BASE_DIR, FOLDER_PREFIX) if FOLDER_PREFIX else BASE_DIR
    if not os.path.isdir(root):
        return downloaded_ids, {}

    try:
        year_entries = list(os.scandir(root))
    except OSError as exc:
        print(f"警告: 无法扫描目标目录 {root}: {exc}")
        return downloaded_ids, {}

    for year_entry in year_entries:
        if not year_entry.is_dir(follow_symlinks=False):
            continue

        try:
            movie_entries = list(os.scandir(year_entry.path))
        except OSError as exc:
            print(f"警告: 无法扫描目录 {year_entry.path}: {exc}")
            continue

        for movie_entry in movie_entries:
            if not movie_entry.is_dir(follow_symlinks=False):
                continue

            tmdb_id = normalize_tmdb_id(movie_entry.name)
            if not tmdb_id:
                continue

            mp4_path = os.path.join(movie_entry.path, f"{tmdb_id}.mp4")
            try:
                size = os.stat(mp4_path, follow_symlinks=False).st_size
            except OSError:
                # 视频不存在（只有 meta/字幕，或目录空）：不算已下载。
                continue

            if size <= 0:
                # 0 字节孤儿：移动中断留下的残骸，直接清掉。
                remove_file(mp4_path)
                orphan_count += 1
                continue

            downloaded_ids.add(tmdb_id)
            locations.setdefault(tmdb_id, []).append(mp4_path)

    if orphan_count:
        print(f"已清理 {orphan_count} 个 0 字节 mp4 孤儿（移动中断留下的残骸）")

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
    # 429 整机故障（§12.21）：实测换 IP/换签名/冷却后恒定 429，退避 20 次纯空耗。
    # ⚠️ 它**不在** _PERMANENT_FAILURE_MARKERS 里——只短路本层重试、尽快换节点，
    # 整片仍可进下一轮重投（主机万一恢复还能救回）。
    _MP4_HOST_BLOCKED_MARKER,
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


# ---------- 画质判死的跨运行持久化（DOWNLOAD_DEAD_LOG） ----------
# 从判死文案里回抽实测码率与门槛。两种措辞都由 bitrate_reject_message 生成：
#   模式 A：分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps
#   模式 B：码率未达到门槛：372 kbps < 1600 kbps（h264，实测 854x480）
# 故一条正则同时覆盖两者：只锚定"码率未达到门槛：N kbps < M kbps"这段公共前缀。
#
# ⚠️ 抽出来的是**末节点**的数值。外层判死文案只嵌 `末节点错误`（见判死处注释），
# 前面节点的实测值在落盘时已不存在。这让 --retry-dead 偏保守（多节点片可能漏救
# 几部），但绝不会误救——不会把仍不达标的片放回去白跑。
_DEAD_BITRATE_RE = re.compile(
    r"码率未达到门槛[：:]\s*([0-9.]+)\s*kbps\s*<\s*([0-9.]+)\s*kbps"
)
# 实测分辨率与编码：模式 B 写在尾部括号里，模式 A 写在开头。两条各自可选。
_DEAD_RESOLUTION_RE = re.compile(r"实测 (\d+x\d+)")
_DEAD_MODE_A_RE = re.compile(r"分辨率 (\d+x\d+) 流（([^）]+)）")
_DEAD_CODEC_B_RE = re.compile(r"kbps（([^，]+)，实测")


def parse_dead_quality_evidence(error_msg):
    """从画质判死文案里抽出判定依据，供门槛变更后离线复判。

    返回 dict（可能只含部分键）或 None（文案里没有码率数值）。
    没有数值的判死——例如"分辨率低于红线"、或全部节点都只报"候选流无一入选"
    的汇总——一律返回 None：没有依据可存，`--retry-dead` 对它们只能整片放回。
    """
    if not error_msg:
        return None
    match = _DEAD_BITRATE_RE.search(error_msg)
    if not match:
        return None
    evidence = {
        "bitrate_kbps": float(match.group(1)),
        "threshold_kbps": float(match.group(2)),
    }
    mode_a = _DEAD_MODE_A_RE.search(error_msg)
    if mode_a:
        evidence["resolution"] = mode_a.group(1)
        evidence["codec"] = mode_a.group(2)
    else:
        resolution = _DEAD_RESOLUTION_RE.search(error_msg)
        if resolution:
            evidence["resolution"] = resolution.group(1)
        codec = _DEAD_CODEC_B_RE.search(error_msg)
        if codec:
            evidence["codec"] = codec.group(1)
    return evidence


# 画质类的归类名（取自 _REJECT_REASON_RULES 的类目名，改那边要同步改这里）。
# 用类目名而非裸 marker：判据演进时 _REJECT_REASON_RULES 是唯一改动点。
_QUALITY_REJECT_CATEGORIES = frozenset({
    "画质整体不达标(判死)",
    "分辨率低于红线",
    "码率未达门槛",
})


def is_quality_dead(retriable, error_msg):
    """这条失败是否属于"因画质被确定性判死"。

    两个条件都要满足：确定性失败（retriable=False）**且**文案是画质类。
    只看文案不够——可重试失败的文案里也可能带画质 marker（多节点片里某个节点
    画质淘汰、另一个节点 502，整片仍可重试）；只看 retriable 更不够，会把
    "不支持的播放列表结构"这类与门槛无关的确定性失败也记进来。
    """
    if retriable:
        return False
    return classify_reject_reason(error_msg) in _QUALITY_REJECT_CATEGORIES


def load_dead_ids(threshold_fn=None):
    """读 DOWNLOAD_DEAD_LOG，返回要跳过的 tmdbId 集合。

    threshold_fn 为 None（默认，正常运行）：全部判死片一律跳过。
    传入函数时（`--retry-dead`）：对每条记录用当前门槛复判，
    **该 id 的全部记录都判定"可以放回"时才放回**（合取语义）。

    🔑 为什么必须按 id 聚合（2026-09-11 代码审查发现）：
    账本是纯追加的，同一部片可能有多行（重取换链接后再次判死、
    `--retry-dead` 放回后又判死）。原先逐行 add/discard 处理，
    **同一 id 的结果取决于哪一行排在最后** —— 一行有依据判"不放回"、
    另一行空依据判"放回"，最终结论随追加顺序漂移，不确定。
    改成合取后：只要有任何一条记录证明它现在仍不达标，就继续跳过。
    这与"宁可多下不误杀"不冲突——那条口径针对的是**无依据**的记录，
    而这里是"有依据且证明不达标"。

    缺依据的记录（parse_dead_quality_evidence 返回 None 时落盘的那些）在
    `--retry-dead` 下一律放回——没有依据就无法证明它现在仍不达标。
    """
    if not os.path.exists(DOWNLOAD_DEAD_LOG):
        return set()

    # 先按 id 归拢全部记录，再统一裁决——避免逐行覆盖带来的行序依赖。
    records_by_id = {}
    with open(DOWNLOAD_DEAD_LOG, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            tmdb_id = normalize_tmdb_id(record.get("tmdbId"))
            if not tmdb_id:
                continue
            records_by_id.setdefault(tmdb_id, []).append(record)

    if threshold_fn is None:
        return set(records_by_id)

    dead = set()
    revived = 0
    for tmdb_id, records in records_by_id.items():
        # 合取：任一条记录判定"仍不达标"，该片就继续跳过。
        if all(threshold_fn(record) for record in records):
            revived += 1
        else:
            dead.add(tmdb_id)
    if revived:
        print(f"[判死复判] {revived} 个 ID 在当前门槛下不再判死，已放回重试队列")
    return dead


def dead_record_passes_now(record):
    """当前配置门槛下，这条判死记录是否该被放回重试。

    口径：用记录里的**实测码率**对比**现在算出来的**门槛。门槛按记录的
    分辨率高度与编码重算（而非沿用记录里的旧门槛），这样 bitrate_* 与
    leniency 任一处改动都能被识别到。

    ⚠️ 留 DEAD_REVIVE_MARGIN 余量：实测码率是**那次采样窗口**的值，本身有波动。
    门槛恰好压在记录值上时放回，重新采样很可能测出略低的值又被判死一次，
    形成来回震荡、每轮都白跑。要求"新门槛 × 余量 <= 记录值"才放回。

    🔴 两个必须挡住的坑（2026-09-11 代码审查发现）：

    1. **跨模式复判**：`bitrate_threshold` 的行为由 RESOLUTION_CHECK_ENABLED
       决定（模式 A 按 (h/1080)² 缩放、模式 B 用绝对线），两者门槛能差 5 倍。
       拿当前模式去复判另一个模式下判死的记录，结论没有意义。
       故记录里存了判死当时的模式，**不一致就不放回**（保守）。
       老记录没有该字段 → 视为未知 → 同样不放回，避免按错误口径误放。

    2. **height 缺失时门槛归零**：模式 B 下探测不到分辨率会写成"未知分辨率"，
       正则抽不出 `\\d+x\\d+`，height 落为 0。此时模式 A 的
       `(0/1080)² = 0` 会让门槛恒为 0，`0 × 余量 <= 任何码率` **恒真** ——
       这批记录会被无条件放回。而实跑中"未知分辨率"占比高达 44%（§12.18），
       量级不小。故 height 缺失时在模式 A 下**一律不放回**。
    """
    evidence = record.get("evidence") or {}
    bitrate = evidence.get("bitrate_kbps")
    if bitrate is None:
        # 没有依据 → 无法证明现在仍不达标，放回（见 load_dead_ids 文档）。
        return True

    # 坑 1：判死时的模式必须与当前一致，否则门槛口径不可比。
    recorded_mode = record.get("resolution_check_enabled")
    if recorded_mode is None or bool(recorded_mode) != RESOLUTION_CHECK_ENABLED:
        return False

    resolution = str(evidence.get("resolution") or "")
    height = 0
    if "x" in resolution:
        try:
            height = int(resolution.split("x")[1])
        except (ValueError, IndexError):
            height = 0
    # 坑 2：模式 A 下 height=0 会让门槛归零 → 恒真放回。保守起见不放回。
    if height <= 0 and RESOLUTION_CHECK_ENABLED:
        return False

    current = bitrate_threshold(height, evidence.get("codec"))
    return current * DEAD_REVIVE_MARGIN <= float(bitrate)


def record_quality_dead(tmdb_id, title, error_msg, urls=None):
    """把一部画质判死片记进 DOWNLOAD_DEAD_LOG（纯追加）。

    同一片可能在不同运行里被记多次（例如 --retry-dead 放回后再次判死、
    或重取换链接后新节点仍不达标）。不做去重：写入端保持纯追加，
    **合并规则放在读取端** `load_dead_ids`（按 tmdbId 聚合），
    避免去重要重写整个文件、与多线程共用的 write_log 冲突。

    `resolution_check_enabled` 必须落盘：画质门槛的计算方式完全由它决定，
    不记下来就无法判断日后的复判是否同口径（见 dead_record_passes_now）。
    """
    write_log(DOWNLOAD_DEAD_LOG, {
        "tmdbId": tmdb_id,
        "title": title or "",
        "dead_at": int(time.time()),
        "reason_class": "quality",
        # 判死当时的画质判定模式，复判时用来确认口径可比。
        "resolution_check_enabled": RESOLUTION_CHECK_ENABLED,
        "error": error_msg,
        "evidence": parse_dead_quality_evidence(error_msg) or {},
        "node_count": len(urls or []),
    })


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

    ⚠️ `auto_refetch.enabled: false` 时两个桶都会是空的（§12.27 D）：
    `_NEEDS_REFETCH_MARKER` 同时在 `_PERMANENT_FAILURE_MARKERS` 里，故带
    marker 的失败 retriable=False；重取再一关，这批片当轮就既不重投也不重取。
    **这是该开关有意的语义**——config 里写明"关掉即等人工 `--refetch-failed`
    处理"，此处不做自动兜底，以免把"人工介入"的选择悄悄改掉。
    但 §12.27 给 m3u8 链路也挂上 marker 后，受影响面从"少量 mp4 签名过期"
    扩大到"所有 m3u8 的 401/403/410"，**关掉该开关的代价比以前大得多**，
    config.yaml 的 auto_refetch 处已补上对应警告。
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


# 「整节点采样全挂」的错误 marker。这类失败的特征是：候选流一条都没选中，
# 且淘汰原因不是画质而是采样阶段拿不到数据（源站 5xx）。
#
# 为什么认这一条就够：画质淘汰走的是 QualityRejectedError，落盘文案含
# "因画质不达标"；只有混合/纯瞬时异常才会落到这句兜底文案上（见
# process_one_entry 里 `if not best_selected` 的两个分支）。
_SOURCE_OUTAGE_MARKER = "候选流无一入选"


def is_source_outage(error_msg):
    """这条失败是不是「源站回源故障」型（决定轮次冷却走长档还是短档）。

    判据只认「整节点采样全挂」：候选流一条都没入选，且不是画质原因。
    实测（§12.18 / 待办 I）这类失败 96.7% 发生在**单节点**片上——没有备用源
    可切，换代理也无效（已实测：直连与 3 个住宅 IP 全是 502），唯一的变量是
    时间，故只能靠拉长冷却去跨越源站的恢复窗口。

    ⚠️ 刻意**不**把 "源站5xx"、"超时" 这类也算进来：它们多是单条流/单个分片的
    瞬时抖动，几十秒就恢复，拉长冷却纯属浪费。只有"整个节点的所有候选流都
    采不到数据"才够格判定为源站级故障。
    """
    return bool(error_msg) and _SOURCE_OUTAGE_MARKER in error_msg


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
                # captions 同属取流产物，必须跟着一起更新：重取那次可能比原先
                # 多拿到（或少拿到）字幕，沿用旧值会让字幕与 urls 来自不同批次。
                new_entry["captions"] = result.get("captions") or []
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


def year_segment(year):
    """把发布年份规范成路径里的一段。

    只保留数字：防脏数据（含 '/'、空格等）拼出畸形路径 / 多层意外目录。
    提取失败或缺失时兜底 unknown_year。本地目录与 R2 对象键共用此函数，
    保证两侧的 year 段永远一致（否则 local_path 与 s3_key 无法互相换算）。
    """
    digits = re.sub(r"\D", "", str(year)) if year not in (None, "") else ""
    return digits if digits else "unknown_year"


def asset_rel_path(tmdb_id, year, asset=None):
    """同一部影片全部资产的公共相对路径：{FOLDER_PREFIX}/{year}/{tmdbId}[/{asset}]。

    这是本地目录与 R2 对象键的**唯一真实来源**：本地把它接在 BASE_DIR 后、
    R2 把它接在 S3_PREFIX 后，两侧因此严格同构、可互相换算。

    asset 为 None 时返回影片目录本身；否则返回目录下某个资产的相对路径
    （如 "12345.mp4"、"meta.json"、"subs/en.srt"）。
    """
    parts = [FOLDER_PREFIX, year_segment(year), str(tmdb_id)]
    if asset:
        parts.append(str(asset).strip("/"))
    return "/".join(part for part in parts if part)


def _file_size(path):
    """返回文件字节数；取不到返回 None（不抛异常，调用方按缺失处理）。"""
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def bytes_to_gb(num_bytes):
    """字节 -> GB，**十进制 1000 进制**（1 GB = 1000^3 字节）。

    刻意不用 1024：存储厂商与 R2 的计费口径都是十进制，用 1024 算出来的数
    比账单小约 7%，对不上账。要 1024 进制的话那个单位叫 GiB，不是 GB。
    """
    if not num_bytes:
        return 0.0
    return num_bytes / 1_000_000_000


def format_size(num_bytes):
    """把字节数格式化成便于阅读的十进制单位字符串。"""
    if not num_bytes:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    value = float(num_bytes)
    for unit in units:
        if value < 1000 or unit == units[-1]:
            return f"{value:.2f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1000
    return f"{value:.2f} PB"


def new_upload_volume():
    """新建一份上传容量累加器。

    三个字段各司其职，缺一不可：
      - uploaded_bytes：已知大小的总字节；
      - uploaded_sized：贡献了字节数的片数（**平均值的分母**）；
      - uploaded_unsized：进了 R2 但取不到大小的片数。单列出来是为了让
        "总量偏小"有据可查——否则少算多少、少算了几部，事后无从追溯。
    """
    return {"uploaded_bytes": 0, "uploaded_sized": 0, "uploaded_unsized": 0}


def add_upload_volume(volume, size):
    """把一部**确已进入 R2** 的成品计入累加器。size 取不到时记 unsized。"""
    if size:
        volume["uploaded_bytes"] += size
        volume["uploaded_sized"] += 1
    else:
        volume["uploaded_unsized"] += 1


def merge_upload_volume(target, other):
    """累加器求和。用于把"主流程"与"收尾补传"两段合成本次运行的总量。

    两段天然不相交：补传只处理主流程里 uploaded=False 的片，故直接相加
    不会重复计数（见 main() 收尾处的说明）。
    """
    for key in target:
        target[key] += other.get(key, 0)
    return target


def print_upload_volume(label, volume):
    """打印一段上传容量。无任何上传（含 unsized）时整段静默，不刷屏。"""
    if not (volume["uploaded_sized"] or volume["uploaded_unsized"]):
        return
    total_bytes = volume["uploaded_bytes"]
    print(
        f"{label} {volume['uploaded_sized']} 部，"
        f"总大小 {bytes_to_gb(total_bytes):.2f} GB"
        f"（{format_size(total_bytes)}，十进制 1 GB = 1000³ 字节）"
    )
    if volume["uploaded_unsized"]:
        print(
            f"  ⚠️ 另有 {volume['uploaded_unsized']} 部未能取到文件大小，"
            f"未计入上述总量"
        )


def build_s3_key(tmdb_id, year=None, asset=None):
    """把一部影片的某个资产映射为 R2 对象键。

    规则：{S3_PREFIX}/{FOLDER_PREFIX}/{发布年份}/{tmdbId}/{资产文件名}
    如 tmdbId 12345、发布年份 2000、资产 12345.mp4 ->
        movies/2000/12345/12345.mp4
    year 缺失时用 unknown_year 兜底，避免拼出畸形 key。

    与旧版的关键差异：**对象键不再含上传日期**。日期段会把同一部片分多次
    上传的视频/元信息/字幕切散到不同前缀下，前端无法按同前缀一次取全；去掉
    后同 key 重传即覆盖（幂等），正是补传想要的语义。
    """
    asset = asset if asset is not None else f"{tmdb_id}.mp4"
    rel = asset_rel_path(tmdb_id, year, asset)
    return f"{S3_PREFIX}/{rel}" if S3_PREFIX else rel


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


def movie_dir(tmdb_id, year):
    """本地影片目录的绝对路径：{BASE_DIR}/{FOLDER_PREFIX}/{year}/{tmdbId}。

    视频、meta.json、subs/ 全部落在这里，与 R2 侧 build_s3_key 的前缀同构。
    """
    return os.path.join(BASE_DIR, *asset_rel_path(tmdb_id, year).split("/"))


def move_to_target_folder(temp_mp4, tmdb_id, year=None):
    """把转封装好的成品移到 {BASE_DIR}/{FOLDER_PREFIX}/{year}/{tmdbId}/{tmdbId}.mp4。

    shutil.move 同时支持跨文件系统移动。

    每部影片独占一个目录，故**不需要任何全局锁**：
      - 目录名由 (tmdbId, year) 唯一决定，不存在"选哪个桶"的共享决策；
      - 同一 tmdbId 的并发已由 processing_ids/processing_lock 挡在上游，
        同一目录不会有两个 worker 同时写。
    这也一并去掉了旧桶号方案里的 0 字节占位文件 —— 占位只是为了让并发的
    worker 在锁内计数时能看见彼此，新结构下无人需要计数。
    """
    folder_path = movie_dir(tmdb_id, year)
    os.makedirs(folder_path, exist_ok=True)
    final_path = os.path.join(folder_path, f"{tmdb_id}.mp4")

    print(f"  [{tmdb_id}] 正在移动到: {final_path}", flush=True)
    # 失败时清掉半成品，交由上层按转封装失败处理。
    # 跨文件系统时 shutil.move 是 copy+del，若 copy 中途失败（目标盘写满/IO
    # 错误）会在 final_path 留下半成品 mp4：它不在 cleanup_paths、去重表也无
    # 登记，会成孤儿。
    try:
        shutil.move(temp_mp4, final_path)
    except Exception:
        remove_file(final_path)
        raise
    return final_path


# ---------- 旁车资产：meta.json 与字幕 ----------
# 这两类资产与视频落在同一个影片目录下，R2 对象键也同前缀，前端按同前缀一次
# 列举即可拿全。**全部按"尽力而为"处理**：任何失败只打印并记录，绝不抛到调用
# 方——一部已经下好的片不该因为元信息/字幕这种附属物被判失败而重跑整个下载。

# 字幕时间轴行（cue timing）。整行匹配、一次拿下起止两端，两端各自的时/分/秒
# 结构用 `[\d:]+` 宽松描述 —— 实测同一个文件里会**混用两种形态**：
#     00:34.958        （MM:SS.mmm，省略小时，1303 行）
#     01:02:03.958     （HH:MM:SS.mmm，1083 行）
# 早先按 `\d{1,2}:\d{2}:\d{2}` 写死三段式，省略小时的那一半完全匹配不到，
# 分隔符没被替换，那批字幕在播放器中直接失效（真实 bug，已修）。
#
# 只改时间轴行、不碰正文（正文里的 "1.500 dollars" 这类小数必须原样保留），
# 所以用 ^...$ + MULTILINE 锚定整行，而不是去局部替换"数字.数字"。
# 行尾允许跟 VTT 的 cue 设置（如 "align:start line:0%"），原样保留。
_CUE_TIMING_RE = re.compile(
    r"^([\d:]+)([.,])(\d{1,3})(\s*-->\s*)([\d:]+)([.,])(\d{1,3})(.*)$",
    re.MULTILINE,
)


def _normalize_timecode_ms(text, separator):
    """把时间轴行的毫秒分隔符统一成 separator（'.' 给 VTT，',' 给 SRT）。

    毫秒不足 3 位右侧补零（野生字幕里 "00:00:01,5" 确实存在），否则播放器会把
    .5 当成 5 毫秒或直接解析失败。
    """
    def fix(match):
        start, _, start_ms, arrow, end, _, end_ms, rest = match.groups()
        return (f"{start}{separator}{start_ms:0<3.3}{arrow}"
                f"{end}{separator}{end_ms:0<3.3}{rest}")

    return _CUE_TIMING_RE.sub(fix, text)


def _decode_subtitle(raw):
    """字幕编码很杂，逐个尝试常见编码，最终统一输出 UTF-8 文本。"""
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "big5", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def srt_to_vtt(text):
    """SRT -> WebVTT。纯文本转换，不需要 ffmpeg。

    浏览器原生 <track> 只认 WebVTT。两者结构几乎一致，差异只有三处：
      1. 必须有 "WEBVTT" 文件头；
      2. 时间轴的毫秒分隔符是 '.' 而非 ','；
      3. 序号行可留可去（VTT 里是可选的 cue 标识），故原样保留。
    """
    body = text.replace("\r\n", "\n").replace("\r", "\n").strip("\ufeff")
    body = _normalize_timecode_ms(body, ".")
    if body.lstrip().upper().startswith("WEBVTT"):
        return body
    return "WEBVTT\n\n" + body.lstrip("\n")


def vtt_to_srt(text):
    """WebVTT -> SRT：去掉 WEBVTT 头与 NOTE/STYLE 块，毫秒分隔符换回逗号。"""
    body = text.replace("\r\n", "\n").replace("\r", "\n").strip("\ufeff").strip()
    blocks = []
    for block in re.split(r"\n{2,}", body):
        head = block.lstrip().upper()
        # WEBVTT 文件头与元数据块在 SRT 里没有对应物，直接丢弃。
        if head.startswith(("WEBVTT", "NOTE", "STYLE", "REGION")):
            continue
        blocks.append(block.strip())

    out = []
    for index, block in enumerate(blocks, 1):
        block = _normalize_timecode_ms(block, ",")
        lines = block.split("\n")
        # VTT 的 cue 标识行是可选的；没有序号时补上，SRT 要求序号必须存在。
        if lines and "-->" in lines[0]:
            lines.insert(0, str(index))
        out.append("\n".join(lines))
    return "\n\n".join(out) + "\n" if out else ""


def _fetch_caption_text(caption):
    """下载一条字幕并解码成文本。返回 (文本, 源格式) 或 None。

    🔑 请求头策略：**默认沿用 Session 的取流站头，只由条目自带的 headers 覆盖**。
    两套 CDN 的鉴权方向完全相反，2026-09-11 实测（temp/verify_caption_headers.py）：
      - peakstorm（vidup/videasy 的 m3u8 与其 subs/*.vtt）：
        带 vidup Referer -> 200；去掉 Referer -> **403**。它**要求** Referer。
      - hakunaymatata（vidlink 的 mp4 直链）：
        带任何 Referer -> 429；okhttp UA + 无 Referer -> 206。它**拒绝** Referer。
    所以不能一刀切去掉 Referer —— 那会把占绝大多数的 peakstorm 字幕全打成 403。
    取流侧已把每条字幕该用的头随 caption 一起存下（vidlink 带 okhttp UA），
    这里让它覆盖默认值即可，两种 CDN 都能正确工作。

    流式读取而非一次性 .content：大小上限必须在**下载过程中**生效，否则源站
    误给一个几 GB 的地址时，整个响应会先进内存（转封装池 8 线程并发即 OOM），
    上限检查形同虚设。
    """
    url = caption.get("url")
    if not url:
        return None

    # 仅在条目显式给出 headers 时才覆盖（vidlink: okhttp UA + Referer=None）。
    headers = dict(caption.get("headers") or {})

    with get_session().get(
        url, timeout=SUBTITLE_TIMEOUT, headers=headers or None, stream=True
    ) as response:
        response.raise_for_status()
        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if not chunk:
                continue
            total += len(chunk)
            # 超限立刻中断连接，不把剩余内容拉完。
            if total > SUBTITLE_MAX_BYTES:
                raise ValueError(
                    f"字幕体积超过上限 {SUBTITLE_MAX_BYTES} 字节，疑似非字幕内容"
                )
            chunks.append(chunk)

    raw = b"".join(chunks)
    if not raw.strip():
        raise ValueError("字幕内容为空")
    text = _decode_subtitle(raw)
    # 一律按**内容**判定格式，不信源站声明的 type。
    #
    # 2026-09-11 实测：subs.api9str25.cfd 的条目声明 type="vtt"，正文却是标准
    # SRT（首行是序号 "1"，时间轴用逗号）。旧逻辑只在 type 不是 srt/vtt 时才
    # 嗅探内容，于是这批被当成 vtt 原样存进 .vtt —— 而 WebVTT 规范要求文件必须
    # 以 "WEBVTT" 开头，缺了它浏览器 <track> 直接拒绝加载。R2 实测 42 个 vtt
    # 里有 8 个（19%）是这样的废文件。
    # 内容是事实，声明只是传闻，冲突时信事实。
    fmt = "vtt" if text.lstrip().upper().startswith("WEBVTT") else "srt"
    return text, fmt


def save_subtitles(tmdb_id, year, captions):
    """下载 captions 里白名单语种的字幕，按 SUBTITLE_FORMATS 落到 {影片目录}/subs/。

    返回已保存的相对路径列表（相对影片目录），供 meta.json 与上传阶段使用。
    单条字幕失败只跳过它自己，不影响其它语种，更不影响整片。
    """
    if not SUBTITLES_ENABLED or not captions:
        return []

    wanted = {}
    for caption in captions:
        if not isinstance(caption, dict):
            continue
        lang = str(caption.get("language") or "").strip().lower()
        # 同语种可能有多条（不同压制组），保留第一条。
        if lang in SUBTITLE_LANGUAGES and lang not in wanted:
            wanted[lang] = caption
    if not wanted:
        return []

    target_dir = os.path.join(movie_dir(tmdb_id, year), SUBS_SUBDIR)
    saved = []
    for lang, caption in wanted.items():
        try:
            fetched = _fetch_caption_text(caption)
            if not fetched:
                continue
            text, source_format = fetched
            # 一份源转出两种格式：避免为同一语种下载两次。
            variants = {}
            if source_format == "srt":
                variants["srt"] = text
                variants["vtt"] = srt_to_vtt(text)
            else:
                variants["vtt"] = text
                variants["srt"] = vtt_to_srt(text)

            os.makedirs(target_dir, exist_ok=True)
            for fmt in SUBTITLE_FORMATS:
                content = variants.get(fmt)
                if not content or not content.strip():
                    continue
                path = os.path.join(target_dir, f"{lang}.{fmt}")
                with open(path, "w", encoding="utf-8", newline="") as fh:
                    fh.write(content)
                saved.append(f"{SUBS_SUBDIR}/{lang}.{fmt}")
        except Exception as exc:  # noqa: BLE001 - 字幕失败绝不影响整片
            print(f"  [{tmdb_id}] ⚠️ 字幕 {lang} 获取失败（已跳过）: {exc}",
                  flush=True)

    if saved:
        print(f"  [{tmdb_id}] 已保存字幕: {', '.join(saved)}", flush=True)
    return saved


def build_meta(entry, success_info, subtitle_files):
    """组装 meta.json 的内容：取流侧已有的元数据 + 本次实测的技术参数。

    全部字段都来自已有数据，不发起任何额外网络请求。
    """
    tmdb_id = success_info.get("tmdbId")
    return {
        "tmdbId": tmdb_id,
        "imdbId": entry.get("imdb_id"),
        "title": success_info.get("title") or "",
        "originalTitle": entry.get("original_title"),
        "year": success_info.get("year"),
        "runtimeMinutes": entry.get("runtime_minutes"),
        "genres": entry.get("genres"),
        "titleType": entry.get("title_type"),
        # 本次下载的实测结果，供前端选播放档位/排查画质问题。
        "video": {
            "file": f"{tmdb_id}.mp4",
            "resolution": success_info.get("resolution"),
            "bitrateKbps": success_info.get("bitrate_kbps"),
            "sizeBytes": success_info.get("file_size_bytes"),
            "missingSegmentCount": success_info.get("missing_segment_count"),
        },
        "subtitles": [
            {"language": os.path.basename(path).rsplit(".", 1)[0],
             "format": path.rsplit(".", 1)[-1],
             "path": path}
            for path in subtitle_files
        ],
        "generatedAt": int(time.time()),
    }


def save_meta(tmdb_id, year, meta):
    """把 meta.json 写进影片目录，返回路径；失败返回 None（不影响整片）。"""
    if not META_ENABLED:
        return None
    try:
        folder = movie_dir(tmdb_id, year)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, "meta.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
        return path
    except Exception as exc:  # noqa: BLE001 - 元信息失败绝不影响整片
        print(f"  [{tmdb_id}] ⚠️ meta.json 写入失败（已跳过）: {exc}", flush=True)
        return None


# ---------- M3U8 解析 ----------
def _assert_playlist_body(text, url):
    """响应体必须是一份 m3u8；否则按"签名过期/源站异常"抛错并挂重取标记。

    🔑 为什么必须有这一关（2026-09-11 实测，§12.29）：
    videasy 的 m3u8 地址带时效 token，**过期后不是回 403/410，而是**

        HTTP 200 | text/html | 16 字节 | 正文 "error time check"

    状态码是 200，`request_with_retry` 视为成功、原样返回响应体；
    `parse_master_playlist` 逐行找不到 `#EXT-X-STREAM-INF` 就返回空列表，
    上层抛出"没有找到媒体播放列表或清晰度变体" —— 这条文案在
    `_PERMANENT_FAILURE_MARKERS` 里，于是**一个只是 token 过期的片被当成
    "源站根本没有这部片"永久淘汰，既不重投也不进重取桶**。
    §12.27 按状态码挂 marker 的做法完全拦不住它（那是 200）。
    实测一次跨运行重试里这样丢掉了 22 部片（占终局失败的 71%）。

    判据刻意用"**是不是 m3u8**"而不是匹配 `error time check` 这个具体文案：
    后者是某一家源站此刻的实现细节，换源站或改措辞就失效；而"拿回来的东西
    不是播放列表"这个事实，对任何 provider 都等价于"这条 url 现在用不了"。

    ⚠️ 只判"根本不是 m3u8"，**不判"是 m3u8 但没有变体"** —— 后者是真的
    没有媒体列表（源站确实只上架了空 master），属确定性失败，保持判死。
    """
    stripped = (text or "").strip()
    if stripped[:7].upper() == "#EXTM3U":
        return
    # 不是播放列表：可能是空体、HTML 验证页、纯文本错误码、JSON 报错。
    # 一律视为"这条签名 url 已不可用"，交给重取流闭环换新链接。
    preview = stripped[:60].replace("\n", " ") or "(空响应体)"
    raise RuntimeError(
        f"播放列表不是 m3u8（疑似签名过期或源站异常）"
        f"，{_NEEDS_REFETCH_MARKER}: {url}; 响应开头: {preview}"
    )


def parse_master_playlist(master_url, retries=None, headers=None):
    """返回 [(resolution, media_playlist_url, declared_bandwidth_kbps), ...]。

    retries 为 None 时用默认强度 PLAYLIST_RETRY_MAX；方案C fallback 里对
    非末节点传更小的值，以便坏节点快速判定并换下一个备用节点。
    headers 为取流阶段记录的节点专属请求头，为空时用全局 HEADERS。

    响应体不是 m3u8 时抛带重取标记的错误（见 `_assert_playlist_body`）。
    """
    text = request_with_retry(
        "GET", master_url, as_text=True,
        retries=PLAYLIST_RETRY_MAX if retries is None else retries,
        backoff=PLAYLIST_RETRY_BACKOFF,
        backoff_max=PLAYLIST_RETRY_BACKOFF_MAX,
        headers=headers,
    )
    _assert_playlist_body(text, master_url)
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

    响应体不是 m3u8 时抛带重取标记的错误（见 `_assert_playlist_body`）——
    master 与 media 两次请求用的是同一个 token，前者过了不代表后者也过。
    """
    text = request_with_retry(
        "GET", playlist_url, as_text=True,
        retries=PLAYLIST_RETRY_MAX,
        backoff=PLAYLIST_RETRY_BACKOFF,
        backoff_max=PLAYLIST_RETRY_BACKOFF_MAX,
        headers=headers,
    )
    _assert_playlist_body(text, playlist_url)
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
    # 该主机已被熔断（整机故障，见 §12.21）：直接放弃，让上层立刻换下一个节点，
    # 不再浪费一次必然 429 的请求。放在函数最前面——连 Session 与请求头都不必
    # 准备。文案不进整片判死表，整片仍可进下一轮重投。
    if _mp4_host_is_tripped(url):
        raise RuntimeError(
            f"{_MP4_HOST_BLOCKED_MARKER}（{_host_of(url)}），跳过该节点: {url}"
        )
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
            if status == 429:
                # 只统计 mp4 直链层的 429，用于主机级熔断判定。
                _mp4_host_record_429(url)
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
                if status == 429:
                    # mp4 直链的 429 实测是**整机故障**而非限流（§12.21）：
                    # 换 IP、换签名、冷却后都恒定 429。继续按限流退避重试
                    # SEG_RETRY_MAX(20) 次纯属空耗，故记入主机熔断并立即上抛，
                    # 让上层尽快换下一个节点。
                    # ⚠️ 仅限 mp4 直链这一层；m3u8 分片层的 429 语义未变。
                    _mp4_host_record_429(url)
                    raise RuntimeError(
                        f"{_MP4_HOST_BLOCKED_MARKER}（HTTP 429，"
                        f"{_host_of(url)}）: {url}"
                    )
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
    # 取流侧带回的外挂字幕（vidlink captions）。转封装阶段落盘时要用，
    # 故随 conversion_job 一起传递。
    captions = entry.get("captions") or []

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
                "captions": captions,
                "entry": entry,
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
        # 本节点是否出现过"签名过期"（401/403/410）。汇总文案会覆盖单条异常的
        # 原文，marker 必须在这里留痕才能带进落盘文案（§12.27）。
        stream_needs_refetch = False

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
                # 签名过期（401/403/410）要单独记住：下面的汇总文案会覆盖掉
                # 本条异常的原文，marker 若不在这里留痕就会彻底丢失，
                # `--refetch-failed` 也就挑不到这部片（§12.27）。
                if needs_refetch(str(exc)):
                    stream_needs_refetch = True
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
            #
            # 若其中有签名过期，把 marker 带进汇总文案：这条文案会原样落进
            # failed.jsonl，是 `--refetch-failed` 唯一的筛选依据（§12.27）。
            refetch_note = (
                f"；另有节点直链已失效，{_NEEDS_REFETCH_MARKER}"
                if stream_needs_refetch else ""
            )
            raise RuntimeError(
                f"本轮候选流无一入选（各流原因见上方日志），下一轮重采{refetch_note}"
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
            "captions": captions,
            "entry": entry,
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
        final_path = move_to_target_folder(
            temp_mp4, tmdb_id, conversion_job.get("year")
        )
        success_info = {
            "tmdbId": tmdb_id,
            "title": conversion_job["title"],
            "year": conversion_job.get("year"),
            "url": conversion_job["url"],
            "final_path": final_path,
            # 成品字节数。必须在这里取——上传成功后本地文件就删了，事后再想
            # 统计总容量只能去 R2 查。取不到时记 None，不影响成片。
            "file_size_bytes": _file_size(final_path),
            "bitrate_kbps": conversion_job["bitrate_kbps"],
            "resolution": _resolve_final_resolution(
                conversion_job["resolution"], final_path, tmdb_id
            ),
            "missing_segment_count": conversion_job["missing_segment_count"],
            "missing_segment_indices": conversion_job[
                "missing_segment_indices"
            ],
        }
        # 旁车资产：字幕 + meta.json。必须在 success_info 组装完之后做——
        # meta 要写入实测的 resolution/bitrate。两者全程"尽力而为"，内部已
        # 自行吞掉异常，不会把一部已落地的成品拖成失败。
        year = conversion_job.get("year")
        subtitle_files = []
        try:
            subtitle_files = save_subtitles(
                tmdb_id, year, conversion_job.get("captions")
            )
        except Exception as exc:  # noqa: BLE001 - 双重兜底
            print(f"  [{tmdb_id}] ⚠️ 字幕阶段异常（已跳过）: {exc}", flush=True)
        success_info["subtitle_files"] = subtitle_files

        # save_meta 内部已吞异常，但 build_meta 仍可能因脏 entry 抛错，故整段再兜一层。
        try:
            if save_meta(
                tmdb_id, year,
                build_meta(
                    conversion_job.get("entry") or {}, success_info,
                    subtitle_files,
                ),
            ):
                success_info["has_meta"] = True
        except Exception as exc:  # noqa: BLE001 - 双重兜底
            print(f"  [{tmdb_id}] ⚠️ meta 阶段异常（已跳过）: {exc}", flush=True)

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


def collect_sidecar_assets(success_info):
    """列出该片的旁车资产相对路径（相对影片目录），如 ["subs/en.vtt", "meta.json"]。

    优先用 success_info 里记录的清单；缺失时（如 pending 记录来自旧版本）回退
    为扫描影片目录 —— reupload 补传时 success_info 可能只是一条 pending 记录，
    没有 subtitle_files/has_meta 字段，此时必须能自己发现资产，否则补传会漏掉。
    """
    assets = list(success_info.get("subtitle_files") or [])
    if success_info.get("has_meta"):
        assets.append("meta.json")
    if assets:
        return assets

    # 回退：扫目录。只认我们自己产出的两类资产，不碰视频与其它文件。
    folder = movie_dir(success_info["tmdbId"], success_info.get("year"))
    found = []
    if os.path.isfile(os.path.join(folder, "meta.json")):
        found.append("meta.json")
    subs = os.path.join(folder, SUBS_SUBDIR)
    if os.path.isdir(subs):
        try:
            for name in sorted(os.listdir(subs)):
                if os.path.isfile(os.path.join(subs, name)):
                    found.append(f"{SUBS_SUBDIR}/{name}")
        except OSError:
            pass
    return found


def _cleanup_movie_dir(folder):
    """删空 subs/ 与影片目录本身。只删空目录，有残留文件就保留。"""
    for path in (os.path.join(folder, SUBS_SUBDIR), folder):
        try:
            os.rmdir(path)
        except OSError:
            # 非空或不存在都走这里：非空说明还有别的文件，不该删。
            pass


def upload_sidecar_assets(success_info, assets=None):
    """把 meta.json 与字幕上传到与视频同前缀的 R2 位置。

    返回已上传的对象键列表。**全程尽力而为**：单个资产失败只打印，不写 pending、
    不影响视频的上传结果——视频才是主体，元信息/字幕缺失顶多是前端少个功能，
    为它把整片打回重传不值得。

    上传成功的资产会按 DELETE_LOCAL_AFTER_UPLOAD 删除本地副本（与视频同口径）：
    R2 已有副本，本地再留一份只会在几十万部规模下累积出海量小文件与 inode，
    而 disk_guard 只监控空间占用、对 inode 耗尽完全失明。
    """
    tmdb_id = success_info["tmdbId"]
    year = success_info.get("year")
    folder = movie_dir(tmdb_id, year)

    if assets is None:
        assets = collect_sidecar_assets(success_info)
    if not assets:
        return []

    uploaded = []
    for rel in assets:
        local = os.path.join(folder, *rel.split("/"))
        if not os.path.isfile(local):
            continue
        try:
            key = build_s3_key(tmdb_id, year, rel)
            ok, reason = upload_to_r2(local, key)
            if ok:
                uploaded.append(key)
                if DELETE_LOCAL_AFTER_UPLOAD:
                    remove_file(local)
            else:
                print(f"  [{tmdb_id}] ⚠️ 资产上传失败（已跳过）{rel}: {reason}",
                      flush=True)
        except Exception as exc:  # noqa: BLE001 - 资产失败不影响视频
            print(f"  [{tmdb_id}] ⚠️ 资产上传异常（已跳过）{rel}: {exc}",
                  flush=True)

    if uploaded:
        print(f"  [{tmdb_id}] 已上传 {len(uploaded)} 个附属资产", flush=True)
    return uploaded


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
        s3_key = build_s3_key(tmdb_id, success_info.get("year"))
        ok, reason = upload_to_r2(local_path, s3_key)
        if ok:
            success_info["uploaded"] = True
            success_info["s3_key"] = s3_key
            # 先传附属资产再删视频：删视频不依赖资产结果，但把两者放在一起
            # 便于日志按片聚集。资产上传内部已完全吞异常。
            success_info["asset_keys"] = upload_sidecar_assets(success_info)
            if DELETE_LOCAL_AFTER_UPLOAD:
                remove_file(local_path)
                # 视频与资产都已进 R2，本地影片目录此时应为空，回收掉它。
                # 只删空目录：还有文件说明有资产没传成功，留着等 reupload。
                _cleanup_movie_dir(movie_dir(tmdb_id, success_info.get("year")))
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
            # 成品字节数：补传时本地文件还在、可以现测，但记下来更省事，
            # 也让 pending 自身就能回答"待补传的量有多大"。
            "file_size_bytes": success_info.get("file_size_bytes"),
            # 资产清单随 pending 一起持久化：视频上传失败时旁车资产**也还没传**
            # （它们在上面的 ok 分支里），reupload 必须知道要补哪些，否则这些
            # 资产永远不会进 R2。
            "subtitle_files": success_info.get("subtitle_files") or [],
            "has_meta": bool(success_info.get("has_meta")),
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
                "file_size_bytes": success_info.get("file_size_bytes"),
                "subtitle_files": success_info.get("subtitle_files") or [],
                "has_meta": bool(success_info.get("has_meta")),
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
        run_volume = _run_pipeline()
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
            # 补传量并入本次运行总量。两段不会重复计数：能进补传的片，在主流程
            # 里一定是 uploaded=False（上传失败或槽位超时降级）、从未被主流程
            # 累加过；反之主流程已上传成功的片不会留在 pending 里。
            merge_upload_volume(run_volume, reupload_pending())
        except (Exception, SystemExit) as exc:
            # 补传失败不影响主流程的成功结论：成品仍留在本地且 pending 记录还在，
            # 随时可以手动 reupload。SystemExit 一并兜住（避免收尾动作把已经跑完
            # 的整次运行判成失败退出），但放过 KeyboardInterrupt。
            print(f"⚠️ 自动补传异常，成品仍留本地待手动 reupload: {exc}", flush=True)

    # 本次运行的最终口径：主流程 + 收尾补传。前面两段是分开打的，中间还隔着
    # 一大段失败聚合日志，不给一个合并行的话，用户得自己翻日志做加法。
    print("\n===== 本次运行上传容量 =====", flush=True)
    if run_volume["uploaded_sized"] or run_volume["uploaded_unsized"]:
        print_upload_volume("累计上传成功", run_volume)
    else:
        # 显式说"没有"，而不是留一段空白让人怀疑统计是不是又漏算了。
        print("本次运行没有新增上传。")
    print(
        "提示：查看所有历次运行的累计容量，跑 "
        "`python download_movies.py storage`"
    )


def _run_pipeline():
    clean_temp_directory()
    print("已清理 temp 目录中的旧临时文件")

    logged_ids = load_success_log_ids()
    disk_ids, duplicate_files = scan_downloaded_mp4_ids()
    # 画质判死片：默认一律跳过；--retry-dead 下按当前门槛复判，够格的放回。
    dead_ids = load_dead_ids(
        dead_record_passes_now if RETRY_DEAD_MODE else None
    )
    processed_ids = logged_ids | disk_ids | dead_ids
    print(
        f"成功日志中有 {len(logged_ids)} 个 ID，"
        f"目标目录中有 {len(disk_ids)} 个已下载 ID，"
        f"画质判死 {len(dead_ids)} 个 ID；"
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
            # 返回空累加器而非 None：main() 收尾要无条件与补传量相加。
            return new_upload_volume()
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
    stats.update(new_upload_volume())
    # 被拒原因聚合（观测性，仅统计下载阶段失败）：按类别计数，分确定性/可重试两组。
    # 确定性失败每片计一次；可重试失败跨轮会重复计（同片多轮重投），打印时分块标注。
    reject_permanent = {}
    reject_retriable = {}

    def handle_done_future(future, round_failed_retriable,
                           round_failed_expired=None,
                           round_source_outage=None):
        """处理一个已完成的 future，按其阶段推进流水线。

        round_failed_retriable 为本轮“可重试下载失败”的收集器（list）；
        round_failed_expired 为本轮“直链过期、可靠重新取流救回”的收集器；
        round_source_outage 为本轮“整节点采样全挂”的标记收集器（决定冷却档位）；
        末轮排空阶段三者都传 None（此时 pending 里只会剩转封装/上传，不会命中下载分支）。
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
            # 画质判死：持久化到 DOWNLOAD_DEAD_LOG，下次运行直接跳过，
            # 不再重新采样求证同一个结论（见 DOWNLOAD_DEAD_LOG 的注释）。
            if is_quality_dead(retriable, error_msg):
                record_quality_dead(
                    tmdb_id,
                    entry.get("title"),
                    error_msg,
                    entry.get("urls"),
                )
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
            # 整节点采样全挂：本轮冷却要走长档（源站回源故障是小时级，
            # 60s 跨不过去）。只记一次标志，不关心具体是哪几部片。
            if (
                should_retry
                and round_source_outage is not None
                and not round_source_outage
                and is_source_outage(error_msg)
            ):
                round_source_outage.append(tmdb_id)

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
                            "file_size_bytes": info.get("file_size_bytes"),
                            "subtitle_files": info.get("subtitle_files") or [],
                            "has_meta": bool(info.get("has_meta")),
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
            else:
                # 只累加真正进了 R2 的成品。三个坑：
                #   1. stats["uploads"] 是提交上传任务时自增的，含最终失败的片，
                #      不能拿它当分母；
                #   2. S3_ENABLED=False 时 upload_one_entry 也返回 True（纯本地
                #      模式），但压根没传，靠 uploaded 标志排除；
                #   3. 多轮重试不会让同一部片在这里过两次——下一轮的投料只来自
                #      merge_next_batch(下载阶段失败的片)，已上传成功的片不在
                #      任何重投桶里，根本没有第二次到达 upload 阶段的路径。
                if info.get("uploaded"):
                    add_upload_volume(stats, info.get("file_size_bytes"))

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
            # 本轮是否出现过「整节点采样全挂」。用 list 而非 bool 是因为
            # handle_done_future 是闭包，需要可变容器才能回写（它没有
            # nonlocal 声明，且同一函数在末轮排空阶段也会被调用）。
            round_source_outage = []
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
                            future, round_failed_retriable, round_failed_expired,
                            round_source_outage,
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
            # 冷却档位：本轮出现过「整节点采样全挂」就走长档。那是源站回源故障，
            # 恢复以小时计，60s 跨不过去（实测 3 轮全撞墙、0 部救回）。
            cooldown = (
                SOURCE_OUTAGE_COOLDOWN_SECONDS if round_source_outage
                else ROUND_COOLDOWN_SECONDS
            )
            outage_note = (
                f"（检测到源站采样全挂，冷却延长至 {cooldown}s 以跨过故障窗口）"
                if round_source_outage else ""
            )
            print(
                f"\n本轮有 {len(round_failed_retriable)} 部可重试下载失败{revived_note}，"
                f"冷却 {cooldown}s 后进入第 {round_no + 1} 轮...{outage_note}",
                flush=True,
            )
            if cooldown > 0:
                time.sleep(cooldown)
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
        print_upload_volume("本次上传成功", stats)

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

    # 交回给 main()：收尾补传跑完后要和补传量合并成"本次运行总量"。
    return stats


def reupload_pending():
    """手动补传：读 upload_pending.jsonl，逐条重传上传失败留在本地的成品。

    一致性铁律：
      1. 补传前检查 os.path.exists(local_path)，文件不在（已被补传/手动清理）则
         直接从 pending 移除，视为已消解，不再重复上传。
      2. 同一 tmdbId 只保留最新一条 pending 记录（去孤儿/去重复）。
      3. 补传成功 -> 删本地 + 从 pending 移除（重写整个文件）+ 更新 SUCCESS_LOG
         标 uploaded:true；仍失败则保留该条 pending。
    """
    # 四个提前返回分支一律返回空累加器（而非 None）：main() 收尾要无条件
    # 与主流程量相加，返回 None 会让调用方每次都得判空。
    if not S3_ENABLED:
        print("s3.enabled=false，未开启远端上传，无需补传。")
        return new_upload_volume()
    if is_main_running():
        print(
            "检测到主流程（download_movies.py）正在运行，"
            "此时手动补传会与主流程并发操作 pending 文件、可能导致记录丢失。"
            "请在主流程结束后再执行 reupload。本次补传已忽略。"
        )
        return new_upload_volume()
    if not os.path.exists(UPLOAD_PENDING_LOG):
        print(f"未找到 pending 日志 {UPLOAD_PENDING_LOG}，无待补传文件。")
        return new_upload_volume()

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
        return new_upload_volume()

    print(f"共 {len(latest_by_id)} 个待补传文件，开始逐条补传...")

    remaining = {}  # tmdbId -> record，仍失败保留
    success_count = 0
    orphan_count = 0
    fail_count = 0
    volume = new_upload_volume()

    for tmdb_id in order:
        record = latest_by_id[tmdb_id]
        local_path = record.get("local_path", "")
        year = record.get("year")
        s3_key = record.get("s3_key") or build_s3_key(tmdb_id, year)

        if not local_path or not os.path.exists(local_path):
            # 视频已不在本地：视为已消解（可能此前已成功补传），从 pending 移除。
            # 但旁车资产可能还留着（它们只在视频上传成功那条分支里才会被传），
            # 所以仍尝试补传一次再放手，否则这些资产永远不会进 R2。
            orphan_count += 1
            try:
                assets = collect_sidecar_assets(record)
                if assets and upload_sidecar_assets(record, assets):
                    _cleanup_movie_dir(movie_dir(tmdb_id, year))
            except Exception as exc:  # noqa: BLE001 - 资产失败不影响消解判定
                print(f"  [{tmdb_id}] ⚠️ 补传资产异常（已跳过）: {exc}")
            print(f"  [{tmdb_id}] 本地视频不存在，跳过并移除 pending: {local_path}")
            continue

        # 必须在删本地之前取大小；旧 pending 记录没有这个字段，就地补测。
        file_size = record.get("file_size_bytes") or _file_size(local_path)

        print(f"  [{tmdb_id}] 补传中 -> {s3_key}")
        ok, reason = upload_to_r2(local_path, s3_key)
        if ok:
            # 口径与主流程一致：取不到大小的记 unsized 并在收尾告警，
            # 不能静默少算（补传恰恰是最容易碰上旧记录缺字段的路径）。
            add_upload_volume(volume, file_size)
            # 视频进 R2 后，旁车资产也要跟着补 —— 它们在首次上传时因为视频失败
            # 而被整段跳过，这里是唯一的补救点。
            try:
                upload_sidecar_assets(record)
            except Exception as exc:  # noqa: BLE001 - 资产失败不影响视频补传结论
                print(f"  [{tmdb_id}] ⚠️ 补传资产异常（已跳过）: {exc}")
            if DELETE_LOCAL_AFTER_UPLOAD:
                remove_file(local_path)
                _cleanup_movie_dir(movie_dir(tmdb_id, year))
            update_success_log(tmdb_id, {
                "tmdbId": tmdb_id,
                "title": record.get("title") or "",
                # year 必须带上：它是影片目录/对象键里的一层，下游 fetch_subtitles
                # 靠 (tmdbId, year) 重建同一个目录。丢了会退化成 unknown_year，
                # 字幕就落到与视频不同的目录下。
                "year": record.get("year"),
                "final_path": local_path,
                "s3_key": s3_key,
                "file_size_bytes": file_size,
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
    print_upload_volume("本次补传上传", volume)
    return volume


def report_storage():
    """汇总 success.jsonl，打印已上传成品的累计容量（十进制 GB）。

    只读，不碰任何文件。同一 tmdbId 多条记录（补传会覆盖写）按最后一条计，
    避免把同一部片算两遍。
    """
    if not os.path.exists(SUCCESS_LOG):
        print(f"找不到 {SUCCESS_LOG}，无可统计的成品。")
        return

    latest = {}
    bad_lines = 0
    with open(SUCCESS_LOG, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue
            tmdb_id = record.get("tmdbId")
            if tmdb_id is not None:
                latest[str(tmdb_id)] = record

    uploaded_bytes = uploaded_count = uploaded_sized = 0
    local_bytes = local_count = 0
    unsized = 0
    for record in latest.values():
        size = record.get("file_size_bytes")
        if not size:
            unsized += 1
        if record.get("uploaded"):
            uploaded_count += 1
            if size:
                uploaded_bytes += size
                uploaded_sized += 1
        else:
            # 下载成功但还没进 R2（上传失败待补传，或 s3.enabled=false）
            local_count += 1
            if size:
                local_bytes += size

    print(f"===== 成品容量统计（{SUCCESS_LOG}）=====")
    print(f"影片总数: {len(latest)}")
    print(
        f"已上传 R2: {uploaded_count} 部，"
        f"{bytes_to_gb(uploaded_bytes):.2f} GB（{format_size(uploaded_bytes)}）"
    )
    if local_count:
        print(
            f"仅在本地: {local_count} 部，"
            f"{bytes_to_gb(local_bytes):.2f} GB（{format_size(local_bytes)}）"
        )
        total = uploaded_bytes + local_bytes
        print(f"合计: {bytes_to_gb(total):.2f} GB（{format_size(total)}）")
    if uploaded_sized:
        # 分母只用"有大小记录的已上传片"，否则平均值会被无大小的片拉低。
        print(f"平均每部: {bytes_to_gb(uploaded_bytes / uploaded_sized):.2f} GB")
    print("注: 采用十进制单位，1 GB = 1000³ 字节（与云存储计费口径一致）")
    if unsized:
        print(
            f"⚠️ {unsized} 部没有 file_size_bytes 字段（本功能上线前下载的），"
            f"未计入容量"
        )
    if bad_lines:
        print(f"⚠️ 跳过 {bad_lines} 行无法解析的记录")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reupload":
        reupload_pending()
    elif len(sys.argv) > 1 and sys.argv[1] == "storage":
        report_storage()
    else:
        # 沿用既有的裸 sys.argv 分派风格（本文件一直没有引入 argparse）。
        # 两个开关可叠加：--retry-only --retry-dead。
        _args = set(sys.argv[1:])
        RETRY_ONLY_MODE = "--retry-only" in _args
        RETRY_DEAD_MODE = "--retry-dead" in _args
        _unknown = _args - {"--retry-only", "--retry-dead"}
        if _unknown:
            # 静默忽略拼错的开关最危险：会让人以为跳过逻辑已生效、实际在跑全量。
            print(f"错误: 无法识别的参数 {' '.join(sorted(_unknown))}")
            print("用法: python download_movies.py [--retry-only] [--retry-dead]")
            print("      python download_movies.py reupload")
            print("      python download_movies.py storage")
            raise SystemExit(2)
        if RETRY_ONLY_MODE:
            print(
                "[重试模式] 只重试 results.jsonl 里尚未成功的片"
                "（跳过 success.jsonl、磁盘已有成品、画质判死）"
            )
        if RETRY_DEAD_MODE:
            print(
                f"[判死复判] 将按当前门槛复判 {DOWNLOAD_DEAD_LOG} 里的片，"
                f"余量系数 {DEAD_REVIVE_MARGIN:.2f}"
            )
        main()
