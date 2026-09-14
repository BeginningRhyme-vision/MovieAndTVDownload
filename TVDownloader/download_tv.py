#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import importlib
import functools
import hashlib
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
    """读取与本脚本同目录的 config.yaml 中 download_tv 段。"""
    config_path = Path(__file__).with_name("config.yaml")
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("download_tv", {}) or {}


_CFG = load_config()

# 脚本所在目录（TVDownloader/），作为相对路径与默认目录的根。
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
# 轮次间冷却：只为源站的**短时**抖动留恢复窗口。原设 300s 是按"等风控解除"
# 定的，电影侧 200 部实跑证伪了该前提（403/429 零触发，502 才是主因，而 502
# 是源站容量问题、冷却再久也不解决），TV 侧同步降到 60s 对齐。
# ⚠️ TV 侧刻意**不设**"源站回源故障长冷却分档"（电影侧的
# SOURCE_OUTAGE_COOLDOWN_SECONDS）：那套机制在电影侧已被实测推翻并默认关闭
# （1800s 既跨不过小时级的故障窗口、又比短档多烧 29 分钟，一轮只救回 2 部）。
# 这类失败交由跨运行重试兜底——failed 的集下次运行本就会自动重投（本脚本只按
# success 跳过），那时往往已过数小时，恰好落在源站真正恢复的窗口里。
ROUND_COOLDOWN_SECONDS = max(0, int(_MULTI_ROUND_CFG.get("cooldown_seconds", 60)))

# ---- 陈旧直链启动预检 ----
# results.jsonl 里的集躺过 STALE_LINK_SECONDS 后，启动时先重新取流换新 url 再
# 下载，而不是等下载失败了才发现链接过期。
#
# 🔑 为什么 TV 侧必须有它：vidlink 出的是带 sign&t 时效签名的 mp4 直链，而取流
# 侧的 load_processed 把 results.jsonl 里**已成功**的集算作"已处理"——重跑取流
# 不会给这些集换链接。没有本预检时闭环是死的：
#   取流不换链接 → 下载拿旧 url → 判"需重新取流" → 什么也没发生 → 下次重复
# 全量取流要跑很多天，第 1 天取到的直链到第 10 天大概率已经过期。
# 此前 fetched_at **只用于同一集多行之间的相对择新**，从不与当前时间比较。
_REFETCH_CFG = _CFG.get("auto_refetch", {}) or {}
AUTO_REFETCH_ENABLED = bool(_REFETCH_CFG.get("enabled", True))
# 0 = 关闭预检（退回旧行为：只在下载失败后才由上游人工重取）。
STALE_LINK_SECONDS = max(0, int(_REFETCH_CFG.get("stale_after_seconds", 86400)))
# 取流走代理、与下载争带宽，故远小于取流侧独立运行时的 max_workers。
AUTO_REFETCH_WORKERS = max(1, int(_REFETCH_CFG.get("workers", 8)))
# 单集在一次运行中最多被重取几次：新链接同样可能在下载排队期间再过期，
# 故允许多次，但必须有上限，否则"取流-过期-重取"可能反复空转。
AUTO_REFETCH_MAX_PER_EPISODE = max(
    1, int(_REFETCH_CFG.get("max_per_episode", 2))
)
# 【TV 侧特有】单次启动预检最多处理多少集。0 = 不限额。
#
# 🔑 为什么电影侧没有而 TV 侧必须有：TV 取流成功约 15-20 万集，跨运行间隔超过
# 24h 后 stale 会是几万到十几万集。单集取流要打多个 provider × 多 server ×
# 12s 超时，8 并发下 600s 最多只够几百集 —— 一次性全 submit 会让绝大多数集
# 排队到总超时被丢弃，而它们的 refetch_counts **不会 +1**（只在收到结果时记），
# 下次运行又从头再来，队尾的集永远轮不到。等于预检只对前几百集有效。
# 故按 fetched_at 升序截断：最旧的直链最可能过期，优先换它们。
AUTO_REFETCH_MAX_PER_RUN = max(0, int(_REFETCH_CFG.get("max_per_run", 2000)))
# 预检是否跳过"上次因画质不达标被判死"的集。TV 侧有源率个位数，这类集占比高，
# 重取回来画质依然不达标，白烧住宅代理配额。
AUTO_REFETCH_SKIP_QUALITY_DEAD = bool(
    _REFETCH_CFG.get("skip_quality_dead", True)
)
# 单次预检的总耗时上限（秒）。预检由主线程同步调用，此刻下载还没开始（不像电影
# 侧的轮次间重取会堵住主事件循环），但仍不能让它无限期堵住启动：全量重跑时
# 一次几千集陈旧很正常，而单集取流要跑多个 provider × 多 server × 超时 × 重试。
AUTO_REFETCH_TIMEOUT = max(
    30, int(_REFETCH_CFG.get("round_timeout_seconds", 600))
)
# 【仅 pipeline.py】异步重取的钩子。由 pipeline.py 在起线程前装上一个
# AsyncRefetcher 实例；单独跑 download_tv.py 时恒为 None，重取走同步老路。
#
# 🔑 为什么必须有这条异步路径（2026-09-14，§0.20）：
#   同步的 refetch_entries 由**主事件循环线程**执行，期间 wait(pending) 整个停摆，
#   已下载完的集无人提交转封装、成品堆在 temp、上传反压链条僵住。pipeline 的
#   整个前提是"主循环一步都不阻塞"，故那条路在 pipeline 模式下不能走。
#   而不走它、又没有异步路径的话，pipeline 模式下过期直链**当次运行零自愈**。
async_refetch_hook = None
# 【仅 pipeline.py】轮末投出异步重取后，等待结果回来的上限（秒）。
# 投完必须等一等再判"还有没有集要重投"——否则会认为没有、直接收尾，
# 重取成功的新链接就没有任何轮次去消费它。期间每秒收一次，在途清零即提前
# 结束，不会白等满。单独跑 download_tv.py 时本项无效（走同步重取）。
ASYNC_REFETCH_WAIT_SECONDS = max(
    1, int(_REFETCH_CFG.get("async_wait_seconds", 120))
)
# ---- 收尾自动补传 ----
# 上传槽位等待超时（upload_slot_wait_timeout）后会降级为"留本地 + 写
# upload_pending.jsonl"，这些成品**不会被任何后续轮次处理**——它们下载是成功的，
# 既不在失败队列里也不在重试桶里，唯一的出路是人跑 `download_tv.py reupload`。
# 而主流程跑完时 R2 往往早已恢复（降级只需积压 300s，主流程还要跑几小时），
# 自动补一次能省掉这次人工介入。手动 reupload 子命令始终保留。
AUTO_REUPLOAD_ENABLED = bool(
    (_CFG.get("auto_reupload", {}) or {}).get("enabled", True)
)
# 两个独立的下载态状态文件（区别于 SUCCESS_LOG/FAILED_LOG）。
DOWNLOAD_OK_LOG = resolve_file(_CFG.get("download_ok_log"), "download_ok.jsonl")
DOWNLOAD_FAIL_LOG = resolve_file(_CFG.get("download_fail_log"), "download_fail.jsonl")
# 【画质判死账本】累计追加，记"因画质被确定性判死"的集 + 当时的判定依据。
# 启动时并入跳过集，避免每次重跑都把注定失败的集重新采样、重新判死一遍。
#
# ⚠️ 与 fail.txt 语义完全不同，勿混用：
#   fail.txt          = 取流侧"源站明确说没有这一集"，永久排除；
#   download_dead_log = "有源但画质不达标"，门槛调松后可用 --retry-dead 放回。
#
# 🔑 为什么 TV 侧比电影侧更需要它：TV 集级有源率只有个位数，而"取到流了但画质
# 不达标"在失败构成里占比很高。此前这类集**每次运行都会被完整投入下载队列**、
# 重新采样求证同一个结论（`load_quality_dead_keys` 只用于跳过重取流，不影响
# 是否下载），几十万集规模下浪费被线性放大。
DOWNLOAD_DEAD_LOG = resolve_file(
    _CFG.get("download_dead_log"), "download_dead.jsonl"
)
# --retry-dead 放回判死集的余量系数：要求 新门槛 × 本值 <= 当时实测码率。
# 为什么要留余量：实测码率是"那次采样窗口"的值、本身有波动。门槛恰好压在
# 记录值上就放回的话，重新采样很可能测出略低的值又被判死，形成来回震荡、
# 每轮都白跑。1.05 = 门槛要比记录值低 5% 以上才放回。设 1.0 = 不留余量。
DEAD_REVIVE_MARGIN = max(1.0, float(_CFG.get("dead_revive_margin", 1.05)))
# ---- failed.jsonl 归档轮转 ----
# failed.jsonl 是纯追加、永不清理的。TV 侧几十万集规模下它会持续膨胀，而
# `load_quality_dead_keys()` **每次启动都要全量顺扫它**，启动成本线性上升。
#
# 超过 max_bytes 就在**运行收尾时**轮转：整个文件移进 archive/failed_<时间戳>.jsonl，
# 再把仍有用的行原样写回一个新的 failed.jsonl。
#
# 🔑 为什么必须回填而不是简单移走：两类行还有人要读 ——
#   ① 带"需重新取流"标记的行（重取闭环靠它挑待重取的集，丢了直接掉成功率）；
#   ② 画质判死行（load_quality_dead_keys 靠它决定预检跳过哪些集）。
# 详见 _failed_row_still_useful。设为 0 可关闭轮转。
_COMPACT_CFG = _CFG.get("failed_log_rotation", {}) or {}
FAILED_LOG_MAX_BYTES = max(
    0, int(_COMPACT_CFG.get("max_bytes", 100 * 1024 * 1024))
)
# 由 __main__ 按命令行开关置位；模块级默认 False，便于测试直接 monkeypatch。
RETRY_DEAD_MODE = False
RETRY_ONLY_MODE = False
BASE_DIR = resolve_dir(_CFG.get("base_dir"), "downloads")
# 本地目录与 R2 对象键共用的根段，使两侧严格同构：
#   本地  {BASE_DIR}/tv/{year}/{tmdbId}/S{ss}/E{ee}/E{ee}.mp4
#   R2    {S3_PREFIX}/tv/{year}/{tmdbId}/S{ss}/E{ee}/E{ee}.mp4
# strip("/") 防止配置写成 "tv/" 或 "/tv" 时拼出双斜杠/前导斜杠。
FOLDER_PREFIX = (_CFG.get("folder_prefix") or "tv").strip().strip("/")

# ---- 元信息与字幕（旁车资产，与视频同目录）----
# 两者都是"锦上添花"：获取/写入失败只记日志，绝不影响整集成败。
_ASSETS_CFG = _CFG.get("assets", {}) or {}
META_ENABLED = bool(_ASSETS_CFG.get("meta_enabled", True))
# 集目录下存放字幕的子目录名（与 fetch_subtitles.py 保持一致）。
SUBS_SUBDIR = "subs"
# 是否抓取取流侧带回的内嵌字幕（results.jsonl 的 captions 字段）。
# 这批字幕零配额白捡：取流响应里本就带着（vidup 的 tracks[]、vidlink 的
# captions[]），顺手下下来即可，不消耗 SubDL 的每日额度。抓不到不算整集失败。
SUBTITLES_ENABLED = bool(_ASSETS_CFG.get("subtitles_enabled", True))
SUBTITLE_TIMEOUT = float(_ASSETS_CFG.get("subtitle_timeout", 30))
# 字幕语种白名单与输出格式。下载侧的内嵌字幕与 fetch_subtitles.py 的 SubDL
# 补抓必须**同源**——两条链路落的是同一个 subs/ 目录，各改一半就会对不上。
SUBTITLE_LANGUAGES = [
    str(lang).strip().lower()
    for lang in (_ASSETS_CFG.get("subtitle_languages") or ["en", "zh"])
    if str(lang).strip()
]
# vtt 供浏览器原生 <track>，srt 供本地播放器；两者由同一份源转换而来。
SUBTITLE_FORMATS = [
    fmt for fmt in (
        str(f).strip().lower().lstrip(".")
        for f in (_ASSETS_CFG.get("subtitle_formats") or ["vtt", "srt"])
    ) if fmt in ("vtt", "srt")
] or ["vtt"]
# 大小上限是防御性的：字幕正常几十 KB，若源站塞来视频/错误页，不设限会整个读进内存。
SUBTITLE_MAX_BYTES = int(_ASSETS_CFG.get("subtitle_max_bytes", 5 * 1024 * 1024))

# 下载线程池固定保持的影片下载数。
# 默认 16（2026-09-14 由 32 下调，与电影侧对齐）：全局连接数 =
# MAX_WORKERS × SEGMENT_CONCURRENCY，32×64=2048 条并发 HTTPS 会触发源站 429；
# 且总吞吐由带宽定死、与并发数无关，多开只是把同一块带宽切得更碎。
# 依据详见 config.yaml 同名项。配置缺失时的兜底必须与 config 默认值一致。
MAX_WORKERS = _CFG.get("max_workers", 16)
# 主循环同时持有的“下载 future”上限（分批投递深度）。
# 一次性把整轮几万集全 submit 进 pending，会让 wait(FIRST_COMPLETED) 每次都对
# 全部未完成 future 挂/摘 waiter，主循环退化成 O(N²)。分批后 wait 规模恒定在
# 槽位量级。必须 > max_workers，否则下载池喂不满、并发上不去。
DOWNLOAD_QUEUE_DEPTH = max(
    int(MAX_WORKERS) + 1,
    int(_CFG.get("download_queue_depth", int(MAX_WORKERS) * 2)),
)
# 流式来源（pipeline 模式：取流↔下载同进程重叠）下，「在途任务已排空但取流
# 还没产出新集」时的轮询间隔（秒）。只有这一种情况才会 sleep——此刻主循环
# 无事可做，不让出 CPU 就是 100% 空转（wait(空集合) 会立刻返回）。
# 单独跑 download_tv.py（非 pipeline 模式）时这段逻辑永不触发。
STREAM_IDLE_POLL_SECONDS = max(
    0.1, float(_CFG.get("stream_idle_poll_seconds", 2))
)
# 独立的 FFmpeg 转封装/移动线程数，不占用上面的下载槽位。
# 默认 8：转封装是 `ffmpeg -c copy` 纯 IO 拷贝，并发过高只会在同一块盘上
# 互抢 IO，吞吐不升反降（兜底值原为 16，与 config.yaml 的 8 不一致，已对齐）。
CONVERT_WORKERS = _CFG.get("convert_workers", 8)
# 单部影片同时下载的分片数。
# 默认 16（2026-09-14 由 64 下调，与电影侧对齐）：这是**每集各开一个**
# ThreadPoolExecutor，故全局连接数 = MAX_WORKERS × 本值。64 会让 2048 条连接
# 去分同一条链路的带宽，每条只有几十 KB/s，且源站会当成攻击行为。
SEGMENT_CONCURRENCY = _CFG.get("segment_concurrency", 16)
TEMP_DIR = resolve_dir(_CFG.get("temp_dir"), "temp")
SAMPLE_COUNT = int(_CFG.get("sample_count", 10))
SEG_RETRY_MAX = int(_CFG.get("seg_retry_max", 20))
SEG_RETRY_DELAY = float(_CFG.get("seg_retry_delay", 1))
# 采样阶段的分片重试预算，与正片**分层**。
#
# 采样只是为了测码率/分辨率来决定"这条候选流要不要"，探不到就换下一条流即可，
# 完全不必按正片那套死磕：正片是"这一集最后的希望"，采样是"四选一里的一个"。
# 共用 SEG_RETRY_MAX=20 的后果（电影侧实跑实测）：源站持续吐 400/502 时，
# 单条流采样要耗 35 分钟，10 部片在采样阶段空转 90 分钟仍无结论。
#
# TV 侧的放大效应更大：一部剧几十上百集，每集 vidup master 有 1080/720/480/360
# 四档候选流，这个损失要乘以集数。**它占死的是下载窗口，本可成功的集会被饿死**，
# 直接触及"不得降低下载成功率"这根红线。
#
# 🔴 2026-09-14 由 3 提到 5，与电影侧对齐（同批把 L1 的 status_forcelist 清空）。
# 改前的 3 是在 L1(urllib3) 仍会**静默重试 2 次**的前提下定的——那时单次循环
# 底下实际有 3 个请求，3 次循环 ≈ 9 个请求。现在状态码重试已全部收归 L3
# （见 get_session），一次循环就是 1 个请求，3 次就真的只试 3 次，对源站几秒级
# 抖动的容错反而变弱、会压低成功率。提到 5 后：
#   实际请求数 5 × 1 = 5 < 改动前的 9，**成本仍是降的**；
#   L3 退避 1+2+4+8 ≈ 15s，足以跨过短抖动，又远低于正片那套（封顶 60s）。
SAMPLE_SEG_RETRY_MAX = max(1, int(_CFG.get("sample_seg_retry_max", 5)))
# 转封装(ffmpeg -c copy)单片超时(秒)：纯拷贝通常几十秒内完成，给足冗余防坏 TS
# 让 ffmpeg 无限阻塞占死 convert worker。超时判失败(可重试)，不拖垮转封装池。
CONVERT_TIMEOUT = int(_CFG.get("convert_timeout", 1800))
# playlist（master/media）解析阶段的请求重试：源站临时 5xx 抽风时，这一层
# 若过早放弃会直接判整集失败。但预算也不能给太大 ——
# 默认 4（2026-09-14 由 10 下调，与电影侧对齐）：退避指数增长、第 7 次起封顶
# 60s，跑满 10 次约 4 分钟，而实测这类 400/500/502 全是 CDN 回源故障（换 IP 无效、
# 恢复是小时级），第 3~4 次就能定论，后面纯属空耗并占死下载窗口。
# 1+2+4 ≈ 7 秒仍足以跨过源站几秒级的真抖动。
PLAYLIST_RETRY_MAX = int(_CFG.get("playlist_retry_max", 4))
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
# 放行整片下载后再验。正片单集通常 20 分钟以上，600s 是个宽松的下界。
# 仅在上游 runtime_minutes 缺失、不得不用样本时长时才参与判断。
MP4_MIN_TRUSTED_DURATION = float(_CFG.get("mp4_min_trusted_duration", 600))
MIN_RESOLUTION_HEIGHT = int(_CFG.get("min_resolution_height", 1080))
# 【分辨率是否参与画质判定】false（默认）= 模式 B：只按码率，分辨率红线整关放行、
# 码率门槛用**不缩放的绝对线**；true = 模式 A：分辨率红线 + 码率门槛按 (h/1080)²
# 随流高度缩放，择优也按高度优先（2026-09-14 之前 TV 侧的唯一口径）。
#
# ⚠️ 为什么 false 时必须同时去掉 (h/1080)² 缩放：那个因子本身就是分辨率在参与
# 判定。若只摘掉红线关却保留缩放，480p 的门槛会被缩到 2000×(480/1080)²≈316
# kbps —— 低分辨率片反而更容易过关，等于把分辨率以更隐蔽的方式又请了回来，
# 与"只按码率判断"的意图正好相反。
RESOLUTION_CHECK_ENABLED = bool(_CFG.get("resolution_check_enabled", False))
# 唯一宽松系数：同时放宽“分辨率红线”与“码率门槛”两关（合并原来的两个容差系数）。
LENIENCY = float(_CFG.get("leniency", 0.8))
# 各编码在 1080p 基准下的最低码率门槛（kbps）。
#   模式 A：门槛 = 基准[codec] × (h/1080)² × LENIENCY（随流高度缩放）
#   模式 B：门槛 = 基准[codec] × LENIENCY（绝对线，任何分辨率同一条）
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
# 【整集画质判死阈值】本轮尝试过的节点中，因画质不达标被确定性淘汰的比例达到
# 该值、且无一节点下载成功时，整集直接判死，不再进入下一轮重投。
#
# 为什么可以用"概率"代替"逐轮求证"：分辨率与码率是**源站侧的固有属性**，
# 不是随机变量——同一条 url 下一轮拿到的还是 480p。
#
# 🔴 TV 侧默认 1.0（全部节点都画质淘汰才判死），**刻意比电影侧的 0.5 保守**：
#   - TV 侧大量集只有 1-2 个节点（vidup 单源占多数）。阈值 0.5 时单节点集
#     只要画质淘汰一次就立刻判死——那不是"概率判死"而是"一次判死"，
#     比电影侧激进得多；
#   - TV 侧集级有源率只有个位数，误杀一集的相对代价更高；
#   - download_tv.py 至今未做过全量实测，没有数据支撑更激进的阈值。
#
# ⚠️ 只统计"画质"这一类确定性淘汰（认 QualityRejectedError 类型），
# 源站 5xx / 采样抖动等瞬时失败**不计入**，故源站抽风不会触发误杀。
#
# 取值语义：
#   1.0  全部节点都因画质淘汰才判死（默认，最保守）
#   0.5  过半即判死（电影侧口径，TV 侧待实测数据支撑后再考虑）
#   >1.0 等于永不判死，回到本功能上线前的乐观口径
QUALITY_KILL_RATIO = float(_CFG.get("quality_kill_ratio", 1.0))

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
# 反压上限至少为 1：配成 0 会让 BoundedSemaphore 初值为 0，主循环首次 acquire
# 就永久阻塞（release 只能由已提交的上传 future 触发，永远不会发生）。
MAX_PENDING_UPLOADS = max(1, int(_S3_CFG.get("max_pending_uploads", 64)))
UPLOAD_RETRY_MAX = _S3_CFG.get("upload_retry_max", 5)
UPLOAD_RETRY_DELAY = _S3_CFG.get("upload_retry_delay", 3)
# boto3 连接/读取超时（秒）：botocore 默认 60s 太长，R2 抖动时会顶满反压。
S3_CONNECT_TIMEOUT = float(_S3_CFG.get("connect_timeout", 15))
S3_READ_TIMEOUT = float(_S3_CFG.get("read_timeout", 120))
# 反压信号量的最长等待（秒）。超时说明 R2 长时间消化不动，此时**不再阻塞主
# 循环**，改为「不提交上传、成品留本地 + 写 pending」的降级模式：下载与转封装
# 继续跑，事后用 `python download_tv.py reupload` 补传。若不设上限，R2 故障会
# 让主事件循环无限期冻结——连已下载完的 future 都没人处理，整条流水线停摆。
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
                    # 显式超时：botocore 默认 60s，R2 不可用时会把每次上传拖到
                    # 分钟级，叠加 UPLOAD_RETRY_MAX 次重试后放大成十几分钟，
                    # 进而顶满反压信号量、拖慢整条流水线。
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

    收到进程级中断时立即返回：此刻再等磁盘放行毫无意义（任务马上就要收尾），
    而 disk_monitor_stop 要到 main() 的 finally 才置位——那时主线程正等着
    线程池收工，卡在这里的 worker 反而会把这个等待拖成死结。
    返回后调用方会在下载入口处再判一次 interrupted，不会真的开下。
    """
    if not DISK_GUARD_ENABLED:
        return
    while not disk_gate.wait(timeout=DISK_CHECK_INTERVAL):
        # 若监控线程已停止（主流程退出中）或收到中断，不再苦等，放行让任务自然收尾。
        if disk_monitor_stop.is_set() or interrupted.is_set():
            return


class UnsupportedPlaylistError(RuntimeError):
    """播放列表使用了当前手工分片下载器不支持的 HLS 功能。"""


class QualityRejectedError(RuntimeError):
    """本条流 / 本个节点因画质不达标被淘汰——同一条 url 重下必然复现。

    🔑 为什么要用异常类型而不是继续认错误文案：
    "画质不达标"这个**语义**是稳定的，但它的**判据是会变的**（当前是
    分辨率红线 + 码率门槛，日后可能改口径）。若上层靠 `"低于红线" in msg`
    这类字符串识别，判据一变就要同步改判定表、统计表、内层聚合三处，必漏。
    改成认类型后，判据怎么演进上层都零改动：新增画质关卡时照样 raise 本异常即可。

    🔴 TV 侧引入它的**直接动因**（与电影侧的历史不同，务必理解这一点）：
    此前内层「全流画质淘汰」与「瞬时采样全挂」共用同一句兜底文案
    「本轮候选流无一入选」，两种语义混装在一起、无法区分。而该文案又被列进了
    画质判死类目 —— 一旦有人把它加进 `_PERMANENT_FAILURE_MARKERS`，
    源站抽风（5xx 采样全挂）的集就会被**永久判死**，直接砍成功率。
    有了类型层，两条路径产生不同的文案与不同的结论，这个雷被拆掉。

    文案仍保留既有 marker（低于红线 / 码率未达到 / 没有找到高度达标），
    因为 `_PERMANENT_FAILURE_MARKERS`、`_REJECT_REASON_RULES` 与历史
    failed.jsonl 都按文案工作，换类型不该破坏它们的兼容性。
    """


# ---- 进程级中断信号 ----
# ⚠️ 为什么必须有它（电影侧服务器实跑踩到的坑，TV 侧同源）：分片重试是
# `time.sleep(退避)` 的长循环，退避第 7 次起封顶 60s，单分片最多 20 次
# （最坏 20×60s ≈ 20 分钟）。Ctrl+C 只会中断**主线程**，线程池里的 worker
# 察觉不到，会各自把重试跑完才罢休。电影侧实测表现：中断统计都打印完了，
# 进程却还挂着 31 个线程继续刷失败日志，`kill -INT` 形同虚设，只能 kill -9。
#
# ⚠️ 而 kill -9 会截断正在写的 results.jsonl / success.jsonl —— 断点续跑的
# 账本被写坏，代价远不止"退出慢一点"。
#
# mp4 直链层早有同款机制（_download_mp4_chunk 的 abort_event），但那是**每集
# 一个**的局部信号，只能在"这一集判失败"时打断自己；进程级中断需要这个全局的。
# 两层并存不冲突：块层同时看 abort_event 与 interrupted，任一置位即放弃退避。
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


# ---------- CDN 主机级 429 熔断（只作用于 mp4 直链）----------
# 背景（电影侧 §12.21 实测确诊）：hakunaymatata 的某台主机整机故障——换 3 个住宅
# IP、换新签名、冷却 30s 后**恒定** 429（Server 头是 nginx，而正常主机是 Tengine），
# 当次 341 条 vidlink url 有 153 条指向它。没有熔断时每部片都要把这台坏主机重试
# 一遍、还跨多轮，白烧大量时间。
#
# TV 侧场景完全对应：vidlink 的 CDN 域名同样高度集中（§0.0 实测命中分布
# bcdn 123 / bcdn4 26 / hcdn3 18 / bcdnxw 4），一台坏主机会牵连成百上千集。
#
# 🔑 四条边界（避免把"省时间"做成"降成功率"）：
#   - **只作用于 mp4 直链**：m3u8 分片层的 429 仍按限流退避重试，语义不变
#     （_NO_RETRY_HTTP_STATUS 的注释明确写了 429/503 属"必须重试"一类）；
#   - **只跳过同一台主机的节点**，其余节点（含同域其它主机）照常尝试；
#   - **不判整集死**：熔断文案不进 _PERMANENT_FAILURE_MARKERS，整集仍可进
#     下一轮重投——万一主机恢复了还能救回来；
#   - 计数仅存活于**本次运行**（模块级字典，进程退出即清空），不落盘。
#     主机故障是临时状态，落盘会让下次运行带着过期结论跑。
_mp4_host_lock = threading.Lock()
_mp4_host_429 = {}
_mp4_host_tripped = set()
# 单台主机累计多少次 429 后熔断。3 次足以区分"偶发限流"（退避后恢复）与
# "整机故障"（次次复现）。
MP4_HOST_CIRCUIT_THRESHOLD = max(
    1, int(_CFG.get("mp4_host_circuit_threshold", 3))
)
# 熔断文案。⚠️ 有意**不**加入 _PERMANENT_FAILURE_MARKERS（见上第三条边界）。
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
    # 打印放在锁外：避免 I/O 拖住其它线程的计数。
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


def _status_of(exc):
    """从异常里取 HTTP 状态码；取不到返回 None。"""
    return getattr(getattr(exc, "response", None), "status_code", None)


def is_permanent_http_failure(exc_or_msg):
    """判断一次失败是否为"重试也没用"的确定性 HTTP 失败。

    统一给三个重试层（playlist 请求 / HLS 分片 / mp4 直链块）复用，避免同一类
    404/403 在某一层被白重试 20 次（累计十几分钟退避），拖死整片下载窗口、
    延误换下一个取流节点——换源越快，单位时间能试的源越多，总成功率越高。
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
        # 连接级与读取级重试。2026-09-14 与电影侧对齐（其 2026-09-10 实测结论）。
        #
        # 原配置 status_forcelist=(429,500,502,503,504) + status=2 会让 urllib3
        # 对 5xx **静默重试 2 次**，且这层对上层完全透明——L3 日志里打印
        # "分片 X 下载失败 (1/20)" 时，底层其实已经发了 3 个请求。
        # 于是单个采样分片最坏 = 3(L3) × 3(L1) = 9 个请求，而日志只显示 3 次。
        # 电影侧 1000 部实跑 `502 Server Error` 出现 7038 次、采样耗尽 2720 次，
        # 两个数字对不上正是因为中间那批请求根本不可见。
        #
        # 交给 L3 的三个理由（逐条已核对在 TV 侧同样成立）：
        #   1. L3 的退避更合理（1/2/4/8s 指数 + 抖动，封顶 60s），
        #      而 L1 的 backoff_factor=0.5 只有 0s、1s，重试过于密集；
        #   2. L3 可被 `interrupted` 打断（download_single_segment 用
        #      interrupted.wait 退避），**L1 的退避叫不醒** —— 这条对 TV 侧
        #      尤其要命：dead_streak_breaker 熔断与 Ctrl+C 都打不断 L1；
        #   3. L3 每次重试都有日志，L1 完全静默，排障时看不见真实请求量。
        #
        # 🔑 顺带修掉一个隐患：429 留在 forcelist 里会让 mp4 主机熔断
        # （MP4_HOST_CIRCUIT_THRESHOLD）延迟生效——我们想"立刻换节点"，
        # urllib3 却会先自己重试 2 次。移除后熔断真正做到即时短路。
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
    """season/episode 字段解析：接受 int 或纯数字字符串，其余返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def episode_key(tmdb_id, season, episode):
    """集级唯一 key：`{tmdbId}_S{season:02d}E{episode:02d}`。

    电视剧一剧多季多集，去重/文件名/ID 锁全部以此为单位（而非剧级 tmdbId）。
    任一字段缺失返回 ""（调用方按“缺 key”处理）。
    """
    tid = normalize_tmdb_id(tmdb_id)
    s = parse_int(season)
    e = parse_int(episode)
    if not tid or s is None or e is None or s < 0 or e < 0:
        return ""
    return f"{tid}_S{s:02d}E{e:02d}"


def record_episode_key(record):
    """从含 tmdbId/season/episode 字段的字典（输入条目/日志记录）生成集级 key。"""
    if not isinstance(record, dict):
        return ""
    return episode_key(
        record.get("tmdbId"), record.get("season"), record.get("episode")
    )


def _entry_identity(entry):
    """提取条目的身份字段（tmdbId/season/episode/title），用于写入失败/成功日志。"""
    if not isinstance(entry, dict):
        entry = {}
    return {
        "tmdbId": entry.get("tmdbId"),
        "season": parse_int(entry.get("season")),
        "episode": parse_int(entry.get("episode")),
        "title": entry.get("title", ""),
    }


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
            key = record_episode_key(data)
            if key:
                processed.add(key)
    return processed


_SEASON_DIR_RE = re.compile(r"^S(\d+)$")
_EPISODE_DIR_RE = re.compile(r"^E(\d+)$")


def _scandir_subdirs(path):
    """列出 path 下的子目录条目；不可读时打印告警并返回空列表。"""
    try:
        with os.scandir(path) as entries:
            return [e for e in entries if e.is_dir(follow_symlinks=False)]
    except OSError as exc:
        print(f"警告: 无法扫描目录 {path}: {exc}")
        return []


def scan_downloaded_mp4_ids():
    """
    扫描目标目录下已经落盘的非空 MP4。

    目录结构 {BASE_DIR}/{FOLDER_PREFIX}/{year}/{tid}/S{ss}/E{ee}/E{ee}.mp4，故按
    「year -> tid -> 季 -> 集」四级 scandir，**身份取自目录名**（不再从文件名
    反解），这样同目录下的 meta.json / subs/ 等非视频资产天然不参与去重判定。

    返回 (key 集合, 重复文件字典)。同一 key 出现在多个 year 目录时只报告，
    不自动删除已有文件。

    顺带清理 0 字节 mp4：那是移动中断留下的残骸，既不是有效成品也不该被误判
    为"已下载"而永久跳过该集。只删大小为 0 的，有内容的一律不动。
    """
    downloaded_ids = set()
    locations = {}
    orphan_count = 0

    root = os.path.join(BASE_DIR, FOLDER_PREFIX) if FOLDER_PREFIX else BASE_DIR
    if not os.path.isdir(root):
        return downloaded_ids, {}

    for year_entry in _scandir_subdirs(root):
        for show_entry in _scandir_subdirs(year_entry.path):
            tmdb_id = normalize_tmdb_id(show_entry.name)
            if not tmdb_id:
                continue

            for season_entry in _scandir_subdirs(show_entry.path):
                season_match = _SEASON_DIR_RE.match(season_entry.name)
                if not season_match:
                    continue
                season = int(season_match.group(1))

                for ep_entry in _scandir_subdirs(season_entry.path):
                    ep_match = _EPISODE_DIR_RE.match(ep_entry.name)
                    if not ep_match:
                        continue
                    episode = int(ep_match.group(1))

                    key = episode_key(tmdb_id, season, episode)
                    if not key:
                        continue

                    mp4_path = os.path.join(
                        ep_entry.path, episode_video_name(episode)
                    )
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

                    downloaded_ids.add(key)
                    locations.setdefault(key, []).append(mp4_path)

    if orphan_count:
        print(f"已清理 {orphan_count} 个 0 字节 mp4 孤儿（移动中断留下的残骸）")

    duplicates = {
        key: paths for key, paths in locations.items() if len(paths) > 1
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
    "缺少 tmdbId/season/episode 或 urls",
    "没有找到媒体播放列表",       # master 解析出来是空
    "不支持的播放列表结构",       # 加密/BYTERANGE/MAP 等手工分片器永久不支持的结构
    "没有找到高度达标",           # 声明分辨率全部低于红线（含容差）
    "低于红线",                   # 实测分辨率低于红线
    "码率未达到",                 # 采样码率未达到按高度平方缩放的门槛
    # 画质汇总判死（内层"全流均因画质不达标" / 外层概率判死）。
    # 这两句都不含上面的单因 marker，不单列就会被判成可重试、白烧后续轮次。
    # ⚠️ 与 QualityRejectedError 类型互为双保险：类型管进程内传递，
    # 文案管落盘后（failed.jsonl 重载时只剩字符串）的判定。
    "均因画质不达标被淘汰",
    "因画质不达标",
    "服务器返回的不是视频分片",   # 源返回 HTML/m3u8，通常是无效源
    # ---- mp4 直链：同一条 url 重下必然复现的确定性失败 ----
    # 本脚本读的是固化的 results.jsonl，没有重新取流的能力，多轮重投拿到的
    # 还是同一条 url，白烧带宽与下载槽位。这类失败要靠重跑 tv_ids_to_links.py
    # 换一条新直链来修复，故在此判死、只留 failed.jsonl 供上游重新取流。
    "需重新取流",                 # 403/410 签名直链已过期
    "直链块不可用",               # 404 直链不存在 / 416 Range 越界
    "直链不支持 Range",           # 服务端不支持 Range 分块
    "服务器未按 Range 响应",      # 块请求被 200 全量响应
    "直链总长异常",               # 探测出的总长 <= 0
    "直链下载长度不符",           # 各块均成功但总长对不上，探测总长本身有误
)

# 注意：_HTTP_PERMANENT_MARKER（401/403/404/410/416）有意**不**列入上面的
# 整集判死表。它只用于"层内短路"——让分片/块/playlist 请求不再空等退避、
# 尽快换下一个取流节点。但整集是否重投要更乐观：403 很多时候是源站的临时
# 风控（record_block_status 正是把 403 当风控信号在统计），冷却一轮后往往
# 就能恢复；若在此判死会把可救回的片永久淘汰，与"尽可能提高成功率"相悖。
# 真正需要判死的 mp4 直链场景已由上面的专用文案（需重新取流/直链块不可用）覆盖。

# mp4 直链（vidlink 签名 url 带 sign&t 时效）返回 403/410 时的文案标记。
# 注意：它同时也在 _PERMANENT_FAILURE_MARKERS 中——本脚本无法重新取流，
# 重试同一条过期 url 必然再挂，判死后交由上游重跑取流修复。
_NEEDS_REFETCH_MARKER = "需重新取流"

# mp4 直链单块下载中“重试也没用”的文案：命中即不再走块级退避重试，直接上抛。
_MP4_CHUNK_NO_RETRY_MARKERS = (
    _NEEDS_REFETCH_MARKER,        # 403/410 直链过期
    "服务器未按 Range 响应",      # 200 全量响应，服务端不支持 Range
    "服务器返回的不是视频分片",   # 首块是 HTML/m3u8
    "直链块不可用",               # 404 直链不存在 / 416 Range 越界
    # 429 整机故障：实测换 IP/换签名/冷却后恒定 429，退避 20 次纯空耗。
    # ⚠️ 它**不在** _PERMANENT_FAILURE_MARKERS 里——只短路本层重试、尽快换
    # 节点，整集仍可进下一轮重投（主机万一恢复还能救回）。
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


def needs_refetch(error_msg):
    """这次失败是不是"直链签名已过期、必须换新 url 才可能成功"。"""
    return bool(error_msg) and _NEEDS_REFETCH_MARKER in error_msg


def plan_retry_buckets(retriable, error_msg, refetch_flag=None):
    """决定一次下载失败要进哪些桶，返回 (要重投, 要重新取流)。

    两者**不互斥**，这是本函数存在的全部理由：
      - retriable 是乐观口径——任一节点可重试，整集就值得下一轮重投；
      - needs_refetch 说明至少有一个 mp4 节点的签名 url 已失效，重投拿到的
        还是同一条、必然再挂。
    多源下"vidup m3u8 挂 5xx + vidlink mp4 签名过期"是常态。若写成互斥分支，
    这类集只会被重投而永远不换新直链，那个 mp4 节点在剩余所有轮次里都是废的，
    白白损失一个可用源。

    refetch_flag：调用方逐节点统计出的显式结论。传 None 表示"没有该信息"，
    此时回退到按 error_msg 文案判断。之所以要这个参数——error_msg 只保留
    **最后一个**节点的错误，过期节点排在非末位时文案里根本没有过期 marker。

    ⚠️ `auto_refetch.enabled: false` 时重取桶恒为空：`_NEEDS_REFETCH_MARKER`
    同时在 `_PERMANENT_FAILURE_MARKERS` 里，故带 marker 的失败 retriable=False，
    两个桶当轮都是空的。这是该开关有意的语义（等下次运行的启动预检处理），
    此处不做自动兜底。
    """
    if refetch_flag is None:
        refetch_flag = needs_refetch(error_msg)
    return (
        bool(retriable),
        AUTO_REFETCH_ENABLED and bool(refetch_flag),
    )


# 被拒/失败原因归类规则：(类别名, 命中关键字元组)，按顺序首个命中者胜出。
# 仅用于收尾聚合统计（观测性），量化各类误杀/失败占比，指导码率门槛校准。
# 不参与任何判定逻辑，改动零风险。
_REJECT_REASON_RULES = (
    ("缺少字段/无媒体列表", ("缺少 tmdbId/season/episode 或 urls", "没有找到媒体播放列表")),
    # 画质汇总判死（内层全流淘汰 / 外层达阈值节点淘汰）：单列类目，便于在收尾
    # 统计里直接看到"被概率口径判死"的集有多少，是评估该口径是否过激的一手数据。
    # ⚠️ 必须排在"候选流无一入选"之前：汇总文案里附带了**末节点**的原始错误，
    # 而末节点常常正是"本轮候选流无一入选"。首个命中者胜出，排在后面就会被抢走，
    # 判死集全被记到"无一入选"类目下，正好污染要用来评估本口径的那份数据。
    ("画质整体不达标(判死)", ("因画质不达标", "均因画质不达标被淘汰")),
    # “候选流无一入选”是汇总文案（不含单因 marker），须先于单因规则匹配。
    ("候选流无一入选", ("候选流无一入选",)),
    ("不支持的播放列表结构", ("不支持的播放列表结构",)),
    ("分辨率低于红线", ("低于红线", "没有找到高度达标")),
    ("码率未达门槛", ("码率未达到",)),
    ("采样探测分辨率失败", ("采样探测分辨率失败",)),
    ("采样数据异常", ("采样数据或采样时长",)),
    ("正片缺片率过高", ("缺片率过高",)),
    ("源返回非视频分片", ("服务器返回的不是视频分片",)),
    ("直链失效需重新取流", (_NEEDS_REFETCH_MARKER,)),
    # mp4 直链专属类目：与上面的 m3u8 类目并列，便于在收尾统计里单独看
    # vidlink 直链的淘汰构成（多源接入后校准门槛/判断源质量的关键数据）。
    # 放在“超时/SSL”之前：这几条是确定性结论，不该被通用网络类目抢先命中。
    ("直链块不可用(404/416)", ("直链块不可用",)),
    ("直链不支持Range", ("直链不支持 Range", "服务器未按 Range 响应")),
    ("直链总长异常", ("直链总长异常", "直链下载长度不符")),
    # 主机级熔断：与下面的"直链块重试耗尽"分开，便于跑完后直接看出
    # "有多少集是被某台坏 CDN 主机拖累的"（该数偏高说明要盯 vidlink 的 CDN）。
    ("直链主机熔断(429)", (_MP4_HOST_BLOCKED_MARKER,)),
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


# 画质类淘汰的类目名。用**类目**而非裸 marker：判据演进时只改 _REJECT_REASON_RULES
# 一处，这里自动跟随。
#
# ⚠️ 只收"源确实给了流、但这条流画质不达标"的类目。源站 5xx / 超时 / 直链失效
# 绝不能进来——那是"今天源站挂了"，重取完全可能换到好流；把它们当画质判死会
# 永久放弃可救回的集，直接违背"尽可能提高成功率"这条红线。
#
# 🔴 为什么"候选流无一入选"**不在**这里（2026-09-13 审查移除）：
# 那句是内层的**兜底汇总文案**，语义混装 —— 它既可能是"全部流真不达标"，
# 也可能是"源站 5xx 导致采样全挂"。TV 侧源站 5xx 频发，后者是常态。
# 此前它被列在这里，只是靠 `_classify_failure` 判它可重试（→ is_quality_dead
# 的第一道闸门挡住）才没出事 —— 那是**巧合性**的安全：一旦有人把该文案加进
# `_PERMANENT_FAILURE_MARKERS`（理由还很正当："全流不达标就是确定性失败"），
# 闸门当场失效，源站抽风的集开始被永久判死，且没有任何测试会拦。
#
# 现在内层已用 QualityRejectedError 把两条路径拆开：
#   - 全流画质淘汰 → 抛 QualityRejectedError，文案"均因画质不达标被淘汰"
#     → 归入"画质整体不达标(判死)"类目 → 本集合收它；
#   - 混合/纯瞬时 → 抛普通 RuntimeError，文案仍是"候选流无一入选"
#     → 可重试，绝不判死。
# 故本集合不再需要（也绝不能）收"候选流无一入选"。
_QUALITY_REJECT_CATEGORIES = frozenset({
    "画质整体不达标(判死)",
    "分辨率低于红线",
    "码率未达门槛",
})


def is_quality_dead(retriable, error_msg, node_failures=None):
    """这次失败是不是"画质不达标"型的确定性淘汰。

    三个条件缺一不可：

      1. `retriable` 为 False —— 可重试的失败一律不算（多节点集里"某节点画质
         淘汰 + 某节点 502"整集仍可重试，算进来等于永久误杀）；
      2. 文案归类命中 `_QUALITY_REJECT_CATEGORIES`；
      3. **全部节点都给出了画质结论** —— 见下方。

    🔴 第 3 条是 2026-09-13 审查补的，缺了它会真实误杀（实测 3/8 场景违背语义）。

    业务红线（用户拍板）：**必须确定该集在所有存在资源的节点上都画质不达标
    才判死，其余情况一律可重试。** 因为有跨运行重试兜底，判死的代价是永久
    丢一集，远高于多跑一轮。

    没有第 3 条时的漏洞：`error_msg` 只保留**末节点**文案，而 `retriable`
    是 `any_retriable or _classify_failure(msg)`。当**非画质的确定性失败**
    （需重新取流 / 不支持的播放列表结构 / 直链块 404）排在前面、画质失败恰好
    落在末位时：

      - 这些节点不会让 `any_retriable` 变 True（它们本身就是确定性失败）；
      - 也不计入 `quality_rejected_nodes`（类型不是 QualityRejectedError，
        故外层概率判死那条路径正确地没有触发）；
      - 但末节点文案是画质的 → 前两个条件双双成立 → 整集被判死。

    而那个"需重新取流"的节点**从未给出过画质结论** —— 换条新直链完全可能是
    1080p。判死它直接违背上面的红线。

    node_failures：`process_one_entry` 的逐节点归因（也落在 failed.jsonl 里）。
      - 非空 → 要求**每个**节点的 reason_class 都在画质类目里（合取）；
      - None / 空 → 没有节点级信息（旧记录，或异常抛在节点循环之外），
        退回前两个条件。这不是放水：那些路径本就没有"多节点"语义，
        单节点集的判死结论与三条件版一致。
    """
    if retriable:
        return False
    if classify_reject_reason(error_msg) not in _QUALITY_REJECT_CATEGORIES:
        return False
    if not node_failures:
        return True
    # 合取：任一节点没给出画质结论（5xx/直链失效/结构不支持/超时…），
    # 就说明"该集在所有节点上都画质不达标"尚未被证实 → 不判死。
    return all(
        (node or {}).get("reason_class") in _QUALITY_REJECT_CATEGORIES
        for node in node_failures
    )


def load_quality_dead_keys():
    """从 FAILED_LOG 读出"上次因画质不达标被判死"的集级 key 集合。

    供启动预检跳过这些集：它们取流是成功的（有 urls），只是流的画质不达标，
    重取一次流回来**大概率还是同样的流、同样不达标**，纯属白烧住宅代理配额。
    TV 侧集级有源率只有个位数百分比，这类集在失败总量里占比很高。

    ⚠️ 读文件失败一律返回空集合：预检是"锦上添花"，绝不能因为读不了日志就
    影响启动。返回空集合的后果只是退回旧行为（全部按时间判断）。
    """
    keys = set()
    if not AUTO_REFETCH_SKIP_QUALITY_DEAD or not os.path.exists(FAILED_LOG):
        return keys
    try:
        with open(FAILED_LOG, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # 只看下载阶段：转封装/上传失败与画质无关。
                if record.get("stage") != "download":
                    continue
                key = record_episode_key(record)
                if not key:
                    continue
                if is_quality_dead(
                    record.get("retriable", False),
                    record.get("error", ""),
                    # 落盘的逐节点归因：同口径复判，避免"内存里不判死、
                    # 重载后却判死"这种前后不一致。旧记录没有该字段 → None，
                    # 退回两条件口径（见 is_quality_dead 文档）。
                    record.get("node_failures"),
                ):
                    keys.add(key)
                else:
                    # 同一集可能先画质判死、后来又因别的原因失败（或反之）。
                    # 以**最后一条**为准：画质门槛可能被调松过，旧的判死结论
                    # 不该永久压住它。
                    keys.discard(key)
    except OSError as exc:
        print(f"⚠️ 读取 {FAILED_LOG} 失败，预检不做画质跳过: {exc}", flush=True)
        return set()
    return keys


# ---------- 画质判死的跨运行持久化（DOWNLOAD_DEAD_LOG） ----------
# 从判死文案里回抽实测码率与门槛。两种模式的措辞不同（见 bitrate_reject_message）：
#   模式 A：分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps
#   模式 B：码率未达到门槛：372 kbps < 1600 kbps（h264，实测 854x480）
# 码率数值只锚定"码率未达到门槛：N kbps < M kbps"这段公共前缀，两模式通用、
# 措辞微调也不会失效；分辨率与编码则各按各的模式抽。
#
# ⚠️ 抽出来的是**末节点**的数值：外层判死文案只嵌末节点错误，前面节点的实测值
# 落盘时已不存在。这让 --retry-dead 偏保守（多节点集可能漏救几集），
# 但绝不会误救——不会把仍不达标的集放回去白跑。
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
    的汇总——一律返回 None：没有依据可存，`--retry-dead` 对它们只能整集放回。
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


def load_dead_keys(threshold_fn=None):
    """读 DOWNLOAD_DEAD_LOG，返回要跳过的**集级 key** 集合。

    threshold_fn 为 None（默认，正常运行）：全部判死集一律跳过。
    传入函数时（`--retry-dead`）：对每条记录用当前门槛复判，
    **该 key 的全部记录都判定"可以放回"时才放回**（合取语义）。

    🔑 为什么必须按 key 聚合：账本是纯追加的，同一集可能有多行（重取换链接后
    再次判死、`--retry-dead` 放回后又判死）。逐行 add/discard 会让结论
    **取决于哪一行排在最后**，随追加顺序漂移、不确定。改成合取后：
    只要有任何一条记录证明它现在仍不达标，就继续跳过。

    ⚠️ 与 load_quality_dead_keys（读 FAILED_LOG、取最后一条）的语义差异是
    **有意的**：那个账本服务"要不要重取流"，宽松些无非多花点代理配额；
    这个账本服务"要不要下载"，误放回要白下整集，故取保守的合取语义。

    缺依据的记录在 `--retry-dead` 下一律放回——没有依据就无法证明它仍不达标。
    """
    if not os.path.exists(DOWNLOAD_DEAD_LOG):
        return set()

    # 先按 key 归拢全部记录，再统一裁决——避免逐行覆盖带来的行序依赖。
    records_by_key = {}
    try:
        with open(DOWNLOAD_DEAD_LOG, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = record_episode_key(record)
                if not key:
                    continue
                records_by_key.setdefault(key, []).append(record)
    except OSError as exc:
        # 读不了账本只退回"不跳过"，绝不因此中断启动。
        print(f"⚠️ 读取 {DOWNLOAD_DEAD_LOG} 失败，不做判死跳过: {exc}", flush=True)
        return set()

    if threshold_fn is None:
        return set(records_by_key)

    dead = set()
    revived = 0
    for key, records in records_by_key.items():
        # 合取：任一条记录判定"仍不达标"，该集就继续跳过。
        if all(threshold_fn(record) for record in records):
            revived += 1
        else:
            dead.add(key)
    if revived:
        print(f"[判死复判] {revived} 集在当前门槛下不再判死，已放回重试队列")
    return dead


def dead_record_passes_now(record):
    """当前配置门槛下，这条判死记录是否该被放回重试。

    口径：用记录里的**实测码率**对比**现在算出来的**门槛。门槛按记录的
    分辨率高度与编码重算（而非沿用记录里的旧门槛），这样 bitrate_* 与
    leniency 任一处改动都能被识别到。

    ⚠️ 留 DEAD_REVIVE_MARGIN 余量：实测码率是**那次采样窗口**的值，本身有波动。
    门槛恰好压在记录值上时放回，重新采样很可能测出略低的值又被判死一次，
    形成来回震荡、每轮都白跑。要求"新门槛 × 余量 <= 记录值"才放回。

    🔴 两个必须挡住的坑：

    1. **跨模式复判**：`bitrate_threshold` 的行为由 RESOLUTION_CHECK_ENABLED
       决定（模式 A 按 (h/1080)² 缩放、模式 B 用绝对线），两者门槛能差 5 倍。
       拿当前模式去复判另一个模式下判死的记录，结论没有意义。
       故记录里存了判死当时的模式，**不一致就不放回**（保守）。
       老记录没有该字段 → 视为未知 → 同样不放回，避免按错误口径误放。

    2. **height 缺失时门槛归零**：模式 A 下 `bitrate_threshold` 按 (h/1080)²
       缩放，height=0 会让门槛恒为 0，`0 × 余量 <= 任何码率` **恒真** ——
       这批记录会被无条件放回。故模式 A 下 height 抽不出来时一律不放回。
       模式 B 下 height 根本不参与计算，不必挡（挡了反而会把"未知分辨率"
       那批本该复判的记录永久关在门外）。
    """
    evidence = record.get("evidence") or {}
    bitrate = evidence.get("bitrate_kbps")
    if bitrate is None:
        # 没有依据 → 无法证明现在仍不达标，放回（见 load_dead_keys 文档）。
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


def record_quality_dead(entry, error_msg, urls=None):
    """把一集画质判死记进 DOWNLOAD_DEAD_LOG（纯追加）。

    同一集可能在不同运行里被记多次（--retry-dead 放回后再次判死、
    重取换链接后新节点仍不达标）。不做去重：写入端保持纯追加，
    **合并规则放在读取端** `load_dead_keys`（按集级 key 聚合），
    避免去重要重写整个文件、与多线程共用的 write_log 冲突。
    """
    write_log(DOWNLOAD_DEAD_LOG, {
        **_entry_identity(entry),
        "dead_at": int(time.time()),
        "reason_class": "quality",
        # 判死当时的画质判定模式，复判时用来确认口径可比（见
        # dead_record_passes_now 的坑 1：两模式门槛能差 5 倍）。
        "resolution_check_enabled": RESOLUTION_CHECK_ENABLED,
        "error": error_msg,
        "evidence": parse_dead_quality_evidence(error_msg) or {},
        "node_count": len(urls or []),
    })


# ---------- failed.jsonl 轮转归档 ----------
def _failed_row_still_useful(record, done_keys, dead_keys):
    """这条 failed.jsonl 记录是否还有人要读；决定轮转时回不回填。

    两类行必须留下（TV 侧比电影侧多一类，漏掉任一类都会造成实际损失）：

    1. **带"需重新取流"标记的行** —— `refetch_entries` 与人工补救都靠它找待重取
       的集。丢了 = 重试链断裂，直接掉下载成功率。
    2. **画质判死行** —— `load_quality_dead_keys` 每次启动扫 FAILED_LOG 推导
       "预检要跳过哪些集"。丢了不会downgrade正确性，但那些集会重新进入重取
       队列、白烧住宅代理配额（TV 侧这类集占比很高）。

    已成功（done_keys）或已进独立判死账本（dead_keys）的集不必再留：
    前者不会再被重取，后者的结论已由 download_dead.jsonl 持久化。
    """
    key = record_episode_key(record)
    if not key or key in done_keys or key in dead_keys:
        return False
    if _NEEDS_REFETCH_MARKER in str(record.get("error", "")):
        return True
    # 画质判死行：仅 stage=download 的行参与（与 load_quality_dead_keys 同口径）。
    if record.get("stage") == "download" and is_quality_dead(
        record.get("retriable", False),
        record.get("error", ""),
        record.get("node_failures"),
    ):
        return True
    return False


def compact_failed_log():
    """failed.jsonl 超限时轮转：整体归档，把仍有用的行写回新文件。

    返回 (是否轮转过, 归档路径, 回填行数)。

    ⚠️ 三条不可动摇的约束：
      1. **历史零丢失** —— 原文件整体 move 进 archive/，不做任何裁剪。
      2. **仍有用的行必须留下** —— 见 _failed_row_still_useful。这是本函数
         最大的风险点：回填口径写窄了会静默掉成功率。
      3. **error 原文逐字保留** —— `_NEEDS_REFETCH_MARKER` 是跨文件字符串契约，
         改写任何一个字都会让闭环静默断开。故回填时原样 dump，不重构字段。

    🔑 TV 侧为什么需要它（电影侧是为了 --refetch-failed 扫描快）：
    `load_quality_dead_keys()` **每次启动都要全量顺扫 FAILED_LOG**。几十万集
    规模下这个文件会持续增长，启动成本线性上升。

    并发：进程内靠 log_lock；**跨进程**靠 is_main_running() 守卫。后者不可省——
    本函数在 release_main_lock() 之后才调用，此刻另一个 downloader 已能启动并
    往 failed.jsonl 追加。那个进程的 fd 指向旧 inode，我们 os.replace 之后
    它写的每一条都进了 archive、新文件里没有 —— 那批记录就此蒸发。
    """
    if FAILED_LOG_MAX_BYTES <= 0 or not os.path.exists(FAILED_LOG):
        return False, None, 0
    # 本进程此刻已释放主锁，故锁在 = 别的进程在跑。让它去，文件大一点没关系。
    if is_main_running():
        return False, None, 0
    try:
        if os.path.getsize(FAILED_LOG) <= FAILED_LOG_MAX_BYTES:
            return False, None, 0
    except OSError:
        return False, None, 0

    # 放在锁外：这两个文件不由本函数改写，且读它们可能较慢。
    done_keys = load_success_log_ids()
    dead_keys = load_dead_keys()
    keep = []

    with log_lock:
        # 锁内复查：拿锁期间别的线程可能已经轮转过了。
        if not os.path.exists(FAILED_LOG):
            return False, None, 0
        try:
            if os.path.getsize(FAILED_LOG) <= FAILED_LOG_MAX_BYTES:
                return False, None, 0
        except OSError:
            return False, None, 0

        archive_dir = os.path.join(str(_SCRIPT_DIR), "archive")
        stamp = time.strftime("%Y%m%d_%H%M%S")
        archive_path = os.path.join(archive_dir, f"failed_{stamp}.jsonl")
        try:
            os.makedirs(archive_dir, exist_ok=True)
            # 先读出要回填的行，再移走原文件。顺序反过来的话，移动成功但读取
            # 失败就会让这些记录彻底丢失。
            with open(FAILED_LOG, "r", encoding="utf-8") as file:
                for line in file:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        record = json.loads(stripped)
                    except json.JSONDecodeError:
                        # 坏行不回填：原文件整体进了 archive/，一个字都没少；
                        # 而坏行按定义解析不出 marker，回填它也没用。
                        continue
                    if _failed_row_still_useful(record, done_keys, dead_keys):
                        keep.append(stripped)   # 原样保留，绝不重构字段
            os.replace(FAILED_LOG, archive_path)
            if keep:
                tmp_path = FAILED_LOG + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as file:
                    file.write("\n".join(keep) + "\n")
                os.replace(tmp_path, FAILED_LOG)
        except OSError as exc:
            # 轮转纯属维护动作，失败了只是文件继续变大，不影响任何业务结论。
            print(f"⚠️ failed.jsonl 轮转失败（已忽略，不影响下载）: {exc}",
                  flush=True)
            return False, None, 0

    return True, archive_path, len(keep)


def update_success_log(key, new_record):
    """按集级 key（tmdbId+season+episode）去重地写 SUCCESS_LOG：同 key 覆盖旧记录，否则追加。

    用于 reupload 补传成功后，避免同一集在 SUCCESS_LOG 中残留
    uploaded:false / uploaded:true 两条记录。理想状态：每个下载成功的
    集只有一条记录（不管上传成功与否）。
    全程 log_lock 保护，读全量 -> 覆盖/追加 -> 写临时文件 -> os.replace 原子替换。
    """
    key = str(key)
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
                    if record_episode_key(record) == key:
                        if not replaced:
                            records.append(new_record)
                            replaced = True
                        # 后续同 key 记录直接丢弃（去重）
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


def remove_upload_failure_from_log(key):
    """从 FAILED_LOG 中删除指定集级 key 的「上传阶段」失败记录（stage=="upload"）。

    用于 reupload 补传成功后清算：这样 FAILED_LOG 里若不再有 upload 阶段的行，
    即可判定所有下载成功的集都已上传成功。
    只删 stage=="upload" 的行，保留 download/conversion/preflight 等其它阶段
    的失败记录（那些不是上传问题，不应被补传成功抹掉）。
    全程 log_lock 保护，读全量 -> 过滤 -> 写临时文件 -> os.replace 原子替换。
    """
    key = str(key)
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
                if (record_episode_key(record) == key
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
    保证两侧的 year 段永远一致（否则 final_path 与 s3_key 无法互相换算）。
    """
    digits = re.sub(r"\D", "", str(year)) if year not in (None, "") else ""
    return digits if digits else "unknown_year"


def asset_rel_path(tmdb_id, season, episode, year, asset=None):
    """同一集全部资产的公共相对路径：
    {FOLDER_PREFIX}/{year}/{tmdbId}/S{season:02d}/E{episode:02d}[/{asset}]。

    这是本地目录与 R2 对象键的**唯一真实来源**：本地把它接在 BASE_DIR 后、
    R2 把它接在 S3_PREFIX 后，两侧因此严格同构、可互相换算。

    year 取**剧的首播年**（非本集播出年），这样一部剧的所有季集聚在同一棵
    子树下；季与集各占一层，便于按剧/按季一次性列举。

    asset 为 None 时返回集目录本身；否则返回目录下某个资产的相对路径
    （如 "E03.mp4"、"meta.json"、"subs/en.srt"）。

    缺 tmdbId/season/episode 时抛 ValueError —— 身份不全就拼不出正确位置，
    静默兜底只会把成片写到错误路径且极难发现。
    """
    tid = normalize_tmdb_id(tmdb_id)
    s = parse_int(season)
    e = parse_int(episode)
    if not tid or s is None or e is None:
        raise ValueError(
            f"asset_rel_path 缺少 tmdbId/season/episode: "
            f"{tmdb_id!r}/{season!r}/{episode!r}"
        )
    parts = [FOLDER_PREFIX, year_segment(year), tid, f"S{s:02d}", f"E{e:02d}"]
    if asset:
        parts.append(str(asset).strip("/"))
    return "/".join(part for part in parts if part)


def episode_video_name(episode):
    """成品视频文件名：E{episode:02d}.mp4（身份信息已全在路径里）。

    episode 非法时抛 ValueError（与 asset_rel_path 同口径）：拼不出正确文件名
    就该当场失败，否则会静默写成 ENone.mp4 之类的畸形名。
    """
    value = parse_int(episode)
    if value is None:
        raise ValueError(f"episode_video_name 的 episode 非法: {episode!r}")
    return f"E{value:02d}.mp4"


def episode_dir(tmdb_id, season, episode, year=None):
    """本地集目录的绝对路径：{BASE_DIR}/{FOLDER_PREFIX}/{year}/{tid}/S{ss}/E{ee}。

    视频、meta.json、subs/ 全部落在这里，与 R2 侧 build_s3_key 的前缀同构。
    """
    rel = asset_rel_path(tmdb_id, season, episode, year)
    return os.path.join(BASE_DIR, *rel.split("/"))


def build_s3_key(tmdb_id, season, episode, year=None, asset=None):
    """把一集的某个资产映射为 R2 对象键。

    规则：{S3_PREFIX}/{FOLDER_PREFIX}/{发布年份}/{tmdbId}/S{ss}/E{ee}/{资产名}
    如剧 12345、第 1 季第 3 集、首播年 2000、资产 E03.mp4 ->
        tv/2000/12345/S01/E03/E03.mp4
    year 缺失时用 unknown_year 兜底，避免拼出畸形 key。

    与旧版的关键差异：**对象键不再含上传日期**。日期段会把同一集分多次上传
    的视频/元信息/字幕切散到不同前缀下，前端无法按同前缀一次取全，字幕脚本
    也无法由 (tid, s, e, year) 纯计算出前缀去判重；去掉后同 key 重传即覆盖
    （幂等），正是补传想要的语义。
    """
    asset = asset if asset is not None else episode_video_name(episode)
    rel = asset_rel_path(tmdb_id, season, episode, year, asset)
    return f"{S3_PREFIX}/{rel}" if S3_PREFIX else rel


def _is_permanent_upload_error(exc):
    """判断上传失败是否"重试也没用"：凭证错误/桶不存在/权限不足等配置类问题。

    这类失败重试 5 次只是白等 30s，而且每次都占着反压槽位、拖慢整条流水线。
    网络类错误（超时/连接重置/5xx）仍然重试——那才是重试真正能救回来的场景。
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = str(response.get("Error", {}).get("Code", ""))
    if status in (400, 401, 403, 404):
        return True
    return code in (
        "InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied",
        "NoSuchBucket", "InvalidBucketName",
    )


def upload_to_r2(local_path, s3_key):
    """带指数退避重试地上传单个文件到 R2。成功返回 (True, None)，失败返回 (False, 原因)。

    确定性失败（凭证/权限/桶不存在）立即返回，不做无谓重试。
    """
    client = get_s3_client()
    last_exc = None
    for attempt in range(1, UPLOAD_RETRY_MAX + 1):
        try:
            client.upload_file(local_path, S3_BUCKET, s3_key)
            return True, None
        except Exception as exc:  # noqa: BLE001 - 网络/凭证/服务端多种异常统一处理
            last_exc = exc
            if _is_permanent_upload_error(exc):
                return False, f"确定性上传失败（不重试）: {exc}"
            if attempt < UPLOAD_RETRY_MAX:
                # 指数退避 + 抖动，封顶 60s：与下载侧各重试层口径一致。
                wait = min(UPLOAD_RETRY_DELAY * (2 ** (attempt - 1)), 60)
                wait += random.uniform(0, min(1.0, wait * 0.2))
                time.sleep(wait)
    return False, str(last_exc)


def write_pending(record):
    """线程安全地向 upload_pending_log 追加一条待补传记录。"""
    with pending_lock:
        with open(UPLOAD_PENDING_LOG, "a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


# provider 归因字段。集中定义避免各写入点漏抄其中一个。
_ATTRIBUTION_KEYS = ("provider", "node_type", "node_index", "node_total")


def _attribution_of(source):
    """从 success_info / conversion_job 抽出 provider 归因字段。

    pending 记录必须带上它们：补传成功后 reupload 会用 pending 里的值重建
    success 记录，漏了这几个键就等于把"这集是哪家下成的"抹掉——而降级留本地的
    集恰恰是上传侧故障时的一大批，抹掉会让各源转化率统计系统性偏低。
    """
    return {key: (source or {}).get(key) for key in _ATTRIBUTION_KEYS}


# 主流程运行标记文件：用于让手动 reupload 检测主流程是否在跑，
# 避免二者并发操作 pending 文件导致记录被覆盖丢失。
MAIN_LOCK_FILE = str((_SCRIPT_DIR / "download_tv.main.lock").resolve())


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
    """主流程启动时抢占 PID 锁；已有存活主流程时直接拒绝启动。

    两个主流程并发跑会各自持有独立的 processing_ids/processed_ids 内存态，
    彼此看不见对方在下哪一集，必然重复下载同一集并交错追加 success/pending，
    还会互相覆盖 os.replace 的原子重写结果导致记录丢失。故此处 fail-fast。
    """
    if is_main_running():
        print(
            "检测到已有 download_tv.py 主流程在运行"
            f"（锁文件 {MAIN_LOCK_FILE}）。并发运行会重复下载并覆盖日志记录，"
            "本次启动已中止。确认上一进程确实已退出后可删除该锁文件重试。",
            flush=True,
        )
        sys.exit(1)
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
        if name.startswith(("sample_", "temp_", "mp4sample_")) and name.endswith(
            (".ts", ".mp4")
        ):
            remove_file(path)


def move_to_target_folder(temp_mp4, tmdb_id, season, episode, year=None):
    """把转封装好的成品移到 {集目录}/E{episode:02d}.mp4。

    shutil.move 同时支持跨文件系统移动。

    每集独占一个目录，故**不需要任何全局锁**：
      - 目录名由 (tmdbId, season, episode, year) 唯一决定，不存在"选哪个桶"
        的共享决策；
      - 同一集的并发已由 processing_ids/processing_lock 挡在上游，同一目录
        不会有两个 worker 同时写。
    这也一并去掉了旧桶号方案里的 0 字节占位文件 —— 占位只是为了让并发的
    worker 在锁内计数时能看见彼此，新结构下无人需要计数。
    """
    folder_path = episode_dir(tmdb_id, season, episode, year)
    os.makedirs(folder_path, exist_ok=True)
    final_path = os.path.join(folder_path, episode_video_name(episode))

    # 失败时清掉半成品，交由上层按转封装失败处理。
    # 跨文件系统时 shutil.move 是 copy+del，若 copy 中途失败（目标盘写满/IO
    # 错误）会在 final_path 留下半成品 mp4：它不在 cleanup_paths、去重表也无
    # 登记，会成孤儿。
    try:
        shutil.move(temp_mp4, final_path)
    except Exception:
        remove_file(final_path)
        raise
    print(f"  [{episode_key(tmdb_id, season, episode)}] 已移动到: {final_path}",
          flush=True)
    return final_path


# ---------- 旁车资产：meta.json ----------
# meta.json 与视频落在同一个集目录下，R2 对象键也同前缀，前端按同前缀一次
# 列举即可拿全。**全部按"尽力而为"处理**：任何失败只打印并记录，绝不抛到调用
# 方——一集已经下好的片不该因为元信息这种附属物被判失败而重跑整个下载。
# 字幕由 fetch_subtitles.py 事后补进同目录的 subs/ 子目录。

# 字幕时间轴行（cue timing）。整行匹配、一次拿下起止两端，两端各自的时/分/秒
# 结构用 `[\d:]+` 宽松描述 —— 实测同一个文件里会**混用两种形态**：
#     00:34.958        （MM:SS.mmm，省略小时）
#     01:02:03.958     （HH:MM:SS.mmm）
# 按三段式写死的话，省略小时的那一半完全匹配不到，分隔符没被替换，
# 那批字幕在播放器中直接失效。
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
    """下载单条字幕，返回 (文本, 源格式) 或 None。失败直接抛异常由调用方吞。"""
    url = caption.get("url")
    if not url:
        return None
    # headers 与各自源站的 CDN 鉴权强绑定（peakstorm 要 Referer、
    # hakunaymatata 拒绝 Referer），取流侧已按源站算好，此处原样带上。
    headers = dict(caption.get("headers") or {})
    with get_session().get(
        url, timeout=SUBTITLE_TIMEOUT, headers=headers or None, stream=True
    ) as response:
        response.raise_for_status()
        chunks = []
        total = 0
        # 流式读 + 边读边计量：源站若塞来视频/错误页，在超限那一刻就停，
        # 不会把它整个读进内存。
        for chunk in response.iter_content(chunk_size=65536):
            if not chunk:
                continue
            total += len(chunk)
            if total > SUBTITLE_MAX_BYTES:
                raise ValueError(
                    f"字幕体积超过上限 {SUBTITLE_MAX_BYTES} 字节，疑似非字幕内容"
                )
            chunks.append(chunk)
    raw = b"".join(chunks)
    if not raw.strip():
        raise ValueError("字幕内容为空")
    text = _decode_subtitle(raw)
    # 一律按**内容**判定格式，不信源站声明的 type：源站把 srt 标成 vtt
    # （或反过来）很常见，按声明转换会产出播放器读不了的残废文件。
    fmt = "vtt" if text.lstrip().upper().startswith("WEBVTT") else "srt"
    return text, fmt


def save_subtitles(tmdb_id, season, episode, year, captions):
    """把取流侧带回的内嵌字幕落到集目录的 subs/ 下，返回相对路径列表。

    尽力而为：任何一条字幕失败都只打印告警并跳过，绝不影响整集成败——
    抓不到的语种后续由 fetch_subtitles.py 从 SubDL 补。
    """
    if not SUBTITLES_ENABLED or not captions:
        return []

    # 先按白名单筛选并去重：取流侧可能给十几种语言，全存会白白放大存储
    # 与上传请求数。同语种只取第一条（取流侧已按源站优先级排好）。
    wanted = {}
    for caption in captions:
        if not isinstance(caption, dict):
            continue
        lang = str(caption.get("language") or "").strip().lower()
        if lang in SUBTITLE_LANGUAGES and lang not in wanted:
            wanted[lang] = caption
    if not wanted:
        return []

    label = episode_key(tmdb_id, season, episode)
    target_dir = os.path.join(
        episode_dir(tmdb_id, season, episode, year), SUBS_SUBDIR
    )
    saved = []
    for lang, caption in wanted.items():
        try:
            fetched = _fetch_caption_text(caption)
            if not fetched:
                continue
            text, source_format = fetched
            # 无论源格式是哪一种，都补齐另一种：vtt 供浏览器原生 <track>，
            # srt 供本地播放器。
            variants = {}
            if source_format == "srt":
                variants["srt"] = text
                variants["vtt"] = srt_to_vtt(text)
            else:
                variants["vtt"] = text
                variants["srt"] = vtt_to_srt(text)
            # 目录推迟到真拿到内容才建，避免留下一堆空 subs/。
            os.makedirs(target_dir, exist_ok=True)
            for fmt in SUBTITLE_FORMATS:
                content = variants.get(fmt)
                if not content or not content.strip():
                    continue
                path = os.path.join(target_dir, f"{lang}.{fmt}")
                with open(path, "w", encoding="utf-8", newline="") as fh:
                    fh.write(content)
                saved.append(f"{SUBS_SUBDIR}/{lang}.{fmt}")
        except Exception as exc:  # noqa: BLE001 - 字幕失败绝不影响整集
            print(f"  [{label}] ⚠️ 字幕 {lang} 获取失败（已跳过）: {exc}",
                  flush=True)
    if saved:
        print(f"  [{label}] 已保存字幕: {', '.join(saved)}", flush=True)
    return saved


def build_meta(entry, success_info, subtitle_files=None):
    """组装 meta.json 的内容：取流侧已有的元数据 + 本次实测的技术参数。

    全部字段都来自已有数据，不发起任何额外网络请求。
    subtitles 填的是本次下载顺手抓到的内嵌字幕；没抓到时为空列表，
    由 fetch_subtitles.py 事后从 SubDL 补抓并就地更新该字段。
    """
    tmdb_id = success_info.get("tmdbId")
    season = success_info.get("season")
    episode = success_info.get("episode")
    return {
        "tmdbId": tmdb_id,
        "imdbId": entry.get("imdb_id"),
        "season": season,
        "episode": episode,
        "title": success_info.get("title") or "",
        "originalTitle": entry.get("original_title"),
        "year": success_info.get("year"),
        "runtimeMinutes": entry.get("runtime_minutes"),
        "genres": entry.get("genres"),
        "titleType": entry.get("title_type"),
        # 本次下载的实测结果，供前端选播放档位/排查画质问题。
        "video": {
            "file": episode_video_name(episode),
            "resolution": success_info.get("resolution"),
            "bitrateKbps": success_info.get("bitrate_kbps"),
            "missingSegmentCount": success_info.get("missing_segment_count"),
        },
        "subtitles": [
            {"language": os.path.basename(path).rsplit(".", 1)[0],
             "format": path.rsplit(".", 1)[-1],
             "path": path}
            for path in (subtitle_files or [])
        ],
        "generatedAt": int(time.time()),
    }


def save_meta(tmdb_id, season, episode, year, meta):
    """把 meta.json 写进集目录，返回路径；失败返回 None（不影响整集）。"""
    if not META_ENABLED:
        return None
    try:
        folder = episode_dir(tmdb_id, season, episode, year)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, "meta.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
        return path
    except Exception as exc:  # noqa: BLE001 - 元信息失败绝不影响整集
        print(f"  [{episode_key(tmdb_id, season, episode)}] "
              f"⚠️ meta.json 写入失败（已跳过）: {exc}", flush=True)
        return None


def collect_sidecar_assets(success_info):
    """列出该集的旁车资产相对路径（相对集目录），如 ["subs/en.vtt", "meta.json"]。

    优先用 success_info 里记录的清单；缺失时（如 pending 记录来自旧版本）回退
    为扫描集目录 —— reupload 补传时 success_info 可能只是一条 pending 记录，
    没有 subtitle_files/has_meta 字段，此时必须能自己发现资产，否则补传会漏掉。
    """
    assets = list(success_info.get("subtitle_files") or [])
    if success_info.get("has_meta"):
        assets.append("meta.json")
    if assets:
        return assets

    # 回退：扫目录。只认我们自己产出的资产，不碰视频与其它文件。
    try:
        folder = episode_dir(
            success_info.get("tmdbId"), success_info.get("season"),
            success_info.get("episode"), success_info.get("year"),
        )
    except ValueError:
        return []
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


def _cleanup_episode_dir(folder):
    """删空 subs/ 与集目录本身。只删空目录，有残留文件就保留。

    只往上删到集目录为止：季/剧/年份目录下通常还有别的集，交给操作系统与
    后续运行自然收敛，逐级试删反而会与并发落盘的其它集抢同一批目录。
    """
    for path in (os.path.join(folder, SUBS_SUBDIR), folder):
        try:
            os.rmdir(path)
        except OSError:
            # 非空或不存在都走这里：非空说明还有别的文件，不该删。
            pass


def upload_sidecar_assets(success_info, assets=None):
    """把 meta.json 等旁车资产上传到与视频同前缀的 R2 位置。

    返回已上传的对象键列表。**全程尽力而为**：单个资产失败只打印，不写 pending、
    不影响视频的上传结果——视频才是主体，元信息缺失顶多是前端少个功能，
    为它把整集打回重传不值得。

    上传成功的资产会按 DELETE_LOCAL_AFTER_UPLOAD 删除本地副本（与视频同口径）：
    R2 已有副本，本地再留一份只会在几十万集规模下累积出海量小文件与 inode，
    而 disk_guard 只监控空间占用、对 inode 耗尽完全失明。
    """
    tmdb_id = success_info["tmdbId"]
    season = success_info.get("season")
    episode = success_info.get("episode")
    year = success_info.get("year")
    key = episode_key(tmdb_id, season, episode)

    if assets is None:
        assets = collect_sidecar_assets(success_info)
    if not assets:
        return []

    try:
        folder = episode_dir(tmdb_id, season, episode, year)
    except ValueError:
        return []

    uploaded = []
    for rel in assets:
        local = os.path.join(folder, *rel.split("/"))
        if not os.path.isfile(local):
            continue
        try:
            s3_key = build_s3_key(tmdb_id, season, episode, year, rel)
            ok, reason = upload_to_r2(local, s3_key)
            if ok:
                uploaded.append(s3_key)
                if DELETE_LOCAL_AFTER_UPLOAD:
                    remove_file(local)
            else:
                print(f"  [{key}] ⚠️ 资产上传失败（已跳过）{rel}: {reason}",
                      flush=True)
        except Exception as exc:  # noqa: BLE001 - 资产失败不影响视频
            print(f"  [{key}] ⚠️ 资产上传异常（已跳过）{rel}: {exc}", flush=True)

    if uploaded:
        print(f"  [{key}] 已上传 {len(uploaded)} 个附属资产", flush=True)
    return uploaded


# ---------- M3U8 解析 ----------
def parse_master_playlist(master_url, retries=None, headers=None):
    """返回 [(resolution, media_playlist_url, declared_bandwidth_kbps), ...]。

    retries 为 None 时用默认强度 PLAYLIST_RETRY_MAX；方案C fallback 里对
    非末节点传更小的值，以便坏节点快速判定并换下一个备用节点。
    headers 为取流阶段记录的节点专属请求头（如特定 Referer/UA），与全局
    HEADERS 合并后使用；为空时行为与旧版完全一致。
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

        # Attributes follow "#EXT-X-STREAM-INF:" so the first one is preceded
        # by ":" rather than "," -- accept both delimiters.
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


# 模式 B 下探测不到分辨率时的占位文案。此时分辨率只是成品元数据、不参与判定，
# 如实标注"未知"即可，不该因为探不到就丢掉一集已经下完的正片。
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

    RESOLUTION_CHECK_ENABLED=True（模式 A）——单一码率曲线：
        门槛 = 基准[codec] × (height / 1080)² × LENIENCY
      - height 用每个流自己的实测高度，不是红线（高分辨率流按自身高度算，
        调低红线时也不会集体免检进伪高清）。
      - 码率需求 ∝ 像素数 ∝ 高度²，故用平方缩放而非线性。

    RESOLUTION_CHECK_ENABLED=False（模式 B，默认）——与分辨率无关的绝对线：
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

    容差用于救回准红线集（如红线 1080 时的 1072/900），避免差几像素被一刀切。

    分辨率判定关闭时恒真——把开关收敛在这一个函数里，三处红线关卡
    （mp4 声明预检 / mp4 样本预检 / mp4 实测复检 / m3u8 流层）与 master 候选
    过滤都会自动放行，无需在每个调用点各写一次 if，也就不会漏掉某一处。
    """
    if not RESOLUTION_CHECK_ENABLED:
        return True
    return height >= MIN_RESOLUTION_HEIGHT * LENIENCY


def bitrate_reject_message(resolution, codec_label, bitrate, min_bitrate):
    """码率不达标的淘汰文案。分辨率在两种模式下的地位不同，措辞也要跟着变。

    模式 A：分辨率是判定标准之一，写在前面合理。
    模式 B：分辨率**根本没参与判定**，若仍以"分辨率 854x480 流…"开头，日后翻
    failed.jsonl 会误以为是分辨率把这一集卡掉的，从而对着一个不生效的
    min_resolution_height 反复调参。故降级为括号里的附带信息。

    ⚠️ 两种措辞都必须保留 `码率未达到门槛` 这段 —— `_PERMANENT_FAILURE_MARKERS`、
    `_REJECT_REASON_RULES` 与 `_DEAD_BITRATE_RE` 都靠它工作，历史 failed.jsonl
    也按它归类。
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


def download_single_segment(url, index, retry_max, delay, headers=None):
    """下载单个 HLS 分片，失败按指数退避重试 retry_max 次。

    确定性失败（401/403/404/410/416，或源站返回 HTML/m3u8 而非视频数据）立即
    上抛、不再退避重试：这类结果重下必然复现，白等十几分钟只会占死下载窗口、
    延误换下一个取流节点。与 mp4 直链块层（_download_mp4_chunk）语义对齐。

    全局 `interrupted` 置位（Ctrl+C / SIGTERM）时立即放弃：开跑前先看一眼，
    退避也用 `interrupted.wait()` 而非裸 sleep。否则 Ctrl+C 后每个 worker 都
    要把剩余重试跑完（最坏 20×60s），进程要拖十几分钟才退得掉——见该事件的注释。
    """
    last_error = None
    for attempt in range(1, retry_max + 1):
        # 已中断：不再发起新请求。抛错让上层按"这一分片失败"处理，
        # 整集随之失败并被记为可重试，下次运行重下即可。
        if interrupted.is_set():
            raise RuntimeError(f"分片 {index + 1} 已取消（收到中断信号）")
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
            # 可打断的退避：中断信号一到立刻醒来，不必空等完这一轮。
            if interrupted.wait(wait):
                raise RuntimeError(
                    f"分片 {index + 1} 已取消（收到中断信号）"
                ) from exc

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
    retry_max 为单分片的重试预算，缺省取 SEG_RETRY_MAX（正片那套死磕）。
    采样调用传 SAMPLE_SEG_RETRY_MAX：探不到就换下一条候选流，不该按正片死磕
    （见该常量的注释——共用预算会把下载窗口占死，本可成功的集会被饿死）。
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
        # init 段同样用本次的 retry_max：采样阶段若在这里死磕 20 次，
        # 分片层分层就白做了（fMP4 源每条候选流都要先过这一关）。
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
# 上游 tv_ids_to_links.py 写入的 urls 元素有两种形态：
#   - 纯 str：历史 results.jsonl（旧 vidup m3u8）；
#   - dict：{"url","provider","type":"m3u8"|"mp4","headers","quality","size"}。
# 这里统一归一成 dict，下游按 type 分支；非法条目返回 None（调用方跳过）。
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
        "quality": parse_int(item.get("quality")),
        "size": parse_int(item.get("size")),
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

    403/410 视为签名直链过期，抛带 _NEEDS_REFETCH_MARKER 的错误（可重试，
    由上层多轮/重新取流兜底）。返回 (total_size, range_ok)：range_ok 仅由
    状态码是否为 206 决定；206 但 Content-Range 总长为 * 时回退到条目声明的 size。
    """
    # 该主机已被熔断（整机故障）：直接放弃，让上层立刻换下一个节点，不再浪费
    # 一次必然 429 的请求。放在函数最前面——连 Session 与请求头都不必准备。
    # 文案不进整集判死表，整集仍可进下一轮重投。
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

    全局 `interrupted`（Ctrl+C / SIGTERM）与 abort_event 同权：任一置位即放弃。
    前者是进程级"整个任务要停"，后者是集级"这一集不用救了"，两者都意味着
    继续退避毫无意义。
    """
    last_error = None
    request_headers = dict(headers)
    request_headers["Range"] = f"bytes={start}-{end}"
    expected = end - start + 1

    def _aborted():
        return interrupted.is_set() or (
            abort_event is not None and abort_event.is_set()
        )

    for attempt in range(1, SEG_RETRY_MAX + 1):
        if _aborted():
            raise RuntimeError(
                f"直链块 {index + 1} 已取消"
                f"（{'收到中断信号' if interrupted.is_set() else '整片已判失败'}）"
            )
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
                    # mp4 直链的 429 实测是**整机故障**而非限流（电影侧 §12.21）：
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
                or _aborted()
            ):
                break
            wait = min(SEG_RETRY_DELAY * (2 ** (attempt - 1)), 60)
            wait += random.uniform(0, min(1.0, wait * 0.2))
            print(
                f"    直链块 {index + 1} 下载失败 "
                f"({attempt}/{SEG_RETRY_MAX}): {exc}; {wait:.1f}s 后重试"
            )
            # 可打断的退避：整片判失败或收到进程级中断时立刻醒来，不空等完退避。
            # ⚠️ 这里绝不能在 abort_event 为 None 时退化成裸 sleep —— 那样
            # 单节点（无 abort_event）的集在 Ctrl+C 后仍会把退避走满。
            # interrupted 是模块级的、永远存在，故直接用它兜底。
            if abort_event is not None:
                if abort_event.wait(wait):
                    break
            elif interrupted.wait(wait):
                break
    raise RuntimeError(
        f"直链块 {index + 1} 重试后仍失败: {last_error}"
    ) from last_error


def _mp4_probe_quality_by_sample(
    url, headers, total_size, sample_path, label, runtime_minutes
):
    """整片下载前先取头部样本验画质，避免整集（GB 级）白下白丢。

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
        if not actual_size:
            print(
                f"  [{label}] 直链头部样本无法探测（moov 可能不在文件头），"
                f"跳过预检、下载整片后再验",
                flush=True,
            )
            return None
        height = actual_size[1]
        resolution = f"{actual_size[0]}x{actual_size[1]}"

        duration = None
        minutes = parse_int(runtime_minutes)
        if minutes and minutes > 0:
            duration = minutes * 60
        else:
            # 无上游时长时才退回样本探测，并做合理性校验：样本只占整片的
            # sample_ratio，若 ffprobe 返回的是样本自身时长，该值会与
            # "整片时长×sample_ratio" 同量级而远小于正常剧集时长。这里用
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


def _download_mp4_direct(node, output_path, label, runtime_minutes=None):
    """mp4 直链：Range 分块并发下载到 output_path，并做画质筛选。

    流程：
      1. quality 声明存在时先过分辨率红线（不达标直接确定性淘汰，省流量）；
      2. Range 探测总长 → 按 MP4_CHUNK_SIZE 分块，MP4_CONCURRENCY 并发，
         严格按块序落盘（复用分片下载的滑动窗口思路）；不做断点续传；
      3. 下载完成后 ffprobe 实测分辨率/编码/时长，码率 = 字节×8/时长 对齐
         bitrate_threshold（与 m3u8 路径同一套门槛）。
    返回 (resolution_str, bitrate_kbps)。任何块失败即整体失败（直链无“缺片豁免”）。
    """
    url = node["url"]
    headers = _mp4_request_headers(node.get("headers"))
    quality = node.get("quality")
    if quality and not meets_resolution_redline(quality):
        raise QualityRejectedError(
            f"声明分辨率 {quality}p 低于红线 {MIN_RESOLUTION_HEIGHT}"
            f"（容差 {LENIENCY:.2f}），跳过"
        )

    total_size, range_ok = _mp4_probe_total_size(url, headers, node.get("size"))
    if not range_ok:
        raise RuntimeError(f"直链不支持 Range 分块下载: {url}")
    if total_size <= 0:
        raise RuntimeError(f"直链总长异常({total_size}): {url}")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    # 画质预检：先取头部样本判分辨率+码率，不达标立刻淘汰，省下整集（GB 级）
    # 的下载流量与下载槽位。判定口径与整片下完后的复检完全一致（同一套红线与
    # bitrate_threshold），所以预检通过的片复检必然也通过，不会重复淘汰。
    #
    # 仅对「显著大于样本」的文件预检：total_size <= MP4_SAMPLE_SIZE 时采样等于
    # 把整片下一遍，之后正片再下一遍 —— 双倍流量却零收益，不如直接走正片下载
    # 后的复检。阈值取样本的 2 倍，保证预检省下的流量至少是样本本身的一倍。
    if total_size > MP4_SAMPLE_SIZE * 2:
        # 样本文件名带 url 摘要：同一集的多个 mp4 节点虽是串行尝试，但摘要能
        # 保证任何调用姿势下都不会两个节点写同一个临时文件。
        sample_path = os.path.join(
            os.path.dirname(output_path) or ".",
            f"mp4sample_{safe_file_token(label)}_"
            f"{hashlib.sha1(url.encode('utf-8')).hexdigest()[:8]}.mp4",
        )
        probed = _mp4_probe_quality_by_sample(
            url, headers, total_size, sample_path, label, runtime_minutes
        )
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


# ---------- 单集处理 ----------
def process_one_entry(entry, processed_ids):
    """处理一集。返回 (label, success, info)，label 为集级 key（缺 key 时回退 tmdbId）。"""
    tmdb_id = entry.get("tmdbId")
    season = parse_int(entry.get("season"))
    episode = parse_int(entry.get("episode"))
    normalized_id = episode_key(tmdb_id, season, episode)
    label = normalized_id or normalize_tmdb_id(tmdb_id)
    title = entry.get("title", "")
    # urls 元素兼容 str（历史 vidup m3u8）与 dict（多源：m3u8/mp4 + headers）。
    urls = [
        node for node in (
            _normalize_url_entry(item) for item in (entry.get("urls") or [])
        ) if node
    ]
    year = entry.get("year")
    runtime_minutes = entry.get("runtime_minutes")

    if not normalized_id or not urls:
        return label, False, {
            "error": "缺少 tmdbId/season/episode 或 urls", "retriable": False,
        }

    # 原子地检查“历史已完成”和“当前正在处理”，防止并发重复下载。
    with processing_lock:
        if normalized_id in processed_ids:
            print(f"跳过已成功处理: {label}")
            return label, False, {"error": "already processed successfully"}
        if normalized_id in processing_ids:
            print(f"跳过当前运行中的重复条目: {label}")
            return label, False, {"error": "duplicate entry currently processing"}
        processing_ids.add(normalized_id)

    # 以下直到 try 之前只有不会抛异常的纯赋值与 def：ID 锁一旦持有，任何可能
    # 抛异常的动作（磁盘闸门等待、建目录）都必须在 try 内，否则会绕过 finally
    # 的 discard，让该集 ID 在本进程内永久卡在“处理中”、后续轮次全被跳过。
    handed_off_to_conversion = False
    # 在 try 之外初始化：下面的 except 会读它，而异常可能在进入节点循环之前
    # 就抛出（磁盘闸门、建目录等），那时它若还没定义就是 NameError。
    any_needs_refetch = False
    # 同理。逐节点失败归因，节点循环之外抛出的异常会让它保持为空列表。
    node_failures = []
    # 同理。因画质被确定性淘汰的节点数，供下方概率判死使用。
    quality_rejected_nodes = 0
    cleanup_paths = set()
    final_ts = os.path.join(TEMP_DIR, f"temp_{safe_file_token(normalized_id)}.ts")
    temp_mp4 = os.path.join(TEMP_DIR, f"temp_{safe_file_token(normalized_id)}.mp4")
    cleanup_paths.update((final_ts, temp_mp4))

    def _attempt_download(node, is_last_node):
        """对单个取流节点尝试完整下载，成功返回 conversion_job，失败抛异常。

        node 为归一化后的 dict；按 type 分支：m3u8 走 playlist 解析 + 采样 +
        分片拼接；mp4 走 Range 分块直链下载（画质筛选在 _download_mp4_direct 内）。
        is_last_node=False（还有备用节点）时，master playlist 解析用短重试
        PLAYLIST_RETRY_FALLBACK，坏节点快速判定即换下一个；末节点/单节点用
        默认 PLAYLIST_RETRY_MAX 死磕，不放过最后的机会。
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
                node, final_ts, label, runtime_minutes
            )
            conversion_job = {
                "tmdbId": tmdb_id,
                "season": season,
                "episode": episode,
                "normalized_id": normalized_id,
                "entry": entry,
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
                # 取流侧带回的内嵌字幕，转封装成功后由 save_subtitles 落盘。
                "captions": entry.get("captions"),
            }
            print(
                f"  [{label}] 直链下载完成，已释放下载槽位并进入转封装队列",
                flush=True,
            )
            return conversion_job

        variants = parse_master_playlist(
            url, retries=None if is_last_node else PLAYLIST_RETRY_FALLBACK,
            headers=node_headers,
        )
        if not variants:
            raise RuntimeError("没有找到媒体播放列表或清晰度变体")

        # 按声明分辨率高度从高到低排，高度相同时优先试 BANDWIDTH 高的。
        # 分辨率未知的流排在最后，等采样后用 ffprobe 探测真实分辨率。
        annotated = [
            (resolution, playlist_url, bandwidth, parse_resolution(resolution))
            for resolution, playlist_url, bandwidth in variants
        ]
        # 模式 A：已声明分辨率且低于红线（含容差）的流直接排除，不必浪费采样流量。
        #   未声明分辨率的流保留，等采样后用 ffprobe 探测真实高度再判。
        # 模式 B：meets_resolution_redline 恒真 → 全部保留，候选一条不筛。
        #   这是对的：低码率流要靠**实测采样码率**才能判，声明分辨率说明不了问题
        #   （480p 也可能是高码率的清晰流）。代价是多采样几条流，但不会误杀。
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

        # 预排序：先试声明高度更高的流，同高度试声明 BANDWIDTH 更高的。
        # 未声明分辨率（item[3] is None）用 -1 排最后，等采样后 ffprobe 探测再定夺。
        # 模式 B 下分辨率不参与判定，改为纯按声明 BANDWIDTH 降序——此时择优只比
        # 码率，高声明带宽的流最可能先胜出，先试它才能让下面的提前终止真正省下采样。
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
        # 内层分流计数（对应 QualityRejectedError 的引入动因）：
        #   quality_rejected_streams —— 确定性画质淘汰的流数
        #   other_failed_streams     —— 瞬时异常（5xx/超时/采样探测失败等）的流数
        # 两者决定"全流无一入选"时该抛确定性异常还是可重试异常。
        # 混在一起就会把源站抽风误判成画质不达标，那是直接砍成功率。
        quality_rejected_streams = 0
        other_failed_streams = 0
        # 签名过期要单独留痕：下面的汇总文案会覆盖掉本条异常原文，
        # marker 不在这里记住就彻底丢失，重取闭环再也挑不到这一集。
        stream_needs_refetch = False

        for resolution, playlist_url, _declared_bandwidth, size in candidates:
            # 【提前终止采样】候选已按声明高度降序排列。走到"声明高度严格低于已
            # 选中流"的候选时，它即便采样也必然落选：有声明分辨率的流下面直接
            # 采信声明值（不做 ffprobe），而择优是"高度绝对优先、同高度才比码率"，
            # height < best_height 时 better 恒为 false。故这里跳过纯属浪费的采样。
            # 一个 master 常有 1080/720/480/360 四档，1080 命中后可省下三次
            # "解析 media playlist + 下载 10 个分片 + 两次 ffprobe"。
            #
            # 四个合取项缺一不可：
            #   RESOLUTION_CHECK_ENABLED —— 本剪枝的正确性完全建立在"择优按高度
            #     绝对优先"之上。模式 B 下择优改成纯比码率，而声明高度低不代表
            #     实测码率低，此时剪枝会真的丢掉更优的流。声明 BANDWIDTH 也不能
            #     拿来剪枝：它是源站声明的峰值带宽，与我们实测的采样码率口径不同，
            #     据此跳过同样会误剪。故模式 B 下老老实实全部采样。
            #   size is not None —— 未声明分辨率的流排在末尾，真实高度未知，
            #     必须采样后 ffprobe，跳过会丢画质；
            #   best_selected —— 只有真正选中过某流才生效，否则最高档瞬时抖动
            #     挂掉后整集会因"无一入选"白白失败，直接损失成功率；
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
                f"sample_{safe_file_token(normalized_id)}_"
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
                        # 采样探不到就换下一条候选流，别按正片那套死磕
                        # （见 SAMPLE_SEG_RETRY_MAX 的注释）。
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
                        # 模式 A：分辨率是判定标准，探不到就无从判断。
                        # 探测失败常是采样片本次没下全/损坏（瞬时抖动），
                        # 不是真无高清流 → 判可重试，下一轮重采样有机会救回。
                        raise RuntimeError("采样探测分辨率失败（可重试）")
                    if actual_size is None:
                        # 模式 B：分辨率不参与判定，采样已经下好了、码率照样算得出，
                        # 仅因探不到分辨率就丢弃这条流是纯粹的误杀。
                        actual_resolution = UNKNOWN_RESOLUTION
                    else:
                        actual_resolution = f"{actual_size[0]}x{actual_size[1]}"
                    print(f"  流 {resolution} 实测分辨率: {actual_resolution}")

                height = actual_size[1] if actual_size else 0
                # 第 1 关 · 分辨率红线（带 LENIENCY 容差）。模式 B 下恒真。
                if not meets_resolution_redline(height):
                    raise QualityRejectedError(
                        f"分辨率 {actual_resolution} 低于红线 "
                        f"{MIN_RESOLUTION_HEIGHT}（容差 {LENIENCY:.2f}），跳过"
                    )

                # 扣除 init 段字节：init 无时长，计入分子会让码率虚高（fMP4 尤甚）。
                media_bytes = max(0, sample_bytes - sample_init_bytes)
                bitrate = media_bytes * 8 / sample_duration / 1000
                print(f"  流 {actual_resolution} 采样码率: {bitrate:.0f} kbps")

                # 第 2 关 · 码率门槛。模式 A 按"该流自身高度"平方缩放并乘 LENIENCY；
                # 模式 B 用不缩放的绝对线。探测不到编码回退 H.264 基准（最严）。
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

                # 择优：模式 A 下实测高度绝对优先、高度完全相同再比采样码率
                # （用实测高度而非粗档 tier，任意分辨率都能精确区分，降档也不退化）；
                # 模式 B 下纯比采样码率——既然高度不再是画质标准，就不该拿它决定
                # "多条合格流选哪条"，否则等于分辨率仍在暗中主导。
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
                # 画质淘汰：本流确定性出局，但**其余流可能达标**，故继续试而不外抛。
                remove_file(sample_path)
                quality_rejected_streams += 1
                print(f"  处理流 {resolution} 失败: {exc}")
            except Exception as exc:
                remove_file(sample_path)
                other_failed_streams += 1
                # 签名过期（401/403/410）要单独记住：下面的汇总文案会覆盖掉
                # 本条异常的原文，marker 若不在这里留痕就会彻底丢失，
                # 重取闭环也就挑不到这一集。
                if needs_refetch(str(exc)):
                    stream_needs_refetch = True
                print(f"  处理流 {resolution} 失败: {exc}")

        if not best_selected:
            # 🔑 两条路径必须分开（这正是引入 QualityRejectedError 的动因）：
            #
            # ① 全部候选流都是画质淘汰、且无一条瞬时异常 —— "重下会不会变好"
            #    已有确定答案：不会。分辨率与码率是源站固有属性，下一轮重采
            #    拿到的还是同样的流。抛确定性异常判死，不再白烧后续轮次。
            #
            #    注意 quality_rejected_streams > 0 这个合取项：candidates 非空时
            #    它必成立，但若未来筛选逻辑演进出"零候选流且零失败"的路径，
            #    没有它就会把空集误判成"全部画质淘汰"。
            if quality_rejected_streams > 0 and other_failed_streams == 0:
                raise QualityRejectedError(
                    f"全部 {quality_rejected_streams} 条候选流均因画质不达标被淘汰"
                    f"（各流原因见上方日志）"
                )
            # ② 混合情形（存在瞬时异常）：无法断定是"真不达标"还是"采样抖动全挂"，
            #    落默认「可重试」交由多轮重采兜底，契合"宁可多下不误杀"。
            #    TV 侧源站 5xx 频发，这条路径是常态，绝不能误判成画质判死。
            refetch_note = (
                f"；另有节点直链已失效，{_NEEDS_REFETCH_MARKER}"
                if stream_needs_refetch else ""
            )
            raise RuntimeError(
                f"本轮候选流无一入选（各流原因见上方日志），下一轮重采{refetch_note}"
            )

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
            "season": season,
            "episode": episode,
            "normalized_id": normalized_id,
            "entry": entry,
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
            # 取流侧带回的内嵌字幕，转封装成功后由 save_subtitles 落盘。
            "captions": entry.get("captions"),
        }
        print(
            f"  [{label}] 分片下载完成，已释放下载槽位并进入转封装队列",
            flush=True,
        )
        return conversion_job

    try:
        # 磁盘水位兜底：仅在此处（尚未开始任何下载动作前）阻塞。磁盘吃紧时新集
        # 在闸门前等待，不会占用 temp/带宽；已在跑的下载不受影响。
        wait_for_disk_gate()
        # 中断后不再开新集：此刻开下只会在第一个分片上立刻取消、白占一次调度，
        # 还会在 temp 留下待清理的残骸。判为可重试失败，下次运行照常重下。
        # （wait_for_disk_gate 收到中断会提前返回，故这一判必须在它之后。）
        if interrupted.is_set():
            return normalized_id, False, {
                "error": "已取消（收到中断信号）", "retriable": True,
            }
        print(f"\n开始处理: {label} - {title}")
        os.makedirs(TEMP_DIR, exist_ok=True)

        # 方案C：依次尝试各取流节点，任一节点下完即成功；全部失败才判失败。
        conversion_job = None
        last_exc = None
        any_retriable = False  # 只要有任一节点是“可重试失败”，整集就值得下一轮重试
        # 🔑 逐节点失败归因：每个节点失败都记一条，而不是只留末节点的 last_exc。
        #
        # 为什么必须逐节点记（没有它就永远查不清）：
        #   - `error` 只保留**最后一个**节点的文案，前面节点的失败原因此前只
        #     print 到 stdout、不落盘，跑完就没了；
        #   - `classify_reject_reason(error_msg)` 是单参数签名，天然只能按整集
        #     归一类，**无法区分同为 m3u8 的 vidup 与 vidfast**；
        #   - 于是"某个源到底为什么失败、该不该摘"只能靠"直链类目 ≈ vidlink"
        #     这种代理指标硬猜（§0.13 P0+ 记的正是这条）。
        # 有了它，pipeline_report 就能直接按 provider 拆分失败原因。
        # （列表本身在 try 之外初始化，见上方注释。）
        # 逐节点记录"是否有节点的直链已过期"（any_needs_refetch 在 try 外初始化）。
        # 必须独立于 error 文案统计——msg 取的是**最后一个**节点的错误，
        # 若过期节点排在前面（如「节点1 vidlink 403 过期 → 节点2 502」），
        # 按文案判断就永远看不到那条 marker，该直链在剩余所有轮次里都是废的。
        for idx, node in enumerate(urls, start=1):
            is_last_node = idx == len(urls)
            try:
                if idx > 1:
                    print(
                        f"  [{label}] 切换备用节点 {idx}/{len(urls)} "
                        f"({node['provider']}/{node['type']})",
                        flush=True,
                    )
                conversion_job = _attempt_download(node, is_last_node)
                # 🔑 provider 归因：在**唯一**知道"是哪个节点成的"的地方贴标签。
                # 两个 _attempt_download 分支（mp4/m3u8）各自构造 conversion_job，
                # 但都拿不到 idx，故统一在此补写，避免两处重复且漏改一处。
                # 没有这两个字段，多节点 fallback 的全部价值都无法量化——
                # 成品里看不出哪家救回来的，也就无从判断某个源该留该撤。
                conversion_job["provider"] = node["provider"]
                conversion_job["node_type"] = node["type"]
                # 第几个节点成功（1-based）。>1 说明 fallback 真的救回了这一集，
                # 是"多源到底有没有用"最直接的证据。
                conversion_job["node_index"] = idx
                conversion_job["node_total"] = len(urls)
                break
            except Exception as exc:
                last_exc = exc
                node_error = str(exc)
                node_retriable = _classify_failure(node_error)
                # 画质淘汰的节点单独计数（认**类型**不认文案，判据演进时零改动）。
                # 供下方概率判死：画质是源站固有属性，不是随机变量。
                if isinstance(exc, QualityRejectedError):
                    quality_rejected_nodes += 1
                # 每个节点一条：provider + 失败类目 + 可否重试。
                # 只存归一化后的类目**和**原始文案：类目供统计聚合，
                # 文案供人工排查（源站措辞变了时类目会落到"其他"，那时只能看原文）。
                node_failures.append({
                    "node_index": idx,
                    "provider": node.get("provider"),
                    "node_type": node.get("type"),
                    "reason_class": classify_reject_reason(node_error),
                    "retriable": node_retriable,
                    "error": node_error,
                })
                # 记录本节点失败是否可重试：任一可重试即让整集进入外层多轮，
                # 避免末节点恰为确定性失败时“连坐”误伤前面本可恢复的瞬时节点。
                if node_retriable:
                    any_retriable = True
                if needs_refetch(node_error):
                    any_needs_refetch = True
                # 本节点失败：清掉本轮残留的 ts，避免污染下一个节点。
                remove_file(final_ts)
                if idx < len(urls):
                    print(f"  [{label}] 节点 {idx} 失败，尝试下一个: {exc}")
        if conversion_job is None:
            raise last_exc if last_exc else RuntimeError("所有取流节点均失败")

        handed_off_to_conversion = True
        return label, True, conversion_job

    except Exception as exc:
        msg = str(exc)
        # 整集可否重试：全节点失败时以“任一节点可重试”为准（乐观，首要目标是下全）；
        # 其它异常路径（单次抛出）回退到按该异常本身分类。
        retriable = any_retriable or _classify_failure(msg)
        # 【画质概率判死】达到阈值比例的节点都因画质确定性淘汰时，推翻上面的乐观口径。
        #
        # 依据：画质是源站**固有属性**而非随机变量——同一条 url 下一轮拿到的还是
        # 480p。既然这些节点都不达标，就没有理由指望重投能变好。
        #
        # 🔴 TV 侧取 1.0（全部节点都画质淘汰才判死），比电影侧的 0.5 保守得多：
        #   - TV 侧大量集**只有 1-2 个节点**（vidup 单源占多数）。阈值 0.5 时
        #     单节点集只要画质淘汰一次就立刻判死 —— 那不是"概率判死"，
        #     是"一次判死"，比电影侧激进得多；
        #   - TV 侧集级有源率只有个位数，误杀一集的相对代价更高；
        #   - 且 download_tv 至今未做过全量实测，没有数据支撑更激进的阈值。
        # 等 §0.0 ③ 跑完、拿到 pipeline_report 的节点数分布与画质淘汰占比后，
        # 再决定要不要调低。调低前务必先看"单节点集占比"。
        #
        # 只在"无一节点成功"的失败路径上生效（本就在 except 里），且要求
        # quality_rejected_nodes > 0，避免 urls 为空等边界被 0/0 蒙混。
        if (
            urls
            and quality_rejected_nodes > 0
            and quality_rejected_nodes / len(urls) >= QUALITY_KILL_RATIO
        ):
            retriable = False
            # 换成汇总文案：不换的话这里留的是**最后一个**节点的错误，会写出
            # 「retriable=False 却写着 502」这类自相矛盾、且让收尾统计归错类的记录。
            msg = (
                f"{len(urls)} 个节点中 {quality_rejected_nodes} 个因画质不达标被"
                f"确定性淘汰（≥ 阈值 {QUALITY_KILL_RATIO:.2f}），判定整集画质不达标；"
                f"末节点错误：{msg}"
            )
        # 过期节点排在非末位时，msg 里读不到过期 marker（或被上面的画质汇总文案
        # 改写掉）。补挂上去，让落盘的 failed.jsonl 也能看出"这集有节点需要重新
        # 取流"——否则只有进程内的标志知道，人工排查与事后统计都看不见。
        if any_needs_refetch and _NEEDS_REFETCH_MARKER not in msg:
            msg = f"{msg}（另有节点{_NEEDS_REFETCH_MARKER}）"
        return label, False, {
            "error": msg,
            "retriable": retriable,
            # 与 error 文案解耦的显式结论，供 plan_retry_buckets 使用。
            "needs_refetch": any_needs_refetch,
            # 逐节点归因（可能为空：节点循环之外抛出的异常没有节点上下文）。
            "node_failures": node_failures,
        }
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
    season = conversion_job["season"]
    episode = conversion_job["episode"]
    normalized_id = conversion_job["normalized_id"]
    final_ts = conversion_job["final_ts"]
    temp_mp4 = conversion_job["temp_mp4"]
    cleanup_paths = conversion_job["cleanup_paths"]
    completed = False

    try:
        print(f"  [{normalized_id}] 开始转封装为 MP4...", flush=True)
        remove_file(temp_mp4)
        if not convert_ts_to_mp4(final_ts, temp_mp4):
            raise RuntimeError("FFmpeg 转换失败")

        print(f"  [{normalized_id}] 转封装完成，准备移动文件", flush=True)
        year = conversion_job.get("year")
        final_path = move_to_target_folder(
            temp_mp4, tmdb_id, season, episode, year
        )
        success_info = {
            "tmdbId": tmdb_id,
            "season": season,
            "episode": episode,
            "title": conversion_job["title"],
            "year": year,
            "url": conversion_job["url"],
            # 取流来源归因：这一集最终是哪家 provider、第几个节点下成的。
            # 用 .get 而非下标：reupload 补传等路径会重建 conversion_job，
            # 缺这几个键也不能让整集在收尾阶段崩掉。
            "provider": conversion_job.get("provider"),
            "node_type": conversion_job.get("node_type"),
            "node_index": conversion_job.get("node_index"),
            "node_total": conversion_job.get("node_total"),
            "final_path": final_path,
            "bitrate_kbps": conversion_job["bitrate_kbps"],
            "resolution": conversion_job["resolution"],
            "missing_segment_count": conversion_job["missing_segment_count"],
            "missing_segment_indices": conversion_job[
                "missing_segment_indices"
            ],
        }

        # 内嵌字幕：成品已落地才抓，抓不到不影响整集。
        # save_subtitles 内部已逐条吞异常，但 episode_dir 等仍可能抛，故再兜一层。
        subtitle_files = []
        try:
            subtitle_files = save_subtitles(
                tmdb_id, season, episode, year,
                conversion_job.get("captions"),
            )
        except Exception as exc:  # noqa: BLE001 - 双重兜底
            print(f"  [{normalized_id}] ⚠️ 字幕阶段异常（已跳过）: {exc}",
                  flush=True)
        success_info["subtitle_files"] = subtitle_files

        # save_meta 内部已吞异常，但 build_meta 仍可能因脏 entry 抛错，故整段再兜一层。
        try:
            if save_meta(
                tmdb_id, season, episode, year,
                build_meta(
                    conversion_job.get("entry") or {}, success_info,
                    subtitle_files,
                ),
            ):
                success_info["has_meta"] = True
        except Exception as exc:  # noqa: BLE001 - 双重兜底
            print(f"  [{normalized_id}] ⚠️ meta 阶段异常（已跳过）: {exc}",
                  flush=True)

        completed = True
        print(f"  [{normalized_id}] 转封装完成: {final_path}", flush=True)
        return normalized_id, True, success_info

    except Exception as exc:
        return normalized_id, False, {"error": str(exc)}
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
    season = success_info["season"]
    episode = success_info["episode"]
    key = episode_key(tmdb_id, season, episode)
    local_path = success_info["final_path"]
    if not S3_ENABLED:
        success_info["uploaded"] = False
        write_log(SUCCESS_LOG, success_info)
        print(f"  [{key}] 成功（未上传，本地保留）: {local_path}",
              flush=True)
        return key, True, success_info

    # 从 build_s3_key 起整段包 try：即使 build_s3_key/write_log/write_pending
    # 等抛异常，也在此兜底为“留本地 + 尽力写 pending + 返回失败”，绝不让异常逃逸
    # 到调用方——否则成品既不删也不进 pending，reupload 无从感知、磁盘永久泄漏。
    try:
        s3_key = build_s3_key(tmdb_id, season, episode, success_info.get("year"))
        ok, reason = upload_to_r2(local_path, s3_key)
        if ok:
            success_info["uploaded"] = True
            success_info["s3_key"] = s3_key
            # 先传附属资产再删视频：删视频不依赖资产结果，但把两者放在一起
            # 便于日志按集聚集。资产上传内部已完全吞异常。
            success_info["asset_keys"] = upload_sidecar_assets(success_info)
            if DELETE_LOCAL_AFTER_UPLOAD:
                remove_file(local_path)
                # 视频与资产都已进 R2，本地集目录此时应为空 —— 删掉它，避免
                # 几十万集规模下留下海量空目录把 inode 吃干净。
                _cleanup_episode_dir(os.path.dirname(local_path))
            write_log(SUCCESS_LOG, success_info)
            print(f"  [{key}] 上传成功: {s3_key}", flush=True)
            return key, True, success_info

        # 上传失败：保留本地文件，写 SUCCESS_LOG(uploaded=false) 防重下 + 写 pending。
        success_info["uploaded"] = False
        success_info["s3_key"] = s3_key
        write_log(SUCCESS_LOG, success_info)
        write_pending({
            "tmdbId": tmdb_id,
            "season": season,
            "episode": episode,
            "title": success_info.get("title", ""),
            "year": success_info.get("year"),
            **_attribution_of(success_info),
            "local_path": local_path,
            "s3_key": s3_key,
            "fail_reason": reason,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        print(f"  [{key}] 上传失败，已留本地待补传: {reason}", flush=True)
        return key, False, {"error": f"上传失败: {reason}"}
    except Exception as exc:
        # 上传流程中任何未预期异常：尽力留本地并补写 pending（pending 写入若也
        # 抛异常则再兜一层，至少保证本地文件不被删、日志有痕迹），返回失败。
        reason = f"上传阶段异常: {exc}"
        try:
            write_pending({
                "tmdbId": tmdb_id,
                "season": season,
                "episode": episode,
                "title": success_info.get("title", ""),
                "year": success_info.get("year"),
                **_attribution_of(success_info),
                "local_path": local_path,
                "s3_key": success_info.get("s3_key", ""),
                "fail_reason": reason,
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as pend_exc:
            print(
                f"  [{key}] ⚠️ 上传异常且 pending 写入失败，本地文件已保留："
                f"{local_path}（{pend_exc}）",
                flush=True,
            )
        print(f"  [{key}] 上传阶段异常，已留本地待补传: {exc}", flush=True)
        return key, False, {"error": reason}





def is_stale_entry(entry, now=None):
    """该集的直链是否已躺过 STALE_LINK_SECONDS。

    ⚠️ **没有 fetched_at 的条目一律判为不陈旧**。旧版 results.jsonl 与手工
    构造的输入都没有这个字段，把它们当成"无限旧"会让整批集在启动时全部去
    重取流——取流配额与耗时双重浪费，且多半是徒劳（那些链接可能好好的）。
    宁可漏判，不可误判。
    """
    if STALE_LINK_SECONDS <= 0:
        return False
    fetched_at = parse_int(entry.get("fetched_at"))
    if fetched_at is None or fetched_at <= 0:
        return False
    return (now or time.time()) - fetched_at > STALE_LINK_SECONDS


def refetch_entries(entries, refetch_counts):
    """就地重新取流：调用取流侧的 provider 拿新 url，返回可重投的 entry 列表。

    与人工重跑 `tv_ids_to_links.py` 不同——取流侧的 load_processed 会把
    results.jsonl 里已成功的集算作"已处理"直接跳过，**根本不会给它们换链接**。
    这里绕开那层跳过，按 (剧, 季, 集) 直接调 process_episode。

    - 新结果会**追加写入 INPUT_JSONL**：与取流侧落盘行为一致，这样即使本次
      运行中途被中断，下次启动也能按 fetched_at 择新直接用上，重取不白做。
    - refetch_counts 按**集级 key** 记数，达 AUTO_REFETCH_MAX_PER_EPISODE 即
      不再重取（新链接同样可能在排队期间再过期，但必须有上限防空转）。
    - 取流侧任何异常都不得逃逸：重取是"锦上添花"的捞回，失败了退回原状即可，
      绝不能让它崩掉整条下载流水线。
    """
    # 延迟导入：取流侧模块 import 时会读 config、要求 TMDB key 与代理凭证并建
    # Session，放在模块级会让"只想跑下载"的场景平白多出这些依赖与副作用。
    #
    # ⚠️ 必须连 SystemExit 一起捕获：tv_ids_to_links 在**模块级**用
    # `raise SystemExit` 做配置校验（缺 TMDB_API_KEY、缺 PROXY_USER/PASSWORD、
    # providers 非法等）。SystemExit 继承 BaseException，`except Exception`
    # 拦不住它。只配了 R2 凭证、没配代理凭证的机器（只跑下载，完全合理）
    # 一旦走到这里，整条流水线会被这个 SystemExit 直接杀掉。
    # 但**不能笼统捕获 BaseException** —— KeyboardInterrupt 必须原样逃逸。
    try:
        import tv_ids_to_links as fetcher
    except (Exception, SystemExit) as exc:
        print(f"⚠️ 无法加载取流模块，跳过重新取流: {exc}", flush=True)
        return []

    pending = []
    for entry in entries:
        key = record_episode_key(entry)
        if not key:
            # 缺 tmdbId/season/episode：定位不到具体一集，无从重取。
            continue
        if refetch_counts.get(key, 0) >= AUTO_REFETCH_MAX_PER_EPISODE:
            continue
        pending.append((key, entry))

    if not pending:
        return []

    print(
        f"\n[自动重取流] {len(pending)} 集就地重新取流"
        f"（并发 {AUTO_REFETCH_WORKERS}）...",
        flush=True,
    )

    revived = []
    executor = ThreadPoolExecutor(max_workers=AUTO_REFETCH_WORKERS)
    try:
        future_to_item = {}
        for key, entry in pending:
            future = executor.submit(
                fetcher.process_episode,
                entry["tmdbId"], entry["season"], entry["episode"],
            )
            future_to_item[future] = (key, entry)
        # 带总超时地收集结果：as_completed 的 timeout 是**整体**预算，超时会抛
        # TimeoutError 中断迭代。此时已完成的部分照常收下，仍在跑的直接放弃。
        try:
            for future in as_completed(
                future_to_item, timeout=AUTO_REFETCH_TIMEOUT
            ):
                key, entry = future_to_item[future]
                refetch_counts[key] = refetch_counts.get(key, 0) + 1
                try:
                    status, result = future.result()
                except (Exception, SystemExit) as exc:
                    # 同 import 处：process_episode 内部也可能触发模块级的
                    # SystemExit 式校验。单集重取失败绝不能带塌整批。
                    print(f"  [重取失败] {key}: {exc}", flush=True)
                    continue
                if status != "ok" or not result or not result.get("urls"):
                    # dead（源站确认无此集）与 retry（瞬时错误耗尽）都不重投：
                    # 前者救不回来，后者留给下次运行——此刻已无新链接可用。
                    print(f"  [重取无果] {key}: {status}", flush=True)
                    continue
                # 落盘新结果，与取流侧行为一致（追加写，下游按 fetched_at 择新）。
                write_log(INPUT_JSONL, result)
                # 用新 urls 覆盖 entry 的取流字段，其余元数据（title/year/
                # runtime_minutes 等）保留：entry 可能带有 result 没有的历史
                # 字段，故逐键覆盖而非整体替换。
                new_entry = dict(entry)
                new_entry["urls"] = result["urls"]
                new_entry["fetched_at"] = result.get("fetched_at")
                # 字幕地址跟旧 urls 一起作废，必须同步刷新（没给就置空）。
                new_entry["captions"] = result.get("captions") or []
                revived.append(new_entry)
                print(
                    f"  [重取成功] {key}: {len(result['urls'])} 个新节点",
                    flush=True,
                )
        except TimeoutError:
            # 超时只是"本次不再等"，不是"作废"：仍在跑的 future 已经把请求发
            # 出去了，那批集本轮沿用旧链接照常下载，不构成损失。
            # 但它们跑完若成功，结果不该白扔——挂回调把新链接落盘，
            # 让"跨运行重试"接力（下次启动按 fetched_at 择新直接用上）。
            done = len(revived)
            late = 0
            for future, (key, _entry) in future_to_item.items():
                if future.done():
                    continue
                future.add_done_callback(
                    functools.partial(_persist_late_refetch, key)
                )
                late += 1
            print(
                f"⚠️ 重取超过 {AUTO_REFETCH_TIMEOUT}s，已收下 {done} 集，"
                f"其余 {late} 集放弃等待（沿用原直链，不影响本次下载；"
                f"若稍后取回成功会落盘供下次运行使用）。",
                flush=True,
            )
    finally:
        # 不等仍在跑的 future：它们最多再跑一个取流超时就自行结束，
        # 而主流程不该为此干等。cancel_futures 砍掉尚未开跑的。
        executor.shutdown(wait=False, cancel_futures=True)
    return revived


def _persist_late_refetch(key, future):
    """refetch_entries 超时后才跑完的重取：成功则落盘 INPUT_JSONL，供下次运行使用。

    在 executor 的 worker 线程里执行（或被 cancel 时在主线程）。本轮主循环早已
    离开 refetch_entries，这里不能再往 revived 里塞，唯一能做的就是把新链接
    持久化，让"跨运行重试"接力。任何异常都不能逃逸——回调里抛错只会被
    concurrent.futures 吞掉记日志，但没必要留这种噪音。
    """
    try:
        if future.cancelled():
            return
        try:
            status, result = future.result()
        except (Exception, SystemExit) as exc:
            print(f"  [重取失败·迟到] {key}: {exc}", flush=True)
            return
        if status != "ok" or not result or not result.get("urls"):
            print(f"  [重取无果·迟到] {key}: {status}", flush=True)
            return
        write_log(INPUT_JSONL, result)
        print(
            f"  [重取成功·迟到] {key}: {len(result['urls'])} 个新节点已落盘，"
            f"下次运行可用",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  [重取迟到落盘异常] {key}: {exc}", flush=True)


def merge_next_batch(round_failed_retriable, revived):
    """合并两条重投路径，按**集级 key** 去重，重取后的新 entry 优先。

    两个桶可能含同一集（既 retriable 又有过期直链，多源下是常态）：
      - 用旧 entry 会让那个 mp4 节点在整轮里继续是废的；
      - 投两份会让同一集被并发下载两次，第二份在 process_one_entry 的
        processing_ids 检查里被判"重复条目"直接丢弃，白占一个下载槽位。
    revived 排在后面，dict 的值取最新者胜出，正好覆盖成新 urls 的版本。

    ⚠️ 去重必须按 (剧, 季, 集) 而非 tmdbId：同一部剧的几十集共用一个 tmdbId，
    按剧去重会让一部剧每轮只剩一集能重投。
    """
    merged = {}
    for entry in list(round_failed_retriable) + list(revived):
        key = record_episode_key(entry)
        if not key:
            continue
        merged[key] = entry
    return list(merged.values())


def _select_stale_entries(entries, label="启动预检"):
    """挑出本次要重取的陈旧条目（TV 侧三步筛选），返回 (待重取列表, 推迟数)。

    同步预检（refresh_stale_entries）与异步预检（dispatch_stale_entries_async）
    **共用同一套口径**——抽出来就是为了这个：两条路径若各写一遍，早晚会漏掉
    画质判死跳过或限额其中一条，而那两条恰恰是 TV 侧规模下最省配额的部分。
    """
    now = time.time()
    stale = [entry for entry in entries if is_stale_entry(entry, now)]
    if not stale:
        return [], 0

    # ① 跳过"上次因画质不达标判死"的集：重取回来还是同样不达标，白烧配额。
    #    TV 侧有源率个位数，这类集在失败总量里占比很高。
    quality_dead = load_quality_dead_keys()
    if quality_dead:
        before = len(stale)
        stale = [
            entry for entry in stale
            if (record_episode_key(entry) or "") not in quality_dead
        ]
        skipped = before - len(stale)
        if skipped:
            print(
                f"[{label}] 跳过 {skipped} 集上次因画质不达标判死的"
                f"（重取回来仍不达标，省下取流配额）",
                flush=True,
            )
        if not stale:
            return [], 0

    # ② 限额 + 最旧优先。TV 全量下 stale 可达几万到十几万集，一次性全投会让
    #    绝大多数集排队到总超时被丢弃（且它们的 refetch_counts 不会 +1，
    #    下次运行又从头再来，队尾永远轮不到）。按 fetched_at 升序截断：
    #    最旧的直链最可能已经过期，优先换它们。
    deferred = 0
    if AUTO_REFETCH_MAX_PER_RUN and len(stale) > AUTO_REFETCH_MAX_PER_RUN:
        # 无 fetched_at 的不会出现在这里（is_stale_entry 已把它们判为不陈旧）。
        stale.sort(key=lambda e: parse_int(e.get("fetched_at")) or 0)
        deferred = len(stale) - AUTO_REFETCH_MAX_PER_RUN
        stale = stale[:AUTO_REFETCH_MAX_PER_RUN]
    return stale, deferred


def dispatch_stale_entries_async(entries, refetch_counts):
    """pipeline 模式的启动预检：把陈旧条目**非阻塞**地投给 async_refetch_hook。

    与 refresh_stale_entries 同一目的、不同手段：pipeline 的前提是主循环一步
    都不阻塞，故不能同步等新链接回来。改为只投递、立即返回——旧链接照常进
    首轮（它未必真失效），新链接由首轮来源（QueueEntrySource）随到随投：若对应
    的旧存量条目还没发出则就地替换掉它，已发出/在下则等它离开处理态再投；
    即使本次运行没赶上消费，AsyncRefetcher 也已把结果追加进 INPUT_JSONL，
    下次运行按 fetched_at 择新直接用上。

    🔑 为什么 pipeline 模式必须有这条路（TV 侧比电影侧更要命）：
    取流侧的 load_processed 只补 results.jsonl 里**没有**的集，不会主动给
    已成功的集换链接。没有这条预检，backlog（断点续跑存量）里的集每次运行都
    拿同一条旧 url 重投 —— 而 §0.0 ③ 要跑很多天，第 1 天的直链到第 10 天
    早已过期，那批集会每次运行都判一遍"需重新取流"然后什么也不做，永久卡死。

    投递计入 refetch_counts（每集重取上限跨预检与轮次共用）。返回投递数。
    """
    hook = async_refetch_hook
    if (
        hook is None or not entries
        or STALE_LINK_SECONDS <= 0 or not AUTO_REFETCH_ENABLED
    ):
        return 0

    stale, deferred = _select_stale_entries(entries)
    to_dispatch = []
    for entry in stale:
        key = record_episode_key(entry)
        if not key:
            continue
        if refetch_counts.get(key, 0) >= AUTO_REFETCH_MAX_PER_EPISODE:
            continue
        refetch_counts[key] = refetch_counts.get(key, 0) + 1
        to_dispatch.append(entry)
    if not to_dispatch:
        return 0

    try:
        hook.dispatch(to_dispatch)
    except Exception as exc:  # noqa: BLE001
        # 预检是锦上添花：投不出去就沿用旧链接，绝不影响首轮下载。
        print(f"⚠️ 启动预检投递失败，沿用原直链继续: {exc}", flush=True)
        return 0

    hours = STALE_LINK_SECONDS / 3600
    print(
        f"\n[启动预检] {len(to_dispatch)}/{len(entries)} 集的直链已超过 "
        f"{hours:.0f} 小时，已交给取流线程异步换新（不阻塞首轮下载；"
        f"旧链接照常先试，新链接回收后随到随投或留待下次运行）。",
        flush=True,
    )
    if deferred:
        print(
            f"[启动预检] 另有 {deferred} 集也已陈旧，本次不处理"
            f"（单次上限 {AUTO_REFETCH_MAX_PER_RUN} 集，按最旧优先）；"
            f"它们照常用原直链下载，下次运行会优先轮到。",
            flush=True,
        )
    return len(to_dispatch)


def refresh_stale_entries(entries, refetch_counts):
    """启动时把陈旧条目换成新直链，返回替换后的完整列表（顺序不变）。

    🔑 **重取无果的条目原样保留**，绝不丢弃。三条理由：
      1. STALE_LINK_SECONDS 只是经验阈值，旧链接未必真失效，试一次不损失什么；
      2. 只配了 R2 凭证、没配代理凭证的机器根本取不了流（refetch_entries 会
         整体返回空），丢弃等于这批集全军覆没；
      3. 真过期的话下载侧会挂 _NEEDS_REFETCH_MARKER 判为确定性失败，
         下次运行的本预检还会再救它一次 —— 退路本来就有。
    所以本函数**只可能让结果变好**，不会比不做更差。
    """
    if not entries or STALE_LINK_SECONDS <= 0 or not AUTO_REFETCH_ENABLED:
        return entries

    # 筛选口径与异步预检完全一致（画质判死跳过 + 限额 + 最旧优先），见该函数。
    stale, deferred = _select_stale_entries(entries)
    if not stale:
        return entries

    hours = STALE_LINK_SECONDS / 3600
    print(
        f"\n[启动预检] {len(stale)}/{len(entries)} 集的直链已超过 "
        f"{hours:.0f} 小时，先换新链接再下载"
        f"（省掉'下完才发现过期'的一整轮无效下载）...",
        flush=True,
    )
    if deferred:
        print(
            f"[启动预检] 另有 {deferred} 集也已陈旧，本次不处理"
            f"（单次上限 {AUTO_REFETCH_MAX_PER_RUN} 集，按最旧优先）；"
            f"它们照常用原直链下载，下次运行会优先轮到。",
            flush=True,
        )
    revived = refetch_entries(stale, refetch_counts)
    by_key = {}
    for entry in revived:
        key = record_episode_key(entry)
        if key:
            by_key[key] = entry
    if not by_key:
        print(
            "[启动预检] 一集都没换到新链接，全部沿用原直链继续下载"
            "（它们未必真失效；真过期会在下载失败后记为确定性失败，"
            "下次运行的预检会再试一次）。",
            flush=True,
        )
        return entries

    refreshed = [
        by_key.get(record_episode_key(entry) or "", entry) for entry in entries
    ]
    print(
        f"[启动预检] {len(by_key)}/{len(stale)} 集换到新链接，"
        f"其余 {len(stale) - len(by_key)} 集沿用原直链照常下载。",
        flush=True,
    )
    return refreshed


class ListEntrySource:
    """把既有的固定 list 包装成来源接口：行为与改造前逐个索引取数完全一致。

    poll() 返回三态 (state, entry)：
      - ("item", entry)  取到一集
      - ("wait", None)   暂时没货但来源未耗尽（只有流式来源会返回）
      - ("done", None)   来源已耗尽

    第二轮起的重试批次仍用它，故多轮语义零改动；首轮在 download_tv.py 单独
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
        from botocore.exceptions import ClientError, BotoCoreError
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
    线程池里正在退避重试的分片 worker 毫不知情，会各自把重试跑完
    （最坏 20×60s）。电影侧服务器实跑实测——中断统计都打印完了，进程还挂着
    31 个线程继续刷失败日志，只能 kill -9；而 kill -9 会截断正在写的
    results.jsonl / success.jsonl，把断点续跑的账本写坏。

    首次收到信号：置位事件 + 恢复默认处理器，然后照常抛 KeyboardInterrupt 走
    正常收尾（落盘、打统计、释放锁）。
    再按一次 Ctrl+C 就是默认行为（立即终止），给"等不及了"留出硬退出的口子。

    pipeline 模式下该处理器同样生效：`downloader.main()` 在主线程里跑，
    取流线程则由 pipeline 的 stop_event 负责收尾，两条路径互不干扰。
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
    # 不会被任何后续轮次处理，只能靠人跑 `download_tv.py reupload`。此时
    # R2 往往已经恢复，自动补一次能省掉这次人工介入。
    #
    # ⚠️ 位置有四个讲究，都不能改：
    #   ① 在 release_main_lock() **之后** —— reupload_pending 内部有
    #      is_main_running() 跨进程守卫，锁还没释放会把自己挡掉、静默什么也不做；
    #   ② 在 finally **之外**（正常路径上）—— 被中断/异常退出时不补传，
    #      此刻状态未知，且用户正想让它停下，不该再发起一批上传；
    #   ③ 在 compact_failed_log() **之前** —— 补传成功会删掉 stage=="upload"
    #      的失败行，先补传再轮转，归档的才是最终态；
    #   ④ `except (Exception, SystemExit)` 兜住 —— 收尾动作失败不该把一次
    #      已经跑完的运行判成失败退出。但**必须放过 KeyboardInterrupt**。
    if AUTO_REUPLOAD_ENABLED and S3_ENABLED:
        print("\n===== 收尾自动补传 =====", flush=True)
        try:
            reupload_pending()
        except (Exception, SystemExit) as exc:
            # 补传失败不影响主流程的成功结论：成品仍在本地且 pending 记录还在，
            # 随时可以手动 reupload。
            print(f"⚠️ 自动补传异常，成品仍留本地待手动 reupload: {exc}",
                  flush=True)

    # failed.jsonl 轮转：纯追加的它在全量跑时会涨到让每次启动的
    # load_quality_dead_keys() 全量顺扫都变慢。
    #
    # ⚠️ 位置有三个讲究，都不能改：
    #   ① 在 finally **之外** —— 被中断/异常退出时不做，此刻状态未知，
    #      维护动作让位于尽快退出（与电影侧同口径）；
    #   ② 在 release_main_lock() **之后** —— compact_failed_log 内部用
    #      is_main_running() 做跨进程守卫，锁还在会把自己挡掉；
    #   ③ 整段 try 住 —— 轮转纯属维护，失败了只是文件继续变大，
    #      绝不能让它把一次成功的运行变成异常退出。
    try:
        rotated, archive_path, kept = compact_failed_log()
        if rotated:
            print(
                f"\n[日志轮转] failed.jsonl 已超过 "
                f"{FAILED_LOG_MAX_BYTES / 1024 / 1024:.0f}MB，"
                f"完整历史已归档到 {archive_path}；"
                f"回填 {kept} 条仍有用的记录（重取闭环与画质预检照常可用）。",
                flush=True,
            )
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ failed.jsonl 轮转异常（已忽略）: {exc}", flush=True)


def _run_pipeline():
    clean_temp_directory()
    print("已清理 temp 目录中的旧临时文件")

    logged_ids = load_success_log_ids()
    disk_ids, duplicate_files = scan_downloaded_mp4_ids()
    # 画质判死账本：这些集"有源但流的画质不达标"，重下必然得到同样结论。
    # --retry-dead 时按当前门槛逐条复判，够格的放回重试队列。
    dead_ids = load_dead_keys(dead_record_passes_now if RETRY_DEAD_MODE else None)
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
    #   - pipeline 模式：集子由**同进程的取流线程**实时产出，全新部署时这个
    #     文件本来就还不存在（TV 侧还要先花时间展开季集才会写出第一条）。
    #     此时若照旧 return，下载侧会在启动瞬间退出，整条流水线只剩取流在跑——
    #     首次部署必现，且表现为"跑完什么都没下"。
    # 故 pipeline 模式（来源由 ListEntrySource 钩子接管）下把它当空存量继续跑，
    # 后续的集全部从队列里来。
    streaming = ListEntrySource is not _ListEntrySource
    if not os.path.exists(INPUT_JSONL):
        if not streaming:
            print(f"错误: 找不到 {INPUT_JSONL}")
            return
        print(f"{INPUT_JSONL} 尚不存在（全新部署），等待取流侧实时产出", flush=True)

    # 读入取流结果。同一集可能因复扫/重跑在文件里留下多行，取 fetched_at 最大
    # 的那条：越新的 url 越可能仍然有效，对 vidlink 这类带时效签名的直链尤其
    # 关键（拿到过期链接等于白跑一次下载）。旧版 results.jsonl 没有该字段，
    # 此时回退到"文件中后出现者胜"——追加写下位置即时序，与旧行为一致。
    by_key = {}
    duplicate_input_count = 0
    invalid_input_count = 0
    skipped_processed_count = 0
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

        normalized_id = record_episode_key(entry)
        if not normalized_id:
            # 缺 tmdbId/season/episode：无法定位到具体一集，下发下去也只会
            # 在 process_one_entry 里判"缺少字段"确定性失败并写一条 FAILED_LOG。
            # 在此直接计数跳过，避免残缺输入放大成等量的失败记录与日志噪声。
            invalid_input_count += 1
            continue
        if normalized_id in processed_ids:
            # 已成功处理过：与其提交进线程池再由 process_one_entry 逐个跳过
            # （每集一次调度 + 一次锁竞争），不如读入阶段直接滤掉。第二次
            # 全量重跑时 success.jsonl 已有数万条，这里能省下等量的空转。
            skipped_processed_count += 1
            continue
        previous = by_key.get(normalized_id)
        if previous is not None:
            duplicate_input_count += 1
            # 无 fetched_at 时视为 -1，保证有戳的一定胜出；两者都无戳则
            # 后出现者胜（不 continue），维持旧的"文件位置即时序"语义。
            new_ts = parse_int(entry.get("fetched_at"))
            old_ts = parse_int(previous.get("fetched_at"))
            if (new_ts if new_ts is not None else -1) < (
                old_ts if old_ts is not None else -1
            ):
                continue
        by_key[normalized_id] = entry

    # dict 保持插入顺序：同一集重复出现时值已被更新者覆盖，但键的位置仍是首次
    # 出现的位置。即"值取最新、位置取最早"——顺序只影响下载先后，不影响正确性。
    entries = list(by_key.values())

    print(
        f"共读取 {len(entries)} 个待处理条目（集）；"
        f"跳过 {skipped_processed_count} 个已处理集、"
        f"{duplicate_input_count} 个重复集（同集取 fetched_at 最新的一条）、"
        f"{invalid_input_count} 个缺少身份字段的条目"
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
        round_failed_expired 为本轮“有节点直链过期、需重新取流”的收集器。
        两个桶**不互斥**：同一集可能既值得重投、又需要换新直链（见
        plan_retry_buckets）。末轮排空阶段两者都传 None（此时 pending 里只会
        剩转封装/上传，不会命中下载分支）。
        """
        stage = stage_of.pop(future, None)

        if stage == "download":
            entry = download_future_to_entry.pop(future)
            ident = _entry_identity(entry)
            try:
                label, download_success, info = future.result()
            except Exception as exc:
                label = record_episode_key(entry) or normalize_tmdb_id(entry.get("tmdbId"))
                download_success = False
                info = {"error": str(exc), "retriable": _classify_failure(str(exc))}

            if download_success:
                # 下载成功：写独立的下载态状态文件（只记下载，不含转封装/上传）。
                # 带上 provider 归因：这是**下载这一级**唯一的落盘点，
                # pipeline_report 靠它算各源的"给出的节点 → 真下成"转化率。
                # info 此刻就是 conversion_job（process_one_entry 的返回值）。
                write_log(DOWNLOAD_OK_LOG, {
                    **ident,
                    "year": entry.get("year"),
                    "provider": info.get("provider"),
                    "node_type": info.get("node_type"),
                    "node_index": info.get("node_index"),
                })
                # submit 若抛异常（如线程池已 shutdown），finalize 永不执行 →
                # processing_ids 锁与临时文件会永久泄漏。故兜底：失败即释放 ID 锁、
                # 清理已交接的临时文件，并当作转封装失败记录（与 upload submit 对称）。
                try:
                    conversion_future = conversion_executor.submit(
                        finalize_one_entry, info, processed_ids
                    )
                except Exception as exc:
                    normalized_id = record_episode_key(entry)
                    with processing_lock:
                        processing_ids.discard(normalized_id)
                    for path in info.get("cleanup_paths", []):
                        remove_file(path)
                    write_log(FAILED_LOG, {
                        **ident,
                        "urls": entry.get("urls", []),
                        "error": f"转封装提交失败: {exc}",
                        "stage": "conversion",
                    })
                    print(f"转封装提交失败: {label}: {exc}")
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
                **ident,
                "urls": entry.get("urls", []),
                "error": error_msg,
                "stage": "download",
                # 跨运行可见的失败性质。DOWNLOAD_FAIL_LOG 虽也记 retriable，
                # 但它每轮开头清空、只反映本轮，跨运行读不到。
                # 启动预检靠这个字段跳过"画质判死"的集（见 load_quality_dead_keys）：
                # 没有它就无法区分"这条流画质不达标"与"今天源站挂了"，
                # 而后者重取完全可能换到好流，绝不能一并跳过。
                "retriable": retriable,
                # 逐节点归因：error 只留末节点文案，这里保留每个节点各自的
                # provider / 类目 / 可否重试，供 pipeline_report 按源拆分失败原因。
                # 空列表表示异常抛在节点循环之外（磁盘闸门、建目录等）。
                "node_failures": info.get("node_failures") or [],
            })
            # 下载态状态文件：本轮下载失败逐条记录（含可否重试）。
            write_log(DOWNLOAD_FAIL_LOG, {
                **ident,
                "error": error_msg,
                "retriable": retriable,
            })
            print(
                f"下载失败: {label}: {error_msg}"
                f"（{'可重试' if retriable else '确定性失败,不重试'}）"
            )
            # 画质判死落账本：下次运行直接跳过，不再重新采样求证同一结论。
            # 只记"源给了流但画质不达标"（is_quality_dead 已挡住瞬时失败），
            # 门槛调松后可用 --retry-dead 按新门槛放回。
            # 🔴 必须传 node_failures：判死要求**全部节点**都给出画质结论，
            # 不传的话"某节点直链失效 + 末节点画质不达标"会被误判死（见其文档）。
            if is_quality_dead(
                retriable, error_msg, info.get("node_failures")
            ):
                record_quality_dead(entry, error_msg, entry.get("urls"))
            # 两条重投路径**不互斥**，判定集中在 plan_retry_buckets（见其文档）。
            # needs_refetch 优先取 info 里的显式标志（节点循环逐节点记录，不受
            # "error 只留末节点文案"的影响）；缺失时回退按文案判断，兼容
            # process_one_entry 之外的异常路径。
            should_retry, should_refetch = plan_retry_buckets(
                retriable, error_msg, info.get("needs_refetch")
            )
            # 仅“可重试”的失败进入下一轮；确定性失败绝不重下。
            if should_retry and round_failed_retriable is not None:
                round_failed_retriable.append(entry)
            # 有节点的签名直链已过期：重投拿到的还是同一条、必然再挂，
            # 必须换新 url 才有意义。
            if should_refetch and round_failed_expired is not None:
                round_failed_expired.append(entry)
                # pipeline 模式下**发现即投递**，不攒到轮末（§0.20）。
                #
                # 🔴 这条路径在 max_rounds=1 时是**唯一**的当次自愈机会：
                # 轮末那条集中重取的判据是 has_more_rounds（round_no < MAX_ROUNDS），
                # 单轮下恒假。首轮来源 QueueEntrySource 会随到随投，
                # 新链接当轮就能下掉；攒到轮末则根本没有下一轮去消费它。
                # 末轮且来源已不是队列时不投：拿到也没轮次用，白耗取流配额。
                if (
                    streaming
                    and async_refetch_hook is not None
                    and AUTO_REFETCH_ENABLED
                    and (round_no == 1 or round_no < MAX_ROUNDS)
                ):
                    key = record_episode_key(entry)
                    used = (
                        refetch_counts.get(key, 0) if key
                        else AUTO_REFETCH_MAX_PER_EPISODE
                    )
                    if key and used < AUTO_REFETCH_MAX_PER_EPISODE:
                        try:
                            accepted = async_refetch_hook.dispatch([entry])
                            if accepted is not None and accepted <= 0:
                                # 该集的重取仍在途（多半是预检投的那次还没回来），
                                # 钩子已合并，不重复计额度。
                                print(
                                    "  → 直链过期，该集重取仍在途，合并等待结果",
                                    flush=True,
                                )
                            else:
                                refetch_counts[key] = used + 1
                                print(
                                    f"  → 直链过期，已即刻投递异步重取"
                                    f"（第 {used + 1}/"
                                    f"{AUTO_REFETCH_MAX_PER_EPISODE} 次）",
                                    flush=True,
                                )
                        except Exception as exc:  # noqa: BLE001
                            # 投递失败不影响本集的失败结论，轮末/下次运行还有退路。
                            print(f"⚠️ 异步重取投递失败: {exc}", flush=True)

        elif stage == "conversion":
            entry = conversion_future_to_entry.pop(future)
            ident = _entry_identity(entry)
            try:
                label, conversion_success, info = future.result()
            except Exception as exc:
                label = record_episode_key(entry)
                conversion_success = False
                info = {"error": str(exc)}

            if not conversion_success:
                write_log(FAILED_LOG, {
                    **ident,
                    "urls": entry.get("urls", []),
                    "error": info.get("error", "未知错误"),
                    "stage": "conversion",
                })
                print(f"转封装失败: {label}: {info.get('error', '未知错误')}")
                return

            # 转封装成功 -> 立即提交上传。反压：先 acquire 信号量（限制
            # 在途+排队的上传总量为 MAX_PENDING_UPLOADS），若上传慢于下载
            # 会在此阻塞主循环，从而钳制本地磁盘占用上限。release 由 future
            # 完成回调对称释放，保证无论上传成功/异常/取消都不泄漏信号量。
            #
            # 但阻塞必须有上限：这里是**主事件循环线程**，无限等待会让整条
            # 流水线冻结（下载完成的 future 也没人处理、转封装同步停摆）。
            # 等满 UPLOAD_SLOT_WAIT_TIMEOUT 仍拿不到槽位，说明 R2 长时间消化
            # 不动，此时降级：不提交上传、成品留本地并写 pending，主循环继续
            # 推进下载与转封装，事后用 reupload 子命令补传。宁可暂时不传，
            # 也不让远端故障拖垮本地下载产能。
            #
            # S3_ENABLED=False 时上传任务只写日志、秒回，不可能积压；万一
            # 仍走到这里也不能写 pending——纯本地模式下的成品无需补传，
            # 塞进 pending 只会污染 reupload 的输入。故降级只对开启上传生效。
            if not upload_semaphore.acquire(timeout=UPLOAD_SLOT_WAIT_TIMEOUT):
                degrade_reason = (
                    f"上传积压超过 {UPLOAD_SLOT_WAIT_TIMEOUT:g}s 未消化，"
                    f"本集降级为留本地待补传"
                )
                if S3_ENABLED:
                    try:
                        write_pending({
                            "tmdbId": info.get("tmdbId"),
                            "season": info.get("season"),
                            "episode": info.get("episode"),
                            "title": info.get("title", ""),
                            "year": info.get("year"),
                            **_attribution_of(info),
                            "local_path": info.get("final_path"),
                            "s3_key": "",
                            "fail_reason": degrade_reason,
                            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    except Exception as exc:
                        print(f"⚠️ 降级写 pending 失败: {label}: {exc}", flush=True)
                # 记 SUCCESS_LOG(uploaded=false) 防止下次运行重新下载。用
                # update_success_log 按集级 key 覆盖写（而非 write_log 追加）：
                # 后续 reupload 补传成功时也走同一函数覆盖同一条，保证
                # SUCCESS_LOG "每集一条" 的设计意图不被破坏。
                info["uploaded"] = False
                try:
                    update_success_log(record_episode_key(info) or label, info)
                except Exception as exc:
                    print(f"⚠️ 降级写 success 日志失败: {label}: {exc}", flush=True)
                write_log(FAILED_LOG, {
                    **ident,
                    "urls": entry.get("urls", []),
                    "error": degrade_reason,
                    "stage": "upload",
                })
                print(f"⚠️ {label}: {degrade_reason}", flush=True)
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
                    **ident,
                    "urls": entry.get("urls", []),
                    "error": f"上传提交失败: {exc}",
                    "stage": "upload",
                })
                print(f"上传提交失败: {label}: {exc}")
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
            ident = _entry_identity(entry)
            try:
                label, upload_success, info = future.result()
            except Exception as exc:
                label = record_episode_key(entry) or normalize_tmdb_id(entry.get("tmdbId"))
                upload_success = False
                info = {"error": str(exc)}

            if not upload_success:
                # upload_one_entry 内部已写 pending 与
                # SUCCESS_LOG(uploaded=false)，这里再落一条 FAILED_LOG
                # 便于统计上传阶段失败。
                write_log(FAILED_LOG, {
                    **ident,
                    "urls": entry.get("urls", []),
                    "error": info.get("error", "未知错误"),
                    "stage": "upload",
                })
                print(f"上传失败: {label}: {info.get('error', '未知错误')}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as download_executor, \
            ThreadPoolExecutor(max_workers=CONVERT_WORKERS) as conversion_executor, \
            ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as upload_executor:

        current_batch = entries
        # 每集被就地重取过几次（集级 key → 次数），跨预检与后续轮次共用同一本账，
        # 防"取流-过期-重取"反复空转。
        refetch_counts = {}
        # 陈旧直链启动预检：两条路径按运行模式分流，**都不会被跳过**。
        #
        #   - 非流式（单独跑 download_tv.py）：同步 refresh_stale_entries，
        #     换完新链接再开跑。此刻下载还没开始，堵一会儿无妨。
        #   - pipeline 模式：异步 dispatch_stale_entries_async，只投递、
        #     立即返回。因为 refetch_entries 是同步的、最长堵 AUTO_REFETCH_TIMEOUT，
        #     而 pipeline 的整个前提是"主循环一步都不阻塞"——堵住它反而会让
        #     队列里的新鲜直链继续变旧，与预检目的正相反。
        #
        # 🔴 2026-09-14（§0.20）之前这里只有非流式一条路，pipeline 模式**完全
        #    不做预检**。那会造成死闭环：取流侧的 load_processed 跳过已成功的集、
        #    不给它们换链接，而 backlog 里的旧 url 每次运行都原样重投一遍 ——
        #    ③ 全量重跑要跑很多天，第 1 天的直链到第 10 天早已过期，
        #    这批集会每次运行都判一遍"需重新取流"然后什么也不做，永久卡死。
        streaming = ListEntrySource is not _ListEntrySource
        if streaming:
            dispatch_stale_entries_async(current_batch, refetch_counts)
        else:
            current_batch = refresh_stale_entries(current_batch, refetch_counts)
        round_no = 1
        while True:
            # 每轮开头清空 download_fail 状态文件，只记录本轮下载失败。
            truncate_log(DOWNLOAD_FAIL_LOG)
            # 本轮待下载条目的来源。首轮在 pipeline 模式下是队列来源（由外部
            # 替换 ListEntrySource 注入），其余轮次仍是 list —— 故多轮语义、
            # 轮次冷却的触发时机全部不变。
            batch_source = (
                current_batch if hasattr(current_batch, "poll")
                else ListEntrySource(current_batch)
            )
            # 队列来源没有确定总量，故取不到长度时显示"持续接收中"而不是崩掉。
            try:
                batch_total = f"{len(batch_source)} 集"
            except TypeError:
                batch_total = "持续接收中"
            if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
                print(
                    f"\n===== 下载轮次 {round_no}/{MAX_ROUNDS}："
                    f"本轮待下载 {batch_total} =====",
                    flush=True,
                )

            # 分批投递：不再一次性把整轮全部集 submit 进 pending。同时存在的
            # 下载 future 上限为 DOWNLOAD_QUEUE_DEPTH，wait() 每次挂/摘 waiter
            # 的规模从 O(整轮集数) 降到 O(槽位数)——全量重跑几万集时，一次性
            # 全投会让主循环退化成 O(N²) 空转，把 CPU 耗在 waiter 管理上。
            # 语义完全不变：本轮每一集仍会被逐一投递，且全部有结论后才进下一轮；
            # 附带收益是磁盘占用更平滑（未投递的集不占 temp）。
            source_exhausted = False
            # 来源暂时没货（只有流式来源会出现）。它决定主循环的 wait 要不要
            # 加超时：见下面循环里的说明。
            source_waiting = False
            round_download_futures = set()

            def submit_downloads():
                """从来源取集填满下载槽位。返回 False 表示"来源暂时没货但未耗尽"。

                取到 "wait" 时立即停止本次投递（而不是原地等），把控制权交回主
                循环去推进在途的转封装/上传——绝不能在这里阻塞，否则已下载完的
                集无人提交转封装、成品堆在 temp、上传信号量不释放。
                """
                nonlocal source_exhausted, source_waiting
                # 每次投递重新判定：槽位填满或来源耗尽而返回时，"饿着"就不再成立。
                source_waiting = False
                while len(round_download_futures) < DOWNLOAD_QUEUE_DEPTH:
                    if source_exhausted:
                        return True
                    state, entry = batch_source.poll()
                    if state == "done":
                        source_exhausted = True
                        return True
                    if state == "wait":
                        source_waiting = True
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
            # 本轮"有节点直链过期"的集。与上面的桶不互斥（见 plan_retry_buckets）。
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
                    # 已排空，但取流侧还没产出新集"（TV 侧季集展开阶段会持续处于
                    # 这个状态）。此时 wait(空集合) 会立刻返回、退化成 100% CPU
                    # 空转，故让出 CPU 后重新问来源要货。
                    # list 来源永不返回 wait，故这段对单独跑下载的场景是死分支。
                    time.sleep(STREAM_IDLE_POLL_SECONDS)
                    submit_downloads()
                    continue
                # 🔑 来源正饿着时必须定时醒来重问：「取流侧产出了新集」**不是
                # future 完成事件**，无法唤醒 wait。若一直无超时阻塞，新集就得
                # 干等到某个在途的转封装/上传恰好完成才被顺带发现——而在途的慢
                # 任务恰恰是上传（一集几百 MB 传 R2），最坏要等上几分钟，期间
                # 下载槽位全空着。
                #
                # 这等于制造了一个"假反压"：效果与反压相同（停止投递新集），
                # 理由却完全不同 —— 取流饿着跟磁盘压力毫无关系。真正的反压有
                # 三道闸门各司其职（DOWNLOAD_QUEUE_DEPTH / upload_semaphore /
                # disk_gate），不需要也不应该靠"主循环恰好没醒"来间接限流。
                #
                # TV 侧取流是瓶颈（有源率个位数、每集要打多个 provider），
                # "饿着"是常态而非边角场景，故这里的损失会被持续放大。
                #
                # 槽位已满或来源已耗尽时 source_waiting 为 False，保持无超时
                # 阻塞，不做无谓唤醒；list 来源永不返回 wait，故单独跑下载时
                # 该值恒为 False，行为与改动前完全一致。
                timeout = STREAM_IDLE_POLL_SECONDS if source_waiting else None
                done, _ = wait(
                    pending, return_when=FIRST_COMPLETED, timeout=timeout
                )
                for future in done:
                    pending.discard(future)
                    round_download_futures.discard(future)
                    # 单个 future 处理若抛异常（如日志写盘 OSError/磁盘满、
                    # 状态字典错位 KeyError），只记录并跳过，绝不让异常逃逸出
                    # 主循环——否则同批其余 future 全丢、整条流水线崩溃、在途
                    # 信号量与 processing_ids 锁无从释放（铁律：宁可单片失败，
                    # 绝不崩主流程）。
                    try:
                        handle_done_future(future, round_failed_retriable,
                                           round_failed_expired)
                    except Exception as exc:
                        print(f"⚠️ future 处理异常，已跳过该条: {exc}", flush=True)
                # 腾出槽位后立即补投，保持下载池始终满载。
                submit_downloads()

            # 本轮下载全部有结论，决定是否再来一轮。
            # 先处理"直链过期"桶：这批集重投同一条 url 必然再挂，必须换新 url。
            #
            # 两条路径（互斥，由运行模式决定）：
            #   - 异步（pipeline 模式）：过期集已在 handle_done_future 里
            #     **发现即投递**。首轮来源 QueueEntrySource 随到随投、且在途
            #     归零前不报 done，故首轮末这里通常无事可做；第 2 轮起来源是
            #     list，不再直接消费重取结果，改由这里等一等再 collect()。
            #   - 同步（只跑下载，无取流线程）：走原有的 refetch_entries 老路。
            revived = []
            has_more_rounds = round_no < MAX_ROUNDS
            if async_refetch_hook is not None:
                # 先收已完成的重取结果——它们是真正救回来的集，
                # 与同步路径的 revived 等价，走同一套 merge_next_batch 合并。
                try:
                    revived = async_refetch_hook.collect()
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️ 收取异步重取结果失败: {exc}", flush=True)
                    revived = []

            if AUTO_REFETCH_ENABLED and round_failed_expired and has_more_rounds:
                try:
                    if async_refetch_hook is not None:
                        if not streaming:
                            # 装了钩子却不是流式来源（正常部署不会出现）：
                            # 没人在轮中投递，退回轮末集中投递。次数上限仍由
                            # 这里把关——钩子只负责投递，不认识 refetch_counts。
                            to_dispatch = []
                            for entry in round_failed_expired:
                                key = record_episode_key(entry)
                                if not key:
                                    continue
                                if refetch_counts.get(key, 0) >= AUTO_REFETCH_MAX_PER_EPISODE:
                                    continue
                                refetch_counts[key] = refetch_counts.get(key, 0) + 1
                                to_dispatch.append(entry)
                            if to_dispatch:
                                async_refetch_hook.dispatch(to_dispatch)
                                print(
                                    f"\n[自动重取流] {len(to_dispatch)} 集因直链过期失败，"
                                    f"已交给取流线程异步重取（不阻塞本轮下载）",
                                    flush=True,
                                )
                        # 有在途就等一等再判 next_batch：dispatch 是异步的，
                        # 立刻判会发现 next_batch 为空而 break，重取成功的新
                        # 链接就没有任何轮次去消费。有上限、且在途清零就提前
                        # 退出，不会白等满。
                        if async_refetch_hook.pending_count() > 0:
                            print(
                                f"\n[自动重取流] 本轮 {len(round_failed_expired)} 集直链过期"
                                f"已投异步重取，等待在途结果"
                                f"（最多 {ASYNC_REFETCH_WAIT_SECONDS}s）...",
                                flush=True,
                            )
                            deadline = time.time() + ASYNC_REFETCH_WAIT_SECONDS
                            while time.time() < deadline:
                                fresh = async_refetch_hook.collect()
                                if fresh:
                                    revived.extend(fresh)
                                # 在途清零即可收工，无需等满。
                                if async_refetch_hook.pending_count() == 0:
                                    revived.extend(async_refetch_hook.collect())
                                    break
                                # 可打断：Ctrl+C 后不该再干等满。
                                if interrupted.wait(1):
                                    break
                        if revived:
                            print(
                                f"[自动重取流] 收回 {len(revived)} 集新直链，并入下一轮",
                                flush=True,
                            )
                    else:
                        revived = refetch_entries(round_failed_expired, refetch_counts)
                except (Exception, SystemExit) as exc:
                    # 重取是尽力而为的捞回，绝不能让它崩掉整条流水线：
                    # 失败就当作没救回，本轮其余结论照常生效。
                    # 连 SystemExit 一起兜（取流侧模块级校验用的就是它），
                    # 但放过 KeyboardInterrupt —— Ctrl+C 该中止整个流程。
                    print(f"⚠️ 就地重取流异常，已跳过本轮重取: {exc}", flush=True)
                    revived = []
            elif round_failed_expired and not has_more_rounds:
                # 末轮不再集中重取：拿到新链接也没有下一轮去消费。
                # pipeline 模式下它们其实**已在本轮被即刻投递过**（见
                # handle_done_future），新链接由首轮来源随到随投；
                # 非流式模式下则留给下次运行的启动预检接手。
                if async_refetch_hook is not None and streaming:
                    print(
                        f"\n本轮有 {len(round_failed_expired)} 集的直链已过期，"
                        f"均已即刻投递异步重取（新链接随到随下；未赶上的已落盘 "
                        f"results.jsonl，下次运行自动使用）。",
                        flush=True,
                    )
                else:
                    print(
                        f"\n本轮有 {len(round_failed_expired)} 集的直链已过期，"
                        f"但已是末轮、不再重取（下次运行的启动预检会换新链接）。",
                        flush=True,
                    )

            next_batch = merge_next_batch(round_failed_retriable, revived)
            if not next_batch:
                if MULTI_ROUND_ENABLED and MAX_ROUNDS > 1:
                    print("\n本轮无可重试的下载失败，多轮下载提前结束。", flush=True)
                break
            if round_no >= MAX_ROUNDS:
                print(
                    f"\n已达最大轮次 {MAX_ROUNDS}，仍有 "
                    f"{len(next_batch)} 集下载失败未成功，停止重试。",
                    flush=True,
                )
                break

            revived_note = (
                f"（其中 {len(revived)} 集已换到新直链）" if revived else ""
            )
            print(
                f"\n本轮有 {len(next_batch)} 集待重投{revived_note}，"
                f"冷却 {ROUND_COOLDOWN_SECONDS}s 后进入第 {round_no + 1} 轮...",
                flush=True,
            )
            if ROUND_COOLDOWN_SECONDS > 0:
                # 可打断的冷却：Ctrl+C 后干等这么久会让收尾看起来像卡死。
                if interrupted.wait(ROUND_COOLDOWN_SECONDS):
                    print("\n收到中断信号，不再进入下一轮。", flush=True)
                    break
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
            f"三级流水线全部完成：转封装 {stats['conversions']} 集，"
            f"上传 {stats['uploads']} 集。"
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


def reupload_pending():
    """手动补传：读 upload_pending.jsonl，逐条重传上传失败留在本地的成品。

    一致性铁律：
      1. 补传前检查 os.path.exists(local_path)，文件不在（已被补传/手动清理）则
         直接从 pending 移除，视为已消解，不再重复上传。
      2. 同一集（tmdbId+season+episode）只保留最新一条 pending 记录（去孤儿/去重复）。
      3. 补传成功 -> 删本地 + 从 pending 移除（重写整个文件）+ 更新 SUCCESS_LOG
         标 uploaded:true；仍失败则保留该条 pending。
    """
    if not S3_ENABLED:
        print("s3.enabled=false，未开启远端上传，无需补传。")
        return
    if is_main_running():
        print(
            "检测到主流程（download_tv.py）正在运行，"
            "此时手动补传会与主流程并发操作 pending 文件、可能导致记录丢失。"
            "请在主流程结束后再执行 reupload。本次补传已忽略。"
        )
        return
    if not os.path.exists(UPLOAD_PENDING_LOG):
        print(f"未找到 pending 日志 {UPLOAD_PENDING_LOG}，无待补传文件。")
        return

    # 读入全部记录，同一集只保留最新一条。
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
            key = record_episode_key(record)
            if not key:
                continue
            if key not in latest_by_id:
                order.append(key)
            latest_by_id[key] = record

    if not latest_by_id:
        print(f"{UPLOAD_PENDING_LOG} 中无有效待补传记录。")
        return

    print(f"共 {len(latest_by_id)} 个待补传文件，开始逐条补传...")

    remaining = {}  # key -> record，仍失败保留
    success_count = 0
    orphan_count = 0
    fail_count = 0

    for key in order:
        record = latest_by_id[key]
        local_path = record.get("local_path", "")
        try:
            s3_key = record.get("s3_key") or build_s3_key(
                record.get("tmdbId"),
                record.get("season"),
                record.get("episode"),
                record.get("year"),
            )
        except ValueError as exc:
            record["fail_reason"] = str(exc)
            remaining[key] = record
            fail_count += 1
            print(f"  [{key}] 无法生成 s3_key，保留 pending: {exc}")
            continue

        if not local_path or not os.path.exists(local_path):
            # 文件已不在本地：视为已消解（可能此前已成功补传），从 pending 移除。
            orphan_count += 1
            print(f"  [{key}] 本地文件不存在，跳过并移除 pending: {local_path}")
            continue

        print(f"  [{key}] 补传中 -> {s3_key}")
        ok, reason = upload_to_r2(local_path, s3_key)
        if ok:
            # 视频补传成功后顺带把旁车资产一并补上（内部已吞异常）。
            asset_keys = upload_sidecar_assets(record)
            if DELETE_LOCAL_AFTER_UPLOAD:
                remove_file(local_path)
                _cleanup_episode_dir(os.path.dirname(local_path))
            update_success_log(key, {
                "tmdbId": record.get("tmdbId"),
                "season": parse_int(record.get("season")),
                "episode": parse_int(record.get("episode")),
                "title": record.get("title", ""),
                "year": record.get("year"),
                # ⚠️ update_success_log 是**整条覆盖**写。provider 归因必须
                # 从 pending 记录里原样带回来，否则补传一次就把"这集是哪家
                # 下成的"抹掉了——而降级留本地的集恰恰是上传侧出故障时的一
                # 大批，抹掉会让各源转化率统计系统性偏低。
                **_attribution_of(record),
                "final_path": local_path,
                "s3_key": s3_key,
                "asset_keys": asset_keys,
                "uploaded": True,
                "reupload": True,
            })
            remove_upload_failure_from_log(key)
            success_count += 1
            print(f"  [{key}] 补传成功: {s3_key}")
        else:
            record["s3_key"] = s3_key
            record["fail_reason"] = reason
            record["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
            remaining[key] = record
            fail_count += 1
            print(f"  [{key}] 补传仍失败，保留 pending: {reason}")

    # 重写整个 pending 文件：仅保留仍失败的记录。用锁保证与在跑主流程互斥。
    with pending_lock:
        with open(UPLOAD_PENDING_LOG, "w", encoding="utf-8") as file:
            for key in order:
                if key in remaining:
                    file.write(
                        json.dumps(remaining[key], ensure_ascii=False) + "\n"
                    )

    print(
        f"补传完成：成功 {success_count}，仍失败 {fail_count}，"
        f"孤儿(本地已无)清理 {orphan_count}；pending 剩余 {len(remaining)} 条。"
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reupload":
        reupload_pending()
    else:
        # 沿用既有的裸 sys.argv 分派风格（本文件一直没有引入 argparse）。
        # 两个开关可叠加：--retry-only --retry-dead。
        _args = set(sys.argv[1:])
        RETRY_ONLY_MODE = "--retry-only" in _args
        RETRY_DEAD_MODE = "--retry-dead" in _args
        _unknown = _args - {"--retry-only", "--retry-dead"}
        if _unknown:
            # 静默忽略拼错的开关最危险：会让人以为跳过逻辑已生效、实际在跑全量。
            # 几十万集规模下这等于白跑一整轮。
            print(f"错误: 无法识别的参数 {' '.join(sorted(_unknown))}")
            print("用法: python download_tv.py [--retry-only] [--retry-dead]")
            print("      python download_tv.py reupload")
            raise SystemExit(2)
        if RETRY_ONLY_MODE:
            print(
                "[重试模式] 只重试 results.jsonl 里尚未成功的集"
                "（跳过 success.jsonl、磁盘已有成品、画质判死）"
            )
        if RETRY_DEAD_MODE:
            print(
                f"[判死复判] 将按当前门槛复判 {DOWNLOAD_DEAD_LOG} 里的集，"
                f"余量系数 {DEAD_REVIVE_MARGIN:.2f}"
            )
        main()
