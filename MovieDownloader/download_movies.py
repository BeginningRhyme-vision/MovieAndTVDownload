#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import random
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry


# ---------- 配置 ----------
INPUT_JSONL = "only_one.jsonl"
SUCCESS_LOG = "success.jsonl"
FAILED_LOG = "failed.jsonl"
BASE_DIR = "/mnt/datasets/pt-movies/movies_02/new_movie"
FOLDER_PREFIX = "movie_"
MAX_VIDEOS_PER_FOLDER = 1000
START_FOLDER_INDEX = 2

# 下载线程池固定保持的影片下载数。
MAX_WORKERS = 30
# 独立的 FFmpeg 转封装/移动线程数，不占用上面的下载槽位。
CONVERT_WORKERS = 4
# 单部影片同时下载的分片数。
SEGMENT_CONCURRENCY = 16
TEMP_DIR = "../temp"
SAMPLE_COUNT = 30
SEG_RETRY_MAX = 20
SEG_RETRY_DELAY = 1
MIN_BITRATE_KBPS = 1850

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

# 因为本脚本明确关闭了 TLS 证书校验，所以关闭对应警告。
urllib3.disable_warnings(InsecureRequestWarning)


class UnsupportedPlaylistError(RuntimeError):
    """播放列表使用了当前手工分片下载器不支持的 HLS 功能。"""


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
            pool_connections=10,
            pool_maxsize=20,
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
    method, url, retries=3, backoff=0.5, as_text=False, **kwargs
):
    """发起 HTTP 请求；成功时返回 str 或 bytes。"""
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
            if attempt == retries - 1:
                break
            time.sleep(backoff * (2**attempt))

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
def parse_master_playlist(master_url):
    """返回 [(resolution, media_playlist_url), ...]。"""
    text = request_with_retry("GET", master_url, as_text=True)
    lines = [line.strip() for line in text.splitlines()]
    variants = []

    for index, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue

        resolution_match = re.search(
            r"(?:^|,)RESOLUTION=(\d+x\d+)(?:,|$)", line, re.IGNORECASE
        )
        resolution = resolution_match.group(1) if resolution_match else "unknown"

        # URI 通常在下一行；跳过中间可能存在的空行或标签行。
        for following in lines[index + 1 :]:
            if not following or following.startswith("#"):
                continue
            variants.append((resolution, urljoin(master_url, following)))
            break

    if not variants and any(line.startswith("#EXTINF:") for line in lines):
        variants.append(("unknown", master_url))

    return variants


def parse_media_playlist(playlist_url):
    """解析普通 MPEG-TS 媒体播放列表，返回分片 URL 和时长。"""
    text = request_with_retry("GET", playlist_url, as_text=True)
    lines = [line.strip() for line in text.splitlines()]

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
            raise UnsupportedPlaylistError(
                "播放列表使用 #EXT-X-MAP（通常是 fMP4），不能按 MPEG-TS 拼接"
            )

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

    return segment_urls, durations


def is_1080p(resolution):
    if not resolution or resolution == "unknown":
        return True
    try:
        width, height = resolution.lower().split("x", 1)
        return int(width) >= 1920 and int(height) >= 1080
    except (TypeError, ValueError):
        return False


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
            time.sleep(random.uniform(3, 5))

    raise RuntimeError(
        f"分片 {index + 1} 重试 {retry_max} 次后仍失败: {last_error}"
    ) from last_error


def download_segments(
    segment_urls,
    output_path,
    start_idx=0,
    end_idx=None,
    concurrency=SEGMENT_CONCURRENCY,
):
    """
    并发下载、按索引顺序写入分片。

    关键修复：输出文件在整个下载过程中只打开一次，不能每批用 wb 重开；
    否则前面已经写入的批次会被清空。

    单个分片耗尽重试次数后会记录并跳过，不中止整部影片。返回值为：
    (成功写入的字节数, 失败分片索引列表)。
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
        for batch_start in range(0, len(indices), concurrency):
            batch_indices = indices[batch_start : batch_start + concurrency]
            results = {}
            failures = {}

            with ThreadPoolExecutor(
                max_workers=min(concurrency, len(batch_indices))
            ) as executor:
                future_to_index = {
                    executor.submit(
                        download_single_segment,
                        segment_urls[index],
                        index,
                        SEG_RETRY_MAX,
                        SEG_RETRY_DELAY,
                    ): index
                    for index in batch_indices
                }

                for future in as_completed(future_to_index):
                    index = future_to_index[future]
                    try:
                        results[index] = future.result()
                    except Exception as exc:
                        failures[index] = str(exc)

            if failures:
                batch_failed_indices = sorted(failures)
                failed_indices.extend(batch_failed_indices)
                print(
                    f"    警告: {len(batch_failed_indices)} 个分片耗尽重试次数，"
                    f"将跳过并继续；索引: {batch_failed_indices}"
                )

            for index in batch_indices:
                if index in failures:
                    continue
                data = results[index]
                output_file.write(data)
                total_bytes += len(data)

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
    url = entry.get("url", "")

    if not normalized_id or not url:
        return tmdb_id, False, {"error": "缺少 tmdbId 或 url"}

    # 原子地检查“历史已完成”和“当前正在处理”，防止并发重复下载。
    with processing_lock:
        if normalized_id in processed_ids:
            print(f"跳过已成功处理: {tmdb_id}")
            return tmdb_id, False, {"error": "already processed successfully"}
        if normalized_id in processing_ids:
            print(f"跳过当前运行中的重复条目: {tmdb_id}")
            return tmdb_id, False, {"error": "duplicate entry currently processing"}
        processing_ids.add(normalized_id)

    print(f"\n开始处理: {tmdb_id} - {title}")
    os.makedirs(TEMP_DIR, exist_ok=True)

    handed_off_to_conversion = False
    cleanup_paths = set()
    final_ts = os.path.join(TEMP_DIR, f"temp_{safe_file_token(tmdb_id)}.ts")
    temp_mp4 = os.path.join(TEMP_DIR, f"temp_{safe_file_token(tmdb_id)}.mp4")
    cleanup_paths.update((final_ts, temp_mp4))

    try:
        variants = parse_master_playlist(url)
        if not variants:
            raise RuntimeError("没有找到媒体播放列表或清晰度变体")

        candidates = [item for item in variants if is_1080p(item[0])]
        if not candidates:
            raise RuntimeError("没有找到 1080p 或更高分辨率的流")

        best_bitrate = 0.0
        best_resolution = None
        best_segment_urls = None
        best_durations = None
        best_sample_bytes = 0
        best_sample_count = 0
        best_sample_path = None
        best_sample_failed_indices = []

        for resolution, playlist_url in candidates:
            print(f"  检测流 {resolution}: {playlist_url}")
            sample_path = os.path.join(
                TEMP_DIR,
                f"sample_{safe_file_token(tmdb_id)}_"
                f"{safe_file_token(resolution)}.ts",
            )
            cleanup_paths.add(sample_path)
            remove_file(sample_path)

            try:
                segment_urls, durations = parse_media_playlist(playlist_url)
                sample_count = min(SAMPLE_COUNT, len(segment_urls))
                sample_bytes, sample_failed_indices = download_segments(
                    segment_urls,
                    sample_path,
                    start_idx=0,
                    end_idx=sample_count,
                    concurrency=min(4, SEGMENT_CONCURRENCY),
                )
                sample_failed_set = set(sample_failed_indices)
                sample_duration = sum(
                    duration
                    for index, duration in enumerate(durations[:sample_count])
                    if index not in sample_failed_set
                )
                if sample_duration <= 0 or sample_bytes <= 0:
                    raise RuntimeError("采样数据或采样时长为 0")

                bitrate = sample_bytes * 8 / sample_duration / 1000
                print(f"  流 {resolution} 采样码率: {bitrate:.0f} kbps")

                if bitrate > best_bitrate:
                    if best_sample_path and best_sample_path != sample_path:
                        remove_file(best_sample_path)
                    best_bitrate = bitrate
                    best_resolution = resolution
                    best_segment_urls = segment_urls
                    best_durations = durations
                    best_sample_bytes = sample_bytes
                    best_sample_count = sample_count
                    best_sample_path = sample_path
                    best_sample_failed_indices = sample_failed_indices
                else:
                    remove_file(sample_path)
            except Exception as exc:
                remove_file(sample_path)
                print(f"  处理流 {resolution} 失败: {exc}")

        if not best_sample_path or best_bitrate <= MIN_BITRATE_KBPS:
            raise RuntimeError(
                f"所有 1080p 流采样码率均 <= {MIN_BITRATE_KBPS} kbps；"
                f"最高为 {best_bitrate:.0f} kbps"
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

        conversion_job = {
            "tmdbId": tmdb_id,
            "normalized_id": normalized_id,
            "title": title,
            "url": url,
            "final_ts": final_ts,
            "temp_mp4": temp_mp4,
            "cleanup_paths": list(cleanup_paths),
            "bitrate_kbps": round(total_bitrate),
            "resolution": best_resolution,
            "missing_segment_count": len(failed_segment_indices),
            "missing_segment_indices": failed_segment_indices,
        }
        handed_off_to_conversion = True
        print(
            f"  [{tmdb_id}] 分片下载完成，已释放下载槽位并进入转封装队列",
            flush=True,
        )
        return tmdb_id, True, conversion_job

    except Exception as exc:
        return tmdb_id, False, {"error": str(exc)}
    finally:
        # 下载成功后临时文件和 ID 锁交给转封装阶段管理。
        if not handed_off_to_conversion:
            for path in cleanup_paths:
                remove_file(path)
            with processing_lock:
                processing_ids.discard(normalized_id)


def finalize_one_entry(conversion_job, processed_ids):
    """独立执行 FFmpeg 转封装和文件移动，不占用下载线程。"""
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
            "url": conversion_job["url"],
            "final_path": final_path,
            "bitrate_kbps": conversion_job["bitrate_kbps"],
            "resolution": conversion_job["resolution"],
            "missing_segment_count": conversion_job["missing_segment_count"],
            "missing_segment_indices": conversion_job[
                "missing_segment_indices"
            ],
        }
        write_log(SUCCESS_LOG, success_info)
        completed = True
        print(f"  [{tmdb_id}] 成功: {final_path}", flush=True)
        return tmdb_id, True, success_info

    except Exception as exc:
        return tmdb_id, False, {"error": str(exc)}
    finally:
        for path in cleanup_paths:
            remove_file(path)
        with processing_lock:
            processing_ids.discard(normalized_id)
            if completed:
                processed_ids.add(normalized_id)


# ---------- 主函数 ----------
def main():
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

    print(
        f"启动两级流水线: {MAX_WORKERS} 个下载槽位，"
        f"{CONVERT_WORKERS} 个独立转封装槽位"
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as download_executor, \
            ThreadPoolExecutor(max_workers=CONVERT_WORKERS) as conversion_executor:
        download_future_to_entry = {
            download_executor.submit(
                process_one_entry, entry, processed_ids
            ): entry
            for entry in entries
        }

        # 每当一部影片下载完成，下载线程会立刻领取下一条；与此同时，
        # 主线程把完成的 TS 交给独立的转封装线程池。
        for future in as_completed(download_future_to_entry):
            entry = download_future_to_entry[future]
            try:
                tmdb_id, download_success, info = future.result()
            except Exception as exc:
                tmdb_id = entry.get("tmdbId")
                download_success = False
                info = {"error": str(exc)}

            if download_success:
                conversion_future = conversion_executor.submit(
                    finalize_one_entry, info, processed_ids
                )
                conversion_future_to_entry[conversion_future] = entry
                continue

            if info.get("error") not in ignored_errors:
                failed_info = {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title", ""),
                    "url": entry.get("url", ""),
                    "error": info.get("error", "未知错误"),
                    "stage": "download",
                }
                write_log(FAILED_LOG, failed_info)
                print(f"下载失败: {tmdb_id}: {failed_info['error']}")

        pending_conversion_count = sum(
            1 for future in conversion_future_to_entry if not future.done()
        )
        print(
            f"所有分片下载任务已结束，共进入转封装队列 "
            f"{len(conversion_future_to_entry)} 部；"
            f"当前仍有 {pending_conversion_count} 部待完成..."
        )

        for future in as_completed(conversion_future_to_entry):
            entry = conversion_future_to_entry[future]
            try:
                tmdb_id, conversion_success, info = future.result()
            except Exception as exc:
                tmdb_id = entry.get("tmdbId")
                conversion_success = False
                info = {"error": str(exc)}

            if not conversion_success:
                failed_info = {
                    "tmdbId": tmdb_id,
                    "title": entry.get("title", ""),
                    "url": entry.get("url", ""),
                    "error": info.get("error", "未知错误"),
                    "stage": "conversion",
                }
                write_log(FAILED_LOG, failed_info)
                print(f"转封装失败: {tmdb_id}: {failed_info['error']}")


if __name__ == "__main__":
    main()
