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

🔑 可以与下载侧（pipeline.py / download_movies.py）**同时运行**，无需错开：
   - 本脚本只处理 success.jsonl 里 uploaded=true 的片，而下载侧是先把视频 +
     meta.json + 字幕全部传进 R2 之后才写这条记录。两边操作的影片集合天然
     不相交，下载侧不会再碰本脚本正在处理的前缀；
   - 唯一可能撞车的是 R2 上的 meta.json（极端时序下双方都 put 同一个 key），
     由 _update_remote_meta 的 ETag 乐观锁（If-Match，412 即重读重试）兜底；
   - R2 模式下本脚本不碰 downloads/ 目录、pending 文件与主流程锁，字幕只写
     临时目录、传完即删。
   注意 success.jsonl 在启动时一次性读入：运行期间新上传的片不会被本次处理，
   等它们落库后再跑一次即可（幂等）。
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
import yaml

from pathlib import Path

# 复用下载侧的配置与字幕转换逻辑：语种白名单、输出格式、目录结构、
# srt<->vtt 互转全部同源，避免两套实现各改一半导致字幕落到不同地方/格式不一致。
# download_movies 模块级只读 config、不建连接、不起线程，import 是安全的（~0.2s）。
sys.path.insert(0, str(Path(__file__).resolve().parent))
import download_movies as dm  # noqa: E402  (import 时已把同目录 .env 载入环境变量)

# 脚本所在目录（MovieDownloader/），作为相对路径与默认目录的根。
_SCRIPT_DIR = Path(__file__).resolve().parent


def _config_api_key():
    """从 config.yaml 的 fetch_subtitles 段读 key，作为 .env 缺失时的回退。

    刻意不复用 dm.load_config()：那个函数写死了只返回 download_movies 段。
    读不到/格式坏掉一律当没配，绝不因为配置文件问题让整个脚本崩掉 ——
    字幕本就是可有可无的附属步骤。
    """
    try:
        with open(_SCRIPT_DIR / "config.yaml", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        section = data.get("fetch_subtitles") or {}
        return str(section.get("subdl_api_key") or "").strip()
    except Exception:  # noqa: BLE001 - 配置读不到不是错误
        return ""


# ========== 配置 ==========
# SubDL API Key：敏感项。环境变量 / 同目录 .env 优先，config.yaml 仅作本地
# 调试回退 —— config.yaml 会进版本库，填在那里等于把 key 公开推到远端。
# 免费申请：https://subdl.com/panel/api
#
# 支持**多账号轮换**：免费档每账号每天只有 50 次下载，单 key 补不了多少片。
# 2026-09-12 实测确认额度按**账号**计而非按 IP 计（同一台机器、同一条 url：
# 旧 key 429、新 key 200、旧 key 复核仍 429），故多 key 轮换确实能叠加额度。
#   .env 里写：SUBDL_API_KEYS=key1,key2,key3
# 单数形式 SUBDL_API_KEY 继续有效（向后兼容）：两者都配时**合并去重**，
# 复数在前。不合并的话，用户加了复数却忘了删单数，那个 key 的额度就白扔了。
def _parse_keys(raw):
    """把逗号/空白/换行分隔的多个 key 解析成有序去重列表。

    保序很重要：用户把额度多的 key 放前面时，应当先用它。
    """
    if not raw:
        return []
    parts = re.split(r"[,\s]+", str(raw))
    seen, out = set(), []
    for part in parts:
        key = part.strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


SUBDL_API_KEYS = _parse_keys(
    " ".join(filter(None, [
        os.environ.get("SUBDL_API_KEYS", ""),
        os.environ.get("SUBDL_API_KEY", ""),
        _config_api_key(),
    ]))
)

SUCCESS_LOG = dm.SUCCESS_LOG
STATE_LOG = str(_SCRIPT_DIR / "subtitles.jsonl")
# 【缺口台账】记录"SubDL 确认没有某片某语种"，下次运行直接跳过这个语种。
#
# 为什么需要它：实测 101 部里 zh 缺口 57 部、en 缺口仅 5 部 —— SubDL 中文库
# 很薄。没有台账时，每次重跑都会把这 57 部的 zh 重查一遍，而每次都是同样的
# 空手而归。配额本就稀缺（免费档每账号 50 次/天），这是纯浪费。
#
# ⚠️ 只记 missing（源站确认没有），绝不记 fetch_failed（我们没取回来）。
# 两者混淆会让一次网络抖动把某个语种永久打入冷宫，见 _fetch_into 的注释。
LEDGER_LOG = str(_SCRIPT_DIR / "subtitles_gaps.jsonl")

# 目录结构、语种白名单、输出格式全部复用 download_movies（单一事实来源）：
# 补来的字幕必须与下载侧落在同一个地方、用同一套格式，否则前端按同前缀
# 列举时会拿到对不上的东西。要改语种/格式请改 download_movies.assets 段。
SUBTITLE_LANGUAGES = dm.SUBTITLE_LANGUAGES
SUBTITLE_FORMATS = dm.SUBTITLE_FORMATS

# SubDL 的 language 字段用大写语言代码（"EN"/"ZH"/"BR_PT"），查询参数也要
# 用同一套。config 里写的是 IETF 风格的小写码（en / zh / zh-cn / pt-br），
# 两者不是简单的 upper()：
#   - 2026-09-12 实测 SubDL 对 ZH-CN/ZH_CN 宽松匹配为 ZH，但 PT_BR 的结果
#     language 字段是 "BR_PT"——不做映射的话 pick_candidates 永远比不上；
#   - **未知码不会报错，而是返回全语种结果**（XX 拿到 64 页），只靠"查到了
#     东西"判断不出配置写错了。
# 故：先查别名表，再按 ^[A-Z]{2,3}(_[A-Z]{2})?$ 校验；不合法的语种启动时
# 告警并整体跳过（不进 pending、不进台账），而不是每部片都白查一遍。
_SUBDL_LANGUAGE_ALIASES = {
    "zh-cn": "ZH", "zh_cn": "ZH", "zh-hans": "ZH", "zh-sg": "ZH", "chs": "ZH",
    "zh-tw": "ZH", "zh_tw": "ZH", "zh-hk": "ZH", "zh-hant": "ZH", "cht": "ZH",
    "pt-br": "BR_PT", "pt_br": "BR_PT",
    "en-us": "EN", "en-gb": "EN",
}
_SUBDL_CODE_RE = re.compile(r"^[A-Z]{2,3}(_[A-Z]{2})?$")


def subdl_language_code(lang):
    """config 语种码 -> SubDL 语种码；无法映射成合法形态时返回 None。"""
    key = str(lang or "").strip().lower()
    code = _SUBDL_LANGUAGE_ALIASES.get(key) or key.upper().replace("-", "_")
    return code if _SUBDL_CODE_RE.match(code) else None


def unsupported_languages():
    """config 里映射不出合法 SubDL 码的语种，供启动时告警。"""
    return [lang for lang in SUBTITLE_LANGUAGES if not subdl_language_code(lang)]

SEARCH_API = "https://api.subdl.com/api/v1/subtitles"
DOWNLOAD_BASE = "https://dl.subdl.com"

MAX_WORKERS = 4
REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_DELAY = 3
# 非配额型 429（短时限流）的全局冷却：所有线程共用一个冷却窗口，撞上就一起
# 等，等完继续用**同一个** key。冷却多少轮仍 429 才落闸停跑。
# 响应里没有 Retry-After 头（2026-09-12 实测），只能自定步长。
THROTTLE_COOLDOWN = 30
THROTTLE_MAX_ROUNDS = 3
# 同一语种最多试几个候选：解压后发现是 ASS/SSA、或 zip 里没有字幕文件时
# 换下一条。每试一条都消耗一次下载额度，所以不能无限试。
MAX_CANDIDATES_PER_LANGUAGE = 2
# SubDL 对"库里没有这个 tmdb_id"的固定错误消息（status=false）。
NOT_FOUND_MARKER = "can't find movie or tv"
# meta.json 的乐观锁重试：并发写同一个 key 的概率本就很低（下载侧此刻传的是
# 还没进 success.jsonl 的片，与我们处理的片天然不相交），撞上也只需重读一次。
# 3 次足够，再多只是拖慢收尾。
META_UPDATE_RETRIES = 3
META_UPDATE_BACKOFF = 0.5
# 防御性上限：字幕 zip 正常只有几十 KB，源站返回异常内容时不能整个读进内存。
MAX_ZIP_BYTES = dm.SUBTITLE_MAX_BYTES

state_lock = threading.Lock()

# 单实例锁：cron 与手动同时触发时，两个实例会各自搜索同一批片、各自消耗
# 同一份 SubDL 下载额度，还会并发改写同一个 meta.json。范式照抄下载侧
# （PID 写入锁文件，陈旧锁按 PID 存活自动清理）。与下载侧互不排斥。
LOCK_FILE = str((_SCRIPT_DIR / "fetch_subtitles.lock").resolve())

# 是否把字幕写进 R2。下载侧开了上传就必须走 R2 —— 本地影片目录早被删了。
REMOTE_MODE = dm.S3_ENABLED

# SubDL 免费账号每天只能**下载** 50 个字幕（搜索不受限）。额度用尽后所有下载
# 一律 429，且当天不会恢复。2026-09-12 实测响应体：
#   {"error":"api_download_limit_exceeded","limit":50,
#    "retryAfterSeconds":13728,"resetAt":"2026-09-12T00:00:00.000Z"}
# 这是**确定性**失败，必须与"源站没这个语种"严格区分开（见 QuotaExhausted）。
QUOTA_ERROR_CODE = "api_download_limit_exceeded"


class KeyPool:
    """多个 SubDL api_key 的轮换池。线程安全。

    🔑 为什么不能简单地"撞 429 就切下一个"：
    4 个工作线程共用当前 key，key1 耗尽时它们会**各自**撞到 429、各自要求切换。
    无脑 `index += 1` 会一次跳过 3 个 key —— 那 3 个账号的额度**原封不动地
    被浪费掉**，而额度正是这里最稀缺的资源。

    解法：调用方报告"是哪个 key 挂了"，只有当它确实是当前 key 时才推进。
    后到的 3 个线程报的是同一个 key1，此时 current 已是 key2，直接忽略。
    """

    def __init__(self, keys):
        self._keys = list(keys)
        self._index = 0
        self._exhausted = {}        # key -> 原因（目前只有 quota）
        self._lock = threading.Lock()

    def __len__(self):
        return len(self._keys)

    def current(self):
        """取当前可用 key；全部耗尽时返回 None。"""
        with self._lock:
            if self._index >= len(self._keys):
                return None
            return self._keys[self._index]

    def retire(self, key, reason):
        """报告某个 key 已不可用，返回接替它的新 key（没有则 None）。

        幂等：同一个 key 被多个线程重复报告时，只有第一次真正推进游标。
        """
        with self._lock:
            if self._index >= len(self._keys):
                return None
            if self._keys[self._index] != key:
                # 已经有别的线程切走了。说明当前 key 是新的，直接沿用，
                # 绝不因为"我也撞墙了"而再跳一次。
                return self._keys[self._index]
            self._exhausted.setdefault(key, reason)
            self._index += 1
            position = self._index
            nxt = self._keys[self._index] if self._index < len(self._keys) else None
        if nxt:
            print(
                f"\n🔑 第 {position} 个 api_key 已用尽（{reason}），"
                f"切换到第 {position + 1}/{len(self._keys)} 个继续。",
                flush=True,
            )
        return nxt

    def exhausted_count(self):
        with self._lock:
            return len(self._exhausted)

    def all_exhausted(self):
        with self._lock:
            return self._index >= len(self._keys)


key_pool = KeyPool(SUBDL_API_KEYS)

# 全局停止闸门：**只有"所有 key 都耗尽"才置位**。
# ⚠️ 与单 key 时代的语义不同：那时"撞额度 = 停跑"，现在"撞额度 = 换 key"，
# 只有换无可换才停。搞混会让多配的 key 一个都用不上。
#
# 注意"跑完了"与"没额度了"是两种**完全不同**的收场，日志必须分开说：
# 前者是圆满完成、无需再跑；后者是被迫中断、明天还要接着跑。
stop_fetching = threading.Event()

# 置位原因，供 download_one 如实报告跳过的理由（而不是笼统说"额度耗尽"）。
# 只在第一次置位时写入，之后只读；配 stop_reason_lock 防并发下写花。
stop_reason = {}
stop_reason_lock = threading.Lock()


def _signal_stop(status, error, reset_at=None):
    """记录停止原因并置位全局闸门。重复调用只保留第一个原因。"""
    with stop_reason_lock:
        if not stop_reason:
            stop_reason.update({"status": status, "error": error})
            if reset_at:
                stop_reason["quotaResetAt"] = reset_at
    stop_fetching.set()


def _retire_key(key, error, reset_at=None):
    """当前 key **额度用尽** -> 换下一个；换无可换才落全局闸门。

    只服务配额型 429。短时限流（SearchThrottled）不走这里——那不是 key 的
    问题，退役它等于白扔一份额度，见 _wait_for_throttle。
    返回 True 表示还有 key 可用（调用方应当重试），False 表示该收摊了。
    """
    if key_pool.retire(key, "quota"):
        return True
    _signal_stop("quota_exhausted", error, reset_at)
    return False


# 短时限流的全局冷却窗口：所有线程共享同一个"冷却到几点"的时间戳。
# 4 个线程几乎同时撞 429 时，只有第一个负责把窗口往后推，其余看到窗口已在
# 未来就直接等到同一时刻——而不是各自再叠加一轮 30s。
_throttle_lock = threading.Lock()
_throttle_until = 0.0


def _wait_for_throttle():
    """被限流后全局冷却一轮。返回冷却时长（秒），供日志。"""
    global _throttle_until
    with _throttle_lock:
        now = time.monotonic()
        if _throttle_until <= now:
            _throttle_until = now + THROTTLE_COOLDOWN
        wait = _throttle_until - now
    if wait > 0:
        time.sleep(wait)
    return wait


def _redact(text):
    """把日志里的 api_key 抹掉。

    ⚠️ 不是可选的洁癖：requests 的 HTTPError 消息里**带完整请求 url**，而
    搜索接口的 key 就在 query 里。2026-09-12 实测，一条 429 报错就把
    `api_key=subdl_xxx` 原样写进了日志文件——日志经常要贴出来排查，等于泄露
    凭证。所有对外打印的异常文本都必须过这一道。
    """
    return re.sub(r"(api_key=)[^&\s\"']+", r"\1REDACTED", str(text))

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

    ⚠️ 只认 SUBTITLE_FORMATS 里的扩展名（srt/vtt）。历史版本曾把 ASS 原样存成
    en.ass，若也算"已有"，该语种就被一个前端 <track> 根本认不得的文件永久
    锁死、永不再补。ASS 现在已不落盘，但 R2 上的旧残留仍要按"没有"处理。
    """
    name = str(filename).rsplit("/", 1)[-1]
    if "." not in name:
        return None
    stem, _, ext = name.rpartition(".")
    if ext.strip().lower() not in SUBTITLE_FORMATS:
        return None
    return stem.strip().lower() or None


def _media_type_of(title_type):
    """IMDB titleType -> SubDL 的 type 参数（movie / tv）。

    SubDL 用 type 决定拿 tmdb_id 去查哪张表：电影与剧集的 tmdb_id 是两个
    独立的编号空间，用错表要么查不到、要么查到一部完全不相干的片。
    写死 "movie" 在只下电影时没事，一旦 success.jsonl 混入剧集就会整批落空
    并被记进台账。缺 titleType（老记录）按电影处理，与历史行为一致。

    注意 tvMovie（电视电影）虽然以 "tv" 开头，但在 TMDB 里落电影编号空间
    （config.yaml keep_types 与 fetch_movie_metadata 的测试都以此为契约），
    必须按 movie 查，所以这里用显式集合而不是前缀匹配。
    """
    value = str(title_type or "").strip().lower()
    return "tv" if value in _TV_TITLE_TYPES else "movie"


# 走剧集编号空间的 IMDB titleType（小写比较）；tvMovie 不在其中，见 _media_type_of。
_TV_TITLE_TYPES = frozenset({"tvseries", "tvminiseries", "tvepisode", "tvspecial", "tvshort"})


def _list_remote_subtitles(tmdb_id, year):
    """列举 R2 上该片 subs/ 下已有的字幕文件名（不含目录）。

    返回 (文件名列表, 是否查询成功)。查询失败时返回 ([], False) —— 调用方
    据此**跳过这一部**，而不是当作"没有字幕"去重抓：列举失败多半是网络抖动，
    此时贸然重抓会把已有字幕再传一遍（虽然幂等覆盖无害，但白烧配额）。

    返回文件名而非语种集合，是因为 pending 为空时还要拿这份清单去对账
    meta.json（见 download_one），语种集合由调用方按 _language_of 推导。
    """
    # ⚠️ 尾斜杠必须在 build_s3_key **之外**补：asset_rel_path 会 strip("/")，
    # 传 "subs/" 进去出来的仍是 ".../subs"，前缀匹配会把 ".../subsXXX" 一并
    # 算进来（虽然目前没有这种目录，但前缀语义就该是"目录下"而非"以此开头"）。
    prefix = dm.build_s3_key(tmdb_id, year, dm.SUBS_SUBDIR) + "/"
    try:
        client = dm.get_s3_client()
        names = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=dm.S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                name = str(obj["Key"])[len(prefix):]
                # 只要直接子文件：带斜杠的是更深层的东西，不是字幕。
                if name and "/" not in name:
                    names.append(name)
        return names, True
    except Exception as exc:  # noqa: BLE001 - 列举失败只跳过这一部
        print(f"  [{tmdb_id}] ⚠️ 列举 R2 字幕失败（已跳过）: {exc}", flush=True)
        return [], False


def _existing_languages_local(target_dir):
    """纯本地模式下扫目录得到已有语种集合（判据与 R2 侧同源）。"""
    try:
        names = os.listdir(target_dir)
    except OSError:
        return set()
    return {lang for lang in map(_language_of, names) if lang}


def _is_precondition_failed(exc):
    """判断异常是不是 If-Match 失败（HTTP 412 PreconditionFailed）。

    只认这一种错误码：别的失败（权限、网络、桶不存在）重试多少次都一样，
    当场放弃比空转三轮更诚实。
    """
    response = getattr(exc, "response", None) or {}
    if isinstance(response, dict):
        meta = response.get("ResponseMetadata") or {}
        if meta.get("HTTPStatusCode") == 412:
            return True
        code = str((response.get("Error") or {}).get("Code") or "")
        if code in ("PreconditionFailed", "412"):
            return True
    return False


def _update_remote_meta(tmdb_id, year, new_files):
    """把字幕文件并进 R2 上 meta.json 的 subtitles[]。**带乐观锁**。

    返回 (是否成功, 新增条数)。"成功但新增 0 条"是合法结果：意味着 meta 已经
    记全了，什么都没写——调用方靠 added 区分"补记了"与"本就齐全"，靠 ok
    区分"齐全"与"失败"。两者混成一个 bool 会让失败与无事可做同形。

    前端按 meta.json 索引字幕时，光传上去文件是不够的——meta 里没有记录就等于
    不存在。meta.json 只有几百字节，下载-改-回传的代价可忽略。

    🔑 为什么必须用 ETag 乐观锁（这是"下载侧运行时也能补字幕"的前提）：
    本函数是"读-改-写"，而下载侧上传成品时会整份覆盖同一个 key。裸写的时序：

        本进程 get(meta v1) -> 下载侧 put(meta v2) -> 本进程 put(v1 + 字幕)
                                                      ↑ v2 的改动被静默吃掉

    带上读取时拿到的 ETag 做 If-Match，中途被人改过就会 412，此时**重读重试**
    即可 —— 第二次读到的就是 v2，合并后回写不丢任何字段。

    整个过程尽力而为：meta 不存在或格式坏掉都只打印，绝不影响字幕本身已经
    成功上传的事实。
    """
    key = dm.build_s3_key(tmdb_id, year, "meta.json")
    try:
        client = dm.get_s3_client()
    except Exception as exc:  # noqa: BLE001
        print(f"  [{tmdb_id}] ⚠️ 连接 R2 失败（字幕已上传，跳过 meta 更新）: {exc}",
              flush=True)
        return False, 0

    for attempt in range(META_UPDATE_RETRIES):
        try:
            obj = client.get_object(Bucket=dm.S3_BUCKET, Key=key)
            etag = obj.get("ETag")
            meta = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - 没有 meta 就不更新，不是错误
            print(
                f"  [{tmdb_id}] ⚠️ 读取 meta.json 失败（字幕已上传，跳过更新）: "
                f"{exc}", flush=True,
            )
            return False, 0

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
            # 只索引前端认得的格式：R2 上历史遗留的 en.ass 之类不进 meta，
            # 否则前端会给 <track> 塞一个它加载不了的文件。
            if fmt.lower() not in SUBTITLE_FORMATS:
                continue
            entries.append({"language": language, "format": fmt, "path": rel})
            known.add(rel)
            added += 1
        if not added:
            return True, 0

        meta["subtitles"] = entries
        # 留痕：标明这份 meta 被字幕补漏流程改过，便于日后排查字幕来源。
        meta["subtitlesUpdatedAt"] = int(time.time())

        put_kwargs = {
            "Bucket": dm.S3_BUCKET, "Key": key,
            "Body": json.dumps(meta, ensure_ascii=False,
                               indent=2).encode("utf-8"),
            "ContentType": "application/json",
        }
        # ETag 拿不到就退化为裸写：总比不写强，且单独跑（下载侧没在跑）时
        # 本就不存在竞争。
        if etag:
            put_kwargs["IfMatch"] = etag
        try:
            client.put_object(**put_kwargs)
            return True, added
        except Exception as exc:  # noqa: BLE001
            if _is_precondition_failed(exc) and attempt < META_UPDATE_RETRIES - 1:
                # 有人在我们读完之后改了它 —— 重读重试，把两边的改动合起来。
                print(
                    f"  [{tmdb_id}] meta.json 被并发修改，重读重试"
                    f"（第 {attempt + 1} 次）",
                    flush=True,
                )
                time.sleep(META_UPDATE_BACKOFF * (attempt + 1))
                continue
            print(f"  [{tmdb_id}] ⚠️ 回写 meta.json 失败（字幕已上传，"
                  f"下次运行会自动对账补记）: {exc}", flush=True)
            return False, 0


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
                # 下载侧 success_info 现在会带 title_type；早期记录没有这个
                # 字段，取到 None -> _media_type_of 按电影处理，与历史行为一致。
                "title_type": data.get("title_type") or data.get("titleType"),
            }

    if skipped_not_uploaded:
        print(
            f"跳过 {skipped_not_uploaded} 部尚未上传 R2 的影片"
            f"（补传成功后再跑一次本脚本即可补字幕）",
            flush=True,
        )
    return list(entries.values())


class QuotaExhausted(Exception):
    """SubDL 当日下载额度已用尽。

    单列一个异常类型，是为了让这类失败在日志里**与"源站没这个语种"彻底分开**。
    2026-09-12 的教训：两者当时都落进 result["missing"]，打印出来都是
    「已保存 [] 缺失 ['zh']」，完全同形。结果一次额度耗尽的空跑被误判成
    "补字幕功能坏了"，排查了很久才发现是配额问题。

    它还是**确定性**失败：对**当前这个 key** 而言重试、换片都没用，当天就是
    没额度了。故：不重试、换下一个 key；换无可换才停。
    """

    def __init__(self, message, retry_after=None, reset_at=None):
        super().__init__(message)
        self.retry_after = retry_after
        self.reset_at = reset_at


class SearchThrottled(Exception):
    """接口被持续限流（重试用尽后仍是 429，且不是配额错误）。

    与 QuotaExhausted **成因不同、处置也不同**，故单列一类：
      - QuotaExhausted：下载额度 50/天 用尽，当天绝不恢复 -> 退役这个 key；
      - SearchThrottled：api.subdl.com 的速率限制，过一阵就好 -> **不退役**，
        全局冷却 THROTTLE_COOLDOWN 秒后用同一个 key 继续。

    为什么不能退役：4 个线程并发打搜索接口，一次短时限流会让它们各自撞墙、
    各自要求换 key，几秒内就能把整池 key 串烧光——而每个 key 的额度都还在。
    只有冷却 THROTTLE_MAX_ROUNDS 轮仍 429 才落闸停跑（见 request_with_keys）。

    2026-09-12 实测：下载额度耗尽后，搜索接口也开始返 429。它走的是
    raise_for_status，于是被归进 search_failed —— 和真正的"SubDL 库里没有这个
    tmdb_id"（错误消息 can't find movie or tv）混在一起，把那个统计数字弄脏了。
    """


def _parse_quota_error(response):
    """从 429 响应里辨认「当日额度用尽」，是则返回 QuotaExhausted，否则 None。

    只认 SubDL 明确给出的 error 码，不靠状态码猜——429 也可能是短时并发限流
    （那种等一会儿就能恢复，该走正常重试）。两者处置方式相反，不能混为一谈。
    """
    if response is None or response.status_code != 429:
        return None
    try:
        data = response.json()
    except Exception:  # noqa: BLE001 - 不是 JSON 就不是这个错误
        return None
    if not isinstance(data, dict) or data.get("error") != QUOTA_ERROR_CODE:
        return None
    return QuotaExhausted(
        data.get("message") or "SubDL 当日下载额度已用尽",
        retry_after=data.get("retryAfterSeconds"),
        reset_at=data.get("resetAt"),
    )


def request_with_retry(method, url, **kwargs):
    last_error = None
    last_status = None
    for attempt in range(RETRY_MAX):
        try:
            response = requests.request(
                method, url, timeout=REQUEST_TIMEOUT, **kwargs
            )
            # 额度耗尽要在 raise_for_status 之前拦截：一旦抛成普通 HTTPError，
            # 就会被当作可重试的瞬时错误，白白重试 3 次。
            quota = _parse_quota_error(response)
            if quota is not None:
                response.close()
                raise quota
            last_status = response.status_code
            try:
                response.raise_for_status()
            except Exception:
                # 失败的响应不会有人去读正文：stream=True 时不 close 连接会
                # 一直占着，直到 GC 顺手回收。
                response.close()
                raise
            return response
        except QuotaExhausted:
            # 确定性失败，重试毫无意义，直接上抛让调用方停下来。
            raise
        except Exception as exc:
            last_error = exc
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    # 重试全部用尽仍是 429：这不是瞬时抖动，而是真的被限流了。
    # 单列一类上抛，调用方才能与"这个片源站没有"区分开（后者是 404/业务错误）。
    if last_status == 429:
        raise SearchThrottled(_redact(last_error))
    raise last_error


def request_with_keys(method, url, params=None, **kwargs):
    """带 api_key 发请求；额度用尽就换下一个 key，被限流就全局冷却后重试。

    这是**唯一**该被业务代码调用的请求入口 —— 把"哪个 key、什么时候换、
    什么时候等"整个收拢在这里，业务侧只管拿结果。

    两种 429 处置相反，绝不能混：
      - 配额型（QuotaExhausted）：这个 key 今天废了 -> 退役、换下一个；
      - 限流型（SearchThrottled）：key 没问题 -> 全局冷却，**同一个 key** 再试。
    只有**所有 key 都用尽**或**冷却 THROTTLE_MAX_ROUNDS 轮仍被限流**才把异常
    抛给调用方（此时全局闸门已落，后续影片直接跳过）。
    """
    params = dict(params or {})
    throttled_rounds = 0
    # 上限 = 每个 key 试一遍 + 每轮冷却各占一次机会；+1 兜底极端并发下
    # current() 刚被别的线程推进过的情况。有限次数，绝不无限循环。
    for _ in range(len(key_pool) + THROTTLE_MAX_ROUNDS + 1):
        if stop_fetching.is_set() and stop_reason.get("status") == "search_throttled":
            # 别的线程已经判定限流不可恢复，没必要再各自冷却一遍。
            raise SearchThrottled(stop_reason.get("error") or "接口被持续限流")
        key = key_pool.current()
        if key is None:
            raise QuotaExhausted("所有 api_key 的当日额度均已用尽")
        params["api_key"] = key
        try:
            return request_with_retry(method, url, params=params, **kwargs)
        except QuotaExhausted as exc:
            if not _retire_key(key, str(exc), exc.reset_at):
                raise
        except SearchThrottled as exc:
            throttled_rounds += 1
            if throttled_rounds > THROTTLE_MAX_ROUNDS:
                _signal_stop("search_throttled", str(exc))
                raise
            waited = _wait_for_throttle()
            print(
                f"⏳ SubDL 限流，全局冷却 {waited:.0f}s 后继续"
                f"（第 {throttled_rounds}/{THROTTLE_MAX_ROUNDS} 轮，不换 key）",
                flush=True,
            )
    raise QuotaExhausted("所有 api_key 的当日额度均已用尽")


def search_subtitles(tmdb_id, languages=None, media_type="movie"):
    """按 tmdb_id **逐语种**查询候选字幕，合并返回。

    为什么不一次 languages="EN,ZH" 混查：SubDL 只返回首页（subs_per_page
    上限 30），且结果按热度排，英文字幕动辄几十上百条，会把中文挤出首页。
    实测混查首页 30 条里 ZH 只剩 1 条、甚至可能为 0 —— 而 pick_candidates
    找不到就会把该语种判成 missing 写进缺口台账，从此**永久放弃**。这是一个
    静默且不可逆的误判，所以每个语种单独查一次（搜索不耗下载额度，只多几次请求）。

    media_type 是 SubDL 的 type 参数（movie / tv），见 _media_type_of。

    SubDL 语义：影片不存在 → status=false + "can't find movie or tv"；
    影片存在但该语种没字幕 → status=true + 空列表。前者对任何语种都一样，
    遇到即整部放弃（上抛 RuntimeError，调用方按错误消息归类）。
    """
    # 调用方传的是 config 里的语种码（en/zh/zh-cn），要映射成 SubDL 码。
    # 映射不出来的语种直接跳过——未知码 SubDL 不报错而是返回全语种结果，
    # 查了也只会拿到一堆比不上的东西（且 download_one 已提前把它们剔出 pending）。
    codes = []
    for lang in (languages or SUBTITLE_LANGUAGES):
        code = subdl_language_code(lang)
        if code and code not in codes:
            codes.append(code)
    merged = []
    for code in codes:
        params = {
            "tmdb_id": tmdb_id,
            "type": media_type,
            "languages": code,
            "subs_per_page": 30,
            "client": "custom_integration",
        }
        response = request_with_keys("GET", SEARCH_API, params=params)
        data = response.json()
        if not data.get("status"):
            raise RuntimeError(data.get("error") or "SubDL 返回 status=false")
        merged.extend(data.get("subtitles") or [])
    return merged


def pick_candidates(subtitles, language):
    """挑出该语种可下载的候选，按 SubDL 给的顺序（相关度）返回。

    同一语种通常有多个版本；只挑一条的问题在于**结果里没有格式字段**，
    下载解压后才知道是不是 ASS。ASS 前端 <track> 不认、我们也不落盘，若只有
    一条候选就会把该语种误判成 missing 进台账。故返回一小串候选，调用方逐条
    试到拿到 srt/vtt 为止（上限 MAX_CANDIDATES_PER_LANGUAGE，每试一条都烧额度）。

    language 是 config 里的语种码（en/zh-cn），与 SubDL 的 language 字段按
    subdl_language_code 映射后比较。
    """
    want = subdl_language_code(language)
    picked = []
    for item in subtitles:
        if (item.get("language") or "").upper() != want:
            continue
        if item.get("full_season"):
            continue
        if item.get("url"):
            picked.append(item)
            if len(picked) >= MAX_CANDIDATES_PER_LANGUAGE:
                break
    return picked


def pick_best(subtitles, language):
    """首选候选（pick_candidates 的第一条），没有则 None。"""
    picked = pick_candidates(subtitles, language)
    return picked[0] if picked else None


def _strip_api_key(url):
    """去掉 SubDL 返回 url 里自带的 api_key，只保留纯路径。

    🔑 多 key 场景下这一步是**必须**的，不只是为了好看：
    搜索结果里的 url 自带的是**当时那个 key**。若原样拼接，即便 KeyPool 已经
    切到了 key2，请求里带的仍是耗尽的 key1 —— 换 key 会完全失效，而且症状
    极隐蔽（看起来在轮换，实际一直在撞同一堵墙）。
    故：一律剥掉自带的 key，由 request_with_keys 统一盖当前 key。

    顺带解决了老问题：自带 key 再追加一个会拼出 `?api_key=K&api_key=K`。
    """
    return str(url).split("?", 1)[0]


def extract_srt(zip_bytes):
    """SubDL 下载回来的是 zip，从里面取出体积最大的字幕文件。

    返回 (正文字节, 扩展名)；没有可用字幕或体积异常时返回 (None, None)。
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        candidates = [
            info
            for info in archive.infolist()
            if not info.is_dir()
            and info.filename.lower().endswith((".srt", ".ass", ".ssa", ".vtt"))
        ]
        if not candidates:
            return None, None
        # 同一个 zip 里既有 srt 又有 ass 时优先 srt/vtt：ass 通常带样式、体积
        # 更大，按"体积最大"会选中它，而 ass 我们并不落盘。
        usable = [
            info for info in candidates
            if info.filename.lower().endswith((".srt", ".vtt"))
        ]
        # 体积最大的通常是完整正片字幕，而不是片头片尾之类的碎片
        best = max(usable or candidates, key=lambda info: info.file_size)
        # 下载阶段的 MAX_ZIP_BYTES 只拦得住**压缩后**的体积；zip 炸弹几十 KB
        # 能解出几 GB，archive.read 会一口气读进内存。file_size 是中央目录里
        # 声明的解压后大小，超限直接放弃，不碰正文。
        if best.file_size > MAX_ZIP_BYTES:
            return None, None
        suffix = os.path.splitext(best.filename)[1].lower()
        return archive.read(best), suffix


def decode_subtitle(raw):
    """字幕编码很杂，逐个尝试常见编码，最终统一输出 UTF-8。

    直接复用下载侧的实现，保证两条路径的解码行为一致。
    """
    return dm._decode_subtitle(raw)


def _sniff_format(text, declared):
    """按**正文**判定字幕格式，扩展名只在正文认不出时兜底。

    返回 "vtt" / "srt" / 其它（不落盘的富文本格式，如 ass/ssa）。

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

    ass/ssa 等非 srt/vtt 格式**不落盘**，返回 []：前端 <track> 只认 vtt，
    转换规则又与 srt/vtt 完全不同。历史版本曾原样存成 en.ass，结果该语种被
    一个用不了的文件"锁死"——_language_of 认为已有，永不再补。调用方应换下
    一条候选（见 _fetch_into）。
    """
    fmt = _sniff_format(text, source_format)
    if fmt not in ("srt", "vtt"):
        return []

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


def _reconcile_meta(tmdb_id, year, remote_files, result):
    """本片不需要再抓任何字幕时，顺手核对 R2 上 meta.json 是否把已有字幕都记全了。

    为什么需要这一步：上一次运行若在"字幕已传、meta 回写失败"处断掉（网络
    抖动、412 重试用尽），R2 上就会留下"文件在、索引没有"的半成品——前端
    按 meta 索引，等于这些字幕不存在。而下次运行走到这里 pending 已为空，
    若直接返回就永远不会再碰 meta，日志里那句"下次自动补记"就成了空话。

    meta 已齐时 _update_remote_meta 不发起任何写请求，稳态成本只是一次 get。
    对账结果并进 result：补记了就把状态改成 meta_repaired；核对失败只加个
    标记，不改状态——字幕本身没缺，不该被报成"这片有问题"。
    """
    if not REMOTE_MODE or not remote_files:
        return result
    ok, added = _update_remote_meta(tmdb_id, year, remote_files)
    if added:
        return {**result, "status": "meta_repaired", "metaAdded": added}
    if not ok:
        return {**result, "metaCheckFailed": True}
    return result


def download_one(entry, ledger=None):
    """为一部片补齐缺失语种的字幕。

    绝不抛异常：任何失败都归到返回值里，让主循环继续跑下一部。

    R2 模式下的流程：问 R2 要已有语种 -> 查缺口台账剔除没戏的语种 ->
    只补剩下的 -> 字幕写临时目录 -> 上传 R2 -> 删临时目录 -> 更新 meta.json。
    本地不留任何残留，与下载侧"上传成功即删本地"的口径一致。

    ledger：{tmdbId: {已确认源站没有的语种}}，传 None 表示不用台账
    （等价于旧行为，每个语种都问一遍）。
    """
    tmdb_id = entry["tmdbId"]
    year = entry.get("year")

    # 全局闸门已落：后面的片一律直接跳过。继续跑只会让每部片各发若干次注定
    # 失败的请求，白等重试间隔，还会把接口问题记成「源站缺这个语种」。
    # 如实沿用置位时的原因（额度耗尽 / 搜索限流），不笼统归成一种。
    if stop_fetching.is_set():
        return tmdb_id, dict(stop_reason) or {"status": "quota_exhausted"}

    # 第一步：确定还缺哪些语种。R2 模式下**必须问 R2** —— 本地影片目录在
    # 视频上传成功那一刻就被删了，扫本地只会得到空集，把每部片都当成缺口。
    if REMOTE_MODE:
        remote_files, ok = _list_remote_subtitles(tmdb_id, year)
        if not ok:
            return tmdb_id, {"status": "list_failed"}
        existing = {lang for lang in map(_language_of, remote_files) if lang}
    else:
        remote_files = []
        # 影片目录已被用户删掉的片没必要补字幕：否则会凭空造出一个只有
        # subs/ 的孤立目录，而 success.jsonl 只记录"下过"，不知道"还在不在"。
        if not os.path.isdir(dm.movie_dir(tmdb_id, year)):
            return tmdb_id, {"status": "local_missing"}
        existing = _existing_languages_local(subs_dir(tmdb_id, year))

    pending = [
        lang for lang in SUBTITLE_LANGUAGES
        if lang.lower() not in existing
        # 映射不出合法 SubDL 码的语种整体跳过（启动时已告警）：查了也是
        # 全语种大杂烩，比不上任何候选，只会被记成 missing 进台账。
        and subdl_language_code(lang)
    ]
    if not pending:
        return tmdb_id, _reconcile_meta(tmdb_id, year, remote_files,
                                        {"status": "skipped"})

    # 查缺口台账：源站已经确认没有的语种不再问第二遍。这是省配额的关键 ——
    # 实测 zh 的缺口占了一半以上，每次重跑都重查等于把额度扔进水里。
    given_up = ledger.get(str(tmdb_id), set()) if ledger else set()
    if given_up:
        remaining = [lang for lang in pending if lang.lower() not in given_up]
        if not remaining:
            # 该片所有还缺的语种都已确认源站没有 —— 这不是"补完了"，
            # 而是"没得补了"，状态上要与 skipped 区分开，否则统计会误导。
            return tmdb_id, _reconcile_meta(
                tmdb_id, year, remote_files,
                {"status": "gap_confirmed", "givenUp": sorted(given_up)},
            )
        pending = remaining

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
        return tmdb_id, _fetch_into(
            tmdb_id, year, pending, target_dir,
            media_type=_media_type_of(entry.get("title_type")),
        )
    finally:
        if REMOTE_MODE:
            # 无论成败都要清临时目录，否则每跑一次就漏一批临时文件。
            shutil.rmtree(target_dir, ignore_errors=True)


def _download_candidate(picked, language, target_dir):
    """下载一条候选并落盘，返回已写文件名列表。

    返回 []：候选**不可用**（zip 里没字幕、或正文是 ASS/SSA 等不落盘的格式），
    调用方应换下一条候选。网络/解压/写盘等**过程性**失败直接抛异常，由调用方
    归入 fetch_failed——两者必须分开：前者换候选还能救，后者下次重试才有意义。
    """
    # 剥掉 url 自带的 key，改由 request_with_keys 盖当前 key。
    # 不剥的话换 key 会静默失效（见 _strip_api_key 注释）。
    download_url = DOWNLOAD_BASE + _strip_api_key(picked["url"])
    response = request_with_keys("GET", download_url, stream=True)
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
        return []
    text = decode_subtitle(content)
    return _write_variants(
        target_dir, language, text, extension.lstrip(".").lower()
    )


def _fetch_into(tmdb_id, year, pending, target_dir, media_type="movie"):
    """把 pending 里各语种的字幕抓进 target_dir；R2 模式下再上传并更新 meta。

    ⚠️ 这里捕获的 QuotaExhausted / SearchThrottled 语义是**所有 key 都用尽**
    / **冷却多轮仍被限流**（单个 key 用尽、单次限流已由 request_with_keys
    就地消化掉了），所以直接收摊。
    """
    try:
        # 只查还缺的语种：已有的语种无需再问，省请求也省限流额度。
        subtitles = search_subtitles(tmdb_id, pending, media_type=media_type)
    except SearchThrottled as exc:
        # 冷却多轮仍被限流：这一部**压根没查成**，与"SubDL 库里没有这个
        # tmdb_id"完全不同。混进 search_failed 会把那个统计弄脏（真查不到的
        # 片值得从待办里剔除，被限流的片下次还要再试）。
        _signal_stop("search_throttled", str(exc))
        return {"status": "search_throttled", "error": str(exc),
                "saved": [], "missing": [], "unattempted": list(pending)}
    except QuotaExhausted as exc:
        _signal_stop("quota_exhausted", str(exc), exc.reset_at)
        result = {"status": "quota_exhausted", "error": str(exc),
                  "saved": [], "missing": [], "unattempted": list(pending)}
        if exc.reset_at:
            result["quotaResetAt"] = exc.reset_at
        return result
    except Exception as exc:  # noqa: BLE001 - 查询失败只跳过这一部
        if NOT_FOUND_MARKER in str(exc).lower():
            # SubDL 库里压根没有这个 tmdb_id：这是**确定性**结论，对所有语种
            # 都成立。把 pending 全部记成 missing 让它进台账——否则每次运行
            # 都会把库里没有的片重搜一遍，库大了以后纯属噪音。
            return {"status": "not_in_subdl", "saved": [],
                    "missing": list(pending), "fetch_failed": []}
        return {"status": "search_failed", "error": _redact(exc)}

    # ⚠️ missing 与 fetch_failed 必须分开，这是缺口台账能否成立的**前提**：
    #   missing      = SubDL 确认没有这个语种 → 进台账，以后不再问（省配额）；
    #   fetch_failed = 我们没取回来（网络抖动、解压失败、写盘失败）→ **不进台账**，
    #                  下次照常重试。
    # 混在一起的后果很隐蔽：一次网络抖动就会让那部片的该语种被永久放弃，
    # 而日志上看起来只是"源站没有"，无从察觉。
    result = {"status": "ok", "saved": [], "missing": [], "fetch_failed": []}

    def _unattempted():
        """本片还没得出任何结论的语种——额度恢复后要接着试的就是这些。

        排除三类已有结论的：已存下、源站确认没有、取回失败（后者虽然也要
        重试，但它已在 fetch_failed 里记着了，重复记一遍会让统计翻倍）。
        """
        return [
            lang for lang in pending
            if lang not in result["missing"]
            and lang not in result["fetch_failed"]
            and not any(s.startswith(f"{lang}.") for s in result["saved"])
        ]

    for language in pending:
        candidates = pick_candidates(subtitles, language)
        if not candidates:
            # 源站没有该语种：这是常态，不是错误
            result["missing"].append(language)
            continue

        try:
            # 逐候选试：解压后发现是 ASS/SSA、或 zip 里没有字幕文件时换下一条。
            # 搜索结果里没有格式字段，只能下载后才知道；每试一条都烧一次额度，
            # 上限由 pick_candidates 控制。
            saved = []
            for picked in candidates:
                saved = _download_candidate(picked, language, target_dir)
                if saved:
                    break
            if saved:
                result["saved"].extend(saved)
            else:
                # 所有候选都不是 srt/vtt（或 zip 里没字幕）。归入 missing 而非
                # fetch_failed 是刻意的：候选选择是确定性的，下次重试会拿到
                # **同一批** zip、得到同样的结果，只是白烧几次下载额度。
                result["missing"].append(language)
        except QuotaExhausted as exc:
            # 额度耗尽：置位全局闸门让后续影片直接跳过，并**立刻停止本片**剩下
            # 的语种——它们同样一个都拿不到。
            # 绝不把这个语种记进 missing：missing 的语义是"源站没有"，而这里是
            # "我们没额度取"。混进去就等于把配额问题伪装成源站缺字幕，下次运行
            # 会以为已经查过了（实际压根没查成），这正是本次排查踩的坑。
            _signal_stop("quota_exhausted", str(exc), exc.reset_at)
            result["status"] = "quota_exhausted"
            result["error"] = str(exc)
            if exc.reset_at:
                result["quotaResetAt"] = exc.reset_at
            # 本片未取到的语种要如实标出来，供下次运行继续尝试。
            result["unattempted"] = _unattempted()
            break
        except SearchThrottled as exc:
            # 下载接口冷却多轮仍被限流，处置同上：停下来、如实标注未尝试的语种。
            _signal_stop("search_throttled", str(exc))
            result["status"] = "search_throttled"
            result["error"] = str(exc)
            result["unattempted"] = _unattempted()
            break
        except Exception as exc:  # noqa: BLE001 - 单语种失败不影响其它语种
            # 取回过程出错（网络、解压、编码、写盘）。**不是** missing——源站
            # 可能明明有，只是这次没拿到。记进 missing 会让台账永久放弃它。
            result["fetch_failed"].append(language)
            result.setdefault("errors", []).append(f"{language}: {_redact(exc)}")

    if REMOTE_MODE and result["saved"]:
        uploaded = _upload_subtitles(tmdb_id, year, target_dir, result["saved"])
        # 上传失败的不算数：本地临时目录马上就删，没进 R2 等于这份字幕不存在，
        # 报成功会让人以为补上了，而下次运行仍会（正确地）再抓一遍。
        result["saved"] = uploaded
        if uploaded:
            ok, _ = _update_remote_meta(tmdb_id, year, uploaded)
            result["metaUpdated"] = ok
        elif result["status"] == "ok":
            # 只在还没有更要紧的结论时才改写：额度耗尽是全局性的，
            # 不该被一次上传失败盖掉（否则日志里看不出该停了）。
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


def load_ledger():
    """读缺口台账，返回 {tmdbId: {已确认源站没有的语种}}。

    纯追加文件，同一片可能有多行（每次运行各记一次）——按 tmdbId **并集**
    合并，与 download_dead.jsonl 的"读取端聚合"范式一致（写入端保持纯追加，
    避免为了去重而重写整个文件、与并发写冲突）。

    文件不存在 / 坏行 / 读失败一律当成空台账：台账是**省配额的优化**，
    不是正确性的一环。读不到最多多花点额度重查一遍，绝不能让它阻断主流程。
    """
    ledger = {}
    if not os.path.exists(LEDGER_LOG):
        return ledger
    try:
        with open(LEDGER_LOG, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                tmdb_id = str(record.get("tmdbId") or "").strip()
                langs = record.get("missing")
                if not tmdb_id or not isinstance(langs, list):
                    continue
                ledger.setdefault(tmdb_id, set()).update(
                    str(lang).lower() for lang in langs if lang
                )
    except OSError as exc:
        print(f"⚠️ 读取缺口台账失败（按空台账处理）: {exc}", flush=True)
    return ledger


def record_gap(tmdb_id, languages):
    """把"源站确认没有"的语种追加进台账。写失败只告警，不影响主流程。"""
    if not languages:
        return
    try:
        with state_lock:
            with open(LEDGER_LOG, "a", encoding="utf-8") as file:
                file.write(json.dumps({
                    "tmdbId": str(tmdb_id),
                    "missing": sorted(set(languages)),
                    "logged_at": int(time.time()),
                }, ensure_ascii=False) + "\n")
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 缺口台账写入失败（已忽略）: {exc}", flush=True)


def write_state(record):
    """追加一条状态记录。写失败不影响主流程——状态日志本身也是可有可无的。"""
    try:
        with state_lock:
            with open(STATE_LOG, "a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 状态日志写入失败（已忽略）: {exc}", flush=True)


def _read_lock_pid():
    try:
        with open(LOCK_FILE, encoding="utf-8") as fh:
            return int(fh.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def acquire_lock():
    """抢单实例锁。已被活进程持有返回 False；陈旧锁（进程已死）自动接管。"""
    pid = _read_lock_pid()
    if pid and pid != os.getpid() and dm._pid_alive(pid):
        return False
    if pid:
        dm.remove_file(LOCK_FILE)
    try:
        with open(LOCK_FILE, "w", encoding="utf-8") as fh:
            fh.write(str(os.getpid()))
    except OSError as exc:
        print(f"⚠️ 写锁文件失败（继续执行，但失去互斥保护）: {exc}", flush=True)
    return True


def release_lock():
    # 只删自己写的锁：若因写锁失败等原因锁已被别的实例接管，不能误删人家的。
    if _read_lock_pid() == os.getpid():
        dm.remove_file(LOCK_FILE)


def main():
    if not acquire_lock():
        # 同样以成功码退出：另一个实例正在补同一批片，本次没必要做。
        print(f"已有 fetch_subtitles 实例在运行（PID {_read_lock_pid()}），"
              f"本次跳过 -> {LOCK_FILE}", flush=True)
        return
    try:
        _run()
    finally:
        release_lock()


def _run():
    # 没配 API Key 就安静跳过：字幕是可有可无的附属步骤，不该让整条流水线
    # 因为缺一个可选凭证而以非零码退出（那会让 cron / && 串联的后续步骤中断）。
    if not SUBDL_API_KEYS:
        print("未配置 SUBDL_API_KEYS/SUBDL_API_KEY，跳过字幕补全"
              "（不影响已下载的影片）", flush=True)
        return

    entries = load_entries()
    if not entries:
        return
    bad_langs = unsupported_languages()
    if bad_langs:
        # 只告警不退出：其余语种照常补。未知码 SubDL 不报错而是返回全语种
        # 结果，若不在这里拦下，这些语种会在每部片上都被判成 missing 进台账。
        print(
            f"⚠️ 配置中的语种 {bad_langs} 无法映射为 SubDL 语种码，本次整体跳过"
            f"（请改用 en / zh / zh-cn / pt-br 之类的形式）",
            flush=True,
        )
    ledger = load_ledger()
    ledger_note = (
        f"；缺口台账已记 {len(ledger)} 部" if ledger else ""
    )
    print(
        f"待处理电影: {len(entries)}（语种: {', '.join(SUBTITLE_LANGUAGES)}；"
        f"落点: {'R2 ' + dm.S3_BUCKET if REMOTE_MODE else '本地 ' + dm.BASE_DIR}；"
        f"可用 api_key: {len(SUBDL_API_KEYS)} 个{ledger_note}）",
        flush=True,
    )

    stats = {}
    saved_count = 0
    gap_count = 0
    stop_announced = False
    # 收尾提示要用的两项，从**结果**里取而不是读 stop_reason：
    # stop_reason 只有在本进程内真正触发过 _signal_stop 时才有值，而结果字典
    # 是每条记录自带的，两者不总是同步（例如首片就被拦下的极端时序）。
    quota_reset_at = None
    blocked_kinds = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(download_one, entry, ledger): entry for entry in entries
        }
        for index, future in enumerate(as_completed(futures), 1):
            entry = futures[future]
            try:
                tmdb_id, result = future.result()
            except Exception as exc:  # noqa: BLE001 - 单片异常不能带塌整批
                tmdb_id = entry.get("tmdbId")
                result = {"status": "failed", "error": _redact(exc)}
                print(f"[{index}/{len(entries)}] {tmdb_id} 异常（已跳过）: "
                      f"{_redact(exc)}", flush=True)

            stats[result["status"]] = stats.get(result["status"], 0) + 1
            saved_count += len(result.get("saved", []))
            # 只把"源站确认没有"记进台账。fetch_failed / unattempted 都不记 ——
            # 它们下次还要再试，记进去等于永久放弃。
            if result.get("missing"):
                record_gap(tmdb_id, result["missing"])
                gap_count += len(result["missing"])
            write_state({"tmdbId": tmdb_id, "title": entry.get("title"),
                         **result})

            if result["status"] == "ok":
                # 取回失败的单独列出来：它与"源站没有"的后续处置完全不同
                # （前者下次还会再试，后者已进台账不再问）。
                retry_note = (
                    f" 取回失败 {result['fetch_failed']}（下次重试）"
                    if result.get("fetch_failed") else ""
                )
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} {entry.get('title')} "
                    f"-> 已保存 {result['saved']} 缺失 {result['missing']}"
                    f"{retry_note}",
                    flush=True,
                )
            elif result["status"] == "meta_repaired":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} {entry.get('title')} "
                    f"-> 字幕已齐，补记 meta.json {result['metaAdded']} 条",
                    flush=True,
                )
            elif result["status"] == "not_in_subdl":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} {entry.get('title')} "
                    f"-> SubDL 库里没有这部片，已记入台账 {result['missing']}",
                    flush=True,
                )
            elif result["status"] == "local_missing":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} 本地影片目录不存在"
                    f"（已删？），跳过",
                    flush=True,
                )
            elif result["status"] == "search_failed":
                print(
                    f"[{index}/{len(entries)}] {tmdb_id} 查询失败（已跳过）: "
                    f"{result['error']}",
                    flush=True,
                )
            elif result["status"] in ("quota_exhausted", "search_throttled"):
                blocked_kinds.add(result["status"])
                if result.get("quotaResetAt"):
                    quota_reset_at = result["quotaResetAt"]
                # 只在第一次撞上时刷一条醒目提示。后续都是被闸门拦下的静默跳过，
                # 每部都打一遍只会把真正有用的信息冲掉。
                if not stop_announced and result.get("error"):
                    stop_announced = True
                    if result["status"] == "quota_exhausted":
                        what = (f"全部 {len(SUBDL_API_KEYS)} 个 api_key "
                                f"的当日下载额度均已用尽")
                        when = (f"额度重置时间(UTC): "
                                f"{result.get('quotaResetAt') or '未提供'}")
                    else:
                        what = "全部 api_key 均被 SubDL 持续限流（重试已用尽）"
                        when = "稍后重试即可（限流通常会自行恢复）"
                    print(
                        f"\n🛑 [{index}/{len(entries)}] {what}，剩余影片全部跳过。\n"
                        f"   原因: {result['error']}\n"
                        f"   {when}\n"
                        f"   ⚠️ 这不是「源站没有字幕」——这些片一个都没查成，"
                        f"恢复后再跑一次本脚本即可继续补。",
                        flush=True,
                    )

    # 两种收场必须**说得明显不同**：
    #   跑完了  -> 圆满，所有待补的片都处理过了，没必要再跑；
    #   没额度了 -> 被迫中断，明天额度重置后还得接着跑。
    # 混为一谈的话，用户看完日志不知道到底还要不要再来一趟。
    blocked = stats.get("quota_exhausted", 0) + stats.get("search_throttled", 0)
    used_keys = key_pool.exhausted_count()

    print(f"\n完成。本次新增字幕文件 {saved_count} 个，统计: {stats}", flush=True)
    if gap_count:
        print(
            f"缺口台账新记 {gap_count} 条（源站确认没有的语种），"
            f"下次运行将直接跳过它们以省配额 -> {LEDGER_LOG}",
            flush=True,
        )
    if stats.get("gap_confirmed"):
        print(
            f"其中 {stats['gap_confirmed']} 部因所缺语种此前已确认源站没有"
            f"而整片跳过（未消耗任何配额）",
            flush=True,
        )

    if blocked:
        # 额度耗尽与被限流的"下一步"不同：前者要等到明天，后者过一阵就能再试。
        # 都说成"等额度重置"会让人白等一天。
        if "quota_exhausted" in blocked_kinds:
            reason = f"当日额度耗尽，额度重置(UTC): {quota_reset_at or '未提供'}"
            nxt = "⏭️  明天额度重置后**再跑一次本脚本**即可接着补"
        else:
            reason = "接口被持续限流"
            nxt = "⏭️  过一段时间**再跑一次本脚本**即可接着补"
        print(
            f"\n🛑 ===== 因配额耗尽而中断（未跑完）=====\n"
            f"   已用尽 {used_keys}/{len(SUBDL_API_KEYS)} 个 api_key；"
            f"{blocked} 部影片本次未处理（{reason}）。\n"
            f"   {nxt}（已补好的会自动跳过）。\n"
            f"   想一次补更多：加配 api_key（SUBDL_API_KEYS=key1,key2,...）"
            f"或升级 SubDL Pro。",
            flush=True,
        )
    else:
        # 跑完了。但"跑完"不等于"一路顺风"——中途可能已经烧掉了几个 key，
        # 只是最后一个撑到了收尾。这个区别直接影响用户要不要再加配 key，
        # 所以必须如实说，不能笼统报"没有触发任何配额限制"。
        if used_keys:
            spent = (f"   期间用尽了 {used_keys}/{len(SUBDL_API_KEYS)} 个 "
                     f"api_key（最后一个仍有余额）。\n")
        else:
            spent = f"   未触发任何配额限制（共 {len(SUBDL_API_KEYS)} 个 api_key）。\n"
        # "都已尝试过"这话对被台账跳过的片不成立——它们这次一个请求都没发。
        # 收尾结论要经得起推敲，否则下次遇到"某片始终没字幕"会白排查一轮。
        skipped = stats.get("gap_confirmed", 0)
        scope = (
            f"   当前 success.jsonl 里的片都已处理过"
            f"（其中 {skipped} 部按缺口台账直接跳过），**无需今天再跑**；\n"
            if skipped else
            "   当前 success.jsonl 里的片都已尝试过，**无需今天再跑**；\n"
        )
        print(
            f"\n✅ ===== 全部待补影片已处理完毕 =====\n"
            f"{spent}"
            f"{scope}"
            f"   等下载侧产出新片后再执行即可。",
            flush=True,
        )


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
