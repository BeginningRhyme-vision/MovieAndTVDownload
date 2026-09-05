"""
IMDB 全量电视剧 + TMDB ID 转换 → JSONL
架构：预建 SQLite 索引（一次性），主流程流式处理，彻底避免大文件全量加载进内存
titleType 覆盖：tvSeries, tvMiniSeries, tvSpecial, tvShort（可在 config.yaml 调整）
相比电影版新增字段：total_seasons, total_episodes, episodes(分集列表，来自 IMDB title.episode)
注意：IMDB 的季/集编号与 TMDB/vidup 可能不一致，此处的 episodes 仅作参考与筛选依据，
取流阶段以 TMDB 的季集结构为准（见 tv_ids_to_links.py）。
"""

import csv
import gzip
import json
import os
import time
import logging
import requests
import shutil
import sqlite3
import threading
import numpy as np
import pandas as pd
import tmdbsimple as tmdb
import yaml
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# ========== 配置（从 config.yaml 的 fetch_tv_metadata 段读取）==========
CONFIG_PATH = Path(__file__).with_name("config.yaml")


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


def load_config() -> dict:
    """读取项目统一配置文件中本脚本对应的段落。"""
    if not CONFIG_PATH.exists():
        raise SystemExit(f"找不到配置文件: {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        full = yaml.safe_load(f) or {}
    cfg = full.get("fetch_tv_metadata")
    if not isinstance(cfg, dict):
        raise SystemExit("config.yaml 缺少 fetch_tv_metadata 配置段")
    return cfg


_CFG = load_config()

# TMDB API Key：敏感项，优先取环境变量 TMDB_API_KEY（同目录 .env），
# 缺省时才回退 config.yaml（便于本地调试）。
TMDB_API_KEY = (os.environ.get("TMDB_API_KEY", "").strip()
                or (_CFG.get("tmdb_api_key") or "").strip())
if not TMDB_API_KEY:
    raise SystemExit("请在 .env 配置 TMDB_API_KEY（或 config.yaml 的 fetch_tv_metadata.tmdb_api_key）")
tmdb.API_KEY = TMDB_API_KEY

_SCRIPT_DIR = Path(__file__).resolve().parent


def _resolve(value, default_name) -> Path:
    """Resolve config paths relative to the script directory (not CWD), consistent
    with the other TVDownloader scripts, so `imdb_data/` stays isolated from MovieDownloader."""
    raw = (value or "").strip() or default_name
    return (_SCRIPT_DIR / raw).resolve()


DATA_DIR = _resolve(_CFG.get("data_dir"), "imdb_data")
INDEX_DB = DATA_DIR / "index.db"  # 辅助索引库（akas/principals/names/episode）
OUTPUT = _resolve(_CFG.get("output"), "tv_series.jsonl")
PROGRESS = _resolve(_CFG.get("progress"), "progress.txt")
LOG_PATH = _resolve(_CFG.get("log_path"), "fetch.log")
# 旁路输出：IMDB 标为 TV 类型（多为 tvSpecial/tvShort），但 TMDB 把它建模成 movie 的条目。
# TV pipeline 的 /tv/{id}/{s}/{e} 路径对它们不适用，留档供电影版 pipeline 接手。
AS_MOVIE_OUTPUT = _resolve(_CFG.get("as_movie_output"), "tv_as_movie.tsv")

KEEP_TYPES = set(_CFG.get("keep_types", ["tvSeries", "tvMiniSeries", "tvSpecial", "tvShort"]))
MAX_WORKERS = int(_CFG.get("max_workers", 8))
SLEEP = float(_CFG.get("sleep", 0.25))

# 复用同一个 requests.Session：并发访问 TMDB 时共享 TCP 连接池，
# 避免每次请求重复 DNS 解析 / TLS 握手，降低连接开销与偶发的连接重置。
_SESSION = requests.Session()
_SESSION.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2,
))
tmdb.REQUESTS_SESSION = _SESSION
# tmdbsimple 默认 REQUESTS_TIMEOUT=None（无超时）：一个卡住的连接会永久占死一个 worker，
# 全部 worker 卡死后进程假活。显式设置 (连接超时, 读超时)，超时会以 requests 异常抛出，
# 由 get_tmdb_id 的重试逻辑接管。
tmdb.REQUESTS_TIMEOUT = (10, 30)

# IMDB 数据集是纯 TSV：不做引号转义，字段内可能出现以 " 开头的值（如 "Weird Al" Yankovic）。
# csv/pandas 默认 QUOTE_MINIMAL 会把它当引号模式，引号不成对时会把后续多行吞进同一字段，静默丢行。
# 所有 IMDB 文件读取统一使用 QUOTE_NONE。
_IMDB_QUOTING = csv.QUOTE_NONE

BASE_URL = "https://datasets.imdbws.com/"
DATASETS = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
    "akas": "title.akas.tsv.gz",
    "crew": "title.crew.tsv.gz",
    "principals": "title.principals.tsv.gz",
    "names": "name.basics.tsv.gz",
    "episode": "title.episode.tsv.gz",  # 电视剧专属：分集信息
}

# ========== 日志 ==========
def _ensure_dirs():
    """data_dir / output / progress / log_path 都允许配成多层相对路径，统一预建父目录。"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for p in (OUTPUT, PROGRESS, LOG_PATH, AS_MOVIE_OUTPUT):
        p.parent.mkdir(parents=True, exist_ok=True)


_ensure_dirs()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
_lock = threading.Lock()


# ========== 进度 ==========
def load_done() -> set:
    if not PROGRESS.exists():
        return set()
    return set(PROGRESS.read_text(encoding="utf-8").splitlines())


def mark_done(imdb_id: str):
    with _lock:
        with open(PROGRESS, "a", encoding="utf-8") as f:
            f.write(imdb_id + "\n")


def commit_as_movie(imdb_id: str, tmdb_movie_id, title_type):
    """TMDB 把该条目建模为 movie：写旁路 TSV（imdb_id\\ttmdb_movie_id\\ttitle_type）并标记完成。
    与 commit_record 同理，两次写入在同一把锁内，顺序为先旁路后 progress。"""
    line = f"{imdb_id}\t{tmdb_movie_id}\t{title_type or ''}\n"
    with _lock:
        with open(AS_MOVIE_OUTPUT, "a", encoding="utf-8") as f:
            f.write(line)
        with open(PROGRESS, "a", encoding="utf-8") as f:
            f.write(imdb_id + "\n")


class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.bool_,)):   return bool(obj)
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        return super().default(obj)


def commit_record(record: dict, imdb_id: str):
    """一次加锁内先写 jsonl 再写 progress：
    - 先序列化再打开文件，序列化失败不会留下半行；
    - 两个文件各自 flush 后才释放锁，其他线程不会交错写入；
    - 若在写完 jsonl、写 progress 之前崩溃，下次重跑会重复写同一行（下游按 tmdb_id dict 覆盖，无害），
      但绝不会出现"progress 已标记而 jsonl 没数据"的丢数据情况。"""
    line = json.dumps(record, ensure_ascii=False, cls=_Encoder) + "\n"
    with _lock:
        with open(OUTPUT, "a", encoding="utf-8") as f:
            f.write(line)
        with open(PROGRESS, "a", encoding="utf-8") as f:
            f.write(imdb_id + "\n")


# ========== 下载（已存在则跳过）==========
def ensure_dataset(key: str) -> Path:
    """下载并解压 IMDB 数据集。
    半成品保护：下载/解压都先写到 .part 临时文件，完成后原子 rename 到最终名。
    这样中途被杀（Ctrl-C/OOM）只会留下 .part，不会留下被截断却被 exists() 误判为完整的 .tsv。"""
    filename = DATASETS[key]
    tsv_path = DATA_DIR / filename.replace(".gz", "")
    if tsv_path.exists():
        log.info(f"已存在跳过: {tsv_path.name}")
        return tsv_path
    gz_path = DATA_DIR / filename
    gz_part = gz_path.with_name(gz_path.name + ".part")
    tsv_part = tsv_path.with_name(tsv_path.name + ".part")
    try:
        if not gz_path.exists():
            log.info(f"下载 {filename} ...")
            with requests.get(BASE_URL + filename, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(gz_part, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
            gz_part.replace(gz_path)
        log.info(f"解压 {filename} ...")
        with gzip.open(gz_path, "rb") as gz, open(tsv_part, "wb") as out:
            shutil.copyfileobj(gz, out)
        tsv_part.replace(tsv_path)
    finally:
        # 无论成功失败都清掉临时文件；成功时它们已被 rename 走，unlink 为 no-op
        gz_part.unlink(missing_ok=True)
        tsv_part.unlink(missing_ok=True)
    gz_path.unlink()
    return tsv_path


# ========== 建 SQLite 索引（只建一次）==========
def build_index():
    """把 akas / principals / names / episode 导入 SQLite，后续按 imdb_id 点查，无需全量加载"""
    import sys
    csv.field_size_limit(min(sys.maxsize, 10_000_000))  # 解除字段长度限制
    conn = sqlite3.connect(INDEX_DB)
    cur = conn.cursor()

    def _index_ready(table: str) -> bool:
        # 仅当"表存在且至少有一行数据"才算建好，避免上次插入途中崩溃留下的半成品空表被误判为完成
        row = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not row:
            return False
        return cur.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None

    def build_table(name, create_sql, index_sql, insert_sql, dataset_key, row_to_tuple):
        # 全有或全无：清掉可能的半成品表 -> 建表建索引 -> 批量插入 -> 提交；
        # 中途任何异常（含 KeyboardInterrupt/SystemExit）回滚并删表，保证不留半成品
        if _index_ready(name):
            log.info(f"索引已存在跳过: {name}")
            return
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        conn.commit()
        log.info(f"建索引: {name} ...")
        try:
            cur.execute(create_sql)
            if index_sql:
                cur.execute(index_sql)
            path = ensure_dataset(dataset_key)
            with open(path, encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t", quoting=_IMDB_QUOTING)
                batch = []
                for row in reader:
                    rec = row_to_tuple(row)
                    if rec is None:
                        continue
                    batch.append(rec)
                    if len(batch) >= 50000:
                        cur.executemany(insert_sql, batch)
                        batch = []
                if batch:
                    cur.executemany(insert_sql, batch)
            conn.commit()
            log.info(f"建索引: {name} 完成")
        except BaseException:
            conn.rollback()
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            conn.commit()
            log.error(f"建索引: {name} 失败，已回滚并清除半成品表")
            raise

    # --- names ---
    build_table(
        "names",
        "CREATE TABLE names (nconst TEXT PRIMARY KEY, primaryName TEXT)",
        None,
        "INSERT OR IGNORE INTO names VALUES (?,?)",
        "names",
        lambda row: (row["nconst"], row.get("primaryName", "")),
    )

    # --- akas ---
    _akas_cols = ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"]
    build_table(
        "akas",
        """CREATE TABLE akas (
            titleId TEXT, ordering TEXT, title TEXT, region TEXT,
            language TEXT, types TEXT, attributes TEXT, isOriginalTitle TEXT
        )""",
        "CREATE INDEX idx_akas_titleId ON akas(titleId)",
        "INSERT INTO akas VALUES (?,?,?,?,?,?,?,?)",
        "akas",
        lambda row: tuple(row.get(c, "") for c in _akas_cols),
    )

    # --- principals ---
    _principals_cols = ["tconst", "ordering", "nconst", "category", "job", "characters"]
    build_table(
        "principals",
        """CREATE TABLE principals (
            tconst TEXT, ordering TEXT, nconst TEXT,
            category TEXT, job TEXT, characters TEXT
        )""",
        "CREATE INDEX idx_principals_tconst ON principals(tconst)",
        "INSERT INTO principals VALUES (?,?,?,?,?,?)",
        "principals",
        lambda row: tuple(row.get(c, "") for c in _principals_cols),
    )

    # --- episode（电视剧专属：按 parentTconst 点查某剧的全部分集）---
    _episode_cols = ["tconst", "parentTconst", "seasonNumber", "episodeNumber"]
    build_table(
        "episode",
        """CREATE TABLE episode (
            tconst TEXT PRIMARY KEY, parentTconst TEXT,
            seasonNumber TEXT, episodeNumber TEXT
        )""",
        "CREATE INDEX idx_episode_parent ON episode(parentTconst)",
        "INSERT OR IGNORE INTO episode VALUES (?,?,?,?)",
        "episode",
        lambda row: tuple(row.get(c, "") for c in _episode_cols),
    )

    conn.close()
    log.info("SQLite 索引全部就绪")


# ========== 按需查询索引 ==========
# 每个线程独立连接
_local = threading.local()


def get_conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(INDEX_DB, check_same_thread=False)
    return _local.conn


def query_akas(imdb_id: str) -> list:
    rows = get_conn().execute(
        "SELECT ordering,title,region,language,types,attributes,isOriginalTitle FROM akas WHERE titleId=?",
        (imdb_id,)
    ).fetchall()
    cols = ["ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"]
    return [{c: (None if v in ("", "\\N") else v) for c, v in zip(cols, r)} for r in rows]


def query_principals(imdb_id: str, names_dict: dict) -> list:
    rows = get_conn().execute(
        "SELECT ordering,nconst,category,job,characters FROM principals WHERE tconst=? ORDER BY ordering",
        (imdb_id,)
    ).fetchall()
    result = []
    for ordering, nconst, category, job, characters in rows:
        result.append({
            "nconst": nconst or None,
            "name": names_dict.get(nconst) if nconst else None,
            "category": None if category in ("", "\\N") else category,
            "job": None if job in ("", "\\N") else job,
            "characters": None if characters in ("", "\\N") else characters,
            "ordering": ordering,
        })
    return result


def query_name(nconst: str, names_dict: dict) -> str:
    # 统一从内存字典取名，与 cast_crew(query_principals) 共用同一数据来源，
    # 避免同一份 names 表既走内存又走 SQLite 造成的重复存储与潜在不一致。
    # 保留原语义：查不到时回退为 nconst 本身。
    return names_dict.get(nconst, nconst)


def _to_int_or_none(v):
    if v is None or v in ("", "\\N"):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def query_episodes(imdb_id: str, ratings) -> dict:
    """返回 {total_seasons, total_episodes, episodes: [...]}
    season/episode 为 IMDB 编号（可能为 None，IMDB 中存在未归季的集）。
    total_seasons 不计 season 0（特辑），与 TMDB number_of_seasons 及日常语义一致；
    episodes / total_episodes 仍包含 S0，因为下游 include_specials=true 时特辑会实际下载，
    总集数需要如实反映下载量。"""
    rows = get_conn().execute(
        "SELECT tconst,seasonNumber,episodeNumber FROM episode WHERE parentTconst=?",
        (imdb_id,)
    ).fetchall()
    if not rows:
        return {"total_seasons": None, "total_episodes": 0, "episodes": []}

    # Batch-fetch ratings with a single reindex: long-running shows (daily soaps)
    # can have 10k+ episodes, and per-episode `.loc` is ~100x slower than one reindex.
    sub = ratings.reindex([r[0] for r in rows])
    rating_vals = sub["averageRating"].tolist()
    votes_vals = sub["numVotes"].tolist()

    episodes = []
    seasons = set()
    for (tconst, season, ep_num), rating, votes in zip(rows, rating_vals, votes_vals):
        s = _to_int_or_none(season)
        e = _to_int_or_none(ep_num)
        episodes.append({
            "episode_imdb_id": tconst,
            "season": s,
            "episode": e,
            "rating": None if pd.isna(rating) else float(rating),
            "votes": None if pd.isna(votes) else int(votes),
        })
        if s is not None and s != 0:
            seasons.add(s)

    # 在 Python 侧排序，None 排最后，避免 SQL 中 "seasonNumber+0" 对 \N 的隐式转换
    episodes.sort(key=lambda x: (x["season"] is None, x["season"] or 0,
                                 x["episode"] is None, x["episode"] or 0))
    return {
        "total_seasons": len(seasons) if seasons else None,
        "total_episodes": len(episodes),
        "episodes": episodes,
    }


# ========== 加载小文件（全量，内存够用）==========
def load_basics() -> pd.DataFrame:
    path = ensure_dataset("basics")
    df = pd.read_csv(path, sep="\t", na_values="\\N", low_memory=False, quoting=_IMDB_QUOTING)
    df = df[df["titleType"].isin(KEEP_TYPES)].copy()
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce")
    df["endYear"] = pd.to_numeric(df["endYear"], errors="coerce")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce")
    # isAdult 官方只有 0/1；脏值（\N、空、其他字符串）一律视为 False，
    # 避免 map 产出 NaN → JSON 输出非法的 NaN 字面量，且下游 filter_to_ids 按布尔判断。
    df["isAdult"] = (
        df["isAdult"].map({"0": False, "1": True, 0: False, 1: True})
        .fillna(False).astype(bool)
    )
    df["genres"] = df["genres"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    df = df.set_index("tconst")
    # tconst 理论上唯一，但对索引去重可防御脏数据：
    # 若存在重复 tconst，basics.loc[imdb_id] 会返回多行 DataFrame 而非单行 Series，
    # 导致后续 row.get(...) 取值异常。保留首次出现的记录。
    dup = df.index.duplicated(keep="first")
    if dup.any():
        log.warning(f"basics: 发现 {int(dup.sum()):,} 个重复 tconst，已保留首次出现的记录")
        df = df[~dup]
    log.info(f"basics (TV): {len(df):,} 条")
    return df


def load_ratings() -> pd.DataFrame:
    path = ensure_dataset("ratings")
    df = pd.read_csv(path, sep="\t", na_values="\\N", quoting=_IMDB_QUOTING)
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce")
    df = df.set_index("tconst")
    dup = df.index.duplicated(keep="first")
    if dup.any():
        log.warning(f"ratings: 发现 {int(dup.sum()):,} 个重复 tconst，已保留首次出现的记录")
        df = df[~dup]
    log.info(f"ratings: {len(df):,} 条")
    return df


def load_crew() -> pd.DataFrame:
    path = ensure_dataset("crew")
    df = pd.read_csv(path, sep="\t", na_values="\\N", quoting=_IMDB_QUOTING)
    df["directors"] = df["directors"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    df["writers"] = df["writers"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    df = df.set_index("tconst")
    dup = df.index.duplicated(keep="first")
    if dup.any():
        log.warning(f"crew: 发现 {int(dup.sum()):,} 个重复 tconst，已保留首次出现的记录")
        df = df[~dup]
    log.info(f"crew: {len(df):,} 条")
    return df


def load_names_dict() -> dict:
    """从 SQLite 读全量 names 到内存。

    name.basics 约 1400 万条，Python dict + 两份 str 对象的开销实际约 2-3GB
    （不是"约 100MB"）。保留全量字典的原因：process() 每条剧集要按 nconst 查
    多个导演/编剧名，走 SQLite 点查会把热路径拖慢一个数量级；只装载被引用的
    nconst 又需要先扫一遍 crew 表做集合运算，复杂度不划算。
    要求运行机器有 >= 4GB 可用内存（与 keep_types 无关，names 表始终全量装载）。
    """
    conn = sqlite3.connect(INDEX_DB)
    rows = conn.execute("SELECT nconst, primaryName FROM names").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


# ========== TMDB ID（查 tv_results）==========
class TMDBLookupError(RuntimeError):
    """TMDB 请求在重试耗尽后仍失败（网络/限速/服务端错误）。
    与"TMDB 里确实查不到"（返回 None）严格区分：前者不能 mark_done，必须留待下次重跑。"""


def get_tmdb_id(imdb_id: str, retry: int = 3):
    """返回 (tv_id, movie_id)，各自在 TMDB 无对应结果时为 None；请求持续失败抛 TMDBLookupError。
    TMDB Find 按媒体类型分桶：IMDB 标为 tvSpecial/tvShort 的条目常落在 movie_results，
    这里把 movie 桶一并带回，由 process 决定走主输出还是旁路，不多耗一次 API。
    429 限速不消耗重试次数（限速是外部节奏问题，不是本条目的问题），
    但设有上限防止 TMDB 长时间限速时线程无限空转。"""
    last_err = None
    rate_limit_hits = 0
    attempt = 0
    while attempt < retry:
        try:
            result = tmdb.Find(imdb_id).info(external_source="imdb_id")
            tv = result.get("tv_results") or []
            movie = result.get("movie_results") or []
            return (tv[0]["id"] if tv else None, movie[0]["id"] if movie else None)
        except Exception as e:
            last_err = e
            if "429" in str(e):
                rate_limit_hits += 1
                if rate_limit_hits > 6:  # 最多等 60s
                    break
                log.warning("限速，等待 10s")
                time.sleep(10)
                continue
            attempt += 1
            log.warning(f"{imdb_id} 第{attempt}次失败: {e}")
            if attempt < retry:
                time.sleep(2 ** attempt)
    raise TMDBLookupError(f"TMDB 查询失败（已重试）: {last_err}")


# ========== 处理单条 ==========
def process(imdb_id: str, basics, ratings, crew, names_dict) -> str:
    """返回状态字符串：ok（写入主输出）/ as_movie（写入旁路）/ skip（TMDB 查无）。
    TMDB 请求持续失败时抛 TMDBLookupError，由调用方决定不标记完成。"""
    tmdb_id, movie_id = get_tmdb_id(imdb_id)
    time.sleep(SLEEP)
    if tmdb_id is None:
        if movie_id is not None:
            commit_as_movie(imdb_id, movie_id, basics.loc[imdb_id].get("titleType"))
            return "as_movie"
        mark_done(imdb_id)
        return "skip"

    row = basics.loc[imdb_id]
    rat = ratings.loc[imdb_id] if imdb_id in ratings.index else None
    cr = crew.loc[imdb_id] if imdb_id in crew.index else None

    directors = [query_name(n, names_dict) for n in (cr["directors"] if cr is not None else [])]
    writers = [query_name(n, names_dict) for n in (cr["writers"] if cr is not None else [])]

    ep_info = query_episodes(imdb_id, ratings)

    record = {
        "imdb_id": imdb_id,
        "tmdb_id": tmdb_id,
        "title_type": row.get("titleType"),
        "primary_title": row.get("primaryTitle"),
        "original_title": row.get("originalTitle"),
        "is_adult": row.get("isAdult"),
        "start_year": None if pd.isna(row.get("startYear")) else int(row["startYear"]),
        "end_year": None if pd.isna(row.get("endYear")) else int(row["endYear"]),
        "runtime_minutes": None if pd.isna(row.get("runtimeMinutes")) else int(row["runtimeMinutes"]),
        "genres": row.get("genres", []),
        "rating": None if rat is None else float(rat["averageRating"]),
        "votes": None if rat is None else int(rat["numVotes"]),
        "directors": directors,
        "writers": writers,
        "cast_crew": query_principals(imdb_id, names_dict),
        "akas": query_akas(imdb_id),
        # 电视剧专属（IMDB 视角）
        "total_seasons": ep_info["total_seasons"],
        "total_episodes": ep_info["total_episodes"],
        "episodes": ep_info["episodes"],
    }

    commit_record(record, imdb_id)
    return "ok"


# ========== 主流程 ==========
def _fmt_stats(stats: dict) -> str:
    return (f"写入:{stats['ok']} 转电影:{stats['as_movie']} "
            f"无TMDB:{stats['skip']} 失败:{stats['error']}")


def run_pool(pending, job, max_workers: int, window: int = None, log_every: int = 200) -> dict:
    """有界提交窗口地跑完 pending，返回各状态计数。

    为什么不用一次性 submit 全部：
    - 首跑 pending 有数十万条，一次性提交会常驻几十万个 Future 对象；
    - `with ThreadPoolExecutor` 退出时 shutdown(wait=True) 会把队列里剩余任务全部跑完，
      Ctrl+C 实际停不下来，只能 kill -9。
    改为窗口内最多 window 个在飞，完成一个补一个；收到 KeyboardInterrupt 时停止补货并
    cancel_futures 取消未开始的任务，只等在飞的那几个收尾，几秒内干净退出。
    job 自身已吞掉所有 Exception 并返回 'error'，这里对 result() 不再兜底。"""
    window = window or max_workers * 4
    stats = {"ok": 0, "as_movie": 0, "skip": 0, "error": 0}
    total = len(pending)
    it = iter(pending)
    in_flight = set()
    finished = 0
    interrupted = False

    def _account(fut):
        nonlocal finished
        finished += 1
        s = fut.result()
        stats[s if s in stats else "error"] += 1
        if log_every and finished % log_every == 0:
            log.info(f"进度 {finished:,}/{total:,} | {_fmt_stats(stats)}")

    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        for iid in it:
            in_flight.add(executor.submit(job, iid))
            if len(in_flight) >= window:
                break
        while in_flight:
            done_set, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done_set:
                _account(fut)
            for iid in it:
                in_flight.add(executor.submit(job, iid))
                if len(in_flight) >= window:
                    break
    except KeyboardInterrupt:
        interrupted = True
        log.warning("收到中断，停止提交新任务，等待在飞任务收尾...")
        executor.shutdown(wait=True, cancel_futures=True)
        for fut in in_flight:
            if fut.done() and not fut.cancelled():
                _account(fut)
        raise
    finally:
        executor.shutdown(wait=True)
        if interrupted:
            log.warning(f"中断退出：已完成 {finished:,}/{total:,} | {_fmt_stats(stats)}（未完成的下次运行自动续跑）")
    return stats


def main():
    # 1. 确保所有数据集已下载
    log.info("=== 检查数据集 ===")
    for key in DATASETS:
        ensure_dataset(key)

    # 2. 建 SQLite 索引（已建则秒过）
    log.info("=== 建立索引 ===")
    build_index()

    # 3. 加载小文件
    log.info("=== 加载小文件 ===")
    basics = load_basics()
    ratings = load_ratings()
    crew = load_crew()
    names_dict = load_names_dict()

    # 4. 开始处理
    done = load_done()
    pending = [iid for iid in basics.index if iid not in done]
    log.info(f"已处理: {len(done):,}  待处理: {len(pending):,}")

    def job(imdb_id):
        try:
            return process(imdb_id, basics, ratings, crew, names_dict)
        except Exception as e:
            # 注意：这里不能 mark_done。异常代表本条“未成功处理”，
            # 若标记为已完成，重跑时会跳过它，造成静默丢数据。
            # 不标记则下次运行会自动重试该 imdb_id。
            log.error(f"{imdb_id} 异常（将于下次运行重试）: {e}")
            return "error"

    stats = run_pool(pending, job, MAX_WORKERS)

    log.info(f"全部完成！{_fmt_stats(stats)}")
    log.info(f"输出: {OUTPUT.resolve()}")
    if stats["as_movie"]:
        log.info(f"TMDB 建模为电影的条目已写入: {AS_MOVIE_OUTPUT.resolve()}（供电影版 pipeline 接手）")


if __name__ == "__main__":
    main()
