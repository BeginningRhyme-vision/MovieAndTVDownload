"""
IMDB 全量电影 + TMDB ID 转换 → JSONL
架构：预建 SQLite 索引（一次性），主流程流式处理，彻底避免大文件全量加载进内存
"""

import csv
import gzip
import json
import time
import logging
import requests
import shutil
import sqlite3
import threading
import pandas as pd
import tmdbsimple as tmdb
import yaml
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 配置（从 config.yaml 的 fetch_movie_metadata 段读取）==========
CONFIG_PATH = Path(__file__).with_name("config.yaml")


def load_config() -> dict:
    """读取项目统一配置文件中本脚本对应的段落。"""
    if not CONFIG_PATH.exists():
        raise SystemExit(f"找不到配置文件: {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        full = yaml.safe_load(f) or {}
    cfg = full.get("fetch_movie_metadata")
    if not isinstance(cfg, dict):
        raise SystemExit("config.yaml 缺少 fetch_movie_metadata 配置段")
    return cfg


_CFG = load_config()

TMDB_API_KEY = _CFG.get("tmdb_api_key") or ""
if not TMDB_API_KEY:
    raise SystemExit("请在 config.yaml 的 fetch_movie_metadata.tmdb_api_key 填写 TMDB API Key")
tmdb.API_KEY = TMDB_API_KEY

DATA_DIR = Path(_CFG.get("data_dir", "imdb_data"))
INDEX_DB = DATA_DIR / "index.db"  # 辅助索引库（akas/principals/names）
OUTPUT = Path(_CFG.get("output", "movies.jsonl"))
PROGRESS = Path(_CFG.get("progress", "progress.txt"))
LOG_PATH = Path(_CFG.get("log_path", "fetch.log"))

KEEP_TYPES = set(_CFG.get("keep_types", ["movie"]))
MAX_WORKERS = int(_CFG.get("max_workers", 8))
SLEEP = float(_CFG.get("sleep", 0.25))

BASE_URL = "https://datasets.imdbws.com/"
DATASETS = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
    "akas": "title.akas.tsv.gz",
    "crew": "title.crew.tsv.gz",
    "principals": "title.principals.tsv.gz",
    "names": "name.basics.tsv.gz",
}

# ========== 日志 ==========
DATA_DIR.mkdir(exist_ok=True)
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


class _Encoder(json.JSONEncoder):
    def default(self, obj):
        import numpy as np
        if isinstance(obj, (np.bool_,)):   return bool(obj)
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        return super().default(obj)


def write_jsonl(record: dict):
    with _lock:
        with open(OUTPUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, cls=_Encoder) + "\n")


# ========== 下载（已存在则跳过）==========
def ensure_dataset(key: str) -> Path:
    filename = DATASETS[key]
    tsv_path = DATA_DIR / filename.replace(".gz", "")
    if tsv_path.exists():
        log.info(f"已存在跳过: {tsv_path.name}")
        return tsv_path
    gz_path = DATA_DIR / filename
    log.info(f"下载 {filename} ...")
    with requests.get(BASE_URL + filename, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)
    log.info(f"解压 {filename} ...")
    with gzip.open(gz_path, "rb") as gz, open(tsv_path, "wb") as out:
        shutil.copyfileobj(gz, out)
    gz_path.unlink()
    return tsv_path


# ========== 建 SQLite 索引（只建一次）==========
def build_index():
    """把 akas / principals / names 导入 SQLite，后续按 imdb_id 点查，无需全量加载"""
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

    def build_table(name, create_sql, index_sql, insert_sql, dataset_key,
                    row_to_tuple, reader_kwargs=None):
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
                reader = csv.DictReader(f, delimiter="\t", **(reader_kwargs or {}))
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
        reader_kwargs={"quoting": csv.QUOTE_NONE},
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


# ========== 加载小文件（全量，内存够用）==========
def load_basics() -> pd.DataFrame:
    path = ensure_dataset("basics")
    df = pd.read_csv(path, sep="\t", na_values="\\N", low_memory=False)
    df = df[df["titleType"].isin(KEEP_TYPES)].copy()
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce")
    df["endYear"] = pd.to_numeric(df["endYear"], errors="coerce")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce")
    df["isAdult"] = df["isAdult"].map({"0": False, "1": True, 0: False, 1: True})
    df["genres"] = df["genres"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    df = df.set_index("tconst")
    # tconst 理论上唯一，但对索引去重可防御脏数据：
    # 若存在重复 tconst，basics.loc[imdb_id] 会返回多行 DataFrame 而非单行 Series，
    # 导致后续 row.get(...) 取值异常。保留首次出现的记录。
    dup = df.index.duplicated(keep="first")
    if dup.any():
        log.warning(f"basics: 发现 {int(dup.sum()):,} 个重复 tconst，已保留首次出现的记录")
        df = df[~dup]
    log.info(f"basics: {len(df):,} 条")
    return df


def load_ratings() -> pd.DataFrame:
    path = ensure_dataset("ratings")
    df = pd.read_csv(path, sep="\t", na_values="\\N")
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
    df = pd.read_csv(path, sep="\t", na_values="\\N")
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
    """从 SQLite 读全量 names 到内存（约 100MB，可接受）"""
    conn = sqlite3.connect(INDEX_DB)
    rows = conn.execute("SELECT nconst, primaryName FROM names").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


# ========== TMDB ID ==========
def get_tmdb_id(imdb_id: str, retry: int = 3):
    for attempt in range(retry):
        try:
            result = tmdb.Find(imdb_id).info(external_source="imdb_id")
            movies = result.get("movie_results", [])
            return movies[0]["id"] if movies else None
        except Exception as e:
            if "429" in str(e):
                log.warning("限速，等待 10s")
                time.sleep(10)
            else:
                log.warning(f"{imdb_id} 第{attempt + 1}次失败: {e}")
                time.sleep(2 ** attempt)
    return None


# ========== 处理单条 ==========
def process(imdb_id: str, basics, ratings, crew, names_dict):
    tmdb_id = get_tmdb_id(imdb_id)
    time.sleep(SLEEP)
    if tmdb_id is None:
        mark_done(imdb_id)
        return None

    row = basics.loc[imdb_id]
    rat = ratings.loc[imdb_id] if imdb_id in ratings.index else None
    cr = crew.loc[imdb_id] if imdb_id in crew.index else None

    directors = [query_name(n, names_dict) for n in (cr["directors"] if cr is not None else [])]
    writers = [query_name(n, names_dict) for n in (cr["writers"] if cr is not None else [])]

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
    }

    write_jsonl(record)
    mark_done(imdb_id)
    return imdb_id


# ========== 主流程 ==========
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
    total = len(pending)
    log.info(f"已处理: {len(done):,}  待处理: {total:,}")

    success = skipped = error = 0

    def job(imdb_id):
        try:
            result = process(imdb_id, basics, ratings, crew, names_dict)
            return "ok" if result else "skip"
        except Exception as e:
            # 注意：这里不能 mark_done。异常代表本条“未成功处理”，
            # 若标记为已完成，重跑时会跳过它，造成静默丢数据。
            # 不标记则下次运行会自动重试该 imdb_id。
            log.error(f"{imdb_id} 异常（将于下次运行重试）: {e}")
            return "error"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(job, iid): iid for iid in pending}
        for i, fut in enumerate(as_completed(futures), 1):
            s = fut.result()
            if s == "ok":
                success += 1
            elif s == "skip":
                skipped += 1
            else:
                error += 1
            if i % 200 == 0:
                log.info(f"进度 {i:,}/{total:,} | 写入:{success} 无TMDB:{skipped} 失败:{error}")

    log.info(f"全部完成！写入:{success} 无TMDB:{skipped} 失败:{error}")
    log.info(f"输出: {OUTPUT.resolve()}")


if __name__ == "__main__":
    main()
