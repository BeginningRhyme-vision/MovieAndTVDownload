"""
IMDB 全量电视剧 + TMDB ID 转换 → JSONL
titleType 覆盖：tvSeries, tvMiniSeries, tvSpecial, tvShort
新增字段：total_seasons, total_episodes, episodes(分集列表)
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
import sys
import pandas as pd
import tmdbsimple as tmdb
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 配置 ==========
TMDB_API_KEY = "83ee7d0b61b4623ae794e741a5883d34"
tmdb.API_KEY = TMDB_API_KEY

DATA_DIR = Path("../movie/imdb_data")  # 与电影脚本共用同一份数据集
INDEX_DB = DATA_DIR / "index.db"  # 共用同一份 SQLite 索引
OUTPUT = Path("tv_series.jsonl")
PROGRESS = Path("tv_progress.txt")  # 独立进度文件，不与电影混用
LOG_PATH = Path("tv_fetch.log")

# 电视剧相关的 titleType
KEEP_TYPES = {"tvSeries", "tvMiniSeries", "tvSpecial", "tvShort"}

MAX_WORKERS = 15
SLEEP = 0.1

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
        if isinstance(obj, np.bool_):    return bool(obj)
        if isinstance(obj, np.integer):  return int(obj)
        if isinstance(obj, np.floating): return float(obj)
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


# ========== 建 / 复用 SQLite 索引 ==========
def build_index():
    csv.field_size_limit(min(sys.maxsize, 10_000_000))
    conn = sqlite3.connect(INDEX_DB)
    cur = conn.cursor()
    tables = {r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    # --- names（与电影共用，已存在跳过）---
    if "names" not in tables:
        log.info("建索引: names ...")
        cur.execute("CREATE TABLE names (nconst TEXT PRIMARY KEY, primaryName TEXT)")
        path = ensure_dataset("names")
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            batch = []
            for row in reader:
                batch.append((row["nconst"], row.get("primaryName", "")))
                if len(batch) >= 50000:
                    cur.executemany("INSERT OR IGNORE INTO names VALUES (?,?)", batch)
                    batch = []
            if batch:
                cur.executemany("INSERT OR IGNORE INTO names VALUES (?,?)", batch)
        conn.commit()
        log.info("建索引: names 完成")
    else:
        log.info("索引已存在跳过: names")

    # --- akas（与电影共用）---
    if "akas" not in tables:
        log.info("建索引: akas ...")
        cur.execute("""CREATE TABLE akas (
            titleId TEXT, ordering TEXT, title TEXT, region TEXT,
            language TEXT, types TEXT, attributes TEXT, isOriginalTitle TEXT
        )""")
        cur.execute("CREATE INDEX idx_akas_titleId ON akas(titleId)")
        path = ensure_dataset("akas")
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            batch = []
            cols = ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"]
            for row in reader:
                batch.append(tuple(row.get(c, "") for c in cols))
                if len(batch) >= 50000:
                    cur.executemany("INSERT INTO akas VALUES (?,?,?,?,?,?,?,?)", batch)
                    batch = []
            if batch:
                cur.executemany("INSERT INTO akas VALUES (?,?,?,?,?,?,?,?)", batch)
        conn.commit()
        log.info("建索引: akas 完成")
    else:
        log.info("索引已存在跳过: akas")

    # --- principals（与电影共用）---
    if "principals" not in tables:
        log.info("建索引: principals ...")
        cur.execute("""CREATE TABLE principals (
            tconst TEXT, ordering TEXT, nconst TEXT,
            category TEXT, job TEXT, characters TEXT
        )""")
        cur.execute("CREATE INDEX idx_principals_tconst ON principals(tconst)")
        path = ensure_dataset("principals")
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            batch = []
            cols = ["tconst", "ordering", "nconst", "category", "job", "characters"]
            for row in reader:
                try:
                    batch.append(tuple(row.get(c, "") for c in cols))
                except Exception:
                    continue
                if len(batch) >= 50000:
                    cur.executemany("INSERT INTO principals VALUES (?,?,?,?,?,?)", batch)
                    batch = []
            if batch:
                cur.executemany("INSERT INTO principals VALUES (?,?,?,?,?,?)", batch)
        conn.commit()
        log.info("建索引: principals 完成")
    else:
        log.info("索引已存在跳过: principals")

    # --- episode（电视剧专属）---
    if "episode" not in tables:
        log.info("建索引: episode ...")
        cur.execute("""CREATE TABLE episode (
            tconst TEXT PRIMARY KEY,
            parentTconst TEXT,
            seasonNumber TEXT,
            episodeNumber TEXT
        )""")
        cur.execute("CREATE INDEX idx_episode_parent ON episode(parentTconst)")
        path = ensure_dataset("episode")
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            batch = []
            for row in reader:
                batch.append((
                    row.get("tconst", ""),
                    row.get("parentTconst", ""),
                    row.get("seasonNumber", ""),
                    row.get("episodeNumber", ""),
                ))
                if len(batch) >= 50000:
                    cur.executemany("INSERT OR IGNORE INTO episode VALUES (?,?,?,?)", batch)
                    batch = []
            if batch:
                cur.executemany("INSERT OR IGNORE INTO episode VALUES (?,?,?,?)", batch)
        conn.commit()
        log.info("建索引: episode 完成")
    else:
        log.info("索引已存在跳过: episode")

    conn.close()
    log.info("SQLite 索引全部就绪")


# ========== 线程本地 SQLite 连接 ==========
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


def query_name(nconst: str) -> str:
    r = get_conn().execute("SELECT primaryName FROM names WHERE nconst=?", (nconst,)).fetchone()
    return r[0] if r else nconst


def query_episodes(imdb_id: str, ratings) -> dict:
    """返回 {total_seasons, total_episodes, episodes: [...]}"""
    rows = get_conn().execute(
        "SELECT tconst,seasonNumber,episodeNumber FROM episode WHERE parentTconst=? ORDER BY seasonNumber+0, episodeNumber+0",
        (imdb_id,)
    ).fetchall()
    if not rows:
        return {"total_seasons": None, "total_episodes": 0, "episodes": []}

    episodes = []
    seasons = set()
    for tconst, season, ep_num in rows:
        rat = ratings.loc[tconst] if tconst in ratings.index else None
        episodes.append({
            "episode_imdb_id": tconst,
            "season": None if season in ("", "\\N") else int(season) if season.isdigit() else season,
            "episode": None if ep_num in ("", "\\N") else int(ep_num) if ep_num.isdigit() else ep_num,
            "rating": None if rat is None else float(rat["averageRating"]),
            "votes": None if rat is None else int(rat["numVotes"]),
        })
        if season and season not in ("", "\\N"):
            seasons.add(season)

    return {
        "total_seasons": len(seasons) if seasons else None,
        "total_episodes": len(episodes),
        "episodes": episodes,
    }


# ========== 加载小文件 ==========
def load_basics() -> pd.DataFrame:
    path = ensure_dataset("basics")
    df = pd.read_csv(path, sep="\t", na_values="\\N", low_memory=False)
    df = df[df["titleType"].isin(KEEP_TYPES)].copy()
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce")
    df["endYear"] = pd.to_numeric(df["endYear"], errors="coerce")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce")
    df["isAdult"] = df["isAdult"].map({"0": False, "1": True, 0: False, 1: True})
    df["genres"] = df["genres"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    log.info(f"basics (TV): {len(df):,} 条")
    return df.set_index("tconst")


def load_ratings() -> pd.DataFrame:
    path = ensure_dataset("ratings")
    df = pd.read_csv(path, sep="\t", na_values="\\N")
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce")
    log.info(f"ratings: {len(df):,} 条")
    return df.set_index("tconst")


def load_crew() -> pd.DataFrame:
    path = ensure_dataset("crew")
    df = pd.read_csv(path, sep="\t", na_values="\\N")
    df["directors"] = df["directors"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    df["writers"] = df["writers"].apply(lambda x: x.split(",") if isinstance(x, str) else [])
    log.info(f"crew: {len(df):,} 条")
    return df.set_index("tconst")


def load_names_dict() -> dict:
    conn = sqlite3.connect(INDEX_DB)
    rows = conn.execute("SELECT nconst, primaryName FROM names").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


# ========== TMDB ID（查 tv_results）==========
def get_tmdb_id(imdb_id: str, retry: int = 3):
    for attempt in range(retry):
        try:
            result = tmdb.Find(imdb_id).info(external_source="imdb_id")
            tv = result.get("tv_results", [])  # ← 电视剧用 tv_results
            return tv[0]["id"] if tv else None
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

    directors = [query_name(n) for n in (cr["directors"] if cr is not None else [])]
    writers = [query_name(n) for n in (cr["writers"] if cr is not None else [])]

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
        # 电视剧专属
        "total_seasons": ep_info["total_seasons"],
        "total_episodes": ep_info["total_episodes"],
        "episodes": ep_info["episodes"],
    }

    write_jsonl(record)
    mark_done(imdb_id)
    return imdb_id


# ========== 主流程 ==========
def main():
    log.info("=== 检查数据集 ===")
    for key in DATASETS:
        ensure_dataset(key)

    log.info("=== 建立索引 ===")
    build_index()

    log.info("=== 加载小文件 ===")
    basics = load_basics()
    ratings = load_ratings()
    crew = load_crew()
    names_dict = load_names_dict()

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
            log.error(f"{imdb_id} 异常: {e}")
            mark_done(imdb_id)
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
