"""Offline unit tests for download_tv.py.

Focus: episode-level key contract, local folder layout, R2 key mapping,
success/failed/pending log bookkeeping, playlist parsing and reupload flow.
No network / ffmpeg / boto3 calls are made.
"""

import builtins
import json
import os
import sys
import threading
import time
import types

import pytest

import download_tv as d


def _read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class _FakeInterrupt:
    """替身中断事件：永不置位，但把每次退避时长记进 sink，供断言退避轮数。"""

    def __init__(self, sink):
        self.sink = sink

    def is_set(self):
        return False

    def wait(self, timeout=None):
        self.sink.append(timeout)
        return False

    def set(self):
        raise AssertionError("测试替身不应被置位")


def _always_set_event():
    ev = threading.Event()
    ev.set()
    return ev


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point every module-level path at tmp_path and reset shared state."""
    base = tmp_path / "downloads"
    temp = tmp_path / "temp"
    monkeypatch.setattr(d, "BASE_DIR", str(base))
    monkeypatch.setattr(d, "TEMP_DIR", str(temp))
    monkeypatch.setattr(d, "SUCCESS_LOG", str(tmp_path / "success.jsonl"))
    monkeypatch.setattr(d, "FAILED_LOG", str(tmp_path / "failed.jsonl"))
    # 判死账本也必须指向 tmp_path：它由 handle_done_future 在画质判死时自动写入，
    # 不隔离的话测试会把记录写进真实工作目录，而下一个用例启动时
    # load_dead_keys() 会把这些集并进跳过集 —— 表现为"待处理条目莫名变 0"，
    # 极难排查（本夹具漏掉它时确实让 5 个既有用例连锁失败过）。
    monkeypatch.setattr(d, "DOWNLOAD_DEAD_LOG", str(tmp_path / "download_dead.jsonl"))
    monkeypatch.setattr(d, "UPLOAD_PENDING_LOG", str(tmp_path / "pending.jsonl"))
    monkeypatch.setattr(d, "MAIN_LOCK_FILE", str(tmp_path / "main.lock"))
    monkeypatch.setattr(d, "FOLDER_PREFIX", "tv")
    monkeypatch.setattr(d, "S3_PREFIX", "")
    monkeypatch.setattr(d, "processing_ids", set())
    return tmp_path


# ---------------------------------------------------------------- key helpers
@pytest.mark.parametrize("value,expected", [
    (3, 3), ("3", 3), (" 12 ", 12), ("0", 0), (0, 0),
    (None, None), (True, None), (False, None), ("", None),
    ("1.5", None), ("abc", None), ([], None),
])
def test_parse_int(value, expected):
    assert d.parse_int(value) == expected


@pytest.mark.parametrize("tid,s,e,expected", [
    (12345, 1, 3, "12345_S01E03"),
    ("12345", "1", "3", "12345_S01E03"),
    (12345, 0, 7, "12345_S00E07"),          # specials season
    (12345, 12, 105, "12345_S12E105"),      # >2 digits not truncated
    (None, 1, 1, ""),
    ("", 1, 1, ""),
    (1, None, 1, ""),
    (1, 1, None, ""),
    (1, -1, 1, ""),
    (1, 1, -1, ""),
    (1, "x", 1, ""),
    (1, True, 1, ""),
])
def test_episode_key(tid, s, e, expected):
    assert d.episode_key(tid, s, e) == expected


def test_record_episode_key_and_identity():
    rec = {"tmdbId": 5, "season": "2", "episode": 9, "title": "T", "junk": 1}
    assert d.record_episode_key(rec) == "5_S02E09"
    assert d.record_episode_key("not a dict") == ""
    assert d.record_episode_key({"tmdbId": 5}) == ""
    assert d._entry_identity(rec) == {
        "tmdbId": 5, "season": 2, "episode": 9, "title": "T",
    }
    assert d._entry_identity(None) == {
        "tmdbId": None, "season": None, "episode": None, "title": "",
    }


# ---------------------------------------------------------------- R2 key mapping
def test_build_s3_key_layout(sandbox):
    assert d.build_s3_key(12345, 1, 3, 2008) == "tv/2008/12345/S01/E03/E03.mp4"
    assert d.build_s3_key("12345", "0", "12", "2008") == (
        "tv/2008/12345/S00/E12/E12.mp4"
    )


def test_build_s3_key_asset_shares_prefix_with_video(sandbox):
    """字幕/meta 与视频必须同前缀：前端与字幕脚本都按同前缀一次列举。"""
    video = d.build_s3_key(12345, 1, 3, 2008)
    meta = d.build_s3_key(12345, 1, 3, 2008, "meta.json")
    sub = d.build_s3_key(12345, 1, 3, 2008, "subs/en.srt")
    prefix = "tv/2008/12345/S01/E03"
    assert video == f"{prefix}/E03.mp4"
    assert meta == f"{prefix}/meta.json"
    assert sub == f"{prefix}/subs/en.srt"


def test_build_s3_key_is_stable_across_days(sandbox, monkeypatch):
    """对象键不含上传日期：同一集重传即幂等覆盖，而不是留下两份。

    这是"字幕脚本能由 (tid, s, e, year) 纯计算出前缀"的前提。
    """
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    first = d.build_s3_key(1, 1, 1, 2008)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260906")
    assert d.build_s3_key(1, 1, 1, 2008) == first


def test_build_s3_key_year_sanitised_and_prefix(sandbox, monkeypatch):
    assert d.build_s3_key(1, 1, 1, " 20/08 ").startswith("tv/2008/1/")
    assert d.build_s3_key(1, 1, 1, None).startswith("tv/unknown_year/1/")
    assert d.build_s3_key(1, 1, 1, "").startswith("tv/unknown_year/1/")
    assert d.build_s3_key(1, 1, 1, "n/a").startswith("tv/unknown_year/1/")
    monkeypatch.setattr(d, "S3_PREFIX", "bucketroot")
    assert d.build_s3_key(1, 1, 1, 2008) == "bucketroot/tv/2008/1/S01/E01/E01.mp4"


@pytest.mark.parametrize("tid,s,e", [
    (None, 1, 1), ("", 1, 1), (1, None, 1), (1, 1, None), (1, "x", 1),
])
def test_build_s3_key_missing_fields(sandbox, tid, s, e):
    with pytest.raises(ValueError):
        d.build_s3_key(tid, s, e, 2008)


def test_local_and_remote_layouts_are_isomorphic(sandbox):
    """本地目录与 R2 对象键必须严格同构，否则两侧无法互相换算。"""
    local = d.episode_dir(12345, 1, 3, 2008)
    rel = d.asset_rel_path(12345, 1, 3, 2008)
    assert rel == "tv/2008/12345/S01/E03"
    assert local == os.path.join(str(sandbox / "downloads"), *rel.split("/"))
    assert d.build_s3_key(12345, 1, 3, 2008, "meta.json") == f"{rel}/meta.json"


# ---------------------------------------------------------------- local layout
def _put_episode(base, year, tid, season, episode, data=b"x"):
    """在新布局下造一个成品：{base}/tv/{year}/{tid}/S{ss}/E{ee}/E{ee}.mp4。"""
    folder = base / "tv" / year / tid / f"S{season:02d}" / f"E{episode:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"E{episode:02d}.mp4"
    path.write_bytes(data)
    return path


def test_scan_downloaded_mp4_ids(sandbox):
    base = sandbox / "downloads"
    _put_episode(base, "2008", "100", 1, 1)
    _put_episode(base, "2008", "100", 1, 2, data=b"")     # empty -> cleaned
    _put_episode(base, "2008", "200", 0, 5)
    # 同一集出现在两个 year 目录下（脏数据）：只报告，不删。
    _put_episode(base, "2009", "100", 1, 1)
    # 旁车资产与非集目录不参与去重判定。
    ep_dir = base / "tv" / "2008" / "100" / "S01" / "E01"
    (ep_dir / "meta.json").write_text("{}", encoding="utf-8")
    (ep_dir / "subs").mkdir()
    (ep_dir / "subs" / "en.srt").write_text("x", encoding="utf-8")
    (base / "tv" / "2008" / "100" / "Extras").mkdir()
    (base / "tv" / "2008" / "notanid").mkdir()

    ids, dups = d.scan_downloaded_mp4_ids()
    assert ids == {"100_S01E01", "200_S00E05"}
    assert set(dups) == {"100_S01E01"}
    assert len(dups["100_S01E01"]) == 2
    # 0 字节孤儿已被清理。
    assert not (
        base / "tv" / "2008" / "100" / "S01" / "E02" / "E02.mp4"
    ).exists()


def test_scan_downloaded_mp4_ids_missing_base(sandbox):
    assert d.scan_downloaded_mp4_ids() == (set(), {})


def test_reconcile_unlogged_downloads_writes_log_and_pending(sandbox, monkeypatch):
    """① 成品落地但未入账（既不在 SUCCESS_LOG 也不在 pending）的集：启动巡检补写
    SUCCESS_LOG(uploaded=false, reconciled=true) + pending，让收尾 reupload 认领。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    base = sandbox / "downloads"
    p_unlogged = _put_episode(base, "2008", "100", 1, 1, data=b"video")
    _put_episode(base, "2008", "200", 0, 5, data=b"video")      # 已入账
    _put_episode(base, "unknown_year", "300", 2, 3, data=b"video")  # 无年份目录

    d.write_log(d.SUCCESS_LOG, {"tmdbId": "200", "season": 0, "episode": 5, "uploaded": True})
    logged = d.load_success_log_ids()
    disk, _ = d.scan_downloaded_mp4_ids()
    assert disk == {"100_S01E01", "200_S00E05", "300_S02E03"}

    assert d.reconcile_unlogged_downloads(disk, logged) == 2

    logs = _read_jsonl(d.SUCCESS_LOG)
    by_key = {d.record_episode_key(r): r for r in logs}
    assert set(by_key) == {"200_S00E05", "100_S01E01", "300_S02E03"}
    r100 = by_key["100_S01E01"]
    assert r100["uploaded"] is False and r100["reconciled"] is True
    assert r100["final_path"] == str(p_unlogged)
    assert r100["year"] == "2008" and r100["s3_key"] == "tv/2008/100/S01/E01/E01.mp4"
    assert r100["provider"] is None       # 归因键齐全、值为 None
    r300 = by_key["300_S02E03"]
    assert r300["year"] is None and r300["s3_key"] == "tv/unknown_year/300/S02/E03/E03.mp4"

    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert {d.record_episode_key(r) for r in pend} == {"100_S01E01", "300_S02E03"}
    p100 = next(r for r in pend if d.record_episode_key(r) == "100_S01E01")
    assert p100["local_path"] == str(p_unlogged)
    assert p100["s3_key"] == r100["s3_key"]
    assert "启动巡检" in p100["fail_reason"]

    # 巡检幂等：补账后 logged 已含这些 key，再跑一次不重复写。
    logged2 = d.load_success_log_ids()
    assert d.reconcile_unlogged_downloads(disk, logged2) == 0
    assert len(_read_jsonl(d.SUCCESS_LOG)) == 3


def test_reconcile_unlogged_downloads_local_mode_no_pending(sandbox, monkeypatch):
    """S3 关闭：成品本就留本地，只补 SUCCESS_LOG 防重下，不写 pending。"""
    monkeypatch.setattr(d, "S3_ENABLED", False)
    base = sandbox / "downloads"
    _put_episode(base, "2008", "100", 1, 1, data=b"video")
    disk, _ = d.scan_downloaded_mp4_ids()
    assert d.reconcile_unlogged_downloads(disk, set()) == 1
    assert len(_read_jsonl(d.SUCCESS_LOG)) == 1
    assert not os.path.exists(d.UPLOAD_PENDING_LOG)


def test_reconcile_unlogged_downloads_skips_missing_and_noop_when_clean(sandbox, monkeypatch):
    """key 在 disk_ids 里但磁盘上已找不到（扫描后被删）→ 跳过不写；无缝隙 → 0。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    assert d.reconcile_unlogged_downloads(set(), set()) == 0
    assert d.reconcile_unlogged_downloads({"100_S01E01", "garbage"}, set()) == 0
    assert not os.path.exists(d.SUCCESS_LOG)
    assert not os.path.exists(d.UPLOAD_PENDING_LOG)


def test_reconciled_pending_is_claimed_by_reupload(sandbox, monkeypatch):
    """端到端：巡检写出的 pending 能被 reupload_pending 正常认领上传并清本地。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    base = sandbox / "downloads"
    p = _put_episode(base, "2008", "100", 1, 1, data=b"video")
    disk, _ = d.scan_downloaded_mp4_ids()
    d.reconcile_unlogged_downloads(disk, set())

    calls = []
    monkeypatch.setattr(d, "upload_to_r2", lambda lp, k: calls.append((lp, k)) or (True, None))
    d.reupload_pending()
    assert calls == [(str(p), "tv/2008/100/S01/E01/E01.mp4")]
    assert not p.exists()
    assert _read_jsonl(d.UPLOAD_PENDING_LOG) == []
    logs = _read_jsonl(d.SUCCESS_LOG)
    assert len(logs) == 1 and logs[0]["uploaded"] is True and logs[0]["reupload"] is True


def test_move_to_target_folder_uses_episode_dir(sandbox):
    base = sandbox / "downloads"
    temp = sandbox / "temp"
    temp.mkdir()

    def mk(name):
        p = temp / name
        p.write_bytes(b"v")
        return str(p)

    p1 = d.move_to_target_folder(mk("a.mp4"), 1, 1, 1, 2008)
    p2 = d.move_to_target_folder(mk("b.mp4"), 1, 1, 2, 2008)
    p3 = d.move_to_target_folder(mk("c.mp4"), 1, 2, 1, 2008)
    assert p1 == str(base / "tv" / "2008" / "1" / "S01" / "E01" / "E01.mp4")
    assert p2 == str(base / "tv" / "2008" / "1" / "S01" / "E02" / "E02.mp4")
    assert p3 == str(base / "tv" / "2008" / "1" / "S02" / "E01" / "E01.mp4")

    # 同一集重复落盘就地覆盖，不额外造目录。
    p1b = d.move_to_target_folder(mk("d.mp4"), 1, 1, 1, 2008)
    assert p1b == p1
    assert os.listdir(base / "tv" / "2008" / "1" / "S01" / "E01") == ["E01.mp4"]
    assert not (temp / "d.mp4").exists()


def test_move_to_target_folder_cleans_partial_on_failure(sandbox, monkeypatch):
    base = sandbox / "downloads"
    temp = sandbox / "temp"
    temp.mkdir()
    src = temp / "a.mp4"
    src.write_bytes(b"v")

    def boom(src_path, dst_path):
        with open(dst_path, "wb") as fh:
            fh.write(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(d.shutil, "move", boom)
    with pytest.raises(OSError):
        d.move_to_target_folder(str(src), 1, 1, 1, 2008)
    assert not (
        base / "tv" / "2008" / "1" / "S01" / "E01" / "E01.mp4"
    ).exists()


def test_clean_temp_directory(sandbox):
    temp = sandbox / "temp"
    temp.mkdir()
    keep = ["other.txt", "temp_x.log", "sample.mp4"]
    drop = ["temp_1_S01E01.ts", "temp_1_S01E01.mp4", "sample_1_S01E01_1080p.ts"]
    for name in keep + drop:
        (temp / name).write_bytes(b"x")
    d.clean_temp_directory()
    assert sorted(os.listdir(temp)) == sorted(keep)


def test_safe_file_token():
    assert d.safe_file_token("12345_S01E03") == "12345_S01E03"
    assert d.safe_file_token("1920x1080") == "1920x1080"
    assert d.safe_file_token("a/b c") == "a_b_c"
    assert d.safe_file_token(None) == "unknown"


# ---------------------------------------------------------------- logs
def test_load_success_log_ids(sandbox):
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": 1, "season": 1, "episode": 1}) + "\n"
        + "not json\n"
        + json.dumps({"tmdbId": 1}) + "\n"                    # movie-style -> ignored
        + json.dumps({"tmdbId": "2", "season": "0", "episode": "3"}) + "\n"
        + "\n",
        encoding="utf-8",
    )
    assert d.load_success_log_ids() == {"1_S01E01", "2_S00E03"}


def test_update_success_log_dedups_by_episode_key(sandbox):
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": 1, "season": 1, "episode": 1, "uploaded": False}) + "\n"
        + json.dumps({"tmdbId": 1, "season": 1, "episode": 2, "uploaded": False}) + "\n"
        + "garbage line\n"
        + json.dumps({"tmdbId": 1, "season": 1, "episode": 1, "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    d.update_success_log("1_S01E01", {
        "tmdbId": 1, "season": 1, "episode": 1, "uploaded": True,
    })
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[2] == "garbage line"
    records = [json.loads(l) for l in lines if l.startswith("{")]
    assert [d.record_episode_key(r) for r in records] == ["1_S01E01", "1_S01E02"]
    assert records[0]["uploaded"] is True
    assert records[1]["uploaded"] is False

    d.update_success_log("9_S02E02", {"tmdbId": 9, "season": 2, "episode": 2})
    assert d.load_success_log_ids() == {"1_S01E01", "1_S01E02", "9_S02E02"}


def test_remove_upload_failure_from_log(sandbox):
    path = sandbox / "failed.jsonl"
    recs = [
        {"tmdbId": 1, "season": 1, "episode": 1, "stage": "upload"},
        {"tmdbId": 1, "season": 1, "episode": 1, "stage": "download"},
        {"tmdbId": 1, "season": 1, "episode": 2, "stage": "upload"},
        {"stage": "preflight", "error": "x"},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in recs) + "bad\n", encoding="utf-8")
    d.remove_upload_failure_from_log("1_S01E01")
    kept = path.read_text(encoding="utf-8").splitlines()
    assert "bad" in kept
    parsed = [json.loads(l) for l in kept if l.startswith("{")]
    assert parsed == recs[1:]

    # No-op when nothing matches (file untouched).
    before = path.read_text(encoding="utf-8")
    d.remove_upload_failure_from_log("nope")
    assert path.read_text(encoding="utf-8") == before


def test_update_success_log_many_single_rewrite(sandbox, monkeypatch):
    """批量版一次重写覆盖多 key、追加新 key、保留坏行；os.replace 只发生一次。"""
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": 1, "season": 1, "episode": 1, "uploaded": False}) + "\n"
        + "garbage line\n"
        + json.dumps({"tmdbId": 1, "season": 1, "episode": 2, "uploaded": False}) + "\n"
        + json.dumps({"tmdbId": 1, "season": 1, "episode": 1, "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    replaces = []
    real_replace = d.os.replace
    monkeypatch.setattr(d.os, "replace", lambda a, b: (replaces.append(b), real_replace(a, b)))
    d.update_success_log_many({
        "1_S01E01": {"tmdbId": 1, "season": 1, "episode": 1, "uploaded": True},
        "1_S01E02": {"tmdbId": 1, "season": 1, "episode": 2, "uploaded": True},
        "9_S02E02": {"tmdbId": 9, "season": 2, "episode": 2, "uploaded": True},
    })
    assert len(replaces) == 1
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[1] == "garbage line"
    recs = [json.loads(l) for l in lines if l.startswith("{")]
    assert [d.record_episode_key(r) for r in recs] == ["1_S01E01", "1_S01E02", "9_S02E02"]
    assert all(r["uploaded"] for r in recs)
    # 空字典不动文件
    d.update_success_log_many({})
    assert len(replaces) == 1


def test_remove_upload_failures_from_log_batch(sandbox, monkeypatch):
    path = sandbox / "failed.jsonl"
    recs = [
        {"tmdbId": 1, "season": 1, "episode": 1, "stage": "upload"},
        {"tmdbId": 1, "season": 1, "episode": 1, "stage": "download"},
        {"tmdbId": 1, "season": 1, "episode": 2, "stage": "upload"},
        {"tmdbId": 1, "season": 1, "episode": 3, "stage": "upload"},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in recs), encoding="utf-8")
    replaces = []
    real_replace = d.os.replace
    monkeypatch.setattr(d.os, "replace", lambda a, b: (replaces.append(b), real_replace(a, b)))
    d.remove_upload_failures_from_log(["1_S01E01", "1_S01E02"])
    assert len(replaces) == 1
    assert _read_jsonl(path) == [recs[1], recs[3]]
    d.remove_upload_failures_from_log([])
    d.remove_upload_failures_from_log(["nope"])
    assert len(replaces) == 1


def test_clean_stale_log_tmp(sandbox, capsys):
    """启动清掉 SUCCESS_LOG/FAILED_LOG/PENDING 的 .tmp 残骸，正式文件不动，缺失不报错。"""
    (sandbox / "success.jsonl").write_text("keep\n", encoding="utf-8")
    (sandbox / "success.jsonl.tmp").write_text("half\n", encoding="utf-8")
    (sandbox / "failed.jsonl.tmp").write_text("half\n", encoding="utf-8")
    (sandbox / "pending.jsonl.tmp").write_text("half\n", encoding="utf-8")
    d.clean_stale_log_tmp()
    assert (sandbox / "success.jsonl").read_text(encoding="utf-8") == "keep\n"
    assert not (sandbox / "success.jsonl.tmp").exists()
    assert not (sandbox / "failed.jsonl.tmp").exists()
    assert not (sandbox / "pending.jsonl.tmp").exists()
    assert capsys.readouterr().out.count("已清理上次强杀") == 3
    d.clean_stale_log_tmp()   # 再跑一次：无残骸、无输出、无异常
    assert capsys.readouterr().out == ""


def test_second_signal_releases_lock_before_force_exit(monkeypatch, capsys):
    """D1：第二次信号不再 SIG_DFL 硬杀——先 release_main_lock 再 os._exit(130)。"""
    events = []
    monkeypatch.setattr(d.signal, "signal", lambda sig, h: events.append(("reg", sig, h)))
    monkeypatch.setattr(d, "release_main_lock", lambda: events.append("release"))
    monkeypatch.setattr(d.os, "_exit", lambda code: events.append(("exit", code)))
    d.interrupted.clear()
    handler, force_exit = d.install_interrupt_handler()
    events.clear()

    with pytest.raises(KeyboardInterrupt):
        handler(d.signal.SIGINT, None)
    assert d.interrupted.is_set()
    # 首次信号把两个信号都切到 _force_exit，而不是 SIG_DFL
    assert [e for e in events if e[0] == "reg"] == [
        ("reg", d.signal.SIGINT, force_exit),
        ("reg", d.signal.SIGTERM, force_exit),
    ]
    events.clear()

    force_exit(d.signal.SIGINT, None)
    assert events == ["release", ("exit", 130)]
    assert "强制退出" in capsys.readouterr().out
    d.interrupted.clear()


def test_write_log_and_pending(sandbox):
    d.write_log(d.FAILED_LOG, {"tmdbId": 1, "season": 1, "episode": 1, "title": "剧"})
    d.write_pending({"tmdbId": 1, "season": 1, "episode": 1})
    assert _read_jsonl(d.FAILED_LOG)[0]["title"] == "剧"
    assert _read_jsonl(d.UPLOAD_PENDING_LOG) == [{"tmdbId": 1, "season": 1, "episode": 1}]
    d.truncate_log(d.FAILED_LOG)
    assert _read_jsonl(d.FAILED_LOG) == []


# ---------------------------------------------------------------- failure classification
@pytest.mark.parametrize("msg,retriable", [
    ("缺少 tmdbId/season/episode 或 urls", False),
    ("没有找到媒体播放列表或清晰度变体", False),
    ("不支持的播放列表结构：含加密分片", False),
    ("没有找到高度达标（≥ 1080×0.80）的流", False),
    ("分辨率 1280x720 低于红线 1080", False),
    ("码率未达到门槛：500 kbps < 900 kbps", False),
    ("服务器返回的不是视频分片: http://x", False),
    ("缺片率过高：缺 30/100 片", True),
    ("本轮候选流无一入选（各流原因见上方日志），下一轮重采", True),
    ("采样探测分辨率失败（可重试）", True),
    ("HTTPSConnectionPool: Read timed out", True),
    ("", True),
    (None, True),
])
def test_classify_failure(msg, retriable):
    assert d._classify_failure(msg) is retriable


def test_permanent_markers_match_movie_version():
    """TV markers must mirror the movie pipeline except for the field-name message."""
    from pathlib import Path

    movie = Path(__file__).resolve().parents[2] / "MovieDownloader" / "download_movies.py"
    src = movie.read_text(encoding="utf-8")
    # mp4 直链相关的 marker 是 TV 侧多源接入（vidlink）独有的，MovieDownloader
    # 目前只有 m3u8 一条路径，故不参与对齐校验。
    tv_only = {
        "需重新取流", "直链块不可用", "直链不支持 Range",
        "服务器未按 Range 响应", "直链总长异常", "直链下载长度不符",
    }
    for marker in d._PERMANENT_FAILURE_MARKERS:
        if marker.startswith("缺少 ") or marker in tv_only:
            continue
        assert marker in src, marker


@pytest.mark.parametrize("msg,category", [
    ("缺少 tmdbId/season/episode 或 urls", "缺少字段/无媒体列表"),
    ("本轮候选流无一入选", "候选流无一入选"),
    ("分辨率 720 低于红线", "分辨率低于红线"),
    ("码率未达到门槛", "码率未达门槛"),
    ("缺片率过高", "正片缺片率过高"),
    ("503 Server Error", "源站5xx"),
    ("Read timed out", "超时"),
    ("SSLError", "SSL/连接错误"),
    ("something else", "其他"),
    ("", "其他"),
])
def test_classify_reject_reason(msg, category):
    assert d.classify_reject_reason(msg) == category


# ---------------------------------------------------------------- playlist parsing
def test_parse_master_playlist(monkeypatch):
    text = (
        "#EXTM3U\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n"
        "\n"
        "1080/index.m3u8\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=1500000\n"
        "720/index.m3u8\n"
    )
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    variants = d.parse_master_playlist("https://cdn/x/master.m3u8")
    assert variants == [
        ("1920x1080", "https://cdn/x/1080/index.m3u8", 5000.0),
        ("unknown", "https://cdn/x/720/index.m3u8", 1500.0),
    ]


def test_parse_master_playlist_attribute_order_and_quoted_codecs(monkeypatch):
    # First attribute follows ":" not ","; CODECS contains commas inside quotes.
    text = (
        "#EXTM3U\n"
        '#EXT-X-STREAM-INF:RESOLUTION=1280x720,CODECS="avc1.4d401f,mp4a.40.2",BANDWIDTH=2500000\n'
        "720/index.m3u8\n"
        '#EXT-X-STREAM-INF:CODECS="avc1.4d401f,mp4a.40.2",BANDWIDTH=800000,RESOLUTION=640x360\n'
        "360/index.m3u8\n"
    )
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    variants = d.parse_master_playlist("https://cdn/x/master.m3u8")
    assert variants == [
        ("1280x720", "https://cdn/x/720/index.m3u8", 2500.0),
        ("640x360", "https://cdn/x/360/index.m3u8", 800.0),
    ]


def test_parse_master_playlist_media_fallback(monkeypatch):
    text = "#EXTM3U\n#EXTINF:4.0,\nseg0.ts\n"
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    assert d.parse_master_playlist("https://cdn/x/index.m3u8") == [
        ("unknown", "https://cdn/x/index.m3u8", 0.0)
    ]


def test_parse_media_playlist_ts(monkeypatch):
    text = "#EXTM3U\n#EXTINF:4.0,\nseg0.ts\n#EXTINF:3.5,\nseg1.ts\n#EXT-X-ENDLIST\n"
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    urls, durations, init = d.parse_media_playlist("https://cdn/x/index.m3u8")
    assert urls == ["https://cdn/x/seg0.ts", "https://cdn/x/seg1.ts"]
    assert durations == [4.0, 3.5]
    assert init is None


def test_playlist_parsers_forward_node_headers(monkeypatch):
    """取流阶段记录的节点专属请求头必须透传到 m3u8 两级 playlist 请求。

    某些源站没有正确的 Referer/UA 会直接 403/428，丢头等于白白损失一个可用源。
    """
    seen = []

    def fake_request(method, url, **kwargs):
        seen.append(kwargs.get("headers"))
        return "#EXTM3U\n#EXTINF:4.0,\nseg0.ts\n"

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    node_headers = {"Referer": "https://src/"}
    d.parse_master_playlist("https://cdn/x/i.m3u8", headers=node_headers)
    d.parse_media_playlist("https://cdn/x/i.m3u8", headers=node_headers)
    assert seen == [node_headers, node_headers]


def test_download_segments_forwards_headers(sandbox, monkeypatch):
    """分片下载同样要带节点头，否则正片阶段会整片 403。"""
    seen = []

    def fake_single(url, index, retry_max, delay, headers=None):
        seen.append(headers)
        return b"x"

    monkeypatch.setattr(d, "download_single_segment", fake_single)
    out = os.path.join(str(sandbox), "o.ts")
    node_headers = {"User-Agent": "okhttp/4.9.3"}
    d.download_segments(
        ["u0", "u1"], out, init_url="i", headers=node_headers, concurrency=1
    )
    # init 段 + 2 个媒体分片都带上了节点头
    assert seen == [node_headers] * 3


def test_parse_media_playlist_fmp4_init(monkeypatch):
    text = '#EXTM3U\n#EXT-X-MAP:URI="init.mp4"\n#EXTINF:4.0,\nseg0.m4s\n'
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    urls, _, init = d.parse_media_playlist("https://cdn/x/index.m3u8")
    assert init == "https://cdn/x/init.mp4"
    assert urls == ["https://cdn/x/seg0.m4s"]


@pytest.mark.parametrize("text", [
    '#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="k"\n#EXTINF:4,\ns.ts\n',
    "#EXTM3U\n#EXT-X-BYTERANGE:100@0\n#EXTINF:4,\ns.ts\n",
    '#EXTM3U\n#EXT-X-MAP:URI="i.mp4",BYTERANGE="1@0"\n#EXTINF:4,\ns.m4s\n',
    "#EXTM3U\n#EXT-X-MAP:FOO=1\n#EXTINF:4,\ns.m4s\n",
])
def test_parse_media_playlist_unsupported(monkeypatch, text):
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    with pytest.raises(d.UnsupportedPlaylistError):
        d.parse_media_playlist("https://cdn/x/index.m3u8")


def test_parse_media_playlist_key_none_allowed(monkeypatch):
    text = "#EXTM3U\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:4,\ns.ts\n"
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: text)
    urls, _, _ = d.parse_media_playlist("https://cdn/x/index.m3u8")
    assert len(urls) == 1


def test_parse_media_playlist_empty(monkeypatch):
    monkeypatch.setattr(d, "request_with_retry", lambda *a, **k: "#EXTM3U\n")
    with pytest.raises(RuntimeError):
        d.parse_media_playlist("https://cdn/x/index.m3u8")


# ---------------------------------------------------------------- quality gates
def test_parse_resolution():
    assert d.parse_resolution("1920x1080") == (1920, 1080)
    assert d.parse_resolution("1920X1080") == (1920, 1080)
    assert d.parse_resolution("unknown") is None
    assert d.parse_resolution("") is None
    assert d.parse_resolution("abc") is None


def test_bitrate_threshold_scaling(monkeypatch):
    """模式 A：门槛按 (h/1080)² 随流高度缩放。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 2000.0, "hevc": 1000.0})
    assert d.bitrate_threshold(1080, "h264") == pytest.approx(2000.0)
    assert d.bitrate_threshold(540, "h264") == pytest.approx(500.0)
    assert d.bitrate_threshold(1080, "hevc") == pytest.approx(1000.0)
    assert d.bitrate_threshold(1080, "unknown-codec") == pytest.approx(2000.0)
    assert d.bitrate_threshold(1080, None) == pytest.approx(2000.0)


def test_bitrate_threshold_is_absolute_when_resolution_check_disabled(monkeypatch):
    """模式 B（默认）：绝对码率线，**不乘 (h/1080)²**。

    🔴 这是模式 B 的核心不变量。若保留缩放，480p 的门槛会被缩到约五分之一，
    低分辨率流反而更容易过关——等于把分辨率以更隐蔽的方式又请了回来，
    与"只按码率判断"的业务意图正好相反。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "LENIENCY", 0.8)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 2000.0, "hevc": 1189.0})
    # 任何高度都是同一条线：2000 × 0.8 = 1600。
    for height in (2160, 1080, 720, 480, 0):
        assert d.bitrate_threshold(height, "h264") == pytest.approx(1600.0)
    # 编码分档仍然生效（只是不再随高度缩放）。
    assert d.bitrate_threshold(480, "hevc") == pytest.approx(1189.0 * 0.8)
    # 探测不到编码回退 H.264 基准（最严）。
    assert d.bitrate_threshold(480, None) == pytest.approx(1600.0)


def test_meets_resolution_redline(monkeypatch):
    """模式 A：红线带 LENIENCY 容差。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 0.8)
    assert d.meets_resolution_redline(864)
    assert not d.meets_resolution_redline(863)


def test_meets_resolution_redline_always_passes_when_check_disabled(monkeypatch):
    """模式 B（默认）：红线整关放行。

    开关刻意收敛在这一个函数里——四处红线关卡（mp4 声明预检 / mp4 样本预检 /
    mp4 实测复检 / m3u8 流层）与 master 候选过滤都靠它自动放行，
    不必在每个调用点各写一次 if，也就不会漏掉某一处。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 0.8)
    for height in (0, 1, 240, 480, 863, 864, 2160):
        assert d.meets_resolution_redline(height)


def test_bitrate_reject_message_wording_per_mode(monkeypatch):
    """两种模式的淘汰文案不同，但都必须保留 `码率未达到门槛` 这段。

    那段是 `_PERMANENT_FAILURE_MARKERS` / `_REJECT_REASON_RULES` /
    `_DEAD_BITRATE_RE` 三处的共同锚点，改掉会同时打断判死、归类与复判。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    mode_a = d.bitrate_reject_message("854x480", "h264", 372, 1600)
    assert mode_a.startswith("分辨率 854x480 流（h264）")
    assert "码率未达到门槛" in mode_a

    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    mode_b = d.bitrate_reject_message("854x480", "h264", 372, 1600)
    # 模式 B 下分辨率没参与判定，不能写在开头误导排查。
    assert not mode_b.startswith("分辨率")
    assert mode_b.startswith("码率未达到门槛")
    assert "实测 854x480" in mode_b
    # 两种文案都要能被判死类目与证据解析识别。
    for msg in (mode_a, mode_b):
        assert d.classify_reject_reason(msg) == "码率未达门槛"
        evidence = d.parse_dead_quality_evidence(msg)
        assert evidence["bitrate_kbps"] == pytest.approx(372.0)
        assert evidence["threshold_kbps"] == pytest.approx(1600.0)
        assert evidence["resolution"] == "854x480"
        assert evidence["codec"] == "h264"


def test_validate_segment_content():
    d.validate_segment_content(b"\x47binary", "u")
    for bad in (b"", b"  <!DOCTYPE html>", b"<html>", b"#EXTM3U"):
        with pytest.raises(RuntimeError):
            d.validate_segment_content(bad, "u")


# ---------------------------------------------------------------- process_one_entry guards
def test_process_one_entry_rejects_missing_key(sandbox):
    for entry in (
        {"tmdbId": 1, "season": 1, "urls": ["u"]},
        {"tmdbId": 1, "episode": 1, "urls": ["u"]},
        {"season": 1, "episode": 1, "urls": ["u"]},
        {"tmdbId": 1, "season": 1, "episode": 1, "urls": []},
        {"tmdbId": 1, "season": 1, "episode": 1},
    ):
        label, ok, info = d.process_one_entry(entry, set())
        assert ok is False
        assert info["retriable"] is False
        assert d._classify_failure(info["error"]) is False
    assert d.processing_ids == set()


def test_process_one_entry_label_falls_back_to_tmdb_id(sandbox):
    label, ok, _ = d.process_one_entry({"tmdbId": 77, "urls": ["u"]}, set())
    assert label == "77"


def test_process_one_entry_skips_processed_and_in_flight(sandbox):
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": ["u"]}
    label, ok, info = d.process_one_entry(entry, {"1_S01E01"})
    assert (label, ok, info["error"]) == ("1_S01E01", False, "already processed successfully")

    d.processing_ids.add("1_S01E01")
    _, ok, info = d.process_one_entry(entry, set())
    assert info["error"] == "duplicate entry currently processing"
    assert "1_S01E01" in d.processing_ids  # not released by the duplicate path


def test_process_one_entry_node_fallback_and_cleanup(sandbox, monkeypatch):
    """Two nodes: first raises a retriable error, second a permanent one -> still retriable."""
    calls = []

    def fake_master(url, retries=None, headers=None):
        calls.append((url, retries))
        if url == "u1":
            raise RuntimeError("Read timed out")
        raise RuntimeError("没有找到高度达标的流")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    entry = {"tmdbId": 1, "season": 2, "episode": 3, "urls": ["u1", "u2"], "title": "t"}
    label, ok, info = d.process_one_entry(entry, set())
    assert label == "1_S02E03"
    assert ok is False
    assert info["retriable"] is True
    assert calls == [("u1", d.PLAYLIST_RETRY_FALLBACK), ("u2", None)]
    assert d.processing_ids == set()
    assert not os.path.exists(os.path.join(d.TEMP_DIR, "temp_1_S02E03.ts"))


def test_process_one_entry_permanent_only(sandbox, monkeypatch):
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: (_ for _ in ()).throw(
            RuntimeError("没有找到媒体播放列表")
        ),
    )
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": ["u"]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False and info["retriable"] is False


def test_process_one_entry_happy_path_builds_job(sandbox, monkeypatch):
    """Drive the full selection path with fakes: one 1080p variant, good bitrate."""
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]
    durations = [4.0] * 20

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: [
            ("1920x1080", "https://cdn/i.m3u8", 5000.0)
        ],
    )
    monkeypatch.setattr(
        d, "parse_media_playlist",
        lambda url, headers=None: (seg_urls, durations, None),
    )
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        # 1 MB per 4 s segment => 2000 kbps, above 1000 threshold
        return n * 1_000_000, [3] if start_idx == 0 else [], 0

    monkeypatch.setattr(d, "download_segments", fake_download)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"],
             "title": "Show", "year": 2010,
             "captions": [{"language": "en", "url": "http://s/en.vtt"}]}
    label, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert label == "55_S01E02"
    assert job["normalized_id"] == "55_S01E02"
    assert (job["tmdbId"], job["season"], job["episode"], job["year"]) == ("55", 1, 2, 2010)
    assert job["resolution"] == "1920x1080"
    assert job["missing_segment_indices"] == [3]
    assert job["final_ts"].endswith("temp_55_S01E02.ts")
    assert job["temp_mp4"].endswith("temp_55_S01E02.mp4")
    # 内嵌字幕要一路带到转封装阶段，否则 finalize 拿不到、字幕静默丢失。
    assert job["captions"] == [{"language": "en", "url": "http://s/en.vtt"}]
    # Sample file removed, ID lock retained for the conversion stage.
    assert not any(n.startswith("sample_") for n in os.listdir(d.TEMP_DIR))
    assert "55_S01E02" in d.processing_ids


def _z1_env(monkeypatch, variants, resolution_check=True):
    """多 variant 采样场景的公共桩：返回记录被采样流 url 的 list。

    resolution_check 默认 True（模式 A）——Z1 提前终止采样、按声明高度排序、
    高度优先择优这一整套都**只在模式 A 下成立**，故这些用例必须显式声明模式，
    不能依赖 config 的当前默认值（默认已是模式 B）。
    """
    sampled = []
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", resolution_check)
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: variants,
    )

    def fake_media(url, headers=None):
        sampled.append(url)
        return seg_urls, [4.0] * 20, None

    monkeypatch.setattr(d, "parse_media_playlist", fake_media)
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    # 红线压到 360：否则 720/480 会在候选过滤阶段（声明高度低于红线）就被剔除，
    # 根本进不了采样循环，测不出"选中后跳过更低流"这条新逻辑。
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 360)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        return n * 1_000_000, [], 0   # 2000 kbps，稳过 1000 门槛

    monkeypatch.setattr(d, "download_segments", fake_download)
    return sampled


def test_sampling_uses_tiered_retry_budget(sandbox, monkeypatch):
    """采样用 SAMPLE_SEG_RETRY_MAX，正片仍用 SEG_RETRY_MAX（默认值）。

    采样只是为择优探码率，探不到就该快速换下一条候选流；正片才需要死磕。
    两者共用同一个预算会让一条烂流把下载窗口占满，直接拖垮成功率。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    monkeypatch.setattr(d, "SAMPLE_SEG_RETRY_MAX", 3)
    budgets = []
    _z1_env(monkeypatch, [("1920x1080", "https://cdn/1080.m3u8", 5000.0)])
    inner = d.download_segments

    def spy(*args, **kwargs):
        budgets.append(kwargs.get("retry_max"))
        return inner(*args, **kwargs)

    monkeypatch.setattr(d, "download_segments", spy)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, _job = d.process_one_entry(entry, set())
    assert ok is True
    # 第一次是采样（分层预算），第二次是正片（None -> 回落 SEG_RETRY_MAX）
    assert budgets == [3, None]


# ------------------------------------------------- 重试分层（L1 交给 L3）
# 2026-09-14 与电影侧对齐。urllib3(L1) 原本 status_forcelist=(429,500,502,
# 503,504) + status=2，会对 5xx **静默重试 2 次**且对上层完全透明——L3 打印
# "分片 X 下载失败 (1/20)" 时底层其实已发了 3 个请求。单个采样分片最坏
# 3(L3)×3(L1)=9 个请求，日志只显示 3 次。
# 现在状态码重试全部收归 L3。这组用例锁死"职责转移而非取消重试"。


def test_l1_does_not_retry_status_codes():
    """L1 不得再对状态码重试——否则与 L3 叠乘且完全静默。

    对 TV 侧还有一层额外代价：L1 的退避叫不醒（不响应 interrupted），
    dead_streak_breaker 熔断与 Ctrl+C 都打不断它。
    """
    session = d.get_session()
    retry = session.get_adapter("https://example.com").max_retries
    assert tuple(retry.status_forcelist or ()) == (), \
        "status_forcelist 必须为空，5xx/429 交给 L3"
    assert retry.status == 0, "status 重试次数必须为 0"


def test_l1_still_retries_connection_level():
    """连接级/读取级重试要保留：socket 抖动在同一条连接上立即重试很划算，
    且不像 5xx 那样会被上层重复覆盖。"""
    session = d.get_session()
    retry = session.get_adapter("https://example.com").max_retries
    assert retry.connect == 2
    assert retry.read == 2


def test_l3_still_retries_502(monkeypatch):
    """🔴 关键：502 的重试**没有消失**，只是从 L1 移到了 L3。

    若 L3 也不重试，源站几秒级抽风会直接判掉整集——那才是真的降成功率。
    """
    calls = []

    def flaky(method, url, **kw):
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("502 Server Error: Bad Gateway")
        return b"\x47" + b"x" * 100      # 第 3 次成功

    monkeypatch.setattr(d, "request_with_retry", flaky)
    monkeypatch.setattr(d, "validate_segment_content", lambda c, u: None)

    content = d.download_single_segment(
        "https://x/seg.ts", 0, retry_max=5, delay=0.001
    )
    assert content is not None
    assert len(calls) == 3, "L3 必须继续重试 502 直到成功"


def test_sample_retry_budget_matches_movie_side():
    """采样预算 5：L1 清空后一次循环只发 1 个请求，需靠循环次数补回容错。

    改前的 3 是在 L1 会静默重试 2 次（一次循环 ≈ 3 个请求）的前提下定的，
    现在那个前提没了，仍留 3 就成了"真的只试 3 次"，容错反而变弱。
    5 × 1 = 5 个请求，仍低于改动前的 9。
    """
    assert d.SAMPLE_SEG_RETRY_MAX == 5
    # 与正片预算保持分层，绝不能被拉平
    assert d.SAMPLE_SEG_RETRY_MAX < d.SEG_RETRY_MAX


def test_sample_retry_fallback_matches_config_default():
    """🔴 代码里的兜底值必须与 config.yaml 的默认值一致。

    上面那条用例读的是 config.yaml 的实际取值，**测不到 .get 的兜底参数**。
    两者不一致时，谁没带配置文件跑就会拿到另一套行为，且悄无声息
    （§0.18 已因同类问题吃过亏：配置与代码兜底各写各的）。
    """
    import inspect
    import re

    source = inspect.getsource(d)
    match = re.search(r'_CFG\.get\("sample_seg_retry_max",\s*(\d+)\)', source)
    assert match, "没找到 sample_seg_retry_max 的兜底取值"
    assert int(match.group(1)) == 5, "代码兜底值必须与 config.yaml 保持一致"


def test_variant_sampling_stops_after_higher_stream_wins(sandbox, monkeypatch):
    """选中 1080 后，声明高度更低的流不再采样。

    候选已按声明高度降序排，且有声明分辨率的流直接采信声明值（不做 ffprobe），
    择优又是"高度绝对优先"——更低的流即便采样也必然落选，那次"解析 media
    playlist + 下载 N 个分片 + 两次 ffprobe"是纯浪费。一个 master 常有
    1080/720/480/360 四档，白花的是三份采样流量。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
        ("854x480", "https://cdn/480.m3u8", 1500.0),
    ])

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert job["resolution"] == "1920x1080"
    assert sampled == ["https://cdn/1080.m3u8"]


def test_variant_sampling_still_compares_same_height(sandbox, monkeypatch):
    """同声明高度的流必须全部采样——要比采样码率才能择优。"""
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/a.m3u8", 5000.0),
        ("1920x1080", "https://cdn/b.m3u8", 4000.0),
        ("1280x720", "https://cdn/c.m3u8", 3000.0),
    ])

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, _job = d.process_one_entry(entry, set())
    assert ok is True
    # 两个 1080 都采样，720 被跳过
    assert sampled == ["https://cdn/a.m3u8", "https://cdn/b.m3u8"]


def test_variant_sampling_still_probes_undeclared(sandbox, monkeypatch):
    """未声明分辨率的流不能跳过：真实高度可能更高，必须采样后 ffprobe。

    master 无 RESOLUTION 属性时这类流被排在末尾（用 -1 排序），若按"声明高度
    更低"一并跳过，就会把实际更清晰的流丢掉，直接违背画质择优目标。
    """
    # 未声明的那条实测为 2160p，应当胜出
    monkeypatch.setattr(d, "probe_resolution", lambda p: (3840, 2160))
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        (None, "https://cdn/unknown.m3u8", 4000.0),
    ])
    # 门槛按 (h/1080)² 缩放，2160p 需 4 倍基准；压低基准让桩数据能过关，
    # 本用例要验的是采样顺序而非码率曲线。
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 100.0})

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert sampled == ["https://cdn/1080.m3u8", "https://cdn/unknown.m3u8"]
    assert job["resolution"] == "3840x2160"


def test_variant_sampling_continues_after_failure(sandbox, monkeypatch):
    """最高档采样失败（未选中）时，后续较低流仍要采样。

    跳过条件绑定 best_selected：只有真正选中过某流才生效。否则瞬时抖动让最高
    档挂掉后，整集会因"无一入选"而白白失败，直接损失成功率。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
    ])
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    def flaky_media(url, headers=None):
        sampled.append(url)
        if "1080" in url:
            raise RuntimeError("采样抖动")
        return seg_urls, [4.0] * 20, None

    monkeypatch.setattr(d, "parse_media_playlist", flaky_media)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert sampled == ["https://cdn/1080.m3u8", "https://cdn/720.m3u8"]
    assert job["resolution"] == "1280x720"


def test_mode_b_samples_every_stream_without_height_pruning(sandbox, monkeypatch):
    """模式 B：Z1 提前终止采样必须**自动关闭**，所有候选流都要采样。

    🔴 该剪枝的正确性完全建立在"择优按高度绝对优先"之上：模式 A 下声明高度更低
    的流必然落选，跳过它纯属省钱。而模式 B 择优改成纯比码率，**声明高度低不代表
    实测码率低**——一条 720p 高码率流完全可能胜过 1080p 糊流。此时按高度剪枝
    会真的把更优的流丢掉，直接违背"只按码率判断"的业务意图。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
        ("854x480", "https://cdn/480.m3u8", 1500.0),
    ], resolution_check=False)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, _job = d.process_one_entry(entry, set())
    assert ok is True
    # 模式 A 下只会采样 1080 这一条（见上面的 Z1 用例）；模式 B 下三条都要采。
    assert sampled == [
        "https://cdn/1080.m3u8",
        "https://cdn/720.m3u8",
        "https://cdn/480.m3u8",
    ]


def test_mode_b_picks_highest_bitrate_not_highest_resolution(sandbox, monkeypatch):
    """模式 B：择优纯比采样码率，低分辨率的高码率流应当胜出。

    这是业务语义"不再考虑分辨率"最直接的体现。若择优仍按高度优先，
    分辨率就还在暗中主导结果，开关等于没生效。
    """
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]
    # 1080 那条码率低（每段 0.5MB），720 那条码率高（每段 2MB）。
    per_stream_bytes = {"1080": 500_000, "720": 2_000_000}
    current = {"name": None}

    sampled = _z1_env(monkeypatch, [
        ("1920x1080", "https://cdn/1080.m3u8", 5000.0),
        ("1280x720", "https://cdn/720.m3u8", 3000.0),
    ], resolution_check=False)
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)

    def fake_media(url, headers=None):
        sampled.append(url)
        current["name"] = "1080" if "1080" in url else "720"
        return seg_urls, [4.0] * 20, None

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if end_idx is None:
            end_idx = len(urls)
        n = end_idx - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        return n * per_stream_bytes[current["name"]], [], 0

    monkeypatch.setattr(d, "parse_media_playlist", fake_media)
    monkeypatch.setattr(d, "download_segments", fake_download)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    # 720p 的采样码率 4000 kbps > 1080p 的 1000 kbps，故应选中 720p。
    assert job["resolution"] == "1280x720"


def test_mode_b_keeps_stream_when_resolution_probe_fails(sandbox, monkeypatch):
    """模式 B：未声明分辨率且 ffprobe 探测失败时**不再判失败**。

    分辨率既然不参与判定，就不该因为探不到它而丢掉一条采样已经下好、
    码率完全算得出来的流。模式 A 下这里必须判可重试（分辨率是判定标准）。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    _z1_env(monkeypatch, [
        (None, "https://cdn/unknown.m3u8", 4000.0),
    ], resolution_check=False)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert job["resolution"] == d.UNKNOWN_RESOLUTION


def test_mode_a_still_fails_when_resolution_probe_fails(sandbox, monkeypatch):
    """对照组：模式 A 下探不到分辨率仍判**可重试**失败（分辨率是判定标准）。

    没有这个对照，上一条用例无法证明差异来自模式开关。
    """
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    _z1_env(monkeypatch, [
        (None, "https://cdn/unknown.m3u8", 4000.0),
    ], resolution_check=True)

    entry = {"tmdbId": "55", "season": 1, "episode": 2, "urls": ["u"]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert info["retriable"] is True


# ---------------------------------------------------------------- multi-source: url entries / mp4 direct
def test_normalize_url_entry_str_and_dict():
    assert d._normalize_url_entry("  https://a/m.m3u8 ") == {
        "url": "https://a/m.m3u8", "provider": "vidup", "type": "m3u8",
        "headers": {}, "quality": None, "size": None,
    }
    node = d._normalize_url_entry({
        "url": "https://cdn/f.mp4", "provider": "vidlink", "type": "MP4",
        "headers": {"User-Agent": "okhttp/4.9.3", "X": None},
        "quality": "1080", "size": "123",
    })
    assert node == {
        "url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4",
        "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": 123,
    }
    # 缺 type/headers 时补默认；非法条目返回 None
    assert d._normalize_url_entry({"url": "u"})["type"] == "m3u8"
    assert d._normalize_url_entry({"url": "u", "headers": "bad"})["headers"] == {}
    for bad in ("", "  ", None, 5, {}, {"url": ""}, {"url": "u", "type": "dash"}):
        assert d._normalize_url_entry(bad) is None


def test_mp4_request_headers_drop_referer_and_xhr():
    headers = d._mp4_request_headers({"User-Agent": "okhttp/4.9.3"})
    assert headers == {
        "Referer": None, "X-Requested-With": None, "User-Agent": "okhttp/4.9.3",
    }
    # 条目显式给 Referer 时以条目为准
    assert d._mp4_request_headers({"Referer": "https://x/"})["Referer"] == "https://x/"


def test_process_one_entry_dict_urls_use_m3u8_path(sandbox, monkeypatch):
    calls = []

    def fake_master(url, retries=None, headers=None):
        calls.append((url, retries))
        raise RuntimeError("Read timed out")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    entry = {
        "tmdbId": 1, "season": 2, "episode": 3,
        "urls": [
            {"url": "u1", "provider": "vidup", "type": "m3u8"},
            "u2",
            {"url": "", "provider": "broken"},  # 非法条目被跳过
        ],
    }
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False and info["retriable"] is True
    assert calls == [("u1", d.PLAYLIST_RETRY_FALLBACK), ("u2", None)]


def test_process_one_entry_mp4_branch_builds_job(sandbox, monkeypatch):
    seen = {}

    def fake_direct(node, output_path, label, runtime_minutes=None):
        seen["node"] = node
        seen["runtime"] = runtime_minutes
        with open(output_path, "wb") as fh:
            fh.write(b"x")
        return "1920x1080", 4321.4

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: pytest.fail(
            "mp4 节点不应走 playlist 解析"
        ),
    )
    node = {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4",
            "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": 1}
    entry = {"tmdbId": "9", "season": 1, "episode": 1, "urls": [node],
             "title": "Show", "year": 2011, "runtime_minutes": 45,
             "captions": [{"language": "zh", "url": "http://s/zh.srt"}]}
    label, ok, job = d.process_one_entry(entry, set())
    assert ok is True and label == "9_S01E01"
    assert seen["node"]["url"] == node["url"] and seen["runtime"] == 45
    assert job["url"] == "https://cdn/f.mp4"  # str，保持 finalize/success_info 契约
    assert job["resolution"] == "1920x1080" and job["bitrate_kbps"] == 4321
    assert job["missing_segment_count"] == 0 and job["missing_segment_indices"] == []
    assert job["final_ts"].endswith("temp_9_S01E01.ts")
    # 内嵌字幕要一路带到转封装阶段（直链分支与分片分支各构造一次 job，别漏）。
    assert job["captions"] == [{"language": "zh", "url": "http://s/zh.srt"}]
    assert "9_S01E01" in d.processing_ids


def test_conversion_job_carries_provider_attribution(sandbox, monkeypatch):
    """🔑 成功的 job 必须带上是哪家 provider、第几个节点下成的。

    没有这几个字段，多节点 fallback 的全部价值都无法量化——成品里看不出
    哪家救回来的，也就无从判断某个源该留该撤（§0.12 待复验 vidfast 去留）。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(
        d, "_download_mp4_direct",
        lambda node, out, label, runtime_minutes=None: ("1920x1080", 4000),
    )
    entry = {"tmdbId": 9, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
    ]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    assert job["provider"] == "vidlink" and job["node_type"] == "mp4"
    assert job["node_index"] == 1 and job["node_total"] == 1


def test_attribution_records_which_node_won_the_fallback(sandbox, monkeypatch):
    """首节点挂掉、次节点救回时，node_index 必须是 2。

    node_index > 1 正是"多源到底有没有用"最直接的证据：它说明这一集
    **只靠 fallback 才拿到**，是判断某个源价值的核心信号。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    def fake_direct(node, output_path, label, runtime_minutes=None):
        raise RuntimeError("直链块不可用（HTTP 404）")

    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: [("1920x1080", "https://m/v", 5000.0)],
    )
    monkeypatch.setattr(
        d, "parse_media_playlist",
        lambda url, headers=None: ([f"https://s/{i}.ts" for i in range(8)],
                                   [4.0] * 8, None),
    )
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1920, 1080))
    monkeypatch.setattr(d, "SAMPLE_COUNT", 2)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 100.0})

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        n = (len(urls) if end_idx is None else end_idx) - start_idx
        with open(out, "wb") as fh:
            fh.write(b"x" * n)
        return n * 1_000_000, [], 0

    monkeypatch.setattr(d, "download_segments", fake_download)
    entry = {"tmdbId": 9, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
        {"url": "https://m/master.m3u8", "provider": "vidfast", "type": "m3u8"},
    ]}
    _, ok, job = d.process_one_entry(entry, set())
    assert ok is True
    # 第二个节点才成功 —— 这一集是被 vidfast 靠 fallback 救回来的
    assert job["provider"] == "vidfast" and job["node_type"] == "m3u8"
    assert job["node_index"] == 2 and job["node_total"] == 2


def test_attribution_of_extracts_all_keys():
    """归因字段集中定义，缺键补 None（reupload 等路径可能没有这些键）。"""
    got = d._attribution_of({
        "provider": "vidup", "node_type": "m3u8",
        "node_index": 1, "node_total": 3, "irrelevant": "x",
    })
    assert got == {
        "provider": "vidup", "node_type": "m3u8",
        "node_index": 1, "node_total": 3,
    }
    assert d._attribution_of({}) == {
        "provider": None, "node_type": None,
        "node_index": None, "node_total": None,
    }
    assert d._attribution_of(None)["provider"] is None


def test_process_one_entry_mp4_then_m3u8_fallback(sandbox, monkeypatch):
    """mp4 直链 403 过期属确定性失败；切到备用 m3u8 节点，残留 ts 被清理。

    末节点是瞬时错误 -> 整集仍判可重试（any_retriable 不被首节点的确定性失败连坐）。
    """
    calls = []

    def fake_direct(node, output_path, label, runtime_minutes=None):
        with open(output_path, "wb") as fh:
            fh.write(b"partial")
        raise RuntimeError(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: x")

    def fake_master(url, retries=None, headers=None):
        calls.append((url, retries))
        raise RuntimeError("Read timed out")

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
        {"url": "https://v/m.m3u8", "provider": "vidfast", "type": "m3u8"},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert info["retriable"] is True
    assert calls == [("https://v/m.m3u8", None)]
    assert not os.path.exists(os.path.join(d.TEMP_DIR, "temp_1_S01E01.ts"))
    assert d.processing_ids == set()


def test_process_one_entry_mp4_expired_only_is_permanent(sandbox, monkeypatch):
    """唯一节点是过期直链时整集判确定性失败：本脚本无法重新取流，重投同一条 url 必然再挂。"""
    def fake_direct(node, output_path, label, runtime_minutes=None):
        raise RuntimeError(f"直链已失效（HTTP 410），{d._NEEDS_REFETCH_MARKER}: x")

    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False and info["retriable"] is False


def test_refetch_marker_is_permanent_and_classified():
    """403/410 过期直链：判死（交由上游重跑取流），但统计上单列一类便于观测。"""
    msg = f"直链已失效（HTTP 410），{d._NEEDS_REFETCH_MARKER}: u"
    assert d._classify_failure(msg) is False
    assert d.classify_reject_reason(msg) == "直链失效需重新取流"


@pytest.mark.parametrize("msg,category", [
    ("直链块不可用（HTTP 404）: u", "直链块不可用(404/416)"),
    ("直链不支持 Range 分块下载: u", "直链不支持Range"),
    ("服务器未按 Range 响应（HTTP 200）", "直链不支持Range"),
    ("直链总长异常(0): u", "直链总长异常"),
    ("直链下载长度不符：1 != 2", "直链总长异常"),
])
def test_mp4_failures_are_permanent_and_classified(msg, category):
    """mp4 直链的确定性失败：同一条 url 重下必然复现，不该占用下一轮下载槽位。"""
    assert d._classify_failure(msg) is False
    assert d.classify_reject_reason(msg) == category


def test_mp4_probe_failure_stays_retriable():
    """探测失败文案同时覆盖网络异常与"无法确定总长"两种来源，无法区分。

    按"宁可多试一轮也不误杀"的取向保持可重试；统计上单列"直链探测失败"类目，
    便于跑完后从占比判断到底是源站抖动还是直链本身不可用。
    """
    for msg in ("直链探测失败: u; Read timed out",
                "直链探测失败：无法确定文件总长: u"):
        assert d._classify_failure(msg) is True
        assert d.classify_reject_reason(msg) == "直链探测失败"


# ---------------------------------------------------------------- retry shortcut
@pytest.mark.parametrize("status,permanent", [
    (401, True), (403, True), (404, True), (410, True), (416, True),
    (429, False), (500, False), (502, False), (503, False), (504, False),
])
def test_is_permanent_http_failure_by_status(status, permanent):
    """429/5xx 是限流/临时故障，退避后有机会成功，必须继续重试；
    401/403/404/410/416 重试必然复现，应立刻短路去换下一个节点。"""
    exc = d.requests.HTTPError("boom", response=_FakeResp(status))
    assert d.is_permanent_http_failure(exc) is permanent


def test_download_single_segment_skips_retry_on_permanent_status(sandbox, monkeypatch):
    """分片层遇 404 立即上抛，不再走 20 次退避（否则白等十几分钟占死下载窗口）。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 20)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise d.requests.HTTPError("boom", response=_FakeResp(404))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    with pytest.raises(RuntimeError, match="重试 20 次后仍失败"):
        d.download_single_segment("u", 0, 20, 1)
    assert len(calls) == 1 and sleeps == []


def test_download_single_segment_still_retries_transient(sandbox, monkeypatch):
    """瞬时错误（503）仍要重试满次数——这才是重试真正能救回来的场景。

    退避走 `interrupted.wait()` 而非裸 sleep，所以这里桩掉整个事件来记录等待。
    """
    sleeps = []
    monkeypatch.setattr(d, "interrupted", _FakeInterrupt(sleeps))
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise d.requests.HTTPError("boom", response=_FakeResp(503))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    with pytest.raises(RuntimeError):
        d.download_single_segment("u", 0, 3, 1)
    assert len(calls) == 3 and len(sleeps) == 2


def test_download_single_segment_aborts_before_request_when_interrupted(
    sandbox, monkeypatch
):
    """中断已置位：连第一个请求都不该发出去，直接上抛取消。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 20)
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        raise AssertionError("中断后不应再发请求")

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    monkeypatch.setattr(d, "interrupted", _always_set_event())
    with pytest.raises(RuntimeError, match="已取消（收到中断信号）"):
        d.download_single_segment("u", 0, 20, 1)
    assert calls == []


def test_download_single_segment_backoff_wakes_up_on_interrupt(
    sandbox, monkeypatch
):
    """退避等待期间收到中断：立刻醒来放弃，不空等完整个退避窗口。"""
    monkeypatch.setattr(d, "interrupted", threading.Event())
    monkeypatch.setattr(d, "SEG_RETRY_DELAY", 30)
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        # 首次失败进入退避，退避中主动置位中断。
        threading.Timer(0.05, d.interrupted.set).start()
        raise d.requests.HTTPError("boom", response=_FakeResp(503))

    monkeypatch.setattr(d, "request_with_retry", fake_request)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="已取消（收到中断信号）"):
        d.download_single_segment("u", 0, 5, 30)
    # 裸 sleep 的旧实现这里要等满 30s。
    assert time.monotonic() - started < 5
    assert len(calls) == 1


def test_request_with_retry_short_circuits_permanent_status(monkeypatch):
    """playlist 层同样短路：404 不该重试 10 次（约 4 分钟）才换节点。"""
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    calls = []

    class _Session:
        def request(self, method, url, **kwargs):
            calls.append(url)
            return _FakeResp(404)

    monkeypatch.setattr(d, "get_session", lambda: _Session())
    with pytest.raises(RuntimeError, match=d._HTTP_PERMANENT_MARKER) as exc:
        d.request_with_retry("GET", "https://cdn/x.m3u8", retries=10)
    assert len(calls) == 1 and sleeps == []
    # 层内短路，但整集仍判可重试：403/404 可能是临时风控，判死会误杀。
    assert d._classify_failure(str(exc.value)) is True
    assert d.classify_reject_reason(str(exc.value)) == "确定性4xx"


# ---------------------------------------------------------------- upload retry
def test_upload_to_r2_skips_retry_on_permanent_error(sandbox, monkeypatch):
    """凭证/权限/桶不存在属配置问题，重试 5 次只是白占反压槽位。"""
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    attempts = []

    class _Client:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            exc = Exception("denied")
            exc.response = {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            }
            raise exc

    monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
    ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is False and "不重试" in reason
    assert len(attempts) == 1 and sleeps == []


def _http_error(status, code="X"):
    exc = Exception(f"http {status} {code}")
    exc.response = {
        "Error": {"Code": code},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return exc


def test_upload_to_r2_http_400_gets_limited_backoff(sandbox, monkeypatch):
    """HTTP 400 不再当永久失败立即放弃：走 UPLOAD_RETRY_MAX_400 的有限退避（默认 2 次）。
    瞬时 400（RequestTimeout/BadDigest）第二次能成功；一直 400 则在 2 次后放弃。"""
    monkeypatch.setattr(d, "UPLOAD_RETRY_MAX", 5)
    monkeypatch.setattr(d, "UPLOAD_RETRY_MAX_400", 2)
    monkeypatch.setattr(d, "UPLOAD_RETRY_DELAY", 3)
    monkeypatch.setattr(d.random, "uniform", lambda a, b: 0.0)
    sleeps = []
    monkeypatch.setattr(d.interrupted, "wait", lambda s: sleeps.append(s) or False)

    # 场景 A：第一次 400（RequestTimeout），第二次成功。
    attempts = []

    class _Flaky:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            if len(attempts) == 1:
                raise _http_error(400, "RequestTimeout")

    monkeypatch.setattr(d, "get_s3_client", lambda: _Flaky())
    ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is True and reason is None
    assert len(attempts) == 2 and sleeps == [3]

    # 场景 B：一直 400 → 恰好尝试 2 次后放弃，原因说明是 400 上限，不是"不重试"。
    attempts.clear()
    sleeps.clear()

    class _Always400:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            raise _http_error(400, "InvalidArgument")

    monkeypatch.setattr(d, "get_s3_client", lambda: _Always400())
    ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is False
    assert "HTTP 400" in reason and "上限(2)" in reason
    assert len(attempts) == 2 and sleeps == [3]

    # 场景 C：先网络错再 400 → 预算只减不增：网络错 1 次 + 400 1 次 = 2 次后停。
    attempts.clear()
    sleeps.clear()

    class _NetThen400:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            if len(attempts) == 1:
                raise OSError("connection reset")
            raise _http_error(400, "RequestTimeout")

    monkeypatch.setattr(d, "get_s3_client", lambda: _NetThen400())
    ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is False and "HTTP 400" in reason
    assert len(attempts) == 2


def test_upload_to_r2_401_403_404_still_permanent(sandbox, monkeypatch):
    """收窄永久列表后 401/403/404 仍然一次即放弃。"""
    monkeypatch.setattr(d.interrupted, "wait", lambda s: (_ for _ in ()).throw(AssertionError("不应退避")))
    for status in (401, 403, 404):
        attempts = []

        class _Client:
            def upload_file(self, path, bucket, key):
                attempts.append(key)
                raise _http_error(status)

        monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
        ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
        assert ok is False and "不重试" in reason, status
        assert len(attempts) == 1, status


def test_upload_to_r2_retries_transient_with_exponential_backoff(sandbox, monkeypatch):
    """网络类错误仍重试，且退避是指数（3/6/12...）而非线性。
    退避走 interrupted.wait（B1，可被中断打断），所以在事件上打桩而非 time.sleep。"""
    monkeypatch.setattr(d, "UPLOAD_RETRY_MAX", 4)
    monkeypatch.setattr(d, "UPLOAD_RETRY_DELAY", 3)
    monkeypatch.setattr(d.random, "uniform", lambda a, b: 0.0)
    sleeps = []
    monkeypatch.setattr(d.interrupted, "wait", lambda s: sleeps.append(s) or False)
    attempts = []

    class _Client:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            raise OSError("connection reset")

    monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
    ok, _ = d.upload_to_r2("/tmp/x.mp4", "k")
    assert ok is False
    assert len(attempts) == 4
    assert sleeps == [3, 6, 12]


def test_upload_to_r2_backoff_is_interruptible(sandbox, monkeypatch):
    """退避期间收到中断：立即返回失败（原因含"中断"），不再发起下一次尝试，
    也不调用裸 time.sleep。走的是与真实失败同一条路径，调用方留本地 + pending。"""
    monkeypatch.setattr(d, "UPLOAD_RETRY_MAX", 5)
    monkeypatch.setattr(d, "UPLOAD_RETRY_DELAY", 60)
    monkeypatch.setattr(d.random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(
        d.time, "sleep", lambda s: (_ for _ in ()).throw(AssertionError("裸 sleep 被调用")),
    )
    attempts = []

    class _Client:
        def upload_file(self, path, bucket, key):
            attempts.append(key)
            # 第一次失败后置位中断：模拟退避期间用户 Ctrl+C。
            d.interrupted.set()
            raise OSError("connection reset")

    monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
    try:
        started = time.monotonic()
        ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
        elapsed = time.monotonic() - started
    finally:
        d.interrupted.clear()
    assert ok is False
    assert "收到中断信号" in reason and "connection reset" in reason
    assert len(attempts) == 1
    assert elapsed < 5   # 没有真等 60s 退避


def test_upload_to_r2_returns_immediately_when_already_interrupted(sandbox, monkeypatch):
    """进入函数时已中断：一次 upload_file 都不发。"""
    attempts = []

    class _Client:
        def upload_file(self, path, bucket, key):
            attempts.append(key)

    monkeypatch.setattr(d, "get_s3_client", lambda: _Client())
    d.interrupted.set()
    try:
        ok, reason = d.upload_to_r2("/tmp/x.mp4", "k")
    finally:
        d.interrupted.clear()
    assert ok is False and "收到中断信号" in reason
    assert attempts == []


# ---------------------------------------------------------------- orphan cleanup
def test_scan_downloaded_removes_zero_byte_orphans(sandbox):
    """0 字节 mp4 是移动中断留下的残骸：既不算已下载，也要清掉。"""
    base = sandbox / "downloads"
    real = _put_episode(base, "2001", "1", 1, 1, data=b"video")
    orphan = _put_episode(base, "2001", "2", 1, 2, data=b"")

    ids, duplicates = d.scan_downloaded_mp4_ids()
    assert ids == {"1_S01E01"} and duplicates == {}
    assert real.exists()
    assert not orphan.exists()


class _FakeResp:
    def __init__(self, status, headers=None, content=b""):
        self.status_code = status
        self.headers = headers or {}
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise d.requests.HTTPError(f"HTTP {self.status_code}", response=self)


class _FakeRangeSession:
    """按 Range 头切片返回 206；status 指定时统一返回该状态码。"""

    def __init__(self, data, status=None):
        self.data = data
        self.status = status
        self.calls = []

    def request(self, method, url, headers=None, **kwargs):
        headers = dict(headers or {})
        self.calls.append((url, headers.get("Range"), headers))
        if self.status is not None:
            return _FakeResp(self.status)
        start, end = (int(x) for x in headers["Range"][6:].split("-"))
        end = min(end, len(self.data) - 1)
        return _FakeResp(
            206,
            {"Content-Range": f"bytes {start}-{end}/{len(self.data)}"},
            self.data[start:end + 1],
        )


@pytest.fixture
def mp4_env(sandbox, monkeypatch):
    monkeypatch.setattr(d, "MP4_CHUNK_SIZE", 4)
    monkeypatch.setattr(d, "MP4_CONCURRENCY", 2)
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 1)
    # 样本 4 字节：预检只对 total_size > MP4_SAMPLE_SIZE*2 的文件生效，
    # 10 字节的测试数据正好越过该阈值。
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 4)
    monkeypatch.setattr(d, "probe_resolution", lambda p: (1920, 1080))
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(d, "_probe_duration", lambda p: None)
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 0.0)
    monkeypatch.setattr(d, "record_block_status", lambda s: None)
    return sandbox


def _install_range_session(monkeypatch, data, status=None):
    session = _FakeRangeSession(data, status)
    monkeypatch.setattr(d, "get_session", lambda: session)
    return session


# ------------------------------------------------ CDN 主机级 429 熔断
@pytest.fixture
def circuit_env(monkeypatch):
    """每个用例独立的熔断计数（模块级字典会跨用例污染）。"""
    monkeypatch.setattr(d, "_mp4_host_429", {})
    monkeypatch.setattr(d, "_mp4_host_tripped", {})
    monkeypatch.setattr(d, "MP4_HOST_CIRCUIT_THRESHOLD", 3)
    monkeypatch.setattr(d, "record_block_status", lambda s: None)


@pytest.mark.parametrize("url,expected", [
    ("https://bcdn.hakunaymatata.com/a/b.mp4", "bcdn.hakunaymatata.com"),
    ("http://HOST.example.com/x", "host.example.com"),   # 归一小写
    ("https://h.example.com", "h.example.com"),          # 无路径段
    ("not-a-url", ""),
    (None, ""),
])
def test_host_of(url, expected):
    assert d._host_of(url) == expected


def test_mp4_host_circuit_trips_at_threshold(circuit_env, capsys):
    url = "https://bcdnxw.hakunaymatata.com/a.mp4"
    for _ in range(2):
        d._mp4_host_record_429(url)
    assert d._mp4_host_is_tripped(url) is False   # 未达阈值不熔断
    d._mp4_host_record_429(url)
    assert d._mp4_host_is_tripped(url) is True
    assert "主机熔断" in capsys.readouterr().out


def test_mp4_host_circuit_is_per_host(circuit_env):
    """🔒 只跳过同一台主机；同域其它主机与别家域名都不受影响。"""
    bad = "https://bcdnxw.hakunaymatata.com/a.mp4"
    for _ in range(3):
        d._mp4_host_record_429(bad)
    assert d._mp4_host_is_tripped(bad) is True
    # 同域不同主机
    assert d._mp4_host_is_tripped("https://bcdn.hakunaymatata.com/b.mp4") is False
    # 完全不同的域
    assert d._mp4_host_is_tripped("https://sun.peakstorm.top/c.m3u8") is False


def test_probe_total_size_short_circuits_tripped_host(circuit_env, monkeypatch):
    """熔断后连请求都不该发出去——这正是本功能省时间的地方。"""
    url = "https://bad.cdn/a.mp4"
    for _ in range(3):
        d._mp4_host_record_429(url)

    def boom(*a, **kw):
        pytest.fail("熔断主机不应再发起请求")

    monkeypatch.setattr(d, "get_session", boom)
    with pytest.raises(RuntimeError, match=d._MP4_HOST_BLOCKED_MARKER):
        d._mp4_probe_total_size(url, {}, None)


def test_probe_total_size_records_429(circuit_env, monkeypatch):
    """探测层的 429 要计入熔断（不只是块层）。"""
    url = "https://slow.cdn/a.mp4"
    _install_range_session(monkeypatch, b"", status=429)
    for _ in range(3):
        with pytest.raises(Exception):
            d._mp4_probe_total_size(url, {}, None)
    assert d._mp4_host_is_tripped(url) is True


def test_mp4_chunk_429_trips_and_aborts_immediately(mp4_env, circuit_env,
                                                    monkeypatch):
    """🔒 块层 429 立即上抛 + 计入熔断，不走 SEG_RETRY_MAX 次退避。

    实测该 429 是整机故障而非限流：换 IP/签名/冷却后恒定 429，
    继续退避 20 次纯属空耗，必须立刻换节点。
    """
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 20)
    url = "https://bad2.cdn/a.mp4"
    session = _install_range_session(monkeypatch, b"", status=429)
    with pytest.raises(RuntimeError, match=d._MP4_HOST_BLOCKED_MARKER):
        d._download_mp4_chunk(url, {}, 0, 3, 0)
    # 关键：只发了一次请求，没有走 20 次退避重试
    assert len(session.calls) == 1
    # 单次 429 只计数不熔断（阈值 3）；凑满阈值后才跳过该主机
    assert d._mp4_host_429[d._host_of(url)] == 1
    assert d._mp4_host_is_tripped(url) is False
    for _ in range(2):
        with pytest.raises(RuntimeError):
            d._download_mp4_chunk(url, {}, 0, 3, 0)
    assert d._mp4_host_is_tripped(url) is True


def test_host_circuit_marker_is_not_a_permanent_failure():
    """🔒 红线：熔断文案绝不能进整集判死表。

    主机故障是临时的，判整集死会让本可救回的集永久丢失。
    它只进块级短路表（不再退避、尽快换节点）。
    """
    msg = f"{d._MP4_HOST_BLOCKED_MARKER}（HTTP 429，bad.cdn）: https://bad.cdn/a.mp4"
    assert d._MP4_HOST_BLOCKED_MARKER not in d._PERMANENT_FAILURE_MARKERS
    assert d._classify_failure(msg) is True          # 仍可重试
    assert d._MP4_HOST_BLOCKED_MARKER in d._MP4_CHUNK_NO_RETRY_MARKERS
    assert d.classify_reject_reason(msg) == "直链主机熔断(429)"


# ------------------------------------------------ 熔断半开恢复
def test_mp4_circuit_half_opens_after_cooldown(circuit_env, monkeypatch):
    """🔒 冷却到点后熔断必须自动复位，否则已恢复的主机被白白放弃整轮。

    实测踩坑：sun.peakstorm.top 熔断后 1 小时已恢复（curl 200），但本次运行
    再也不碰它，白丢 226 集；peakstorm 只有 moon/sun 两台，两台都熔断等于
    vidup/vidfast 全废。
    """
    monkeypatch.setattr(d, "HOST_CIRCUIT_COOLDOWN_SEC", 15)
    url = "https://flaky.cdn/a.mp4"
    for _ in range(3):
        d._mp4_host_record_429(url)
    assert d._mp4_host_is_tripped(url) is True

    # 冷却未到：仍然跳过（base 先取值，避免 patch 后的 lambda 自递归）
    base = time.monotonic()
    monkeypatch.setattr(d.time, "monotonic", lambda: base + 14)
    assert d._mp4_host_is_tripped(url) is True

    # 冷却到点：解除熔断，且计数已清零（还坏就得重新攒满 3 次）
    monkeypatch.setattr(d.time, "monotonic", lambda: base + 16)
    assert d._mp4_host_is_tripped(url) is False
    assert d._mp4_host_429[d._host_of(url)] == 0
    assert d._host_of(url) not in d._mp4_host_tripped


def test_mp4_circuit_retrips_if_still_broken(circuit_env, monkeypatch):
    """半开后主机仍坏：重新攒满阈值就再次熔断（不会无限放行）。"""
    monkeypatch.setattr(d, "HOST_CIRCUIT_COOLDOWN_SEC", 15)
    url = "https://stillbad.cdn/a.mp4"
    for _ in range(3):
        d._mp4_host_record_429(url)
    base = time.monotonic()
    monkeypatch.setattr(d.time, "monotonic", lambda: base + 100)
    assert d._mp4_host_is_tripped(url) is False      # 复位放行
    for _ in range(3):
        d._mp4_host_record_429(url)
    assert d._mp4_host_is_tripped(url) is True       # 再次熔断


@pytest.fixture
def hls_circuit_env(monkeypatch):
    monkeypatch.setattr(d, "_hls_host_5xx", {})
    monkeypatch.setattr(d, "_hls_host_tripped", {})
    monkeypatch.setattr(d, "HLS_HOST_CIRCUIT_THRESHOLD", 8)
    monkeypatch.setattr(d, "HOST_CIRCUIT_COOLDOWN_SEC", 15)


def test_hls_circuit_half_opens_after_cooldown(hls_circuit_env, monkeypatch):
    """🔒 HLS 侧同样要半开：peakstorm 恢复后必须能重新用。"""
    url = "https://sun.peakstorm.top/x/seg.ts"
    for _ in range(8):
        d._hls_host_record_result(url, 502)
    assert d._hls_host_is_tripped(url) is True

    base = time.monotonic()
    monkeypatch.setattr(d.time, "monotonic", lambda: base + 14)
    assert d._hls_host_is_tripped(url) is True
    monkeypatch.setattr(d.time, "monotonic", lambda: base + 16)
    assert d._hls_host_is_tripped(url) is False
    assert d._hls_host_5xx[d._host_of(url)] == 0


def test_hls_circuit_clears_immediately_on_success(hls_circuit_env):
    """一次非 5xx 响应即刻解除熔断——真实证据比等冷却更可靠。"""
    url = "https://moon.peakstorm.top/x/seg.ts"
    for _ in range(8):
        d._hls_host_record_result(url, 503)
    assert d._hls_host_is_tripped(url) is True
    # 无需等冷却：拿到 200 说明主机活了
    assert d._hls_host_record_result(url, 200) is False
    assert d._hls_host_is_tripped(url) is False
    assert d._host_of(url) not in d._hls_host_tripped


def test_hls_circuit_other_host_unaffected_by_cooldown(hls_circuit_env):
    """🔒 边界不变：熔断只作用于同一主机。"""
    bad = "https://sun.peakstorm.top/x/seg.ts"
    good = "https://moon.peakstorm.top/x/seg.ts"
    for _ in range(8):
        d._hls_host_record_result(bad, 502)
    assert d._hls_host_is_tripped(bad) is True
    assert d._hls_host_is_tripped(good) is False


# ------------------------------------------------ 失败侧逐节点归因
def test_node_failures_record_each_provider(sandbox, monkeypatch):
    """🔒 每个失败节点各记一条：provider / 类目 / retriable / 原文。

    没有它就只能看末节点的 error，**无法区分同为 m3u8 的 vidup 与 vidfast**。
    """
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    # 节点 1（vidup / m3u8）瞬时 5xx；节点 2（vidlink / mp4）签名过期。
    # 走真实的节点循环，只把两条下载路径的入口换成桩。
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("HTTP Error 502")),
    )
    monkeypatch.setattr(
        d, "_download_mp4_direct",
        lambda *a, **kw: (_ for _ in ()).throw(
            RuntimeError("直链已失效（HTTP 403），需重新取流")
        ),
    )

    entry = {
        "tmdbId": "10", "season": 1, "episode": 1,
        "urls": [
            {"url": "https://a/1.m3u8", "provider": "vidup", "type": "m3u8",
             "headers": {}, "quality": None, "size": None},
            {"url": "https://b/2.mp4", "provider": "vidlink", "type": "mp4",
             "headers": {}, "quality": None, "size": None},
        ],
    }
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    nodes = info["node_failures"]
    assert [n["provider"] for n in nodes] == ["vidup", "vidlink"]
    assert [n["node_index"] for n in nodes] == [1, 2]
    assert nodes[0]["reason_class"] == "源站5xx"
    assert nodes[0]["retriable"] is True
    assert nodes[1]["reason_class"] == "直链失效需重新取流"
    # 整集结论仍由既有逻辑给出，不受影响
    assert info["retriable"] is True
    assert info["needs_refetch"] is True


def test_node_failures_absent_on_early_return(sandbox, monkeypatch):
    """缺字段的早退路径在拿 ID 锁之前就 return，本就没有节点上下文。

    此时 info 里没有 node_failures 键是**正确的**——消费端统一用
    `info.get("node_failures") or []` 兜底，落盘为空列表。
    """
    monkeypatch.setattr(d, "processing_ids", set())
    label, ok, info = d.process_one_entry(
        {"tmdbId": "11", "season": 1, "episode": 1, "urls": []}, set()
    )
    assert ok is False
    assert info["error"] == "缺少 tmdbId/season/episode 或 urls"
    assert (info.get("node_failures") or []) == []


def test_node_failures_reach_failed_log(sandbox, monkeypatch):
    """端到端：node_failures 必须落进 failed.jsonl，否则统计侧读不到。"""
    line = {"tmdbId": "12", "season": 1, "episode": 1,
            "urls": [{"url": "https://a/1.m3u8", "provider": "vidup",
                      "type": "m3u8", "headers": {}, "quality": None,
                      "size": None}]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, processed: ("12_S01E01", False, {
            "error": "HTTP Error 502",
            "retriable": True,
            "node_failures": [
                {"node_index": 1, "provider": "vidup", "node_type": "m3u8",
                 "reason_class": "源站5xx", "retriable": True,
                 "error": "HTTP Error 502"},
            ],
        }),
    )
    d._run_pipeline()
    rows = [r for r in _read_jsonl(sandbox / "failed.jsonl")
            if r.get("stage") == "download"]
    assert rows and rows[0]["node_failures"][0]["provider"] == "vidup"


def test_download_mp4_direct_chunks_and_headers(mp4_env, monkeypatch):
    data = b"abcdefghij"
    session = _install_range_session(monkeypatch, data)
    out = os.path.join(d.TEMP_DIR, "temp_x.ts")
    node = {"url": "https://cdn/f.mp4", "type": "mp4",
            "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": None}
    resolution, bitrate = d._download_mp4_direct(node, out, "x", runtime_minutes=1)
    assert resolution == "1920x1080"
    assert bitrate == pytest.approx(len(data) * 8 / 60 / 1000)
    with open(out, "rb") as fh:
        assert fh.read() == data
    ranges = sorted(r for _, r, _ in session.calls)
    # bytes=0-0 探测总长；bytes=0-3 画质预检样本（MP4_SAMPLE_SIZE=4）；
    # 其余是 MP4_CHUNK_SIZE=4 的正片分块（0-3 与样本 range 相同，去重后可见）。
    assert ranges == [
        "bytes=0-0", "bytes=0-3", "bytes=0-3", "bytes=4-7", "bytes=8-9",
    ]
    for _, _, headers in session.calls:
        assert headers["User-Agent"] == "okhttp/4.9.3"
        assert headers["Referer"] is None and headers["X-Requested-With"] is None


def test_download_mp4_direct_overwrites_stale_file(mp4_env, monkeypatch):
    """残留 ts（无论长度）一律重下，不做续传。"""
    data = b"abcdefghij"
    session = _install_range_session(monkeypatch, data)
    out = os.path.join(d.TEMP_DIR, "temp_x.ts")
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    for stale in (data[:4], data, data + b"zz"):
        with open(out, "wb") as fh:
            fh.write(stale)
        session.calls.clear()
        d._download_mp4_direct({"url": "u", "type": "mp4", "headers": {},
                                "quality": None, "size": None}, out, "x", runtime_minutes=1)
        with open(out, "rb") as fh:
            assert fh.read() == data
        assert sorted(r for _, r, _ in session.calls) == [
            "bytes=0-0", "bytes=0-3", "bytes=0-3", "bytes=4-7", "bytes=8-9"]


def test_download_mp4_direct_no_range_support_fails_fast(mp4_env, monkeypatch):
    """探测返回 200（服务端不支持 Range）时：即便带 declared size 也直接判不支持，
    不进入分块下载；块级 200 亦不做退避重试。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))

    class _FullSession:
        def __init__(self):
            self.calls = 0

        def request(self, method, url, headers=None, **kwargs):
            self.calls += 1
            return _FakeResp(200, {}, b"abc")

    session = _FullSession()
    monkeypatch.setattr(d, "get_session", lambda: session)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": 10}
    with pytest.raises(RuntimeError, match="不支持 Range"):
        d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    assert session.calls == 1

    with pytest.raises(RuntimeError, match="未按 Range 响应"):
        d._download_mp4_chunk("u", {}, 0, 2, 0)
    assert session.calls == 2 and sleeps == []


def test_download_mp4_chunk_non_video_first_block_no_retry(mp4_env, monkeypatch):
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    session = _install_range_session(monkeypatch, b"<html>nope</html>")
    with pytest.raises(RuntimeError, match="不是视频分片"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    assert len(session.calls) == 1 and sleeps == []
    # 非首块不做内容校验
    assert d._download_mp4_chunk("u", {}, 10, 16, 1) == b"</html>"


@pytest.mark.parametrize("status", [404, 416])
def test_download_mp4_chunk_404_416_no_retry(mp4_env, monkeypatch, status):
    # 404 直链不存在 / 416 Range 越界：同一 url 重试无意义，块级不退避
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    sleeps = []
    monkeypatch.setattr(d.time, "sleep", lambda s: sleeps.append(s))
    session = _install_range_session(monkeypatch, b"", status=status)
    with pytest.raises(RuntimeError, match=f"直链块不可用（HTTP {status}）"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    assert len(session.calls) == 1 and sleeps == []


def test_download_mp4_chunk_aborts_immediately_when_event_set(mp4_env, monkeypatch):
    """整片已判失败时，在途块必须立刻放弃、不再发请求也不再退避。

    否则同片其它块会跑满 SEG_RETRY_MAX 次退避（最长十几分钟），
    占死下载槽位、拖慢换下一个取流节点。
    """
    import threading

    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    session = _install_range_session(monkeypatch, b"", status=500)
    abort = threading.Event()
    abort.set()
    with pytest.raises(RuntimeError, match="已取消（整片已判失败）"):
        d._download_mp4_chunk("u", {}, 0, 9, 0, abort)
    assert session.calls == []


def test_download_mp4_chunk_aborts_immediately_when_interrupted(
    mp4_env, monkeypatch
):
    """进程级中断置位：即便没有 abort_event，块层也要立刻放弃。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    monkeypatch.setattr(d, "interrupted", _always_set_event())
    session = _install_range_session(monkeypatch, b"", status=500)
    with pytest.raises(RuntimeError, match="已取消（收到中断信号）"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    assert session.calls == []


def test_download_mp4_chunk_backoff_wakes_up_on_interrupt(mp4_env, monkeypatch):
    """无 abort_event 时退避也必须可打断（不能退化成裸 sleep）。"""
    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    monkeypatch.setattr(d, "SEG_RETRY_DELAY", 30)
    monkeypatch.setattr(d, "interrupted", threading.Event())
    session = _install_range_session(monkeypatch, b"", status=500)
    threading.Timer(0.05, d.interrupted.set).start()
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="重试后仍失败"):
        d._download_mp4_chunk("u", {}, 0, 9, 0)
    # 裸 sleep 的旧实现这里要等满 30s。
    assert time.monotonic() - started < 5
    assert len(session.calls) == 1


def test_download_mp4_chunk_abort_wakes_up_backoff(mp4_env, monkeypatch):
    """退避等待期间整片判失败：用 Event.wait 立刻醒来，不空等完整个退避。"""
    import threading

    monkeypatch.setattr(d, "SEG_RETRY_MAX", 5)
    monkeypatch.setattr(d, "SEG_RETRY_DELAY", 30)
    abort = threading.Event()
    session = _install_range_session(monkeypatch, b"", status=500)
    # 首次请求失败进入退避，退避中 abort 被置位 -> 直接结束，不再重试。
    monkeypatch.setattr(abort, "wait", lambda timeout: True)
    monkeypatch.setattr(
        d.time, "sleep", lambda s: pytest.fail("有 abort_event 时不该用 sleep")
    )
    with pytest.raises(RuntimeError, match="重试后仍失败"):
        d._download_mp4_chunk("u", {}, 0, 9, 0, abort)
    assert len(session.calls) == 1


def test_download_mp4_direct_expired_link_needs_refetch(mp4_env, monkeypatch):
    _install_range_session(monkeypatch, b"", status=403)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": 10}
    with pytest.raises(RuntimeError, match=d._NEEDS_REFETCH_MARKER) as exc:
        d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    # 判死：本脚本读固化的 results.jsonl，无法重新取流，重投同一条 url 必然再挂。
    assert d._classify_failure(str(exc.value)) is False


def test_download_mp4_direct_quality_prefilter(mp4_env, monkeypatch):
    """模式 A：声明 quality 低于红线时一个字节都不下。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    session = _install_range_session(monkeypatch, b"abc")
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": 480, "size": None}
    with pytest.raises(RuntimeError, match="低于红线") as exc:
        d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    assert d._classify_failure(str(exc.value)) is False
    assert session.calls == []  # 声明画质不达标：一个字节都不下


def test_mode_b_mp4_declared_quality_does_not_prefilter(mp4_env, monkeypatch):
    """模式 B：声明 quality=480 **不再**被红线拦下，继续按码率判定。

    业务语义是"不再考虑分辨率"：一条 480p 的高码率直链完全可能合格，
    在这里按声明高度提前淘汰等于分辨率仍在把关。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    session = _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "probe_resolution", lambda p: (854, 480))
    # 同上：桩数据码率为 0，把门槛压到 0，本例只验"声明 480p 不被红线拦下"。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 0.0)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": 480, "size": None}
    resolution, _bitrate = d._download_mp4_direct(
        node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
    )
    assert resolution == "854x480"
    assert session.calls  # 确实下载了，不是被提前拦下


def test_download_mp4_direct_bitrate_gate(mp4_env, monkeypatch):
    session = _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 5000.0)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    with pytest.raises(RuntimeError, match="码率未达到") as exc:
        d._download_mp4_direct(
            node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
        )
    assert d._classify_failure(str(exc.value)) is False
    # 关键：码率不达标在「预检」阶段就淘汰，不该下载任何正片分块。
    # 只应有 bytes=0-0（探总长）与 bytes=0-3（头部样本）两次请求。
    assert sorted(r for _, r, _ in session.calls) == ["bytes=0-0", "bytes=0-3"]


def test_mp4_sample_prefilter_rejects_low_resolution(mp4_env, monkeypatch):
    """模式 A：样本预检判分辨率不达标，整片一个分块都不下，省下 GB 级流量。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    session = _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "probe_resolution", lambda p: (640, 480))
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    with pytest.raises(RuntimeError, match="低于红线") as exc:
        d._download_mp4_direct(
            node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
        )
    assert d._classify_failure(str(exc.value)) is False
    assert sorted(r for _, r, _ in session.calls) == ["bytes=0-0", "bytes=0-3"]


def test_mode_b_mp4_keeps_going_when_resolution_probe_fails(mp4_env, monkeypatch):
    """模式 B：整片下完后探不到分辨率**不再判失败**，按 UNKNOWN 如实标注。

    片子已经完整下完了，此刻仅因探不到一个不参与判定的属性就丢弃它，
    是纯粹的误杀——白白浪费了整集的下载流量。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    # 桩数据只有 10 字节 / 45 分钟，码率必然是 0；本例要验的是"探不到分辨率
    # 也不判失败"，不是码率关，故把门槛压到 0 让它过关。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 0.0)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    resolution, _bitrate = d._download_mp4_direct(
        node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
    )
    assert resolution == d.UNKNOWN_RESOLUTION


def test_mode_a_mp4_fails_when_resolution_probe_fails(mp4_env, monkeypatch):
    """对照组：模式 A 下整片探不到分辨率仍判可重试失败。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "probe_resolution", lambda p: None)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    with pytest.raises(RuntimeError, match="采样探测分辨率失败") as exc:
        d._download_mp4_direct(
            node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
        )
    # 可重试：不带确定性 marker。
    assert d._classify_failure(str(exc.value)) is True


def test_mp4_sample_unprobeable_falls_through_to_full_download(mp4_env, monkeypatch):
    """moov 在文件尾部导致样本探测失败时必须放行整片下载，绝不误杀。

    预检只是省流量的优化，探不出结果时应回退到"下完整片再验"的老路径，
    否则会把本可下载成功的片错判为失败，与"尽可能提高成功率"相悖。
    """
    data = b"abcdefghij"
    session = _install_range_session(monkeypatch, data)
    calls = {"n": 0}

    def flaky_probe(path):
        # 第一次（样本）探测失败，第二次（整片）成功。
        calls["n"] += 1
        return None if calls["n"] == 1 else (1920, 1080)

    monkeypatch.setattr(d, "probe_resolution", flaky_probe)
    out = os.path.join(d.TEMP_DIR, "t.ts")
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    resolution, _ = d._download_mp4_direct(node, out, "x", runtime_minutes=1)
    assert resolution == "1920x1080"
    with open(out, "rb") as fh:
        assert fh.read() == data
    # 正片分块照常下载
    assert "bytes=4-7" in [r for _, r, _ in session.calls]


def test_mp4_sample_file_is_always_removed(mp4_env, monkeypatch):
    """无论预检通过与否，样本文件都不得残留在 temp 目录。"""
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "_probe_duration", lambda p: 100.0)
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    d._download_mp4_direct(node, os.path.join(d.TEMP_DIR, "t.ts"), "x")
    assert not any(
        n.startswith("mp4sample_") for n in os.listdir(d.TEMP_DIR)
    )


def test_mp4_sample_prefers_upstream_runtime_over_sample_duration(mp4_env, monkeypatch):
    """码率必须用上游 runtime_minutes 算，不能用样本自身时长。

    样本是被截断的文件，ffprobe 从残缺 moov 读出的可能是"样本时长"而非整片
    时长。若用它做分母，bitrate 会虚高几十倍，让本该淘汰的低码率片通过预检、
    预检形同虚设。这里让 _probe_duration 返回一个极小值，断言它未被采用。
    """
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "_probe_duration", lambda p: 0.01)  # 若被采用码率会爆表
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    probed = d._mp4_probe_quality_by_sample(
        "u", {}, 10, os.path.join(d.TEMP_DIR, "s.mp4"), "x", runtime_minutes=45
    )
    assert probed is not None
    _, _, bitrate, _ = probed
    # 用 runtime_minutes=45 -> 2700s：10 字节 × 8 / 2700 / 1000
    assert bitrate == pytest.approx(10 * 8 / 2700 / 1000)


def test_mp4_sample_rejects_suspicious_sample_duration(mp4_env, monkeypatch):
    """无上游时长时，样本探测出的可疑短时长不可信 -> 返回 None 放行整片下载。

    绝不用可疑值去淘汰片子（宁可多下也不误杀）。
    """
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 4)
    monkeypatch.setattr(d, "MP4_MIN_TRUSTED_DURATION", 600)
    # 样本 4 字节 / 总长 10 字节 = 40% < 50%，且 5s < 600s -> 判不可信
    monkeypatch.setattr(d, "_probe_duration", lambda p: 5.0)
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    probed = d._mp4_probe_quality_by_sample(
        "u", {}, 10, os.path.join(d.TEMP_DIR, "s.mp4"), "x", runtime_minutes=None
    )
    assert probed is None


def test_mp4_sample_accepts_plausible_sample_duration(mp4_env, monkeypatch):
    """样本时长足够长（像整片时长）时可以采用。"""
    _install_range_session(monkeypatch, b"abcdefghij")
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 4)
    monkeypatch.setattr(d, "MP4_MIN_TRUSTED_DURATION", 600)
    monkeypatch.setattr(d, "_probe_duration", lambda p: 2700.0)
    os.makedirs(d.TEMP_DIR, exist_ok=True)
    probed = d._mp4_probe_quality_by_sample(
        "u", {}, 10, os.path.join(d.TEMP_DIR, "s.mp4"), "x", runtime_minutes=None
    )
    assert probed is not None
    assert probed[2] == pytest.approx(10 * 8 / 2700 / 1000)


def test_mp4_skips_prefilter_for_small_files(mp4_env, monkeypatch):
    """总长不足样本 2 倍时跳过预检：采样等于把整片下一遍，双倍流量零收益。"""
    monkeypatch.setattr(d, "MP4_SAMPLE_SIZE", 8)
    session = _install_range_session(monkeypatch, b"abcdefghij")  # 10 字节 < 16
    node = {"url": "u", "type": "mp4", "headers": {}, "quality": None, "size": None}
    d._download_mp4_direct(
        node, os.path.join(d.TEMP_DIR, "t.ts"), "x", runtime_minutes=45
    )
    # 只有探测总长 + 正片分块，没有额外的样本请求
    assert sorted(r for _, r, _ in session.calls) == [
        "bytes=0-0", "bytes=0-3", "bytes=4-7", "bytes=8-9"]


# ---------------------------------------------------------------- finalize / upload
def test_finalize_one_entry_success_and_failure(sandbox, monkeypatch):
    temp = sandbox / "temp"
    temp.mkdir()
    ts = temp / "temp_1_S01E01.ts"
    mp4 = temp / "temp_1_S01E01.mp4"
    sample = temp / "sample_1_S01E01_1080.ts"
    for p in (ts, sample):
        p.write_bytes(b"x")

    def fake_convert(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"mp4")
        return True

    monkeypatch.setattr(d, "convert_ts_to_mp4", fake_convert)
    d.processing_ids.add("1_S01E01")
    job = {
        "tmdbId": "1", "season": 1, "episode": 1, "normalized_id": "1_S01E01",
        "title": "T", "year": 2001, "url": "u", "final_ts": str(ts),
        "temp_mp4": str(mp4), "cleanup_paths": [str(ts), str(mp4), str(sample)],
        "bitrate_kbps": 1234, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
    }
    processed = set()
    key, ok, info = d.finalize_one_entry(job, processed)
    assert (key, ok) == ("1_S01E01", True)
    assert info["final_path"] == str(
        sandbox / "downloads" / "tv" / "2001" / "1" / "S01" / "E01" / "E01.mp4"
    )
    assert os.path.exists(info["final_path"])
    assert (info["season"], info["episode"], info["year"]) == (1, 1, 2001)
    assert processed == {"1_S01E01"}
    assert d.processing_ids == set()
    assert not ts.exists() and not sample.exists()
    # 旁车 meta.json 与视频同目录落盘，并在 success_info 里留标记供上传阶段收集。
    assert info["has_meta"] is True
    meta_path = os.path.join(os.path.dirname(info["final_path"]), "meta.json")
    with open(meta_path, encoding="utf-8") as fh:
        meta = json.load(fh)
    assert meta["tmdbId"] == "1"
    assert (meta["season"], meta["episode"]) == (1, 1)
    assert meta["video"]["file"] == "E01.mp4"
    assert meta["subtitles"] == []

    # Failure path: ffmpeg fails -> lock released, nothing registered.
    ts.write_bytes(b"x")
    monkeypatch.setattr(d, "convert_ts_to_mp4", lambda s, t: False)
    d.processing_ids.add("1_S01E01")
    key, ok, info = d.finalize_one_entry(job, processed2 := set())
    assert ok is False and "FFmpeg" in info["error"]
    assert processed2 == set() and d.processing_ids == set()
    assert not ts.exists()


def _success_info(sandbox, key="1_S01E01"):
    tid, _, rest = key.partition("_S")
    season, _, episode = rest.partition("E")
    s, e = int(season), int(episode)
    folder = (
        sandbox / "downloads" / "tv" / "2001" / tid / f"S{s:02d}" / f"E{e:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"E{e:02d}.mp4"
    path.write_bytes(b"mp4")
    return {
        "tmdbId": tid, "season": s, "episode": e, "title": "T", "year": 2001,
        "url": "u", "final_path": str(path), "bitrate_kbps": 1, "resolution": "r",
        "missing_segment_count": 0, "missing_segment_indices": [],
    }


def test_upload_one_entry_s3_disabled(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", False)
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert (key, ok) == ("1_S01E01", True)
    assert out["uploaded"] is False
    assert os.path.exists(info["final_path"])
    assert _read_jsonl(d.SUCCESS_LOG)[0]["uploaded"] is False


def test_upload_one_entry_success_deletes_local(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    uploaded = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (uploaded.append((p, k)) or (True, None)))
    info = _success_info(sandbox)
    # 旁车资产与视频同目录，应随视频一并上传并删除本地。
    meta_path = os.path.join(os.path.dirname(info["final_path"]), "meta.json")
    with open(meta_path, "w", encoding="utf-8") as fh:
        fh.write("{}")
    info["has_meta"] = True

    key, ok, out = d.upload_one_entry(info)
    assert ok is True
    video_key = "tv/2001/1/S01/E01/E01.mp4"
    meta_key = "tv/2001/1/S01/E01/meta.json"
    assert uploaded == [(info["final_path"], video_key), (meta_path, meta_key)]
    assert out["asset_keys"] == [meta_key]
    assert not os.path.exists(info["final_path"])
    assert not os.path.exists(meta_path)
    # 视频与资产都已进 R2，空的集目录被清掉，不留空壳吃 inode。
    assert not os.path.isdir(os.path.dirname(info["final_path"]))
    rec = _read_jsonl(d.SUCCESS_LOG)[0]
    assert rec["uploaded"] is True and rec["s3_key"] == video_key
    assert not os.path.exists(d.UPLOAD_PENDING_LOG)


def test_upload_one_entry_failure_keeps_local_and_writes_pending(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (False, "boom"))
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert ok is False and "boom" in out["error"]
    assert os.path.exists(info["final_path"])
    assert _read_jsonl(d.SUCCESS_LOG)[0]["uploaded"] is False
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)[0]
    assert d.record_episode_key(pend) == "1_S01E01"
    assert pend["local_path"] == info["final_path"]
    assert pend["s3_key"].endswith("/E01.mp4")
    assert pend["fail_reason"] == "boom"


def test_upload_one_entry_exception_is_contained(sandbox, monkeypatch):
    monkeypatch.setattr(d, "S3_ENABLED", True)

    def explode(p, k):
        raise RuntimeError("client crashed")

    monkeypatch.setattr(d, "upload_to_r2", explode)
    info = _success_info(sandbox)
    key, ok, out = d.upload_one_entry(info)
    assert ok is False and "client crashed" in out["error"]
    assert os.path.exists(info["final_path"])
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)[0]
    assert d.record_episode_key(pend) == "1_S01E01"


def _write_sidecars(info, subs=("en.vtt",), meta=True):
    """在 info 的集目录里落 meta.json / subs/*，返回 {rel: abs_path}。"""
    folder = os.path.dirname(info["final_path"])
    out = {}
    if meta:
        p = os.path.join(folder, "meta.json")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("{}")
        out["meta.json"] = p
    if subs:
        os.makedirs(os.path.join(folder, d.SUBS_SUBDIR), exist_ok=True)
        for name in subs:
            p = os.path.join(folder, d.SUBS_SUBDIR, name)
            with open(p, "w", encoding="utf-8") as fh:
                fh.write("WEBVTT")
            out[f"{d.SUBS_SUBDIR}/{name}"] = p
    return out


def test_upload_one_entry_sidecar_failure_writes_assets_only_pending(sandbox, monkeypatch):
    """③ 视频传成功、meta.json 失败：视频照删、SUCCESS_LOG 记 uploaded=true，
    但要写一条 assets_only pending 让 reupload 能补传——否则集目录里只剩
    meta.json，既没人认领也没人清理。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    info = _success_info(sandbox)
    info["provider"] = "vidsrc"
    paths = _write_sidecars(info)

    def fake_upload(p, k):
        if k.endswith("meta.json"):
            return False, "HTTP 400 达到有限重试上限(2): bad"
        return True, None

    monkeypatch.setattr(d, "upload_to_r2", fake_upload)
    key, ok, out = d.upload_one_entry(info)
    assert ok is True
    assert out["asset_keys"] == ["tv/2001/1/S01/E01/subs/en.vtt"]
    assert not os.path.exists(info["final_path"])
    assert not os.path.exists(paths["subs/en.vtt"])
    assert os.path.exists(paths["meta.json"])          # 失败资产留在本地
    assert os.path.isdir(os.path.dirname(info["final_path"]))  # 目录不能被清
    rec = _read_jsonl(d.SUCCESS_LOG)[0]
    assert rec["uploaded"] is True
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert len(pend) == 1
    assert pend[0]["assets_only"] is True
    assert pend[0]["failed_assets"] == ["meta.json"]
    assert pend[0]["provider"] == "vidsrc"
    assert pend[0]["local_path"] == info["final_path"]
    assert pend[0]["s3_key"] == "tv/2001/1/S01/E01/E01.mp4"
    assert "meta.json" in pend[0]["fail_reason"]


def test_upload_one_entry_sidecar_exception_writes_assets_only_pending(sandbox, monkeypatch):
    """资产上传抛异常（而非返回 False）同样计入 failed 并写 pending。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    info = _success_info(sandbox)
    _write_sidecars(info, subs=())

    def fake_upload(p, k):
        if k.endswith("meta.json"):
            raise RuntimeError("client crashed")
        return True, None

    monkeypatch.setattr(d, "upload_to_r2", fake_upload)
    key, ok, out = d.upload_one_entry(info)
    assert ok is True and out["asset_keys"] == []
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert [r.get("assets_only") for r in pend] == [True]
    assert pend[0]["failed_assets"] == ["meta.json"]


def test_reupload_assets_only_success_clears_pending_and_dir(sandbox, monkeypatch, capsys):
    """assets_only pending：视频本地已不存在也**不算孤儿**；回扫集目录补传
    meta.json/subs，全部成功 → 移出 pending + 清空目录 + SUCCESS_LOG 不动。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    info = _success_info(sandbox)
    os.remove(info["final_path"])                      # 视频早已上传并删除
    paths = _write_sidecars(info, subs=("en.vtt", "zh.srt"))
    (sandbox / "success.jsonl").write_text(
        json.dumps({"tmdbId": "1", "season": 1, "episode": 1, "uploaded": True,
                    "s3_key": "tv/2001/1/S01/E01/E01.mp4"}) + "\n",
        encoding="utf-8",
    )
    (sandbox / "pending.jsonl").write_text(json.dumps({
        "tmdbId": "1", "season": 1, "episode": 1, "year": 2001, "title": "T",
        "local_path": info["final_path"], "s3_key": "tv/2001/1/S01/E01/E01.mp4",
        "assets_only": True, "failed_assets": ["meta.json"],
        "fail_reason": "旁车资产上传失败: meta.json", "ts": "old",
    }) + "\n", encoding="utf-8")

    calls = []
    monkeypatch.setattr(d, "upload_to_r2",
                        lambda p, k: (calls.append(k) or (True, None)))
    d.reupload_pending()

    # 回扫以目录实况为准：不只补 failed_assets 里的 meta.json，字幕也一并传。
    assert calls == [
        "tv/2001/1/S01/E01/meta.json",
        "tv/2001/1/S01/E01/subs/en.vtt",
        "tv/2001/1/S01/E01/subs/zh.srt",
    ]
    assert not any(os.path.exists(p) for p in paths.values())
    assert not os.path.isdir(os.path.dirname(info["final_path"]))
    assert _read_jsonl(d.UPLOAD_PENDING_LOG) == []
    succ = _read_jsonl(d.SUCCESS_LOG)
    assert len(succ) == 1 and succ[0]["uploaded"] is True
    out = capsys.readouterr().out
    assert "旁车资产补齐 1" in out
    assert "孤儿(本地已无)清理 0" in out


def test_reupload_assets_only_partial_failure_keeps_pending(sandbox, monkeypatch, capsys):
    """部分资产仍失败：成功的删本地，失败的留下；pending 保留并更新
    failed_assets / fail_reason / ts；目录不清。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "NEW-TS")
    info = _success_info(sandbox)
    os.remove(info["final_path"])
    paths = _write_sidecars(info, subs=("en.vtt",))
    (sandbox / "pending.jsonl").write_text(json.dumps({
        "tmdbId": "1", "season": 1, "episode": 1, "year": 2001,
        "local_path": info["final_path"], "s3_key": "tv/2001/1/S01/E01/E01.mp4",
        "assets_only": True, "failed_assets": ["meta.json", "subs/en.vtt"],
        "fail_reason": "old", "ts": "old",
    }) + "\n", encoding="utf-8")

    monkeypatch.setattr(
        d, "upload_to_r2",
        lambda p, k: (True, None) if k.endswith(".vtt") else (False, "still 400"),
    )
    d.reupload_pending()

    assert not os.path.exists(paths["subs/en.vtt"])
    assert os.path.exists(paths["meta.json"])
    assert os.path.isdir(os.path.dirname(info["final_path"]))
    remaining = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert len(remaining) == 1
    assert remaining[0]["assets_only"] is True
    assert remaining[0]["failed_assets"] == ["meta.json"]
    assert remaining[0]["ts"] == "NEW-TS"
    assert "meta.json" in remaining[0]["fail_reason"]
    out = capsys.readouterr().out
    assert "仍失败 1" in out


def test_reupload_assets_only_empty_dir_is_resolved(sandbox, monkeypatch, capsys):
    """集目录里已无资产（被上次补传/手动清理）：视为消解，移出 pending。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    info = _success_info(sandbox)
    os.remove(info["final_path"])
    (sandbox / "pending.jsonl").write_text(json.dumps({
        "tmdbId": "1", "season": 1, "episode": 1, "year": 2001,
        "local_path": info["final_path"], "s3_key": "k",
        "assets_only": True, "failed_assets": ["meta.json"],
    }) + "\n", encoding="utf-8")
    calls = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (calls.append(k) or (True, None)))
    d.reupload_pending()
    assert calls == []
    assert _read_jsonl(d.UPLOAD_PENDING_LOG) == []
    assert "孤儿(本地已无)清理 1" in capsys.readouterr().out


def test_reupload_video_ok_but_sidecar_fails_degrades_to_assets_only(sandbox, monkeypatch):
    """普通 pending 补传：视频成功、资产失败 → 视频删、SUCCESS_LOG 标 uploaded，
    该集在 pending 里降级为 assets_only 记录而不是消失。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    info = _success_info(sandbox)
    paths = _write_sidecars(info, subs=())
    (sandbox / "pending.jsonl").write_text(json.dumps({
        "tmdbId": "1", "season": 1, "episode": 1, "year": 2001, "provider": "p1",
        "local_path": info["final_path"], "s3_key": "", "fail_reason": "orig",
    }) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        d, "upload_to_r2",
        lambda p, k: (False, "nope") if k.endswith("meta.json") else (True, None),
    )
    d.reupload_pending()

    assert not os.path.exists(info["final_path"])
    assert os.path.exists(paths["meta.json"])
    succ = _read_jsonl(d.SUCCESS_LOG)
    assert len(succ) == 1 and succ[0]["uploaded"] is True and succ[0]["provider"] == "p1"
    remaining = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert len(remaining) == 1
    assert remaining[0]["assets_only"] is True
    assert remaining[0]["failed_assets"] == ["meta.json"]
    assert remaining[0]["s3_key"] == "tv/2001/1/S01/E01/E01.mp4"
    assert remaining[0]["provider"] == "p1"


# ---------------------------------------------------------------- main lock
def test_main_lock_lifecycle(sandbox):
    assert d.is_main_running() is False
    d.acquire_main_lock()
    assert d.is_main_running() is True
    d.release_main_lock()
    assert not os.path.exists(d.MAIN_LOCK_FILE)

    # Stale lock (dead pid) is cleaned up.
    with open(d.MAIN_LOCK_FILE, "w") as fh:
        fh.write("999999999")
    assert d.is_main_running() is False
    assert not os.path.exists(d.MAIN_LOCK_FILE)

    # release only removes a lock owned by this process.
    with open(d.MAIN_LOCK_FILE, "w") as fh:
        fh.write("1")
    d.release_main_lock()
    assert os.path.exists(d.MAIN_LOCK_FILE)


def test_acquire_main_lock_rejects_concurrent_run(sandbox, monkeypatch):
    """已有存活主流程时必须 fail-fast：并发跑会重复下载并互相覆盖日志。"""
    monkeypatch.setattr(d, "is_main_running", lambda: True)
    with pytest.raises(SystemExit) as exc:
        d.acquire_main_lock()
    assert exc.value.code == 1
    assert not os.path.exists(d.MAIN_LOCK_FILE)


# ---------------------------------------------------------------- pipeline batching
def test_run_pipeline_batches_download_submissions(sandbox, monkeypatch):
    """分批投递：同时在途的下载 future 不超过 DOWNLOAD_QUEUE_DEPTH，但每一集都要跑到。

    一次性把整轮全部集 submit 进 pending，会让 wait(FIRST_COMPLETED) 每次都对
    全部未完成 future 挂/摘 waiter，几万集时主循环退化成 O(N²)。分批后 wait
    规模恒定在槽位量级，语义（每集都处理、失败分类不变）必须完全一致。
    """
    entries = [
        {"tmdbId": str(i), "season": 1, "episode": 1, "urls": ["u"]}
        for i in range(20)
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MAX_WORKERS", 2)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 4)
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    lock = __import__("threading").Lock()
    state = {"inflight": 0, "peak": 0, "seen": []}

    def fake_process(entry, processed_ids):
        with lock:
            state["inflight"] += 1
            state["peak"] = max(state["peak"], state["inflight"])
            state["seen"].append(entry["tmdbId"])
        try:
            # 确定性失败：不进转封装，也不触发下一轮。
            return entry["tmdbId"], False, {
                "error": "没有找到媒体播放列表", "retriable": False,
            }
        finally:
            with lock:
                state["inflight"] -= 1

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    assert sorted(state["seen"], key=int) == [str(i) for i in range(20)]
    assert state["peak"] <= d.DOWNLOAD_QUEUE_DEPTH
    failed = _read_jsonl(d.FAILED_LOG)
    assert len(failed) == 20 and all(r["stage"] == "download" for r in failed)


# ------------------------------------------------ 陈旧直链启动预检
def _stale_env(monkeypatch, sandbox, *, enabled=True, stale_after=86400):
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", enabled)
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", stale_after)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_EPISODE", 2)
    monkeypatch.setattr(d, "AUTO_REFETCH_WORKERS", 2)
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "results.jsonl"))


def _entry(tid, *, fetched_at=None, url="old"):
    e = {"tmdbId": tid, "season": 1, "episode": 2, "urls": [url]}
    if fetched_at is not None:
        e["fetched_at"] = fetched_at
    return e


def _install_fetcher(monkeypatch, results):
    """把假的 tv_ids_to_links 塞进 sys.modules，供 refetch_entries 延迟导入。

    results: {集级 key: (status, result)}；未列出的 key 返回 ("dead", None)。
    返回记录被调用集的 list。
    """
    calls = []
    module = types.ModuleType("tv_ids_to_links")

    def process_episode(tid, season, episode):
        key = d.episode_key(tid, season, episode)
        calls.append(key)
        return results.get(key, ("dead", None))

    module.process_episode = process_episode
    monkeypatch.setitem(sys.modules, "tv_ids_to_links", module)
    return calls


def test_is_stale_entry_needs_fetched_at(monkeypatch, sandbox):
    """没有 fetched_at 的条目一律判不陈旧——宁可漏判，不可误判。

    旧版 results.jsonl 与手工输入都没有该字段，当成"无限旧"会让整批集在启动时
    全部去重取流，取流配额与耗时双重浪费，且多半徒劳。
    """
    _stale_env(monkeypatch, sandbox)
    now = 1_000_000
    assert d.is_stale_entry(_entry("1"), now) is False
    assert d.is_stale_entry(_entry("1", fetched_at=0), now) is False
    assert d.is_stale_entry(_entry("1", fetched_at=now - 10), now) is False
    assert d.is_stale_entry(_entry("1", fetched_at=now - 86401), now) is True


def test_is_stale_entry_disabled_by_zero(monkeypatch, sandbox):
    """stale_after_seconds=0 关闭判定，再旧也不算陈旧。"""
    _stale_env(monkeypatch, sandbox, stale_after=0)
    assert d.is_stale_entry(_entry("1", fetched_at=1), 10**9) is False


def test_refresh_stale_entries_replaces_only_stale_and_keeps_order(
    monkeypatch, sandbox
):
    """只换陈旧的那条，顺序不变，新鲜条目不该被送去重取。"""
    _stale_env(monkeypatch, sandbox)
    now = int(time.time())
    fresh = _entry("1", fetched_at=now)
    stale = _entry("2", fetched_at=now - 90000)
    revived = {
        "tmdbId": "2", "season": 1, "episode": 2,
        "urls": ["new"], "fetched_at": now,
    }
    calls = _install_fetcher(monkeypatch, {"2_S01E02": ("ok", revived)})

    out = d.refresh_stale_entries([fresh, stale], {})
    # 只有陈旧的那集被重取
    assert calls == ["2_S01E02"]
    # 顺序保持，新鲜条目原样
    assert out[0] is fresh
    assert out[1]["urls"] == ["new"] and out[1]["fetched_at"] == now
    # 新结果落盘 INPUT_JSONL，供下次运行按 fetched_at 择新
    assert _read_jsonl(d.INPUT_JSONL) == [revived]


def test_refresh_stale_entries_keeps_original_when_refetch_fails(
    monkeypatch, sandbox
):
    """🔑 重取无果必须原样保留，绝不丢弃。

    阈值只是经验值、旧链接未必真失效；且没配代理凭证的机器根本取不了流，
    丢弃等于整批集全军覆没。本功能绝不允许反过来降低下载成功率。
    """
    _stale_env(monkeypatch, sandbox)
    stale = _entry("2", fetched_at=1)
    _install_fetcher(monkeypatch, {})          # 全部返回 dead

    out = d.refresh_stale_entries([stale], {})
    assert out == [stale] and out[0]["urls"] == ["old"]


def test_refresh_stale_entries_lets_keyboard_interrupt_escape(
    monkeypatch, sandbox
):
    """Ctrl+C 必须原样逃逸：重取只吞 Exception/SystemExit，绝不笼统吞 BaseException。"""
    _stale_env(monkeypatch, sandbox)

    def boom(entries, counts):
        raise KeyboardInterrupt

    monkeypatch.setattr(d, "refetch_entries", boom)
    with pytest.raises(KeyboardInterrupt):
        d.refresh_stale_entries([_entry("2", fetched_at=1)], {})


def test_refetch_entries_import_systemexit_is_swallowed(monkeypatch, sandbox):
    """import 阶段的 SystemExit 被吞掉并返回空列表（沿用旧链接继续下载）。"""
    _stale_env(monkeypatch, sandbox)
    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "tv_ids_to_links":
            raise SystemExit("缺少代理凭证")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert d.refetch_entries([_entry("2", fetched_at=1)], {}) == []


def test_refetch_entries_respects_per_episode_cap(monkeypatch, sandbox):
    """达到每集重取上限后不再重取，防'取流-过期-重取'反复空转。"""
    _stale_env(monkeypatch, sandbox)
    calls = _install_fetcher(monkeypatch, {})
    counts = {"2_S01E02": 2}          # 已达上限 AUTO_REFETCH_MAX_PER_EPISODE
    assert d.refetch_entries([_entry("2", fetched_at=1)], counts) == []
    assert calls == []


def test_refetch_entries_counts_are_per_episode_not_per_show(
    monkeypatch, sandbox
):
    """🔑 记数粒度必须是集级：同一部剧的几十集共用一个 tmdbId。

    若按剧记数，同剧只要有 max_per_episode 集被重取过，**剩下所有集都会被
    永久挡掉**——一部 100 集的剧只救得回 2 集。这里跑两轮来暴露它：第一轮
    把计数填起来，第二轮换别的集，按集记数时它们仍应被重取。
    """
    _stale_env(monkeypatch, sandbox)
    counts = {}
    mk = lambda ep: {  # noqa: E731
        "tmdbId": "7", "season": 1, "episode": ep, "urls": ["o"],
        "fetched_at": 1,
    }
    calls = _install_fetcher(monkeypatch, {})

    # 第一轮：S01E01/E02 各被重取一次 → 按剧记数的话 counts["7"] 会累到 2
    d.refetch_entries([mk(1), mk(2)], counts)
    calls.clear()
    # 第二轮：换两集全新的。按集记数它们的计数是 0，必须照样被重取。
    d.refetch_entries([mk(3), mk(4)], counts)

    assert sorted(calls) == ["7_S01E03", "7_S01E04"]
    # 计数按集分开存放，不会挤在一个剧级键上
    assert counts.get("7") is None
    assert counts["7_S01E01"] == 1 and counts["7_S01E03"] == 1


def test_refetch_entries_single_failure_does_not_kill_batch(
    monkeypatch, sandbox
):
    """单集重取抛异常不影响同批其余集。"""
    _stale_env(monkeypatch, sandbox)
    now = int(time.time())
    good = {"tmdbId": "9", "season": 1, "episode": 2,
            "urls": ["new"], "fetched_at": now}
    module = types.ModuleType("tv_ids_to_links")

    def process_episode(tid, season, episode):
        if str(tid) == "8":
            raise RuntimeError("boom")
        return "ok", good

    module.process_episode = process_episode
    monkeypatch.setitem(sys.modules, "tv_ids_to_links", module)

    out = d.refetch_entries(
        [_entry("8", fetched_at=1), _entry("9", fetched_at=1)], {}
    )
    assert [e["tmdbId"] for e in out] == ["9"]


def test_refetch_entries_preserves_extra_metadata(monkeypatch, sandbox):
    """逐键覆盖而非整体替换：entry 上的历史元数据必须保留。"""
    _stale_env(monkeypatch, sandbox)
    now = int(time.time())
    entry = _entry("2", fetched_at=1)
    entry["year"] = 2021
    entry["runtime_minutes"] = 42
    revived = {"tmdbId": "2", "season": 1, "episode": 2,
               "urls": ["new"], "fetched_at": now}
    _install_fetcher(monkeypatch, {"2_S01E02": ("ok", revived)})

    out = d.refetch_entries([entry], {})
    assert out[0]["year"] == 2021 and out[0]["runtime_minutes"] == 42
    assert out[0]["urls"] == ["new"]


def test_refresh_stale_entries_noop_when_disabled(monkeypatch, sandbox):
    """关闭开关后原样返回，且绝不碰取流模块。"""
    _stale_env(monkeypatch, sandbox, enabled=False)
    calls = _install_fetcher(monkeypatch, {})
    entries = [_entry("2", fetched_at=1)]
    assert d.refresh_stale_entries(entries, {}) is entries
    assert calls == []


def test_precheck_caps_batch_and_prefers_oldest(monkeypatch, sandbox):
    """🔑 单次限额 + 最旧优先。

    TV 全量下 stale 可达几万到十几万集，一次性全 submit 会让绝大多数排队到
    总超时被丢弃，且它们的 refetch_counts 不会 +1，下次运行又从头再来，
    队尾永远轮不到 —— 等于预检只对前几百集有效。
    """
    _stale_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 2)
    # 故意让最旧的排在列表末尾，验证是按时间而非按出现顺序挑
    entries = [
        _entry("1", fetched_at=500),
        _entry("2", fetched_at=100),
        _entry("3", fetched_at=300),
        _entry("4", fetched_at=200),
    ]
    calls = _install_fetcher(monkeypatch, {})
    d.refresh_stale_entries(entries, {})
    # 只处理 2 集，且是 fetched_at 最小（最旧）的两集
    assert sorted(calls) == ["2_S01E02", "4_S01E02"]


def test_precheck_cap_zero_means_unlimited(monkeypatch, sandbox):
    """max_per_run=0 表示不限额（小批量场景）。"""
    _stale_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 0)
    entries = [_entry(str(i), fetched_at=i + 1) for i in range(5)]
    calls = _install_fetcher(monkeypatch, {})
    d.refresh_stale_entries(entries, {})
    assert len(calls) == 5


def test_precheck_skips_quality_dead_episodes(monkeypatch, sandbox):
    """🔑 上次因画质不达标判死的集不再重取——重取回来仍不达标，白烧配额。

    TV 侧集级有源率个位数，这类集在失败总量里占比很高。
    """
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "分辨率 640x360 低于红线 720（容差 0.95），跳过",
        "stage": "download", "retriable": False,
    })
    calls = _install_fetcher(monkeypatch, {})
    out = d.refresh_stale_entries([_entry("2", fetched_at=1)], {})
    assert calls == []              # 一次取流都不发
    assert out[0]["urls"] == ["old"]  # 条目原样保留，照常用旧链接下载


def test_precheck_does_not_skip_transient_failures(monkeypatch, sandbox):
    """🔴 红线：源站 5xx / 超时**绝不能**被当成画质判死跳过。

    那是"今天源站挂了"，重取完全可能换到好流；跳过等于永久放弃可救回的集。
    """
    _stale_env(monkeypatch, sandbox)
    for err, retriable in [
        ("HTTP Error 502", True),
        ("Read timed out", True),
        ("直链已失效（HTTP 403），需重新取流: x", False),
    ]:
        d.write_log(d.FAILED_LOG, {
            "tmdbId": "2", "season": 1, "episode": 2,
            "error": err, "stage": "download", "retriable": retriable,
        })
        calls = _install_fetcher(monkeypatch, {})
        d.refresh_stale_entries([_entry("2", fetched_at=1)], {})
        assert calls == ["2_S01E02"], f"{err} 不该被跳过"
        os.remove(d.FAILED_LOG)


def test_quality_dead_takes_the_latest_verdict(monkeypatch, sandbox):
    """同一集先画质判死、后来因别的原因失败 → 以最后一条为准，不再跳过。

    画质门槛可能被调松过，旧的判死结论不该永久压住它。
    """
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "码率未达到门槛", "stage": "download", "retriable": False,
    })
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "HTTP Error 502", "stage": "download", "retriable": True,
    })
    assert d.load_quality_dead_keys() == set()


def test_load_quality_dead_keys_honors_node_failures(monkeypatch, sandbox):
    """🔒 落盘重载同口径：failed.jsonl 里有节点没给出画质结论 → 不算判死。

    这条路径决定"下次运行的启动预检要不要跳过这集重取"。若只看
    retriable+文案而忽略 node_failures，进程内正确判为"可重试"的集会在
    **重启后**被当成画质判死永久跳过 —— 判死结论凭空复活。
    """
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "分辨率 640x360 低于红线 1080",
        "stage": "download", "retriable": False,
        "node_failures": [
            {"reason_class": "直链失效需重新取流"},   # 从未给出画质结论
            {"reason_class": "分辨率低于红线"},
        ],
    })
    assert d.load_quality_dead_keys() == set()


def test_load_quality_dead_keys_kills_when_all_nodes_report_quality(monkeypatch,
                                                                     sandbox):
    """对照组：全部节点都是画质结论 → 重载后仍判死（避免上条测试变成平凡通过）。"""
    _stale_env(monkeypatch, sandbox)
    d.write_log(d.FAILED_LOG, {
        "tmdbId": "2", "season": 1, "episode": 2,
        "error": "分辨率 640x360 低于红线 1080",
        "stage": "download", "retriable": False,
        "node_failures": [
            {"reason_class": "分辨率低于红线"},
            {"reason_class": "码率未达门槛"},
        ],
    })
    assert d.load_quality_dead_keys() == {"2_S01E02"}


def test_is_quality_dead_requires_non_retriable():
    """可重试的失败一律不算画质判死（多节点集里"画质淘汰+502"整集仍可重试）。"""
    assert d.is_quality_dead(False, "分辨率 640x360 低于红线 720") is True
    assert d.is_quality_dead(False, "码率未达到门槛") is True
    # 画质汇总判死（内层全流淘汰 / 外层概率判死）也算
    assert d.is_quality_dead(
        False, "全部 3 条候选流均因画质不达标被淘汰（各流原因见上方日志）"
    ) is True
    # 同样的文案，retriable=True 就不算
    assert d.is_quality_dead(True, "分辨率 640x360 低于红线 720") is False
    # 非画质类的确定性失败也不算
    assert d.is_quality_dead(False, "直链块不可用（HTTP 404）") is False
    assert d.is_quality_dead(False, "HTTP Error 502") is False


# --- 第 3 道闸门：必须**全部节点**都给出画质结论才判死 -----------------------
# 业务红线（用户拍板）：判死必须确定"该集在所有存在资源的节点上都画质不达标"，
# 其余情况一律可重试。有跨运行重试兜底时，误判死 = 永久丢一集，代价远高于多跑一轮。
_QUALITY_NODE = {"reason_class": "分辨率低于红线"}
_BITRATE_NODE = {"reason_class": "码率未达门槛"}
_REFETCH_NODE = {"reason_class": "直链失效需重新取流"}
_5XX_NODE = {"reason_class": "源站5xx"}
_STRUCT_NODE = {"reason_class": "不支持的播放列表结构"}
_CHUNK404_NODE = {"reason_class": "直链块不可用(404/416)"}


def test_quality_dead_requires_every_node_to_report_quality():
    """全部节点都是画质类结论 → 判死（这才是"所有节点都不达标"）。"""
    msg = "分辨率 640x360 低于红线 1080"
    assert d.is_quality_dead(False, msg, [_QUALITY_NODE]) is True
    assert d.is_quality_dead(False, msg, [_QUALITY_NODE, _BITRATE_NODE]) is True


@pytest.mark.parametrize("other_node, label", [
    (_REFETCH_NODE, "直链失效（换新链接可能就是 1080p）"),
    (_5XX_NODE, "源站 5xx（今天挂了、明天可能好）"),
    (_STRUCT_NODE, "结构不支持（该节点从未给出画质结论）"),
    (_CHUNK404_NODE, "直链块 404"),
])
def test_quality_dead_blocked_when_any_node_lacks_quality_verdict(other_node, label):
    """🔒 红线：任一节点没给出画质结论 → 绝不判死，无论它排在第几位。

    这是 2026-09-13 审查实测出的真实误杀（3/8 场景违背语义）。成因：
    error_msg 只留**末节点**文案，而"需重新取流/结构不支持/直链块404"这类
    **非画质的确定性失败**既不会让 any_retriable 变 True、也不计入
    quality_rejected_nodes，于是画质失败恰好落在末位时前两道闸门双双失守。
    """
    msg = "分辨率 640x360 低于红线 1080"
    # 画质在末位（原漏洞触发顺序）
    assert d.is_quality_dead(False, msg, [other_node, _QUALITY_NODE]) is False, label
    # 画质在首位
    assert d.is_quality_dead(False, msg, [_QUALITY_NODE, other_node]) is False, label


def test_quality_dead_without_node_failures_falls_back_to_two_conditions():
    """无逐节点信息（旧记录 / 异常抛在节点循环之外）→ 退回两条件口径。

    不是放水：那些路径本就没有"多节点"语义，单节点集结论与三条件版一致。
    """
    msg = "分辨率 640x360 低于红线 1080"
    assert d.is_quality_dead(False, msg, None) is True
    assert d.is_quality_dead(False, msg, []) is True


def test_refetch_node_plus_quality_node_is_not_dead_end_to_end(quality_env,
                                                               monkeypatch):
    """🔒 端到端红线：节点1 直链失效 + 节点2 画质不达标 → **不判死**。

    走真实的 process_one_entry，验证的是"整条链路"而不只是 is_quality_dead
    的单元行为。这个组合正是审查实测出的误杀场景：两个节点都是确定性失败
    （any_retriable 保持 False），末节点文案又是画质的，第 3 道闸门是唯一防线。

    业务含义：那个直链失效的节点换条新 url 完全可能是 1080p，凭什么替它
    断定"整集画质不达标"。
    """
    calls = []

    def fake_master(url, *a, **kw):
        calls.append(url)
        if len(calls) == 1:
            raise RuntimeError("直链签名已过期，需重新取流")
        raise d.QualityRejectedError("分辨率 640x360 低于红线 1080")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert ok is False
    # 两个节点都是确定性失败，整集 retriable 确实是 False
    assert info["retriable"] is False
    # 但**不是**画质判死：有节点从未给出画质结论
    assert d.is_quality_dead(
        info["retriable"], info["error"], info.get("node_failures")
    ) is False
    # 且仍会进重取桶——这才是这集真正的出路
    assert info.get("needs_refetch") is True


def test_quality_kill_dead_log_skips_when_a_node_lacks_verdict(quality_env,
                                                                monkeypatch):
    """🔒 端到端：上述场景**不能**落进 download_dead.jsonl（否则永久丢集）。"""
    line = {"tmdbId": "70", "season": 1, "episode": 1,
            "urls": _quality_entry(2)["urls"]}
    input_path = quality_env / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(quality_env / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(quality_env / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    calls = []

    def fake_master(url, *a, **kw):
        calls.append(url)
        if len(calls) == 1:
            raise RuntimeError("直链签名已过期，需重新取流")
        raise d.QualityRejectedError("分辨率 640x360 低于红线 1080")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    d._run_pipeline()
    assert d.load_dead_keys() == set()     # 账本必须是空的


def test_stream_outage_is_never_quality_dead():
    """🔒 红线：「候选流无一入选」绝不能被当成画质判死。

    那是内层的**兜底汇总文案**，语义混装——既可能是"全部流真不达标"，
    也可能是"源站 5xx 导致采样全挂"。TV 侧源站 5xx 频发，后者是常态。

    此前它被列在 _QUALITY_REJECT_CATEGORIES 里，只是靠 _classify_failure
    判它可重试才没出事——那是**巧合性**的安全。现在内层已用
    QualityRejectedError 把两条路径拆开，该文案只代表"混合/纯瞬时"。

    ⚠️ 若哪天有人把它加回画质类目或判死表，源站抽风的集会被永久判死。
    本用例就是那道护栏。
    """
    outage = "本轮候选流无一入选（各流原因见上方日志），下一轮重采"
    # ① 不在画质判死类目里
    assert d.classify_reject_reason(outage) == "候选流无一入选"
    assert "候选流无一入选" not in d._QUALITY_REJECT_CATEGORIES
    # ② 不在整集判死表里 → 仍可重试
    assert d._classify_failure(outage) is True
    # ③ 即便强行传 retriable=False，也不该算画质判死
    assert d.is_quality_dead(False, outage) is False


def test_quality_summary_takes_precedence_over_outage_category():
    """🔒 汇总判死文案里嵌着末节点错误，归类必须优先命中"画质整体不达标"。

    外层概率判死的文案形如「…判定整集画质不达标；末节点错误：本轮候选流无一入选」。
    若"候选流无一入选"规则排在前面，判死集会全被记到那个类目下，
    正好污染要用来评估本口径是否过激的那份数据。
    """
    msg = (
        "2 个节点中 2 个因画质不达标被确定性淘汰（≥ 阈值 1.00），"
        "判定整集画质不达标；末节点错误：本轮候选流无一入选"
    )
    assert d.classify_reject_reason(msg) == "画质整体不达标(判死)"
    assert d._classify_failure(msg) is False      # 确定性失败
    assert d.is_quality_dead(False, msg) is True


# ------------------------------------------- 画质判死：内层分流 + 外层概率判死
def _quality_entry(node_count):
    """构造 node_count 个 m3u8 节点的 entry。"""
    return {
        "tmdbId": "70", "season": 1, "episode": 1,
        "urls": [
            {"url": f"https://s{i}/p.m3u8", "provider": f"p{i}",
             "type": "m3u8", "headers": {}, "quality": None, "size": None}
            for i in range(1, node_count + 1)
        ],
    }


@pytest.fixture
def quality_env(sandbox, monkeypatch):
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    return sandbox


def test_all_nodes_quality_rejected_kills_episode(quality_env, monkeypatch):
    """全部节点画质淘汰 → 概率判死：retriable=False，不再进下一轮。"""
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("分辨率 640x360 低于红线 1080（容差 0.80），跳过")
        ),
    )
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert ok is False
    assert info["retriable"] is False                    # 判死
    assert "判定整集画质不达标" in info["error"]
    assert d.is_quality_dead(info["retriable"], info["error"]) is True


def test_partial_quality_rejection_does_not_kill_at_ratio_1(quality_env,
                                                            monkeypatch):
    """🔒 阈值 1.0 下，只有部分节点画质淘汰**绝不能**判死。

    这正是 TV 侧取 1.0 的意义：另一个节点是源站 5xx（今天挂了、明天可能好），
    整集必须留在重投队列里。
    """
    calls = []

    def fake_master(url, *a, **kw):
        calls.append(url)
        if len(calls) == 1:
            raise d.QualityRejectedError("分辨率 640x360 低于红线 1080")
        raise RuntimeError("HTTP Error 502")

    monkeypatch.setattr(d, "parse_master_playlist", fake_master)
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert ok is False
    assert info["retriable"] is True                     # 仍可重试
    assert "判定整集画质不达标" not in info["error"]
    assert d.is_quality_dead(info["retriable"], info["error"]) is False


def test_single_node_quality_rejection_kills_at_ratio_1(quality_env, monkeypatch):
    """单节点集画质淘汰：1/1 = 1.0 >= 1.0 → 判死。

    ⚠️ 这说明阈值 1.0 对**单节点集**仍等价于"一次判死"——TV 侧单节点集占多数，
    这是该阈值下唯一激进的场景，调阈值前必须知道这一点。
    但它是正确的：只有一个节点且该节点画质确定性不达标，确实没有别的指望。
    """
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("码率未达到门槛：300 kbps < 1480 kbps")
        ),
    )
    label, ok, info = d.process_one_entry(_quality_entry(1), set())
    assert info["retriable"] is False


def test_transient_failures_never_trigger_quality_kill(quality_env, monkeypatch):
    """🔒 红线：纯瞬时失败（源站 5xx）绝不触发画质判死。"""
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("HTTP Error 502")),
    )
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    assert info["retriable"] is True
    assert "判定整集画质不达标" not in info["error"]


def test_quality_kill_ratio_above_one_only_disables_the_summary(quality_env,
                                                                monkeypatch):
    """>1.0 关掉的是**汇总改写**，不是"画质失败变可重试"。

    ⚠️ 这里有个容易误解的点（写测试时踩过）：画质文案本身
    （"低于红线"/"码率未达到"）**已经在 `_PERMANENT_FAILURE_MARKERS` 里**，
    所以全节点画质淘汰时 `any_retriable` 本来就是 False —— 那是单节点文案
    决定的，与概率判死无关。

    概率判死真正做的两件事是：
      ① 把 msg 换成汇总文案（否则留的是末节点错误，会出现
         「retriable=False 却写着 502」这类自相矛盾的记录）；
      ② 在**混合场景**下推翻乐观口径（部分画质+部分瞬时时才有区别）。
    故调高阈值只是不再改写文案，判死与否仍由文案决定。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.5)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("分辨率 640x360 低于红线 1080")
        ),
    )
    label, ok, info = d.process_one_entry(_quality_entry(2), set())
    # 仍是确定性失败（文案决定），但**没有**汇总改写
    assert info["retriable"] is False
    assert "判定整集画质不达标" not in info["error"]


def test_quality_kill_records_to_dead_log(quality_env, monkeypatch):
    """端到端：概率判死的集要落进 download_dead.jsonl，下次运行直接跳过。"""
    line = {"tmdbId": "70", "season": 1, "episode": 1,
            "urls": _quality_entry(2)["urls"]}
    input_path = quality_env / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(quality_env / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(quality_env / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: (_ for _ in ()).throw(
            d.QualityRejectedError("分辨率 640x360 低于红线 1080")
        ),
    )
    d._run_pipeline()
    assert d.load_dead_keys() == {"70_S01E01"}


def test_inner_all_streams_quality_rejected_raises_typed_error(sandbox, monkeypatch):
    """🔒 内层分流：全流画质淘汰 → QualityRejectedError（确定性）。

    与"混合/纯瞬时"必须产生**不同的异常类型与文案**，否则源站抽风会被
    误判成画质不达标。

    ⚠️ 走的是**模式 A 的候选预筛**路径（声明高度低于红线 → 全被排除），
    故必须显式开启红线判定：模式 B 下候选一条不筛，根本走不到这里。
    """
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 0.8)
    # 两条候选流都声明低于红线 → 候选预筛阶段就全被排除
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda *a, **kw: [("640x360", "https://a/1.m3u8", 100),
                          ("854x480", "https://a/2.m3u8", 200)],
    )
    node = {"url": "https://a/m.m3u8", "provider": "vidup", "type": "m3u8",
            "headers": {}, "quality": None, "size": None}
    entry = {"tmdbId": "71", "season": 1, "episode": 1, "urls": [node]}
    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    # 候选预筛的"没有找到高度达标"本身就是 QualityRejectedError
    assert info["retriable"] is False
    assert d.is_quality_dead(info["retriable"], info["error"]) is True


def _inner_loop_env(monkeypatch, stream_outcomes, node_count=1):
    """驱动**内层候选流循环**的公共桩。

    上面那批画质判死用例都是在 parse_master_playlist 上直接抛异常，内层
    「全流画质淘汰 vs 混合失败」的分流代码其实一行都没跑到（反向验证实测：
    把 `quality_rejected_streams > 0 and other_failed_streams == 0` 改成 `or`，
    整个测试文件依旧全绿）。这里让流真的走完采样→红线→码率，才测得到分流。

    stream_outcomes: [("quality" | "transient"), ...]，按候选流顺序生效。
      quality   —— 采样码率远低于门槛 → 内层抛 QualityRejectedError
      transient —— 采样下载抛 502 → 内层记 other_failed_streams
    """
    variants = [
        ("1920x1080", f"https://cdn/v{i}.m3u8", 5000 - i)
        for i in range(len(stream_outcomes))
    ]
    outcome_of = {v[1]: o for v, o in zip(variants, stream_outcomes)}
    seg_urls = [f"https://cdn/s{i}.ts" for i in range(20)]

    monkeypatch.setattr(d, "processing_ids", set())
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)
    monkeypatch.setattr(d, "MIN_RESOLUTION_HEIGHT", 1080)
    monkeypatch.setattr(d, "LENIENCY", 1.0)
    monkeypatch.setattr(d, "SAMPLE_COUNT", 4)
    monkeypatch.setattr(d, "BITRATE_BASELINE", {"h264": 1000.0})
    monkeypatch.setattr(d, "probe_codec", lambda p: "h264")
    monkeypatch.setattr(
        d, "parse_master_playlist", lambda *a, **kw: list(variants)
    )

    current = {"url": None}

    def fake_media(url, headers=None):
        current["url"] = url
        return seg_urls, [4.0] * 20, None

    monkeypatch.setattr(d, "parse_media_playlist", fake_media)

    def fake_download(urls, out, start_idx=0, end_idx=None, concurrency=1,
                      init_url=None, force_init=False, headers=None,
                      retry_max=None):
        if outcome_of[current["url"]] == "transient":
            raise RuntimeError("HTTP Error 502")
        with open(out, "wb") as fh:
            fh.write(b"x")
        # 16s 采样 / 100KB → 50 kbps，远低于 1000 门槛 → 确定性画质淘汰
        return 100_000, [], 0

    monkeypatch.setattr(d, "download_segments", fake_download)

    return {
        "tmdbId": "72", "season": 1, "episode": 1,
        "urls": [
            {"url": f"https://n{i}/m.m3u8", "provider": f"p{i}", "type": "m3u8",
             "headers": {}, "quality": None, "size": None}
            for i in range(1, node_count + 1)
        ],
    }


def test_inner_all_streams_bitrate_rejected_kills_episode(sandbox, monkeypatch):
    """🔒 内层全流因**码率**淘汰 → 抛 QualityRejectedError → 外层计入判死。

    这是走完整条链路（采样→红线→码率→内层分流→外层概率判死）的用例。
    断言 "判定整集画质不达标" 还兼做类型护栏：内层若退回普通 RuntimeError，
    外层的 isinstance 计数就不会加，汇总文案随之消失。
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    entry = _inner_loop_env(monkeypatch, ["quality", "quality"], node_count=1)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "均因画质不达标被淘汰" in info["error"]     # 内层汇总（确定性）
    assert "判定整集画质不达标" in info["error"]       # 外层判死（依赖异常类型）
    assert info["retriable"] is False
    assert d.is_quality_dead(info["retriable"], info["error"]) is True


def test_inner_mixed_failure_never_becomes_quality_kill(sandbox, monkeypatch):
    """🔒 红线：一条流画质淘汰 + 一条流 502 → **绝不能**判死。

    TV 侧源站 5xx 频发，这条混合路径是常态。把它误判成画质不达标等于
    每次源站抽风就永久丢一集，是本功能最危险的失效模式。
    （反向验证靶子：内层分流条件从 and 改成 or，本用例必须转红。）
    """
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    entry = _inner_loop_env(monkeypatch, ["quality", "transient"], node_count=1)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "候选流无一入选" in info["error"]           # 走可重试汇总
    assert "均因画质不达标被淘汰" not in info["error"]
    assert "判定整集画质不达标" not in info["error"]
    assert info["retriable"] is True                   # 留在重投队列
    assert d.is_quality_dead(info["retriable"], info["error"]) is False


def test_inner_all_streams_transient_never_becomes_quality_kill(sandbox,
                                                                 monkeypatch):
    """🔒 红线：全部流都是 502（源站整体抽风）→ 可重试，不判死。"""
    monkeypatch.setattr(d, "QUALITY_KILL_RATIO", 1.0)
    entry = _inner_loop_env(monkeypatch, ["transient", "transient"], node_count=1)
    label, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    assert "候选流无一入选" in info["error"]
    assert info["retriable"] is True
    assert d.is_quality_dead(info["retriable"], info["error"]) is False


def test_permanent_markers_cover_quality_summary_after_reload():
    """🔒 落盘后只剩字符串：汇总判死文案必须能被 _classify_failure 认出。

    failed.jsonl 重载时拿不到异常类型，只有文案。类型与文案互为双保险，
    缺了文案这一层，重载后的判死记录会被当成可重试。
    """
    inner = "全部 3 条候选流均因画质不达标被淘汰（各流原因见上方日志）"
    outer = "2 个节点中 2 个因画质不达标被确定性淘汰（≥ 阈值 1.00），判定整集画质不达标"
    assert d._classify_failure(inner) is False
    assert d._classify_failure(outer) is False


def test_plan_retry_buckets_are_not_mutually_exclusive(monkeypatch):
    """🔑 重投与重取两个桶不互斥。

    多源下"vidup m3u8 挂 5xx + vidlink mp4 签名过期"是常态。若写成互斥分支，
    这类集只会被重投而永远不换新直链，那个 mp4 节点在剩余轮次里都是废的。
    """
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    # 显式标志优先：error 文案里没有 marker，但节点循环发现过期了
    assert d.plan_retry_buckets(True, "HTTP Error 502", True) == (True, True)
    # 缺显式标志时回退按文案判断
    assert d.plan_retry_buckets(
        True, f"x {d._NEEDS_REFETCH_MARKER} y", None
    ) == (True, True)
    assert d.plan_retry_buckets(True, "HTTP Error 502", None) == (True, False)
    # 关掉开关后重取桶恒空
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", False)
    assert d.plan_retry_buckets(True, "x", True) == (True, False)


def test_needs_refetch_flag_survives_non_last_node(sandbox, monkeypatch):
    """🔑 过期节点排在**非末位**时，标志必须仍然为真。

    error 文案只保留最后一个节点的错误。若只按文案判断，
    「节点1 vidlink 403 过期 → 节点2 502」这种常见组合永远读不到过期 marker，
    该直链在剩余所有轮次里都是废的。
    """
    monkeypatch.setattr(d, "wait_for_disk_gate", lambda: None)

    def fake_direct(node, output_path, label, runtime_minutes=None):
        raise RuntimeError(f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: x")

    monkeypatch.setattr(d, "_download_mp4_direct", fake_direct)
    monkeypatch.setattr(
        d, "parse_master_playlist",
        lambda url, retries=None, headers=None: (_ for _ in ()).throw(
            RuntimeError("HTTP Error 502")
        ),
    )
    entry = {"tmdbId": 1, "season": 1, "episode": 1, "urls": [
        {"url": "https://cdn/f.mp4", "provider": "vidlink", "type": "mp4"},
        {"url": "https://m/x.m3u8", "provider": "vidup", "type": "m3u8"},
    ]}
    _, ok, info = d.process_one_entry(entry, set())
    assert ok is False
    # 末节点是 502，文案里本来没有过期 marker
    assert info["needs_refetch"] is True
    assert info["retriable"] is True
    # 补挂到文案里，落盘的 failed.jsonl 也能看出来
    assert d._NEEDS_REFETCH_MARKER in info["error"]
    assert d.plan_retry_buckets(
        info["retriable"], info["error"], info["needs_refetch"]
    ) == (True, True)


def test_merge_next_batch_dedupes_per_episode(sandbox):
    """🔑 合并两个桶时按**集级 key** 去重，重取后的新 entry 优先。

    按 tmdbId 去重会让一部剧每轮只剩一集能重投 —— 这是 TV 侧最容易踩的坑。
    """
    old_e1 = {"tmdbId": "7", "season": 1, "episode": 1, "urls": ["old"]}
    old_e2 = {"tmdbId": "7", "season": 1, "episode": 2, "urls": ["old"]}
    new_e1 = {"tmdbId": "7", "season": 1, "episode": 1, "urls": ["new"]}

    out = d.merge_next_batch([old_e1, old_e2], [new_e1])
    # 同剧两集都在（没被按 tmdbId 误合成一条）
    assert len(out) == 2
    by_key = {d.record_episode_key(e): e for e in out}
    # E01 取重取后的新 urls，E02 保持原样
    assert by_key["7_S01E01"]["urls"] == ["new"]
    assert by_key["7_S01E02"]["urls"] == ["old"]


def test_startup_precheck_dispatches_async_in_streaming_mode(monkeypatch, sandbox):
    """启动预检按运行模式分流，**两种模式都不会被跳过**。

    - 非流式：同步 refresh_stale_entries（换完新链接再开跑）；
    - pipeline：异步 dispatch_stale_entries_async（只投递、立即返回）。
      同步那条路会堵住主事件循环最长 AUTO_REFETCH_TIMEOUT，而 pipeline 的
      前提是主循环一步都不阻塞。

    🔴 2026-09-14 之前流式模式**完全不做预检**，那会造成死闭环：取流侧
    load_processed 跳过已成功的集、不换链接，backlog 里的旧 url 每次运行
    原样重投，永久卡死（§0.20）。
    """
    entries = [_entry("2", fetched_at=1)]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entries[0], ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, ids: (
            "x", False, {"error": "没有找到媒体播放列表", "retriable": False}
        ),
    )
    sync_seen = []
    async_seen = []
    monkeypatch.setattr(
        d, "refresh_stale_entries",
        lambda batch, counts: sync_seen.append(len(batch)) or batch,
    )
    monkeypatch.setattr(
        d, "dispatch_stale_entries_async",
        lambda batch, counts: async_seen.append(len(batch)) or 0,
    )

    # ① 流式模式：pipeline.py 会把 ListEntrySource 换成自己的工厂
    monkeypatch.setattr(d, "ListEntrySource", lambda es: d._ListEntrySource(es))
    d._run_pipeline()
    assert sync_seen == []        # 同步预检不能跑（会堵住主循环）
    assert async_seen == [1]      # 但异步预检必须跑

    # ② 非流式（单独跑 download_tv.py）：走同步预检
    monkeypatch.setattr(d, "ListEntrySource", d._ListEntrySource)
    d._run_pipeline()
    assert sync_seen == [1]
    assert async_seen == [1]      # 未再增加


def test_async_dispatch_respects_quality_dead_and_cap(monkeypatch, sandbox):
    """异步预检与同步预检**共用同一套筛选口径**（_select_stale_entries）。

    两条路径若各写一遍筛选，早晚会漏掉画质判死跳过或限额其中一条，
    而那两条恰恰是 TV 规模下最省代理配额的部分。
    """
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", 10)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 2)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_EPISODE", 2)
    # 第 2 集画质判死 → 必须被跳过
    monkeypatch.setattr(d, "load_quality_dead_keys", lambda: {"2_S01E02"})

    dispatched = []

    class _Hook:
        def dispatch(self, entries):
            dispatched.extend(entries)
            return len(entries)

    monkeypatch.setattr(d, "async_refetch_hook", _Hook())
    # fetched_at 越小越旧；限额 2 时应留下最旧的两集（1 与 3，2 已被判死剔除）
    entries = [
        _entry("1", fetched_at=100),
        _entry("2", fetched_at=50),
        _entry("3", fetched_at=200),
        _entry("4", fetched_at=900),
    ]
    counts = {}
    sent = d.dispatch_stale_entries_async(entries, counts)
    assert sent == 2
    keys = sorted(d.record_episode_key(e) for e in dispatched)
    assert keys == ["1_S01E02", "3_S01E02"]   # 判死的 2 不在；最旧优先
    # 投递即计数，防"预检投一次、下载失败又投一次"超过每集上限
    assert counts["1_S01E02"] == 1


def test_async_dispatch_is_noop_without_hook(monkeypatch):
    """没装钩子（单独跑 download_tv.py）时异步预检是空操作，绝不报错。"""
    monkeypatch.setattr(d, "async_refetch_hook", None)
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", 10)
    assert d.dispatch_stale_entries_async([_entry("1", fetched_at=1)], {}) == 0


def test_async_dispatch_does_not_count_rejected_inflight(monkeypatch, sandbox):
    """钩子拒收（同集重取已在途、返回 0）的集**不计**重取额度。

    AsyncRefetcher.dispatch 对在途同集返回 0；预检若先计数再投，这一次
    "没发生的重取"会白白吃掉每集上限，与 download 分支的口径不一致。
    """
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", 10)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 10)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_EPISODE", 2)
    monkeypatch.setattr(d, "load_quality_dead_keys", lambda: set())

    class _Hook:
        def dispatch(self, entries):
            # 第 2 集"已在途"→拒收
            return 0 if d.record_episode_key(entries[0]) == "2_S01E02" else 1

    monkeypatch.setattr(d, "async_refetch_hook", _Hook())
    counts = {}
    sent = d.dispatch_stale_entries_async(
        [_entry("1", fetched_at=1), _entry("2", fetched_at=1)], counts
    )
    assert sent == 1
    assert counts == {"1_S01E02": 1}


def test_async_dispatch_exception_keeps_already_sent_counts(monkeypatch, sandbox):
    """逐条投递中途抛异常：已投出的照常计数，后续的不再投、不计数。"""
    monkeypatch.setattr(d, "STALE_LINK_SECONDS", 10)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_RUN", 10)
    monkeypatch.setattr(d, "AUTO_REFETCH_MAX_PER_EPISODE", 2)
    monkeypatch.setattr(d, "load_quality_dead_keys", lambda: set())

    class _Hook:
        calls = 0

        def dispatch(self, entries):
            _Hook.calls += 1
            if _Hook.calls == 2:
                raise RuntimeError("boom")
            return 1

    monkeypatch.setattr(d, "async_refetch_hook", _Hook())
    counts = {}
    sent = d.dispatch_stale_entries_async(
        [_entry("1", fetched_at=1), _entry("2", fetched_at=2),
         _entry("3", fetched_at=3)],
        counts,
    )
    assert sent == 1
    assert counts == {"1_S01E02": 1}
    assert _Hook.calls == 2


def test_sync_refetch_refuses_when_async_hook_installed(monkeypatch, capsys):
    """装了 async_refetch_hook 后同步 refetch_entries 必须拒入（锁契约守卫）。

    同步路径用下载侧 log_lock 写 INPUT_JSONL，AsyncRefetcher 用取流侧锁；
    两者并行会交叉追加。守卫返回空并打警告，不动 refetch_counts。
    """
    monkeypatch.setattr(d, "async_refetch_hook", object())
    counts = {}
    assert d.refetch_entries([_entry("1", fetched_at=1)], counts) == []
    assert counts == {}
    assert "同步 refetch_entries 不应被调用" in capsys.readouterr().out


# ------------------------------------------------ 流式来源（pipeline 模式）
def test_list_entry_source_is_three_state():
    """list 来源永不返回 wait —— 它的存货是确定的。"""
    source = d.ListEntrySource([{"tmdbId": "1"}, {"tmdbId": "2"}])
    assert len(source) == 2
    assert source.poll() == ("item", {"tmdbId": "1"})
    assert source.poll() == ("item", {"tmdbId": "2"})
    assert source.poll() == ("done", None)
    # 耗尽后必须稳定返回 done（主循环会重复问）
    assert source.poll() == ("done", None)


def test_missing_input_exits_in_standalone_but_continues_when_streaming(
    sandbox, monkeypatch, capsys,
):
    """results.jsonl 缺失在两种模式下含义完全不同。

    单独跑下载：它是唯一片源，没有就无事可做。
    pipeline 模式：全新部署时它本来就还不存在（TV 侧还要先展开季集才会写出
    第一条），此时若照旧 return，下载侧会在启动瞬间退出、整条流水线只剩取流
    在跑——首次部署必现。
    """
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "nope.jsonl"))
    d._run_pipeline()
    assert "找不到" in capsys.readouterr().out

    # 装上流式来源钩子后，缺文件不再退出，而是等取流实时产出。
    real = d.ListEntrySource
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    class _EmptyStream:
        """立刻收工的流式来源：验的是"缺文件不退出"，不是投递本身。"""

        def __init__(self, entries):
            assert list(entries) == []      # 缺文件 -> 空存量

        def poll(self):
            return "done", None

    monkeypatch.setattr(d, "ListEntrySource", _EmptyStream)
    try:
        d._run_pipeline()
    finally:
        monkeypatch.setattr(d, "ListEntrySource", real)
    assert "等待取流侧实时产出" in capsys.readouterr().out


def test_streaming_source_wait_does_not_block_the_main_loop(sandbox, monkeypatch):
    """来源返回 wait 时主循环必须继续推进在途任务，而不是原地卡住。

    这是 TV 侧季集展开阶段（全新部署约 2.5 小时无产出）的常态：
    poll 一直 wait，此刻 pending 为空，主循环靠 STREAM_IDLE_POLL_SECONDS
    让出 CPU 后重新问来源要货；若写成 wait(空集合) 就是 100% CPU 空转。
    """
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "nope.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    seen = []

    class _SlowStream:
        """前 3 次 wait（模拟展开阶段），然后出一集，再收工。"""

        def __init__(self, entries):
            self.calls = 0

        def poll(self):
            self.calls += 1
            if self.calls <= 3:
                return "wait", None
            if self.calls == 4:
                return "item", {
                    "tmdbId": "1", "season": 1, "episode": 1, "urls": ["u"],
                }
            return "done", None

    monkeypatch.setattr(d, "ListEntrySource", _SlowStream)
    monkeypatch.setattr(d, "process_one_entry", lambda entry, processed: (
        seen.append(d.record_episode_key(entry)),
        (d.record_episode_key(entry), False,
         {"error": "没有找到媒体播放列表", "retriable": False}),
    )[1])

    d._run_pipeline()
    # wait 没有让主循环卡死，后到的那一集仍被消费
    assert seen == ["1_S01E01"]


def test_streaming_new_entry_is_picked_up_while_a_slow_task_is_inflight(
    sandbox, monkeypatch,
):
    """🔑 来源饿着时，新集不能干等在途慢任务完成才被投递。

    「取流侧产出了新集」不是 future 完成事件，无法唤醒 wait(FIRST_COMPLETED)。
    若 wait 无超时，新集就要等某个在途的转封装/上传恰好完成才被顺带发现——
    在途的慢任务恰恰是上传（一集几百 MB 传 R2），最坏几分钟，期间下载槽位全空。

    这构成"假反压"：效果与反压相同却与磁盘压力无关。TV 侧取流是瓶颈，
    "饿着"是常态，损失会被持续放大。

    本例：投出一个 3 秒的在途任务后来源持续 wait，**0.5 秒后**才放出第二集
    （用定时器延后，确保主循环此时已经真正阻塞在 wait 上；若让工作线程立刻
    放行，主线程会在同一次投递循环里就取走它，压根走不到 wait，测不出问题）。
    测量"第二集被投递的时刻"而非总耗时——总耗时必然包含慢任务的 3s。
    """
    monkeypatch.setattr(d, "INPUT_JSONL", str(sandbox / "nope.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    released = threading.Event()
    seen = []
    slow_started_at = {}
    fast_started_at = {}

    class _StarvingStream:
        """先出"慢任务"，随后持续 wait；0.5s 后才放出第二集。

        第二集的可见时机与"在途任务完成"完全解耦，正好检验主循环是否靠超时
        轮询主动发现它。
        """

        def __init__(self, entries):
            self.sent_slow = False
            self.sent_fast = False

        def poll(self):
            if not self.sent_slow:
                self.sent_slow = True
                return "item", {
                    "tmdbId": "1", "season": 1, "episode": 1, "urls": ["slow"],
                }
            if not released.is_set():
                return "wait", None       # 来源饿着
            if not self.sent_fast:
                self.sent_fast = True
                return "item", {
                    "tmdbId": "2", "season": 1, "episode": 1, "urls": ["fast"],
                }
            return "done", None

    def fake_process(entry, processed):
        key = d.record_episode_key(entry)
        seen.append(key)
        if entry["urls"] == ["slow"]:
            # 模拟在途慢任务（上传一集大文件）：它完成前，新集不该被它拖着。
            slow_started_at["t"] = time.time()
            time.sleep(3)
        else:
            fast_started_at["t"] = time.time()
        return key, False, {"error": "没有找到媒体播放列表", "retriable": False}

    monkeypatch.setattr(d, "ListEntrySource", _StarvingStream)
    monkeypatch.setattr(d, "process_one_entry", fake_process)

    timer = threading.Timer(0.5, released.set)
    timer.daemon = True
    timer.start()
    try:
        d._run_pipeline()
    finally:
        timer.cancel()

    assert seen == ["1_S01E01", "2_S01E01"]
    # 关键断言：第二集应在它可取之后（慢任务开始 +0.5s）很快被投递，
    # 而不是等慢任务跑完 3s。无超时的旧实现必然 ≈3s。
    lag = fast_started_at["t"] - slow_started_at["t"]
    assert lag < 1.5, (
        f"新集在慢任务开始 {lag:.1f}s 后才被投递，"
        f"说明 wait 没有按 source_waiting 加超时"
    )


def test_run_pipeline_retries_only_retriable_entries(sandbox, monkeypatch):
    """分批投递不影响多轮语义：只有可重试的失败才进下一轮。"""
    entries = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["u"]},
        {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["u"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", True)
    monkeypatch.setattr(d, "MAX_ROUNDS", 3)
    monkeypatch.setattr(d, "ROUND_COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 1)

    attempts = []

    def fake_process(entry, processed_ids):
        attempts.append(entry["tmdbId"])
        if entry["tmdbId"] == "1":
            return "1", False, {"error": "Read timed out", "retriable": True}
        return "2", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    # tmdbId=1 可重试 -> 跑满 3 轮；tmdbId=2 确定性失败 -> 只跑第一轮。
    assert attempts.count("1") == 3 and attempts.count("2") == 1


def test_round_end_waits_for_inflight_async_refetch_even_without_new_expiry(
    sandbox, monkeypatch,
):
    """🔴 轮末即使本轮没有新过期集，只要异步重取仍有在途也要等它回来并入下一轮。

    第 2 轮起来源是 list，不再直接消费重取结果；预检/上一轮投出的重取若在
    本轮末尚未回来，旧逻辑只在 round_failed_expired 非空时才等——本轮恰好没
    新过期集时直接 break，那批新直链只能靠落盘兜底、当次运行救不回。
    """
    entry_a = {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entry_a, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", True)
    monkeypatch.setattr(d, "MAX_ROUNDS", 2)
    monkeypatch.setattr(d, "ROUND_COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(d, "AUTO_REFETCH_ENABLED", True)
    monkeypatch.setattr(d, "ASYNC_REFETCH_WAIT_SECONDS", 5)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)
    # pipeline 模式：ListEntrySource 被外部替换（这里只需触发 streaming 判定）
    monkeypatch.setattr(d, "ListEntrySource", lambda es: d._ListEntrySource(es))
    monkeypatch.setattr(d, "dispatch_stale_entries_async", lambda batch, counts: 0)

    revived_b = {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["fresh"],
                 "fetched_at": 999}

    class _Hook:
        """模拟"预检投出的重取在首轮末仍在途、稍后才回来"。"""

        def __init__(self):
            self.collects = 0

        def dispatch(self, entries):
            return 0

        def pending_count(self):
            return 0 if self.collects >= 2 else 1

        def collect(self):
            self.collects += 1
            return [revived_b] if self.collects == 2 else []

    monkeypatch.setattr(d, "async_refetch_hook", _Hook())

    attempts = []

    def fake_process(entry, processed_ids):
        attempts.append(d.record_episode_key(entry))
        # 确定性失败且不是直链过期：本轮 round_failed_expired 为空
        return d.record_episode_key(entry), False, {
            "error": "低于红线", "retriable": False,
        }

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    # 首轮只有 A；轮末等到在途重取回来的 B 并入第 2 轮
    assert attempts == ["1_S01E01", "2_S01E01"]


def test_run_pipeline_degrades_when_upload_slots_exhausted(sandbox, monkeypatch):
    """R2 长时间消化不动时不得冻结主循环：降级为留本地 + 写 pending，继续跑下载。

    upload_semaphore 是在**主事件循环线程**里 acquire 的，若无限等待，下载完成的
    future 也没人处理、转封装同步停摆，整条流水线冻结。这里把信号量占满模拟该
    场景，断言：不提交上传、成品仍在本地、pending 有记录、SUCCESS_LOG 标
    uploaded=false（防下次重新下载），且主循环正常收尾。
    """
    entry = {"tmdbId": "7", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entry, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "S3_ENABLED", True)
    # 信号量容量 1 且预先占满 -> acquire 必然超时。
    monkeypatch.setattr(d, "upload_semaphore", __import__("threading").Semaphore(1))
    d.upload_semaphore.acquire()
    monkeypatch.setattr(d, "UPLOAD_SLOT_WAIT_TIMEOUT", 0.05)
    # 等待队列的超时降级靠主循环/排空阶段定时醒来驱动，缩短轮询让测试秒回。
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    final_path = str(sandbox / "downloads" / "tv_000001" / "7_S01E01.mp4")

    monkeypatch.setattr(
        d, "process_one_entry",
        lambda e, ids: ("7_S01E01", True, {"cleanup_paths": []}),
    )
    monkeypatch.setattr(
        d, "finalize_one_entry",
        lambda info, ids: ("7_S01E01", True, {
            "tmdbId": "7", "season": 1, "episode": 1, "title": "S",
            "year": 2020, "final_path": final_path,
        }),
    )
    monkeypatch.setattr(
        d, "upload_one_entry",
        lambda info: pytest.fail("槽位耗尽时不应提交上传"),
    )

    d._run_pipeline()

    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert len(pend) == 1
    assert pend[0]["local_path"] == final_path
    assert "降级为留本地待补传" in pend[0]["fail_reason"]
    success = _read_jsonl(d.SUCCESS_LOG)
    assert len(success) == 1 and success[0]["uploaded"] is False


def test_degrade_writes_no_pending_when_s3_disabled(sandbox, monkeypatch):
    """纯本地模式（s3.enabled=false）降级时不得写 pending。

    本地模式的成品本来就不需要补传，塞进 pending 只会污染 reupload 的输入。
    但仍要写 SUCCESS_LOG(uploaded=false) 以免下次运行重新下载。
    """
    entry = {"tmdbId": "8", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        json.dumps(entry, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "S3_ENABLED", False)
    monkeypatch.setattr(d, "upload_semaphore", __import__("threading").Semaphore(1))
    d.upload_semaphore.acquire()
    monkeypatch.setattr(d, "UPLOAD_SLOT_WAIT_TIMEOUT", 0.05)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)

    final_path = str(sandbox / "downloads" / "tv_000001" / "8_S01E01.mp4")
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda e, ids: ("8_S01E01", True, {"cleanup_paths": []}),
    )
    monkeypatch.setattr(
        d, "finalize_one_entry",
        lambda info, ids: ("8_S01E01", True, {
            "tmdbId": "8", "season": 1, "episode": 1, "title": "S",
            "year": 2020, "final_path": final_path,
        }),
    )

    d._run_pipeline()

    assert not os.path.exists(d.UPLOAD_PENDING_LOG)
    success = _read_jsonl(d.SUCCESS_LOG)
    assert len(success) == 1 and success[0]["uploaded"] is False


def _pipeline_two_entries(sandbox, monkeypatch, s3_enabled=True):
    """两集串行下载的最小流水线环境：MAX_WORKERS=1 保证集 1 先于集 2 处理。"""
    entries = [
        {"tmdbId": "11", "season": 1, "episode": 1, "urls": ["u"]},
        {"tmdbId": "12", "season": 1, "episode": 1, "urls": ["u"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in entries),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "MAX_WORKERS", 1)
    monkeypatch.setattr(d, "S3_ENABLED", s3_enabled)
    monkeypatch.setattr(d, "STREAM_IDLE_POLL_SECONDS", 0.01)
    # 容量 1 且预先占满：转封装完的集拿不到槽，只能进等待队列。
    monkeypatch.setattr(d, "upload_semaphore", threading.Semaphore(1))
    d.upload_semaphore.acquire()


def test_upload_slot_wait_does_not_block_the_main_loop(sandbox, monkeypatch):
    """等上传槽位期间主循环必须继续收割下载 future、提交后续转封装。

    A2 修复前，conversion 分支在主事件循环线程里 acquire(timeout=300)：集 1
    转封装完拿不到槽，主循环冻结 300s——期间集 2 下载完也没人提交转封装，
    pipeline 模式下更会连带取流队列打满。修复后拿不到槽只是挂进等待队列。

    编排：集 2 的下载**等集 1 转封装完成后**才结束（保证集 1 已在等槽）；
    集 2 的转封装一旦被提交（说明主循环没被冻结），后台线程释放槽位 ->
    集 1 应被正常提交上传而非降级。若主循环仍会冻结，集 2 的转封装要等
    集 1 超时降级后才会提交，pending 里就会出现集 1。
    """
    _pipeline_two_entries(sandbox, monkeypatch)
    monkeypatch.setattr(d, "UPLOAD_SLOT_WAIT_TIMEOUT", 2.0)

    conv1_done = threading.Event()
    conv2_started = threading.Event()
    uploaded = []

    def fake_process(entry, ids):
        key = d.record_episode_key(entry)
        if key == "12_S01E01":
            assert conv1_done.wait(5), "集 1 转封装迟迟未完成"
            time.sleep(0.05)   # 留给主循环收割集 1 的转封装 future 并入队
        return key, True, {"cleanup_paths": []}

    def fake_finalize(info, ids):
        # process_one_entry 的返回里没带身份，靠调用顺序区分：第 1 次是集 1
        # （MAX_WORKERS=1 且集 2 的下载要等集 1 转封装完才结束）。
        if not conv1_done.is_set():
            conv1_done.set()
            tmdb = "11"
        else:
            conv2_started.set()
            tmdb = "12"
        return f"{tmdb}_S01E01", True, {
            "tmdbId": tmdb, "season": 1, "episode": 1, "title": "S",
            "year": 2020,
            "final_path": str(sandbox / "downloads" / f"{tmdb}_S01E01.mp4"),
        }

    def fake_upload(info):
        uploaded.append(info["tmdbId"])
        return f"{info['tmdbId']}_S01E01", True, info

    def release_when_conv2_submitted():
        if conv2_started.wait(5):
            d.upload_semaphore.release()

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    monkeypatch.setattr(d, "finalize_one_entry", fake_finalize)
    monkeypatch.setattr(d, "upload_one_entry", fake_upload)
    threading.Thread(target=release_when_conv2_submitted, daemon=True).start()

    started = time.monotonic()
    d._run_pipeline()

    assert sorted(uploaded) == ["11", "12"]
    assert not os.path.exists(d.UPLOAD_PENDING_LOG)
    # 没有任何一集等满 UPLOAD_SLOT_WAIT_TIMEOUT。
    assert time.monotonic() - started < 2.0


def test_upload_slot_waiters_degrade_immediately_on_interrupt(sandbox, monkeypatch):
    """Ctrl+C 后等槽位的集不得再干等到超时，应立即降级收尾。

    修复前的 acquire(timeout) 不看 interrupted，Ctrl+C 后最长还要卡 300s。
    """
    _pipeline_two_entries(sandbox, monkeypatch)
    monkeypatch.setattr(d, "UPLOAD_SLOT_WAIT_TIMEOUT", 30.0)
    ev = threading.Event()
    monkeypatch.setattr(d, "interrupted", ev)

    def fake_finalize(info, ids):
        # 转封装完成后不久置位中断：此刻该集已在等待队列里（或马上进队）。
        threading.Timer(0.05, ev.set).start()
        return "x", True, {
            "tmdbId": "11", "season": 1, "episode": 1, "title": "S",
            "year": 2020,
            "final_path": str(sandbox / "downloads" / "11_S01E01.mp4"),
        }

    monkeypatch.setattr(
        d, "process_one_entry",
        lambda e, ids: (d.record_episode_key(e), True, {"cleanup_paths": []}),
    )
    monkeypatch.setattr(d, "finalize_one_entry", fake_finalize)
    monkeypatch.setattr(
        d, "upload_one_entry",
        lambda info: pytest.fail("中断后不应再提交上传"),
    )

    started = time.monotonic()
    d._run_pipeline()

    assert time.monotonic() - started < 5.0
    pend = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert pend and all("收到中断信号" in p["fail_reason"] for p in pend)
    success = _read_jsonl(d.SUCCESS_LOG)
    assert success and all(s["uploaded"] is False for s in success)


def test_degrade_success_log_is_deduped_by_key(sandbox, monkeypatch):
    """降级写 SUCCESS_LOG 必须按集级 key 覆盖，保持"每集一条"。

    若用追加写，同一集在降级后又被 reupload 补传成功，会残留两条记录，
    违背 SUCCESS_LOG 的设计意图。
    """
    monkeypatch.setattr(d, "S3_ENABLED", True)
    # 预置一条旧记录，模拟该集此前已被写过。
    (sandbox / "success.jsonl").write_text(
        json.dumps({
            "tmdbId": "9", "season": 1, "episode": 1, "uploaded": True,
            "s3_key": "old/key.mp4",
        }) + "\n",
        encoding="utf-8",
    )
    d.update_success_log("9_S01E01", {
        "tmdbId": "9", "season": 1, "episode": 1,
        "final_path": "/tmp/x.mp4", "uploaded": False,
    })
    records = _read_jsonl(d.SUCCESS_LOG)
    assert len(records) == 1
    assert records[0]["uploaded"] is False
    assert "s3_key" not in records[0]


def test_run_pipeline_keeps_latest_duplicate_and_skips_invalid(sandbox, monkeypatch):
    """输入去重保留最后一条；缺身份字段的行读入即跳过。

    同一集在多次运行/复扫后会有多行，越晚写入的 url 越新 —— 对 vidlink 这类
    带时效签名的直链，保留首次出现等于拿一条早已过期的链接白跑一趟。
    缺 tmdbId/season/episode 的行无法定位到集，放进流水线只会放大成等量的
    确定性失败记录，故在读入阶段就计数跳过。
    """
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["old"]},
        {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["only"]},
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["new"]},  # 覆盖首条
        {"urls": ["x"]},                       # 缺全部身份字段
        {"tmdbId": "3", "urls": ["y"]},        # 缺 season/episode
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    seen = []

    def fake_process(entry, processed_ids):
        seen.append((d.record_episode_key(entry), entry.get("urls")))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()

    keys = sorted(k for k, _ in seen)
    assert keys == ["1_S01E01", "2_S01E01"]      # 残缺行未进入流水线
    urls_of_1 = dict(seen)["1_S01E01"]
    assert urls_of_1 == ["new"]                   # 保留的是最后一条


def test_run_pipeline_preserves_input_order(sandbox, monkeypatch):
    """去重改用 dict 后仍须保持输入文件顺序（dict 保插入序）。"""
    lines = [
        {"tmdbId": str(i), "season": 1, "episode": 1, "urls": ["u"]}
        for i in (5, 3, 9, 1)
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "DOWNLOAD_QUEUE_DEPTH", 1)
    monkeypatch.setattr(d, "MAX_WORKERS", 1)

    order = []

    def fake_process(entry, processed_ids):
        order.append(entry["tmdbId"])
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert order == ["5", "3", "9", "1"]


def test_run_pipeline_duplicate_picks_newest_fetched_at(sandbox, monkeypatch):
    """同集多行时按 fetched_at 取最新，而非文件里最后一条。

    取流侧多轮重试会让"更早成功、后写入"的情况真实存在（例如某集第 1 轮
    拿到 url 后写入，第 3 轮复扫又补写了一条更旧缓存）。vidlink 直链带时效
    签名，用旧链接下载等于确定失败，所以时间戳必须压过文件位置。
    """
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["newest"],
         "fetched_at": 2000},
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["older"],
         "fetched_at": 1000},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(entry.get("urls"))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert seen == [["newest"]]


def test_run_pipeline_duplicate_stamped_beats_unstamped(sandbox, monkeypatch):
    """有 fetched_at 的一定胜过无戳的旧格式行，即便无戳行排在后面。"""
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["stamped"],
         "fetched_at": 100},
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["legacy"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(entry.get("urls"))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert seen == [["stamped"]]


def test_run_pipeline_skips_processed_ids_at_read_time(sandbox, monkeypatch):
    """已处理集在读入阶段就被滤掉，不进线程池。

    第二次全量重跑时 success.jsonl 已有数万条，逐个提交再由
    process_one_entry 跳过等于白白付出等量的调度与锁竞争开销。
    """
    lines = [
        {"tmdbId": "1", "season": 1, "episode": 1, "urls": ["a"]},
        {"tmdbId": "2", "season": 1, "episode": 1, "urls": ["b"]},
    ]
    input_path = sandbox / "results.jsonl"
    input_path.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines),
        encoding="utf-8",
    )
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: {"1_S01E01"})
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(d.record_episode_key(entry))
        return "x", False, {"error": "低于红线", "retriable": False}

    monkeypatch.setattr(d, "process_one_entry", fake_process)
    d._run_pipeline()
    assert seen == ["2_S01E01"]


# --------------------------------------------- 画质判死账本（download_dead.jsonl）
def test_parse_dead_quality_evidence_extracts_numbers():
    """从判死文案回抽码率/门槛/分辨率/编码，供门槛变更后离线复判。"""
    msg = "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps"
    evidence = d.parse_dead_quality_evidence(msg)
    assert evidence == {
        "bitrate_kbps": 372.0,
        "threshold_kbps": 1600.0,
        "resolution": "854x480",
        "codec": "h264",
    }


@pytest.mark.parametrize("msg", [
    "",
    None,
    "分辨率 640x360 低于红线 1080（容差 0.80），跳过",   # 无码率数值
    "候选流无一入选",
])
def test_parse_dead_quality_evidence_returns_none_without_bitrate(msg):
    """没有码率数值就没有可存的依据，--retry-dead 对这些只能整集放回。"""
    assert d.parse_dead_quality_evidence(msg) is None


def test_record_and_load_dead_keys_roundtrip(sandbox):
    """写进账本的集，下次启动会被 load_dead_keys 认出来。"""
    entry = {"tmdbId": "7", "season": 1, "episode": 2, "title": "T"}
    msg = "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps"
    d.record_quality_dead(entry, msg, urls=["u1", "u2"])

    records = _read_jsonl(sandbox / "download_dead.jsonl")
    assert len(records) == 1
    assert records[0]["tmdbId"] == "7"
    assert records[0]["reason_class"] == "quality"
    assert records[0]["node_count"] == 2
    assert records[0]["evidence"]["bitrate_kbps"] == 372.0

    assert d.load_dead_keys() == {"7_S01E02"}


def test_load_dead_keys_uses_conjunction_not_row_order(sandbox):
    """🔒 同一集多行时用**合取**裁决，结论不随追加顺序漂移。

    账本是纯追加的，同一集会有多行。若逐行 add/discard，最终结论取决于哪一行
    排在最后——这里放一条"有依据且仍不达标"和一条"无依据"，无论顺序如何，
    只要有任一条证明仍不达标就必须继续跳过。
    """
    # 第一条有依据且远低于门槛；第二条无依据（单看它会被放回）。
    d.record_quality_dead(
        {"tmdbId": "9", "season": 1, "episode": 1},
        "分辨率 854x480 流（h264）码率未达到门槛：100 kbps < 1600 kbps",
    )
    d.record_quality_dead(
        {"tmdbId": "9", "season": 1, "episode": 1},
        "候选流无一入选",          # parse 不出依据
    )
    # 正常运行：全部判死集一律跳过。
    assert d.load_dead_keys() == {"9_S01E01"}
    # --retry-dead：合取语义下，那条"仍不达标"的记录把它按住。
    assert d.load_dead_keys(d.dead_record_passes_now) == {"9_S01E01"}


def test_dead_record_passes_now_respects_current_threshold(monkeypatch):
    """门槛调松到实测码率之下（且留足余量）时才放回。"""
    monkeypatch.setattr(d, "DEAD_REVIVE_MARGIN", 1.05)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    record = {"evidence": {"bitrate_kbps": 1000.0, "resolution": "1920x1080",
                           "codec": "h264"},
              # 判死当时的模式必须与当前一致，否则门槛口径不可比、一律不放回。
              "resolution_check_enabled": False}
    # 门槛 2000 → 远高于实测 1000，仍不达标。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 2000.0)
    assert d.dead_record_passes_now(record) is False
    # 门槛降到 900：900 × 1.05 = 945 <= 1000 → 放回。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 900.0)
    assert d.dead_record_passes_now(record) is True
    # 门槛 960：960 × 1.05 = 1008 > 1000 → 余量不足，不放回（防来回震荡）。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 960.0)
    assert d.dead_record_passes_now(record) is False


def test_dead_record_passes_now_blocks_cross_mode_records(monkeypatch):
    """🔒 判死当时的模式与当前不一致 → 一律不放回。

    两种模式的门槛能差 5 倍（模式 A 的 480p 门槛约 316 kbps，模式 B 是
    1600 kbps）。拿当前模式的门槛去复判另一个模式下判死的记录，结论毫无意义：
    切到模式 B 后，一批模式 A 下判死的低清集会因为"旧门槛低"而被误放回去白跑。
    老记录没有该字段 → 视为未知 → 同样不放回。
    """
    monkeypatch.setattr(d, "DEAD_REVIVE_MARGIN", 1.0)
    # 门槛远低于实测码率：若不是模式守卫拦着，下面三条都会放回。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 1.0)
    evidence = {"bitrate_kbps": 1000.0, "resolution": "1920x1080",
                "codec": "h264"}

    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    # 模式一致 → 正常复判，放回。
    assert d.dead_record_passes_now(
        {"evidence": evidence, "resolution_check_enabled": False}) is True
    # 模式不一致 → 不放回。
    assert d.dead_record_passes_now(
        {"evidence": evidence, "resolution_check_enabled": True}) is False
    # 老记录缺字段 → 视为未知 → 不放回。
    assert d.dead_record_passes_now({"evidence": evidence}) is False


def test_dead_record_passes_now_revives_when_no_evidence():
    """没有依据就无法证明它现在仍不达标 → 放回（宁可多下不误杀）。"""
    assert d.dead_record_passes_now({"evidence": {}}) is True
    assert d.dead_record_passes_now({}) is True


def test_dead_record_passes_now_blocks_missing_height(monkeypatch):
    """🔒 模式 A 下 height 抽不出来时一律不放回。

    模式 A 的 bitrate_threshold 按 (h/1080)² 缩放，height=0 会让门槛恒为 0，
    `0 × 余量 <= 任何码率` 恒真 —— 这批记录会被无条件放回去白跑。

    ⚠️ 该守卫**只在模式 A 下需要**：模式 B 的门槛是绝对线、height 根本不参与
    计算，此时挡住反而会把"未知分辨率"那批本该正常复判的记录永久关在门外。
    """
    monkeypatch.setattr(d, "DEAD_REVIVE_MARGIN", 1.05)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", True)
    record = {"evidence": {"bitrate_kbps": 50.0, "resolution": "未知分辨率"},
              "resolution_check_enabled": True}
    assert d.dead_record_passes_now(record) is False


def test_dead_record_missing_height_still_judged_in_mode_b(monkeypatch):
    """对照组：模式 B 下 height 缺失不该挡住复判（门槛与 height 无关）。"""
    monkeypatch.setattr(d, "DEAD_REVIVE_MARGIN", 1.0)
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    record = {"evidence": {"bitrate_kbps": 1000.0,
                           "resolution": d.UNKNOWN_RESOLUTION,
                           "codec": "h264"},
              "resolution_check_enabled": False}
    # 门槛降到 500 → 500 <= 1000 → 正常放回，不因 height 缺失被一刀切。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 500.0)
    assert d.dead_record_passes_now(record) is True
    # 门槛仍高 → 照常挡住。
    monkeypatch.setattr(d, "bitrate_threshold", lambda h, c: 1600.0)
    assert d.dead_record_passes_now(record) is False


def test_record_quality_dead_stores_current_mode(sandbox, monkeypatch):
    """🔒 判死落盘必须带上当时的模式，否则跨模式守卫无从判断。"""
    monkeypatch.setattr(d, "RESOLUTION_CHECK_ENABLED", False)
    entry = {"tmdbId": "9", "season": 1, "episode": 3, "title": "t"}
    d.record_quality_dead(entry, "码率未达到门槛：372 kbps < 1600 kbps（h264，实测 854x480）")
    rows = [
        json.loads(line)
        for line in open(d.DOWNLOAD_DEAD_LOG, encoding="utf-8").read().splitlines()
        if line.strip()
    ]
    assert rows[-1]["resolution_check_enabled"] is False
    # 模式 B 的文案也要能抽出证据（分辨率/编码写在尾部括号里）。
    assert rows[-1]["evidence"]["resolution"] == "854x480"
    assert rows[-1]["evidence"]["codec"] == "h264"


def test_quality_dead_is_recorded_and_skipped_next_run(sandbox, monkeypatch):
    """端到端：画质判死的集落账本，下一次运行不再被投递。"""
    line = {"tmdbId": "5", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))

    seen = []

    def fake_process(entry, processed_ids):
        seen.append(d.record_episode_key(entry))
        return "5_S01E01", False, {
            "error": "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps",
            "retriable": False,
        }

    monkeypatch.setattr(d, "process_one_entry", fake_process)

    d._run_pipeline()
    assert seen == ["5_S01E01"]                 # 第一轮确实跑了
    assert d.load_dead_keys() == {"5_S01E01"}   # 判死已落账本

    # 第二次运行：同样的输入，这次应被跳过（不再重新采样求证同一结论）。
    seen.clear()
    d._run_pipeline()
    assert seen == []


def test_transient_failure_is_not_recorded_as_dead(sandbox, monkeypatch):
    """🔒 红线：瞬时失败绝不能进判死账本。

    源站 5xx / 超时是"今天源站挂了"，重取完全可能换到好流；
    把它们当画质判死会永久放弃可救回的集，直接违背"尽可能提高成功率"。
    """
    line = {"tmdbId": "6", "season": 1, "episode": 1, "urls": ["u"]}
    input_path = sandbox / "results.jsonl"
    input_path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    monkeypatch.setattr(d, "INPUT_JSONL", str(input_path))
    monkeypatch.setattr(d, "DOWNLOAD_OK_LOG", str(sandbox / "download_ok.jsonl"))
    monkeypatch.setattr(d, "DOWNLOAD_FAIL_LOG", str(sandbox / "download_fail.jsonl"))
    monkeypatch.setattr(d, "MULTI_ROUND_ENABLED", False)
    monkeypatch.setattr(d, "MAX_ROUNDS", 1)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    monkeypatch.setattr(d, "scan_downloaded_mp4_ids", lambda: (set(), {}))
    monkeypatch.setattr(
        d, "process_one_entry",
        lambda entry, processed: ("6_S01E01", False,
                                  {"error": "HTTP Error 502", "retriable": True}),
    )
    d._run_pipeline()
    assert d.load_dead_keys() == set()


# ------------------------------------------------ failed.jsonl 轮转
def test_compact_failed_log_keeps_useful_rows(sandbox, monkeypatch):
    """轮转后：历史整体归档，两类仍有用的行原样回填。"""
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 1)   # 任何非空文件都超限
    monkeypatch.setattr(d, "_SCRIPT_DIR", sandbox)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: {"3_S01E01"})

    rows = [
        # ① 待重取：必须回填，否则重取闭环断裂
        {"tmdbId": "1", "season": 1, "episode": 1, "stage": "download",
         "error": "直链失效，需重新取流", "retriable": False},
        # ② 画质判死：必须回填，load_quality_dead_keys 要读
        {"tmdbId": "2", "season": 1, "episode": 1, "stage": "download",
         "error": "分辨率 854x480 流（h264）码率未达到门槛：372 kbps < 1600 kbps",
         "retriable": False},
        # ③ 已成功的集：不必再留
        {"tmdbId": "3", "season": 1, "episode": 1, "stage": "download",
         "error": "需重新取流", "retriable": False},
        # ④ 普通瞬时失败：不必留（下次运行本来就会自动重投）
        {"tmdbId": "4", "season": 1, "episode": 1, "stage": "download",
         "error": "HTTP Error 502", "retriable": True},
        # ⑤ 上传阶段失败：与重取/画质都无关
        {"tmdbId": "5", "season": 1, "episode": 1, "stage": "upload",
         "error": "R2 timeout", "retriable": True},
    ]
    with open(sandbox / "failed.jsonl", "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    rotated, archive_path, kept = d.compact_failed_log()
    assert rotated is True and kept == 2
    # 历史零丢失：归档里是完整的 5 行
    assert len(_read_jsonl(archive_path)) == 5
    # 新文件只剩 ①②
    remaining = {r["tmdbId"] for r in _read_jsonl(sandbox / "failed.jsonl")}
    assert remaining == {"1", "2"}


def test_compact_keeps_non_dead_quality_row_via_refetch_marker(sandbox,
                                                                monkeypatch):
    """🔒 轮转口径与判死口径一致：有节点没给出画质结论的行**不是**判死行。

    它靠"需重新取流"这条规则回填（那才是这集真正的出路）。若轮转时忽略
    node_failures，它会被当成判死行回填——回填结果碰巧一样，但 reason 全错，
    且下游 load_quality_dead_keys 会据此永久跳过它。
    """
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 1)
    monkeypatch.setattr(d, "_SCRIPT_DIR", sandbox)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())

    row = {
        "tmdbId": "7", "season": 1, "episode": 1, "stage": "download",
        # 末节点文案是画质的，但节点1 从未给出画质结论
        "error": "分辨率 640x360 低于红线 1080",
        "retriable": False,
        "node_failures": [
            {"reason_class": "源站5xx"},
            {"reason_class": "分辨率低于红线"},
        ],
    }
    with open(sandbox / "failed.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    d.compact_failed_log()
    # 不被判死：重载时不进画质判死集合
    monkeypatch.setattr(d, "FAILED_LOG", str(sandbox / "failed.jsonl"))
    monkeypatch.setattr(d, "AUTO_REFETCH_SKIP_QUALITY_DEAD", True)
    assert d.load_quality_dead_keys() == set()


def test_failed_row_useful_honors_node_failures(sandbox, monkeypatch):
    """🔒 轮转口径必须与判死口径一致（`_failed_row_still_useful`）。

    一行"末节点画质不达标、但另有节点只报 5xx"的记录**不是**判死行：
    它整集仍可重试，下次运行本来就会自动重投，无须占用回填名额。
    忽略 node_failures 的话它会被误当判死行长期回填，且下游
    load_quality_dead_keys 会据此永久跳过它。
    """
    monkeypatch.setattr(d, "load_success_log_ids", lambda: set())
    row = {
        "tmdbId": "7", "season": 1, "episode": 1, "stage": "download",
        "error": "分辨率 640x360 低于红线 1080",   # 末节点文案是画质的
        "retriable": False,
        "node_failures": [
            {"reason_class": "源站5xx"},          # 从未给出画质结论
            {"reason_class": "分辨率低于红线"},
        ],
    }
    assert d._failed_row_still_useful(row, set(), set()) is False

    # 对照组：全部节点都是画质结论 → 真判死行，必须回填
    dead_row = dict(row, node_failures=[
        {"reason_class": "分辨率低于红线"},
        {"reason_class": "码率未达门槛"},
    ])
    assert d._failed_row_still_useful(dead_row, set(), set()) is True


def test_compact_failed_log_skips_when_another_process_runs(sandbox, monkeypatch):
    """🔒 跨进程守卫：别的 downloader 在跑时绝不轮转。

    否则那个进程的 fd 指向旧 inode，os.replace 之后它写的每一条都进了
    archive、新文件里没有 —— 那批待重取记录就此蒸发。
    """
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 1)
    monkeypatch.setattr(d, "_SCRIPT_DIR", sandbox)
    monkeypatch.setattr(d, "is_main_running", lambda: True)
    (sandbox / "failed.jsonl").write_text('{"tmdbId":"1"}\n', encoding="utf-8")

    assert d.compact_failed_log() == (False, None, 0)
    # 原文件原封不动
    assert (sandbox / "failed.jsonl").read_text(encoding="utf-8")


def test_compact_failed_log_disabled_by_zero(sandbox, monkeypatch):
    monkeypatch.setattr(d, "FAILED_LOG_MAX_BYTES", 0)
    monkeypatch.setattr(d, "is_main_running", lambda: False)
    (sandbox / "failed.jsonl").write_text('{"tmdbId":"1"}\n', encoding="utf-8")
    assert d.compact_failed_log() == (False, None, 0)


# ---------------------------------------------------------------- reupload
def test_reupload_pending_flow(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "20260905")
    folder = sandbox / "downloads" / "tv_000001"
    folder.mkdir(parents=True)
    ok_file = folder / "1_S01E01.mp4"
    ok_file.write_bytes(b"a")
    bad_file = folder / "2_S01E01.mp4"
    bad_file.write_bytes(b"b")

    # Pre-existing logs to be reconciled.
    (sandbox / "success.jsonl").write_text(
        json.dumps({"tmdbId": "1", "season": 1, "episode": 1, "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    (sandbox / "failed.jsonl").write_text(
        json.dumps({"tmdbId": "1", "season": 1, "episode": 1, "stage": "upload"}) + "\n"
        + json.dumps({"tmdbId": "2", "season": 1, "episode": 1, "stage": "upload"}) + "\n",
        encoding="utf-8",
    )
    pend = [
        # older duplicate for same episode (should be superseded)
        {"tmdbId": "1", "season": 1, "episode": 1, "local_path": "/nope", "s3_key": "old"},
        {"tmdbId": "1", "season": 1, "episode": 1, "local_path": str(ok_file),
         "s3_key": "", "year": 2001, "title": "T"},
        {"tmdbId": "2", "season": 1, "episode": 1, "local_path": str(bad_file),
         "s3_key": "2002/2/S01/20260901/E01.mp4"},
        {"tmdbId": "3", "season": 1, "episode": 1, "local_path": "/gone/3.mp4",
         "s3_key": "x"},                                        # orphan
        {"tmdbId": "4", "local_path": "/x"},                    # movie-style -> ignored
        {"tmdbId": "5", "season": "x", "episode": 1, "local_path": "/x"},  # bad key -> ignored
    ]
    (sandbox / "pending.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in pend), encoding="utf-8",
    )

    calls = []

    def fake_upload(path, key):
        calls.append((path, key))
        return (True, None) if path == str(ok_file) else (False, "still down")

    monkeypatch.setattr(d, "upload_to_r2", fake_upload)
    d.reupload_pending()

    # Episode 1: rebuilt key from year, uploaded, local removed, logs reconciled.
    assert calls == [
        (str(ok_file), "tv/2001/1/S01/E01/E01.mp4"),
        (str(bad_file), "2002/2/S01/20260901/E01.mp4"),   # existing s3_key reused
    ]
    assert not ok_file.exists()
    assert bad_file.exists()
    succ = _read_jsonl(d.SUCCESS_LOG)
    assert len(succ) == 1
    assert succ[0]["uploaded"] is True and succ[0]["reupload"] is True
    assert succ[0]["year"] == 2001 and succ[0]["season"] == 1
    failed = _read_jsonl(d.FAILED_LOG)
    assert [d.record_episode_key(r) for r in failed] == ["2_S01E01"]
    remaining = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert [d.record_episode_key(r) for r in remaining] == ["2_S01E01"]
    assert remaining[0]["fail_reason"] == "still down"
    out = capsys.readouterr().out
    assert "孤儿(本地已无)清理 1" in out
    assert "成功 1" in out and "仍失败 1" in out


def _pending_fixture(sandbox, n):
    """n 条本地文件都在的 pending 记录（tmdbId 1..n），返回 [(record, path)]。"""
    folder = sandbox / "downloads" / "tv_pend"
    folder.mkdir(parents=True, exist_ok=True)
    items = []
    for i in range(1, n + 1):
        path = folder / f"{i}_S01E01.mp4"
        path.write_bytes(b"v")
        items.append(({
            "tmdbId": str(i), "season": 1, "episode": 1, "year": 2000 + i,
            "local_path": str(path), "s3_key": f"k{i}", "fail_reason": "orig",
            "ts": "orig-ts",
        }, path))
    (sandbox / "pending.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r, _ in items), encoding="utf-8",
    )
    return items


def test_reupload_pending_interrupt_keeps_untouched_records(sandbox, monkeypatch, capsys):
    """中断后：已成功的移出 pending，已失败的更新原因，**未轮到的原样保留**。
    这是 B3 的核心不变量——否则未轮到的记录会被"只写 remaining"抹掉，
    本地文件还在却再无人认领。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", True)
    monkeypatch.setattr(d.time, "strftime", lambda fmt: "new-ts")
    items = _pending_fixture(sandbox, 5)
    calls = []

    def fake_upload(path, key):
        calls.append(key)
        if key == "k1":
            return True, None
        # 第 2 条失败，并在此刻模拟用户 Ctrl+C：第 3~5 条不应再被尝试。
        d.interrupted.set()
        return False, "net down"

    monkeypatch.setattr(d, "upload_to_r2", fake_upload)
    try:
        d.reupload_pending()
    finally:
        d.interrupted.clear()

    assert calls == ["k1", "k2"]
    assert not items[0][1].exists()                # 成功的已删本地
    assert all(p.exists() for _, p in items[1:])   # 其余本地都还在
    remaining = _read_jsonl(d.UPLOAD_PENDING_LOG)
    assert [r["tmdbId"] for r in remaining] == ["2", "3", "4", "5"]
    # 失败的那条更新了原因/时间戳；未轮到的三条与原记录逐字段一致。
    assert remaining[0]["fail_reason"] == "net down" and remaining[0]["ts"] == "new-ts"
    for got, (orig, _) in zip(remaining[1:], items[2:]):
        assert got == orig
    succ = _read_jsonl(d.SUCCESS_LOG)
    assert [r["tmdbId"] for r in succ] == ["1"] and succ[0]["uploaded"] is True
    assert not os.path.exists(d.UPLOAD_PENDING_LOG + ".tmp")
    out = capsys.readouterr().out
    assert "收到中断信号" in out and "剩余 3 条" in out
    assert "中断保留 3" in out and "pending 剩余 4 条" in out


def test_reupload_pending_interrupt_before_first_keeps_all(sandbox, monkeypatch):
    """进入循环前就已中断：一条都不传，pending 逐字节等价于原内容（去重后）。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    items = _pending_fixture(sandbox, 3)
    calls = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: calls.append(k) or (True, None))
    d.interrupted.set()
    try:
        d.reupload_pending()
    finally:
        d.interrupted.clear()
    assert calls == []
    assert _read_jsonl(d.UPLOAD_PENDING_LOG) == [r for r, _ in items]
    assert all(p.exists() for _, p in items)


def test_reupload_pending_rewrites_atomically(sandbox, monkeypatch):
    """pending 重写走 tmp + os.replace：replace 之前旧文件必须完整无损，
    这样二次信号 os._exit / 强杀落在中间时不会留下半截 pending。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DELETE_LOCAL_AFTER_UPLOAD", False)
    _pending_fixture(sandbox, 3)
    original = (sandbox / "pending.jsonl").read_text(encoding="utf-8")
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: (k == "k1", None if k == "k1" else "x"))

    seen = {}
    real_replace = os.replace

    def spy_replace(src, dst):
        # SUCCESS_LOG / FAILED_LOG 的重写也走 os.replace，只盯 pending 那一次。
        if dst == d.UPLOAD_PENDING_LOG:
            seen["src"], seen["dst"] = src, dst
            seen["old_intact"] = open(dst, encoding="utf-8").read() == original
            seen["tmp_lines"] = open(src, encoding="utf-8").read().count("\n")
        return real_replace(src, dst)

    monkeypatch.setattr(d.os, "replace", spy_replace)
    d.reupload_pending()

    assert seen["dst"] == d.UPLOAD_PENDING_LOG
    assert seen["src"] == d.UPLOAD_PENDING_LOG + ".tmp"
    assert seen["old_intact"] is True
    assert seen["tmp_lines"] == 2
    assert [r["tmdbId"] for r in _read_jsonl(d.UPLOAD_PENDING_LOG)] == ["2", "3"]
    assert not os.path.exists(d.UPLOAD_PENDING_LOG + ".tmp")


def test_reupload_pending_refuses_when_main_running(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(d, "S3_ENABLED", True)
    d.acquire_main_lock()
    (sandbox / "pending.jsonl").write_text("{}\n", encoding="utf-8")
    called = []
    monkeypatch.setattr(d, "upload_to_r2", lambda p, k: called.append(1))
    d.reupload_pending()
    assert called == []
    assert "正在运行" in capsys.readouterr().out
    d.release_main_lock()


def test_reupload_pending_disabled_or_missing(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(d, "S3_ENABLED", False)
    d.reupload_pending()
    assert "无需补传" in capsys.readouterr().out
    monkeypatch.setattr(d, "S3_ENABLED", True)
    d.reupload_pending()
    assert "无待补传文件" in capsys.readouterr().out


# ---------------------------------------------------------------- path resolution
def test_resolve_file_and_dir_anchor_to_script_dir():
    root = str(d._SCRIPT_DIR)
    assert d.resolve_file(None, "x.jsonl") == os.path.join(root, "x.jsonl")
    assert d.resolve_file("  ", "x.jsonl") == os.path.join(root, "x.jsonl")
    assert d.resolve_file("sub/y.jsonl", "x") == os.path.join(root, "sub", "y.jsonl")
    assert d.resolve_dir("/abs/dir", "x") == "/abs/dir"
    assert d.BASE_DIR.startswith(root)
    assert d.MAIN_LOCK_FILE == os.path.join(root, "download_tv.main.lock")


# ------------------------------------------------ 收尾自动补传（auto_reupload）
def _main_env(monkeypatch, sandbox):
    """把 main() 的重活全部桩掉，只留收尾段的接线可观测。"""
    monkeypatch.setattr(d, "S3_ENABLED", True)
    monkeypatch.setattr(d, "DISK_GUARD_ENABLED", False)
    monkeypatch.setattr(d, "install_interrupt_handler", lambda: None)
    monkeypatch.setattr(d, "preflight_check_ffmpeg", lambda: None)
    monkeypatch.setattr(d, "preflight_check_s3", lambda: None)
    monkeypatch.setattr(d, "_run_pipeline", lambda: None)
    monkeypatch.setattr(d, "compact_failed_log", lambda: (False, None, 0))


def test_auto_reupload_runs_after_lock_release(sandbox, monkeypatch):
    """🔴 自动补传必须在 release_main_lock() **之后**。

    reupload_pending 内部有 is_main_running() 跨进程守卫：锁还没释放时它会
    认为"主流程正在跑"而直接返回，静默什么也不做 —— 功能等于没接上，
    且不会有任何报错提示。
    """
    _main_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REUPLOAD_ENABLED", True)
    seen = {}
    monkeypatch.setattr(
        d, "reupload_pending",
        lambda: seen.update(main_running=d.is_main_running()),
    )
    d.main()
    # 被调用了，且调用时锁已释放（否则内部守卫会把它挡掉）
    assert seen == {"main_running": False}


def test_auto_reupload_runs_before_log_rotation(sandbox, monkeypatch):
    """🔴 补传必须在 compact_failed_log() **之前**。

    补传成功会删掉 stage=="upload" 的失败行；先轮转再补传的话，
    归档下来的就不是最终态（那些行本该已被消解）。
    """
    _main_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REUPLOAD_ENABLED", True)
    order = []
    monkeypatch.setattr(d, "reupload_pending", lambda: order.append("reupload"))
    monkeypatch.setattr(
        d, "compact_failed_log",
        lambda: order.append("rotate") or (False, None, 0),
    )
    d.main()
    assert order == ["reupload", "rotate"]


def test_auto_reupload_skipped_when_pipeline_raises(sandbox, monkeypatch):
    """🔴 主流程异常/中断时**不补传**：状态未知，且用户正想让它停下。

    这也是它必须放在 finally **之外**（正常路径上）的原因。
    """
    _main_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REUPLOAD_ENABLED", True)
    called = []
    monkeypatch.setattr(d, "reupload_pending", lambda: called.append(1))

    def boom():
        raise RuntimeError("下载流水线炸了")

    monkeypatch.setattr(d, "_run_pipeline", boom)
    with pytest.raises(RuntimeError):
        d.main()
    assert called == []
    # 锁仍必须被释放（finally 里）
    assert not os.path.exists(d.MAIN_LOCK_FILE)

    # Ctrl+C 同理
    called.clear()

    def interrupted():
        raise KeyboardInterrupt()

    monkeypatch.setattr(d, "_run_pipeline", interrupted)
    with pytest.raises(KeyboardInterrupt):
        d.main()
    assert called == []


def test_auto_reupload_failure_does_not_fail_the_run(sandbox, monkeypatch):
    """补传是收尾动作，它失败不该把一次已跑完的运行变成异常退出。

    ⚠️ 必须连 SystemExit 一起兜住（reupload 内部可能因配置问题抛它），
    但 KeyboardInterrupt 要放过 —— Ctrl+C 该中止整个进程。
    """
    _main_env(monkeypatch, sandbox)
    monkeypatch.setattr(d, "AUTO_REUPLOAD_ENABLED", True)
    rotated = []
    monkeypatch.setattr(
        d, "compact_failed_log",
        lambda: rotated.append(1) or (False, None, 0),
    )

    for exc in (RuntimeError("R2 挂了"), SystemExit(2)):
        rotated.clear()

        def boom():
            raise exc

        monkeypatch.setattr(d, "reupload_pending", boom)
        d.main()                 # 不应抛出
        assert rotated == [1]    # 后续收尾照常执行

    # KeyboardInterrupt 必须逃逸
    def ctrl_c():
        raise KeyboardInterrupt()

    monkeypatch.setattr(d, "reupload_pending", ctrl_c)
    with pytest.raises(KeyboardInterrupt):
        d.main()


def test_auto_reupload_respects_switches(sandbox, monkeypatch):
    """开关关掉、或未开启 R2 上传时都不补传（纯本地模式没有 pending 可言）。"""
    _main_env(monkeypatch, sandbox)
    called = []
    monkeypatch.setattr(d, "reupload_pending", lambda: called.append(1))

    monkeypatch.setattr(d, "AUTO_REUPLOAD_ENABLED", False)
    d.main()
    assert called == []

    monkeypatch.setattr(d, "AUTO_REUPLOAD_ENABLED", True)
    monkeypatch.setattr(d, "S3_ENABLED", False)
    d.main()
    assert called == []


# ---------------------------------------------------------------- 内嵌字幕


class _FakeSubResp:
    """字幕下载用的假响应：iter_content 按 chunk 吐出 body。"""

    def __init__(self, body, status=200):
        self.body = body
        self.status_code = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise d.requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i:i + chunk_size]


class _FakeSubSession:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def get(self, url, timeout=None, headers=None, stream=None):
        self.calls.append((url, headers))
        body = self.mapping[url]
        if isinstance(body, Exception):
            raise body
        return _FakeSubResp(body)


def _sub_env(monkeypatch, mapping):
    session = _FakeSubSession(mapping)
    monkeypatch.setattr(d, "get_session", lambda: session)
    monkeypatch.setattr(d, "SUBTITLES_ENABLED", True)
    monkeypatch.setattr(d, "SUBTITLE_LANGUAGES", ["en", "zh"])
    monkeypatch.setattr(d, "SUBTITLE_FORMATS", ["vtt", "srt"])
    return session


def test_save_subtitles_writes_both_formats(sandbox, monkeypatch):
    """srt 源必须同时落 srt 与 vtt 两份，路径相对集目录。"""
    srt = "1\n00:00:01,000 --> 00:00:02,000\nhi\n"
    _sub_env(monkeypatch, {"http://s/en.srt": srt.encode()})

    saved = d.save_subtitles(7, 1, 3, 2020, [
        {"language": "en", "url": "http://s/en.srt"},
    ])
    assert saved == ["subs/en.vtt", "subs/en.srt"]

    folder = os.path.join(d.episode_dir(7, 1, 3, 2020), "subs")
    vtt = open(os.path.join(folder, "en.vtt"), encoding="utf-8").read()
    # 格式转换必须真的发生：VTT 要有文件头、毫秒分隔符是点。
    assert vtt.startswith("WEBVTT")
    assert "00:00:01.000" in vtt
    assert open(os.path.join(folder, "en.srt"), encoding="utf-8").read() == srt


def test_save_subtitles_detects_format_by_content(sandbox, monkeypatch):
    """源站声明的 type 不可信，一律按内容判定：body 是 vtt 就不能再套一层头。"""
    _sub_env(monkeypatch, {
        "http://s/x": b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nhi\n",
    })
    d.save_subtitles(7, 1, 3, 2020, [
        {"language": "en", "url": "http://s/x", "type": "srt"},
    ])
    folder = os.path.join(d.episode_dir(7, 1, 3, 2020), "subs")
    vtt = open(os.path.join(folder, "en.vtt"), encoding="utf-8").read()
    assert vtt.count("WEBVTT") == 1
    srt = open(os.path.join(folder, "en.srt"), encoding="utf-8").read()
    assert "WEBVTT" not in srt and "00:00:01,000" in srt


def test_save_subtitles_filters_languages_and_dedupes(sandbox, monkeypatch):
    """白名单外的语种一律丢弃；同语种只取第一条。"""
    session = _sub_env(monkeypatch, {
        "http://s/en1": b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\na\n",
    })
    saved = d.save_subtitles(7, 1, 3, 2020, [
        {"language": "fr", "url": "http://s/fr"},
        {"language": "en", "url": "http://s/en1"},
        {"language": "en", "url": "http://s/en2"},
        "not-a-dict",
    ])
    assert saved == ["subs/en.vtt", "subs/en.srt"]
    assert [c[0] for c in session.calls] == ["http://s/en1"]


def test_save_subtitles_passes_caption_headers(sandbox, monkeypatch):
    """取流侧算好的 headers 必须原样带上——两套 CDN 鉴权方向相反，丢了就拉不到。"""
    session = _sub_env(monkeypatch, {
        "http://s/en": b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\na\n",
    })
    d.save_subtitles(7, 1, 3, 2020, [
        {"language": "en", "url": "http://s/en",
         "headers": {"Referer": "https://peak/"}},
    ])
    assert session.calls[0][1] == {"Referer": "https://peak/"}


def test_save_subtitles_failure_is_non_fatal(sandbox, monkeypatch):
    """单条字幕失败只跳过该语种，其余照常保存，整体不抛异常。"""
    _sub_env(monkeypatch, {
        "http://s/en": RuntimeError("boom"),
        "http://s/zh": "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\n\u4f60\u597d\n".encode(),
    })
    saved = d.save_subtitles(7, 1, 3, 2020, [
        {"language": "en", "url": "http://s/en"},
        {"language": "zh", "url": "http://s/zh"},
    ])
    assert saved == ["subs/zh.vtt", "subs/zh.srt"]


def test_save_subtitles_rejects_oversized_body(sandbox, monkeypatch):
    """超过上限立刻中止，不把疑似视频/错误页整个读进内存，也不留下空文件。"""
    _sub_env(monkeypatch, {"http://s/en": b"x" * 5000})
    monkeypatch.setattr(d, "SUBTITLE_MAX_BYTES", 100)
    saved = d.save_subtitles(7, 1, 3, 2020, [
        {"language": "en", "url": "http://s/en"},
    ])
    assert saved == []
    # 目录推迟到真拿到内容才建，失败时不该留空 subs/
    assert not os.path.isdir(os.path.join(d.episode_dir(7, 1, 3, 2020), "subs"))


def test_save_subtitles_disabled_or_empty(sandbox, monkeypatch):
    """开关关掉、或取流侧没给 captions 时都直接返回空，不发任何请求。"""
    session = _sub_env(monkeypatch, {})
    monkeypatch.setattr(d, "SUBTITLES_ENABLED", False)
    assert d.save_subtitles(7, 1, 3, 2020,
                            [{"language": "en", "url": "http://s/en"}]) == []

    monkeypatch.setattr(d, "SUBTITLES_ENABLED", True)
    assert d.save_subtitles(7, 1, 3, 2020, None) == []
    assert session.calls == []


def test_build_meta_lists_subtitles(sandbox):
    """meta.json 的 subtitles[] 要按 语种/格式/路径 三元组填实。"""
    meta = d.build_meta({}, {"tmdbId": 7, "season": 1, "episode": 3},
                        ["subs/en.vtt", "subs/zh.srt"])
    assert meta["subtitles"] == [
        {"language": "en", "format": "vtt", "path": "subs/en.vtt"},
        {"language": "zh", "format": "srt", "path": "subs/zh.srt"},
    ]
    # 没抓到字幕时为空列表，等 fetch_subtitles.py 事后补
    assert d.build_meta({}, {"tmdbId": 7, "season": 1,
                             "episode": 3})["subtitles"] == []


def test_collect_sidecar_assets_includes_subtitles(sandbox):
    """上传清单必须带上字幕，否则字幕只留在本地、传不到 R2。"""
    assets = d.collect_sidecar_assets({
        "tmdbId": 7, "season": 1, "episode": 3, "year": 2020,
        "subtitle_files": ["subs/en.vtt", "subs/en.srt"],
        "has_meta": True,
    })
    assert assets == ["subs/en.vtt", "subs/en.srt", "meta.json"]


def test_finalize_one_entry_saves_captions(sandbox, monkeypatch):
    """端到端接线：conversion_job 的 captions -> subs/ 落盘 -> meta + success_info。

    这条链路断在任何一环，字幕都只会静默消失（不报错、不影响成败），
    所以必须有一个覆盖全链的用例把它钉住。
    """
    temp = sandbox / "temp"
    temp.mkdir()
    ts = temp / "t.ts"
    ts.write_bytes(b"x")
    monkeypatch.setattr(d, "convert_ts_to_mp4",
                        lambda s, dst: open(dst, "wb").write(b"mp4") and True)
    _sub_env(monkeypatch, {
        "http://s/en": b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nhi\n",
    })

    d.processing_ids.add("1_S01E01")
    _, ok, info = d.finalize_one_entry({
        "tmdbId": "1", "season": 1, "episode": 1, "normalized_id": "1_S01E01",
        "title": "T", "year": 2001, "url": "u", "final_ts": str(ts),
        "temp_mp4": str(temp / "t.mp4"), "cleanup_paths": [str(ts)],
        "bitrate_kbps": 1, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
        "captions": [{"language": "en", "url": "http://s/en"}],
    }, set())

    assert ok is True
    assert info["subtitle_files"] == ["subs/en.vtt", "subs/en.srt"]
    folder = os.path.dirname(info["final_path"])
    assert os.path.isfile(os.path.join(folder, "subs", "en.vtt"))
    with open(os.path.join(folder, "meta.json"), encoding="utf-8") as fh:
        assert json.load(fh)["subtitles"][0]["language"] == "en"
    # 上传清单必须带上字幕
    assert "subs/en.vtt" in d.collect_sidecar_assets(info)


def test_finalize_one_entry_survives_subtitle_crash(sandbox, monkeypatch):
    """🔴 字幕阶段整个炸掉也不能把已下好的一集判失败——尽力而为。"""
    temp = sandbox / "temp"
    temp.mkdir()
    ts = temp / "t.ts"
    ts.write_bytes(b"x")
    monkeypatch.setattr(d, "convert_ts_to_mp4",
                        lambda s, dst: open(dst, "wb").write(b"mp4") and True)

    def boom(*a, **kw):
        raise RuntimeError("字幕模块整个炸了")

    monkeypatch.setattr(d, "save_subtitles", boom)

    d.processing_ids.add("1_S01E01")
    _, ok, info = d.finalize_one_entry({
        "tmdbId": "1", "season": 1, "episode": 1, "normalized_id": "1_S01E01",
        "title": "T", "year": 2001, "url": "u", "final_ts": str(ts),
        "temp_mp4": str(temp / "t.mp4"), "cleanup_paths": [str(ts)],
        "bitrate_kbps": 1, "resolution": "1920x1080",
        "missing_segment_count": 0, "missing_segment_indices": [],
        "captions": [{"language": "en", "url": "http://s/en"}],
    }, set())

    assert ok is True and info["subtitle_files"] == []
    assert os.path.exists(info["final_path"])
    assert info["has_meta"] is True          # meta 照常生成
