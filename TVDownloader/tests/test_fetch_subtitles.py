"""fetch_subtitles.py 的离线用例（TV 版，不联网）。

核心约束：
  1. **字幕是"可有可无"的附属物** —— 取不到、出错、缺配置都不能让流程失败
     或以非零码退出；
  2. **落点与下载侧严格同构** —— 字幕必须落在集目录下的 subs/，R2 对象键
     与视频同前缀，否则前端按同前缀列举拿不到；
  3. **季集匹配必须精确** —— 多集包/整季包绝不能把别的集的字幕存成本集。
"""

import io
import json
import os
import shutil
import threading
import zipfile

import pytest

import fetch_subtitles as f


def _zip_bytes(files):
    """构造一个内存 zip：{文件名: 文本内容}。"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    return buffer.getvalue()


class _FakeZipResp:
    """模拟流式响应：SubDL zip 下载走 stream=True + iter_content。"""

    def __init__(self, content=b""):
        self._content = content

    def iter_content(self, chunk_size=65536):
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i:i + chunk_size]

    def close(self):
        pass


_SRT = "1\n00:00:01,000 --> 00:00:03,500\nHello\n"


def _entry(tid="55", season=1, episode=3, year=2000, title="T"):
    """构造一条与 load_entries 输出同构的条目。"""
    return {
        "key": f.episode_key(tid, season, episode),
        "tmdbId": tid, "season": season, "episode": episode,
        "year": year, "title": title,
    }


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """把落盘目录指到 tmp_path，并给定固定的语种/格式。

    默认关掉 REMOTE_MODE：这批用例验的是抓取/转换/季集匹配本身，走本地落盘
    最直接。R2 模式（生产默认）的专属行为在 remote_sandbox 那组用例里单独锁。
    """
    monkeypatch.setattr(f.dt, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(f.dt, "FOLDER_PREFIX", "tv")
    monkeypatch.setattr(f.dt, "S3_PREFIX", "")
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en", "zh"])
    monkeypatch.setattr(f, "SUBTITLE_FORMATS", ["vtt", "srt"])
    monkeypatch.setattr(f, "SUBDL_API_KEYS", ["TESTKEY"])
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["TESTKEY"]))
    monkeypatch.setattr(f, "STATE_LOG", str(tmp_path / "subtitles.jsonl"))
    monkeypatch.setattr(f, "LEDGER_LOG", str(tmp_path / "subtitles_gaps.jsonl"))
    monkeypatch.setattr(f, "REMOTE_MODE", False)
    # 停止闸门与其原因都是模块级共享状态。不换新的话，一旦某个用例把它置位，
    # 后面所有用例的 download_one 都会直接返回跳过 —— 用例间互相污染。
    monkeypatch.setattr(f, "stop_fetching", threading.Event())
    monkeypatch.setattr(f, "stop_reason", {})
    # 限流冷却是真 sleep，用例里一律归零；冷却窗口也要重置，免得串台。
    monkeypatch.setattr(f, "THROTTLE_COOLDOWN", 0)
    monkeypatch.setattr(f, "_throttle_until", 0.0)
    # 本地模式只给"集目录还在"的集补字幕；用例里常用的几个先把目录造出来。
    for tid in ("1", "2", "55", "56"):
        os.makedirs(f.dt.episode_dir(tid, 1, 3, 2000), exist_ok=True)
    return tmp_path


# ------------------------------------------------ 目录结构与下载侧保持同构

def test_subs_dir_sits_next_to_the_video(sandbox):
    """字幕必须落在集目录下的 subs/，与视频、meta.json 同级。"""
    assert f.subs_dir("55", 1, 3, 2000) == os.path.join(
        f.dt.BASE_DIR, "tv", "2000", "55", "S01", "E03", "subs"
    )


def test_subs_dir_uses_same_year_fallback_as_downloader(sandbox):
    """year 缺失时两侧必须落到同一个 unknown_year，否则字幕与视频分家。"""
    assert f.subs_dir("55", 1, 3, None).startswith(
        f.dt.episode_dir("55", 1, 3, None)
    )
    assert "unknown_year" in f.subs_dir("55", 1, 3, None)


def test_subtitle_s3_key_shares_prefix_with_video(sandbox):
    """R2 侧字幕键必须与视频同前缀 —— 这是前端一次列举拿全的前提。"""
    video = f.dt.build_s3_key("55", 1, 3, 2000)
    sub = f.dt.build_s3_key("55", 1, 3, 2000, f"{f.dt.SUBS_SUBDIR}/en.srt")
    assert video == "tv/2000/55/S01/E03/E03.mp4"
    assert sub == "tv/2000/55/S01/E03/subs/en.srt"


def test_local_mode_skips_deleted_episode_dir(sandbox, monkeypatch):
    """集目录已被用户删掉：不该凭空造出一个只有 subs/ 的孤立目录。"""
    shutil.rmtree(f.dt.episode_dir("55", 1, 3, 2000))
    called = []
    monkeypatch.setattr(f, "search_subtitles",
                        lambda *a, **k: (called.append(1), [])[1])
    _, result = f.download_one(_entry())
    assert result["status"] == "local_missing"
    assert called == []
    assert not os.path.exists(f.dt.episode_dir("55", 1, 3, 2000))


# ---------------------------------------------- 「可有可无」：不因字幕而失败

def test_missing_api_key_exits_quietly(monkeypatch, capsys):
    """没配 API Key 只跳过，不能抛 SystemExit —— 否则 cron/&& 串联会中断。"""
    monkeypatch.setattr(f, "SUBDL_API_KEYS", [])
    f.main()   # 不抛异常即通过
    assert "跳过字幕补全" in capsys.readouterr().out


def test_missing_success_log_returns_empty(sandbox, monkeypatch, capsys):
    """success.jsonl 不存在是正常状态（还没下过片），不是错误。"""
    monkeypatch.setattr(f, "SUCCESS_LOG", str(sandbox / "nope.jsonl"))
    assert f.load_entries() == []
    assert "无需补字幕" in capsys.readouterr().out


def test_search_failure_does_not_raise(sandbox, monkeypatch):
    """SubDL 查询失败只标记该集，不抛异常。"""
    def boom(*_a, **_k):
        raise RuntimeError("network down")

    monkeypatch.setattr(f, "search_subtitles", boom)
    _, result = f.download_one(_entry())
    assert result["status"] == "search_failed"
    assert "network down" in result["error"]


def test_worker_exception_does_not_kill_the_batch(sandbox, monkeypatch, capsys):
    """单集异常必须被主循环兜住，后面的集照跑。"""
    monkeypatch.setattr(f, "SUCCESS_LOG", str(sandbox / "success.jsonl"))
    with open(f.SUCCESS_LOG, "w", encoding="utf-8") as fh:
        for ep in (3, 4):
            fh.write(json.dumps({
                "tmdbId": "55", "season": 1, "episode": ep, "year": 2000,
            }) + "\n")

    def explode(entry, ledger=None):
        if entry["episode"] == 3:
            raise RuntimeError("worker blew up")
        return entry["key"], {"status": "ok", "saved": [], "missing": []}

    monkeypatch.setattr(f, "download_one", explode)
    f._run()
    out = capsys.readouterr().out
    assert "worker blew up" in out
    assert "全部待补剧集已处理完毕" in out


def test_state_log_failure_is_swallowed(sandbox, monkeypatch, capsys):
    """状态日志写不进去也不能影响主流程 —— 它本身就是可有可无的。"""
    monkeypatch.setattr(f, "STATE_LOG", str(sandbox / "nodir" / "s.jsonl"))
    f.write_state({"key": "55_S01E03"})
    assert "状态日志写入失败" in capsys.readouterr().out


# ------------------------------------------------------ 已有语种不重复抓

def test_languages_already_present_are_skipped(sandbox, monkeypatch):
    """本地已有全部语种就整集跳过，一个请求都不该发。"""
    target = f.subs_dir("55", 1, 3, 2000)
    os.makedirs(target, exist_ok=True)
    for name in ("en.srt", "en.vtt", "zh.srt", "zh.vtt"):
        with open(os.path.join(target, name), "w", encoding="utf-8") as fh:
            fh.write(_SRT)

    called = []
    monkeypatch.setattr(f, "search_subtitles",
                        lambda *a, **k: (called.append(1), [])[1])
    _, result = f.download_one(_entry())
    assert result["status"] == "skipped"
    assert called == []


def test_only_missing_language_is_fetched(sandbox, monkeypatch):
    """已有 en 时只查 zh：省请求也省额度。"""
    target = f.subs_dir("55", 1, 3, 2000)
    os.makedirs(target, exist_ok=True)
    for name in ("en.srt", "en.vtt"):
        with open(os.path.join(target, name), "w", encoding="utf-8") as fh:
            fh.write(_SRT)

    asked = {}

    def fake_search(tid, season, episode, languages=None):
        asked["languages"] = list(languages or [])
        return []

    monkeypatch.setattr(f, "search_subtitles", fake_search)
    _, result = f.download_one(_entry())
    assert asked["languages"] == ["zh"]
    assert result["missing"] == ["zh"]


# ------------------------------------------------------------ 季集匹配（TV 特有）

def test_search_sends_season_and_episode_as_tv(sandbox, monkeypatch):
    """查询必须带 type=tv + season_number + episode_number。

    用错 type 会查到电影编号空间里一部完全不相干的片。
    """
    seen = []

    class _Resp:
        def json(self):
            return {"status": True, "subtitles": []}

    def fake_request(method, url, params=None, **kwargs):
        seen.append(dict(params or {}))
        return _Resp()

    monkeypatch.setattr(f, "request_with_keys", fake_request)
    f.search_subtitles("55", 1, 3, ["en"])
    assert seen[0]["type"] == "tv"
    assert seen[0]["season_number"] == 1
    assert seen[0]["episode_number"] == 3
    assert seen[0]["tmdb_id"] == "55"


def test_pick_candidates_rejects_other_seasons():
    """季不符的条目必须被剔除，否则会把 S02 的字幕配到 S01 上。"""
    subs = [
        {"language": "EN", "season": 2, "episode": 3, "url": "/wrong"},
        {"language": "EN", "season": 1, "episode": 3, "url": "/right"},
    ]
    picked = f.pick_candidates(subs, "en", 1, 3)
    assert [p["url"] for p in picked] == ["/right"]


def test_pick_candidates_rejects_other_episodes():
    subs = [
        {"language": "EN", "season": 1, "episode": 4, "url": "/wrong"},
        {"language": "EN", "season": 1, "episode": 3, "url": "/right"},
    ]
    picked = f.pick_candidates(subs, "en", 1, 3)
    assert [p["url"] for p in picked] == ["/right"]


def test_pick_candidates_skips_full_season_packs():
    """整季包没有可靠的集标记，一律不用。"""
    subs = [
        {"language": "EN", "season": 1, "episode": 3, "full_season": True,
         "url": "/pack"},
        {"language": "EN", "season": 1, "episode": 3, "url": "/single"},
    ]
    picked = f.pick_candidates(subs, "en", 1, 3)
    assert [p["url"] for p in picked] == ["/single"]


def test_multi_episode_pack_is_matched_by_range():
    """多集包的 episode 字段只是起始集，必须按 [from, end] 范围判定覆盖。

    按 episode 精确比对的话，一个 E01-E10 的合集会被误判成只有 E01，
    E02..E10 全部白白错过。
    """
    pack = {"language": "EN", "season": 1, "episode": 1,
            "episode_from": 1, "episode_end": 10, "url": "/pack"}
    assert f.pick_candidates([pack], "en", 1, 5) == [pack]
    assert f.pick_candidates([pack], "en", 1, 11) == []


def test_episode_range_detects_packs():
    assert f._episode_range({"episode_from": 1, "episode_end": 10}) == (1, 10)
    # 乱序也要归一
    assert f._episode_range({"episode_from": 10, "episode_end": 1}) == (1, 10)
    # 单集条目不是多集包
    assert f._episode_range({"episode_from": 3, "episode_end": 3}) is None
    assert f._episode_range({"episode": 3}) is None


def test_pack_extracts_the_right_episode_file(sandbox):
    """多集包里必须按文件名的集标记挑出本集。"""
    data = _zip_bytes({
        "Show.S01E03.srt": _SRT + "third\n",
        "Show.S01E04.srt": _SRT + "fourth-and-longer-content\n" * 5,
    })
    content, ext = f.extract_srt(data, 1, 3, require_match=True)
    assert ext == ".srt"
    assert b"third" in content
    assert b"fourth" not in content


def test_pack_matches_the_1x03_naming_style(sandbox):
    data = _zip_bytes({"Show.1x03.srt": _SRT + "third\n",
                       "Show.1x04.srt": _SRT + "fourth\n"})
    content, _ = f.extract_srt(data, 1, 3, require_match=True)
    assert b"third" in content


def test_episode_marker_does_not_match_longer_numbers(sandbox):
    """S01E03 不能命中 S01E030 —— 前后紧邻数字必须排除。"""
    data = _zip_bytes({"Show.S01E030.srt": _SRT})
    assert f.extract_srt(data, 1, 3, require_match=True) == (None, None)


def test_pack_without_our_episode_is_given_up(sandbox):
    """多集包里挑不出本集时宁可放弃，绝不能把别的集存成本集。"""
    data = _zip_bytes({"Show.S01E07.srt": _SRT, "Show.S01E08.srt": _SRT})
    assert f.extract_srt(data, 1, 3, require_match=True) == (None, None)


def test_single_pack_falls_back_to_largest_file(sandbox):
    """单集包里文件名常是通用的，没有集标记时回退到体积最大的那个。"""
    data = _zip_bytes({"generic.srt": _SRT + "x" * 100,
                       "tiny.srt": _SRT})
    content, _ = f.extract_srt(data, 1, 3, require_match=False)
    assert b"x" * 100 in content


def test_download_candidate_requires_match_only_for_packs(sandbox, monkeypatch):
    """单集条目不强制集标记，多集包强制 —— require_match 的取值来自条目本身。"""
    seen = {}
    real_extract = f.extract_srt

    def spy(data, season=None, episode=None, require_match=False):
        seen["require_match"] = require_match
        return real_extract(data, season, episode, require_match)

    monkeypatch.setattr(f, "extract_srt", spy)
    monkeypatch.setattr(
        f, "request_with_keys",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"g.srt": _SRT})),
    )
    target = sandbox / "out"
    target.mkdir()

    f._download_candidate({"url": "/a"}, "en", 1, 3, str(target))
    assert seen["require_match"] is False

    f._download_candidate(
        {"url": "/a", "episode_from": 1, "episode_end": 10}, "en", 1, 3,
        str(target),
    )
    assert seen["require_match"] is True


# ------------------------------------------------------------ 格式转换与落盘

def test_srt_source_is_written_as_both_formats(sandbox):
    target = sandbox / "out"
    target.mkdir()
    saved = f._write_variants(str(target), "en", _SRT, "srt")
    assert sorted(saved) == ["en.srt", "en.vtt"]
    assert (target / "en.vtt").read_text(encoding="utf-8").startswith("WEBVTT")


def test_ass_is_not_written(sandbox):
    """ASS 前端 <track> 不认，落盘会把该语种永久"锁死"（_language_of 认为已有）。"""
    target = sandbox / "out"
    target.mkdir()
    ass = "[Script Info]\nTitle: x\n[V4+ Styles]\n"
    assert f._write_variants(str(target), "en", ass, "ass") == []
    assert os.listdir(target) == []


def test_stale_ass_file_does_not_count_as_existing(sandbox):
    """R2/本地遗留的 en.ass 不能算"已有 en"，否则该语种永不再补。"""
    assert f._language_of("en.ass") is None
    assert f._language_of("en.srt") == "en"


@pytest.mark.parametrize("name,expected", [
    ("en.srt", "en"), ("zh.vtt", "zh"), ("zh-CN.srt", "zh-cn"),
    ("subs/en.srt", "en"), ("noext", None), ("en.ass", None), (".srt", None),
])
def test_language_extraction(name, expected):
    assert f._language_of(name) == expected


@pytest.mark.parametrize("text,declared,expected", [
    ("WEBVTT\n\n00:01.000 --> 00:02.000\nhi\n", "srt", "vtt"),
    ("1\n00:00:01,000 --> 00:00:02,000\nhi\n", "vtt", "srt"),
    ("[Script Info]\nTitle: x\n", "srt", "ass"),
])
def test_sniff_format_trusts_content_over_extension(text, declared, expected):
    """扩展名是传闻，正文才是事实：源站两个方向都出过名实不符的情况。"""
    assert f._sniff_format(text, declared) == expected


def test_extract_prefers_srt_over_bigger_ass_in_same_zip():
    data = _zip_bytes({"a.ass": "[Script Info]\n" + "x" * 500, "b.srt": _SRT})
    content, ext = f.extract_srt(data)
    assert ext == ".srt"
    assert b"Hello" in content


def test_oversized_zip_entry_is_rejected(sandbox, monkeypatch):
    """zip 炸弹：压缩后很小、解压后巨大，必须按声明的解压大小拦下。"""
    monkeypatch.setattr(f, "MAX_ZIP_BYTES", 100)
    data = _zip_bytes({"big.srt": _SRT + "x" * 5000})
    assert f.extract_srt(data) == (None, None)


# ------------------------------------------------------------ load_entries

def test_load_entries_dedupes_and_keeps_identity(sandbox, monkeypatch):
    log = sandbox / "success.jsonl"
    lines = [
        {"tmdbId": "346", "season": 1, "episode": 3, "title": "A",
         "year": 2000, "uploaded": True},
        "not json",
        {"tmdbId": "346", "season": "1", "episode": "3", "title": "A2",
         "year": 2000, "uploaded": True},
        {"tmdbId": "346", "season": 1, "episode": 4, "title": "A",
         "year": 2000, "uploaded": True},
        {"tmdbId": "999", "title": "no season/episode", "uploaded": True},
        {"tmdbId": "", "season": 1, "episode": 1, "uploaded": True},
        {"tmdbId": "500", "season": 0, "episode": 1, "title": "special",
         "year": 1999, "uploaded": True},
    ]
    with open(log, "w", encoding="utf-8") as fh:
        for item in lines:
            fh.write((item if isinstance(item, str) else json.dumps(item)) + "\n")
    monkeypatch.setattr(f, "SUCCESS_LOG", str(log))

    entries = {e["key"]: e for e in f.load_entries()}
    assert set(entries) == {"346_S01E03", "346_S01E04", "500_S00E01"}
    # 同一集后出现者胜
    assert entries["346_S01E03"]["title"] == "A2"
    assert entries["346_S01E04"]["season"] == 1
    assert entries["346_S01E04"]["episode"] == 4
    # year 直接取自记录，不再从 final_path 反解（本地视频早被删了）
    assert entries["346_S01E04"]["year"] == 2000
    assert entries["500_S00E01"]["season"] == 0


def test_remote_mode_skips_episodes_not_yet_uploaded(sandbox, monkeypatch, capsys):
    """uploaded=false 的集还没进 R2：给它传字幕会造出"有字幕没视频"的畸形目录。"""
    log = sandbox / "success.jsonl"
    with open(log, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "55", "season": 1, "episode": 3,
                             "year": 2000, "uploaded": False}) + "\n")
        fh.write(json.dumps({"tmdbId": "56", "season": 1, "episode": 3,
                             "year": 2000, "uploaded": True}) + "\n")
    monkeypatch.setattr(f, "SUCCESS_LOG", str(log))
    monkeypatch.setattr(f, "REMOTE_MODE", True)

    keys = {e["key"] for e in f.load_entries()}
    assert keys == {"56_S01E03"}
    assert "尚未上传 R2" in capsys.readouterr().out


def test_local_mode_takes_every_downloaded_episode(sandbox, monkeypatch):
    """本地模式没有"进没进 R2"的概念，uploaded 字段不该参与过滤。"""
    log = sandbox / "success.jsonl"
    with open(log, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "55", "season": 1, "episode": 3,
                             "year": 2000, "uploaded": False}) + "\n")
    monkeypatch.setattr(f, "SUCCESS_LOG", str(log))
    assert {e["key"] for e in f.load_entries()} == {"55_S01E03"}


# ------------------------------------------------------------ R2 模式

class _FakeS3:
    """最小 S3 桩：记录 upload/put，按预置对象列表回答 list/get。

    支持 ETag + If-Match，用来验证 meta.json 的乐观锁：
      - get_object 返回当前 etag；
      - put_object 带 IfMatch 且与当前 etag 不符时抛 412；
      - on_get 钩子可在"读之后、写之前"模拟别人改了对象（制造竞争窗口）。
    """

    def __init__(self, keys=None, meta=None):
        self.keys = list(keys or [])
        self.meta = meta
        self.uploaded = []      # [(local_path, key)]
        self.put_objects = {}   # key -> bytes
        self.list_error = None
        self.etag = '"v1"'
        self.on_get = None
        self.get_calls = 0
        self.put_calls = 0
        self.conflicts = 0

    def get_paginator(self, _name):
        outer = self

        class _P:
            def paginate(self, Bucket=None, Prefix=""):
                if outer.list_error:
                    raise outer.list_error
                yield {"Contents": [{"Key": k} for k in outer.keys
                                    if k.startswith(Prefix)]}
        return _P()

    def get_object(self, Bucket=None, Key=None):
        if self.meta is None:
            raise RuntimeError("NoSuchKey")
        self.get_calls += 1
        body = json.dumps(self.meta).encode("utf-8")
        etag = self.etag
        if self.on_get:
            self.on_get(self)
        return {"Body": io.BytesIO(body), "ETag": etag}

    def put_object(self, Bucket=None, Key=None, Body=None, **kwargs):
        self.put_calls += 1
        if_match = kwargs.get("IfMatch")
        if if_match is not None and if_match != self.etag:
            self.conflicts += 1
            raise _PreconditionFailed()
        self.put_objects[Key] = Body
        if Key.endswith("meta.json"):
            self.meta = json.loads(Body.decode("utf-8"))
            self.etag = f'"after-{self.put_calls}"'


class _PreconditionFailed(Exception):
    """模拟 botocore 的 ClientError(412)，结构与真实对象一致。"""

    def __init__(self):
        super().__init__("PreconditionFailed")
        self.response = {
            "Error": {"Code": "PreconditionFailed"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        }


@pytest.fixture
def remote_sandbox(sandbox, monkeypatch):
    """R2 模式：REMOTE_MODE=True，S3 client 换成可断言的假对象。"""
    monkeypatch.setattr(f, "REMOTE_MODE", True)
    monkeypatch.setattr(f.dt, "S3_BUCKET", "test-bucket")
    # R2 模式下本地集目录在上传成功那一刻就删了：把 sandbox 预建的目录清掉，
    # 才能验"本地不留任何残留"。
    shutil.rmtree(f.dt.BASE_DIR, ignore_errors=True)
    fake = _FakeS3()
    monkeypatch.setattr(f.dt, "get_s3_client", lambda: fake)
    monkeypatch.setattr(
        f.dt, "upload_to_r2",
        lambda local, key: (fake.uploaded.append((local, key)), (True, None))[1],
    )
    return fake


def test_remote_existing_languages_come_from_r2(remote_sandbox, monkeypatch):
    """R2 上已有 en，就只补 zh —— 哪怕本地目录根本不存在。

    这是整个 R2 模式的立身之本：本地目录不存在是**最正常**的状态
    （上传成功即删），按本地判重会把每一集都当成缺口重抓。
    """
    remote_sandbox.keys = [
        "tv/2000/55/S01/E03/subs/en.srt", "tv/2000/55/S01/E03/subs/en.vtt",
    ]
    asked = []
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda tid, s, e, langs=None: (asked.append(list(langs or [])), [])[1],
    )
    _, result = f.download_one(_entry())
    assert result["status"] == "ok"
    assert result["missing"] == ["zh"]
    assert asked == [["zh"]]


def test_remote_listing_is_scoped_to_this_episode(remote_sandbox, monkeypatch):
    """列举必须严格限定在本集的 subs/ 前缀下，不能串到别的集。"""
    remote_sandbox.keys = [
        "tv/2000/55/S01/E04/subs/en.srt",   # 邻集，不该算数
        "tv/2000/55/S01/E03/subs/zh.srt",   # 本集
    ]
    names, ok = f._list_remote_subtitles("55", 1, 3, 2000)
    assert ok is True
    assert names == ["zh.srt"]


def test_remote_list_failure_skips_the_episode(remote_sandbox, monkeypatch, capsys):
    """列举失败多半是网络抖动：跳过这一集，而不是当作"没有字幕"去重抓。"""
    remote_sandbox.list_error = RuntimeError("r2 down")
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: pytest.fail("列举失败时不该再查 SubDL"),
    )
    _, result = f.download_one(_entry())
    assert result["status"] == "list_failed"
    assert "列举 R2 字幕失败" in capsys.readouterr().out


def test_remote_uploads_subtitles_and_leaves_no_local_files(
    remote_sandbox, monkeypatch, tmp_path,
):
    """字幕写临时目录 -> 传 R2 -> 删临时目录，本地不留任何残留。"""
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: [{"language": "EN", "season": 1, "episode": 3,
                          "url": "/dl/en"}],
    )
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en"])
    monkeypatch.setattr(
        f, "request_with_keys",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"Show.S01E03.srt": _SRT})),
    )
    _, result = f.download_one(_entry())
    assert result["status"] == "ok"
    assert sorted(result["saved"]) == ["en.srt", "en.vtt"]
    keys = sorted(k for _, k in remote_sandbox.uploaded)
    assert keys == [
        "tv/2000/55/S01/E03/subs/en.srt",
        "tv/2000/55/S01/E03/subs/en.vtt",
    ]
    # 临时目录已清空，本地不留残留
    assert not os.path.exists(f.dt.BASE_DIR)
    for local, _ in remote_sandbox.uploaded:
        assert not os.path.exists(local)


def test_remote_upload_failure_is_not_reported_as_saved(
    remote_sandbox, monkeypatch,
):
    """没进 R2 等于这份字幕不存在：报成功会让人以为补上了。"""
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en"])
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: [{"language": "EN", "season": 1, "episode": 3,
                          "url": "/dl/en"}],
    )
    monkeypatch.setattr(
        f, "request_with_keys",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"Show.S01E03.srt": _SRT})),
    )
    monkeypatch.setattr(f.dt, "upload_to_r2", lambda l, k: (False, "r2 down"))
    _, result = f.download_one(_entry())
    assert result["saved"] == []
    assert result["status"] == "upload_failed"


def test_remote_meta_json_gets_the_new_subtitles(remote_sandbox, monkeypatch):
    """光传文件不够：meta 里没记录，前端就等于看不到这些字幕。"""
    remote_sandbox.meta = {"tmdbId": "55", "season": 1, "episode": 3,
                           "subtitles": []}
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en"])
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: [{"language": "EN", "season": 1, "episode": 3,
                          "url": "/dl/en"}],
    )
    monkeypatch.setattr(
        f, "request_with_keys",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"Show.S01E03.srt": _SRT})),
    )
    _, result = f.download_one(_entry())
    assert result["metaUpdated"] is True
    paths = {e["path"] for e in remote_sandbox.meta["subtitles"]}
    assert paths == {"subs/en.srt", "subs/en.vtt"}
    assert "tv/2000/55/S01/E03/meta.json" in remote_sandbox.put_objects


def test_remote_meta_update_does_not_duplicate_entries(remote_sandbox):
    remote_sandbox.meta = {
        "subtitles": [{"language": "en", "format": "srt", "path": "subs/en.srt"}]
    }
    ok, added = f._update_remote_meta("55", 1, 3, 2000, ["en.srt"])
    assert (ok, added) == (True, 0)
    assert remote_sandbox.put_calls == 0   # 无事可做时不发写请求


def test_remote_meta_ignores_non_track_formats(remote_sandbox):
    """ass 不该进 meta：前端会给 <track> 塞一个它加载不了的文件。"""
    remote_sandbox.meta = {"subtitles": []}
    ok, added = f._update_remote_meta("55", 1, 3, 2000, ["en.ass", "en.srt"])
    assert (ok, added) == (True, 1)
    assert [e["path"] for e in remote_sandbox.meta["subtitles"]] == ["subs/en.srt"]


def test_remote_missing_meta_does_not_break_subtitles(
    remote_sandbox, monkeypatch, capsys,
):
    """meta.json 不存在只是少了个索引，字幕本身已经传上去了。"""
    remote_sandbox.meta = None
    ok, added = f._update_remote_meta("55", 1, 3, 2000, ["en.srt"])
    assert (ok, added) == (False, 0)
    assert "读取 meta.json 失败" in capsys.readouterr().out


def test_meta_update_sends_if_match(remote_sandbox):
    """必须带 ETag 乐观锁，否则下载侧的并发覆盖会被静默吃掉。"""
    remote_sandbox.meta = {"subtitles": []}
    captured = {}
    real_put = remote_sandbox.put_object

    def spy(**kwargs):
        captured.update(kwargs)
        return real_put(**kwargs)

    remote_sandbox.put_object = spy
    f._update_remote_meta("55", 1, 3, 2000, ["en.srt"])
    assert captured["IfMatch"] == '"v1"'


def test_meta_update_retries_and_preserves_concurrent_change(remote_sandbox):
    """412 时重读重试：第二次读到的是新版本，合并后不丢对方的字段。"""
    remote_sandbox.meta = {"subtitles": [], "keep": "v1"}

    def bump(fake):
        # 模拟下载侧在我们读完之后整份覆盖了 meta
        if fake.get_calls == 1:
            fake.meta = {"subtitles": [], "keep": "v2"}
            fake.etag = '"v2"'

    remote_sandbox.on_get = bump
    ok, added = f._update_remote_meta("55", 1, 3, 2000, ["en.srt"])
    assert (ok, added) == (True, 1)
    assert remote_sandbox.conflicts == 1
    assert remote_sandbox.meta["keep"] == "v2"      # 对方的改动没被吃掉
    assert remote_sandbox.meta["subtitles"][0]["path"] == "subs/en.srt"


@pytest.mark.parametrize("exc,expected", [
    (_PreconditionFailed(), True),
    (RuntimeError("boom"), False),
])
def test_precondition_detector(exc, expected):
    assert f._is_precondition_failed(exc) is expected


def test_reconcile_meta_repairs_orphaned_files(remote_sandbox, monkeypatch):
    """上次跑在"字幕已传、meta 回写失败"处断掉：这次要能自动对账补记。"""
    remote_sandbox.keys = [
        "tv/2000/55/S01/E03/subs/en.srt", "tv/2000/55/S01/E03/subs/en.vtt",
        "tv/2000/55/S01/E03/subs/zh.srt", "tv/2000/55/S01/E03/subs/zh.vtt",
    ]
    remote_sandbox.meta = {"subtitles": []}
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: pytest.fail("字幕已齐时不该再查 SubDL"),
    )
    _, result = f.download_one(_entry())
    assert result["status"] == "meta_repaired"
    assert result["metaAdded"] == 4


# ------------------------------------------------------------ 配额与 key 轮换

def _quota_response():
    class _R:
        status_code = 429

        def json(self):
            return {"error": f.QUOTA_ERROR_CODE, "limit": 50,
                    "retryAfterSeconds": 100, "resetAt": "2026-09-13T00:00:00Z"}

        def close(self):
            pass
    return _R()


def _throttled_response():
    class _R:
        status_code = 429

        def json(self):
            return {"error": "too many requests"}

        def raise_for_status(self):
            raise RuntimeError("429 Too Many Requests")

        def close(self):
            pass
    return _R()


def test_quota_error_is_recognized_from_the_response_body():
    exc = f._parse_quota_error(_quota_response())
    assert isinstance(exc, f.QuotaExhausted)
    assert exc.reset_at == "2026-09-13T00:00:00Z"


def test_plain_429_is_not_treated_as_quota_exhaustion():
    """短时限流与额度耗尽处置相反，绝不能靠状态码猜。"""
    assert f._parse_quota_error(_throttled_response()) is None


def test_quota_exhaustion_is_not_retried(sandbox, monkeypatch):
    """确定性失败：重试毫无意义，只会白等退避。"""
    calls = []
    monkeypatch.setattr(
        f.requests, "request",
        lambda *a, **k: (calls.append(1), _quota_response())[1],
    )
    with pytest.raises(f.QuotaExhausted):
        f.request_with_retry("GET", "u")
    assert len(calls) == 1


def test_quota_exhaustion_does_not_pollute_missing(sandbox, monkeypatch):
    """"我们没额度取"绝不能记成"源站没有" —— 否则台账会永久放弃这个语种。"""
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en", "zh"])
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: [
            {"language": "EN", "season": 1, "episode": 3, "url": "/en"},
            {"language": "ZH", "season": 1, "episode": 3, "url": "/zh"},
        ],
    )

    def boom(*_a, **_k):
        raise f.QuotaExhausted("no quota", reset_at="2026-09-13T00:00:00Z")

    monkeypatch.setattr(f, "request_with_keys", boom)
    _, result = f.download_one(_entry())
    assert result["status"] == "quota_exhausted"
    assert result["missing"] == []
    assert sorted(result["unattempted"]) == ["en", "zh"]


def test_quota_exhaustion_stops_remaining_episodes(sandbox, monkeypatch):
    """闸门落下后，后面的集直接跳过，不再各发注定失败的请求。"""
    f._signal_stop("quota_exhausted", "no quota")
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: pytest.fail("闸门已落，不该再查"),
    )
    _, result = f.download_one(_entry())
    assert result["status"] == "quota_exhausted"


def test_pool_hands_out_keys_in_order():
    pool = f.KeyPool(["k1", "k2"])
    assert pool.current() == "k1"
    assert pool.retire("k1", "quota") == "k2"
    assert pool.current() == "k2"
    assert pool.retire("k2", "quota") is None
    assert pool.current() is None
    assert pool.all_exhausted() is True


def test_pool_ignores_stale_reports_from_other_threads():
    """多线程各自撞 429 时，无脑推进会把没用过的 key 整个浪费掉。"""
    pool = f.KeyPool(["k1", "k2", "k3"])
    assert pool.retire("k1", "quota") == "k2"
    # 后到的线程报的还是 k1：此时 current 已是 k2，必须原地不动
    assert pool.retire("k1", "quota") == "k2"
    assert pool.retire("k1", "quota") == "k2"
    assert pool.current() == "k2"
    assert pool.exhausted_count() == 1


def test_pool_is_thread_safe_under_real_contention():
    pool = f.KeyPool([f"k{i}" for i in range(20)])
    start = threading.Barrier(8)

    def worker():
        start.wait()
        for _ in range(10):
            key = pool.current()
            if key is not None:
                pool.retire(key, "quota")

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert pool.exhausted_count() == 20


def test_request_switches_key_on_quota_and_succeeds(sandbox, monkeypatch):
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1", "k2"]))
    seen = []

    class _Ok:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, params=None, **kwargs):
        seen.append(params["api_key"])
        if params["api_key"] == "k1":
            return _quota_response()
        return _Ok()

    monkeypatch.setattr(f.requests, "request", fake_request)
    f.request_with_keys("GET", "u")
    assert seen == ["k1", "k2"]
    assert f.stop_fetching.is_set() is False   # 还有 key，不该落闸


def test_stop_only_after_every_key_is_exhausted(sandbox, monkeypatch):
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1", "k2"]))
    monkeypatch.setattr(
        f.requests, "request", lambda *a, **k: _quota_response()
    )
    with pytest.raises(f.QuotaExhausted):
        f.request_with_keys("GET", "u")
    assert f.key_pool.all_exhausted() is True


def test_throttled_key_is_not_retired(sandbox, monkeypatch):
    """限流不是 key 的问题：退役它等于白扔一份额度。"""
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1", "k2"]))
    monkeypatch.setattr(f, "RETRY_DELAY", 0)
    attempts = {"n": 0}

    class _Ok:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, params=None, **kwargs):
        attempts["n"] += 1
        if attempts["n"] <= f.RETRY_MAX:
            return _throttled_response()
        return _Ok()

    monkeypatch.setattr(f.requests, "request", fake_request)
    f.request_with_keys("GET", "u")
    assert f.key_pool.current() == "k1"        # 仍是第一个 key
    assert f.key_pool.exhausted_count() == 0


def test_persistent_throttle_eventually_stops(sandbox, monkeypatch):
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1"]))
    monkeypatch.setattr(f, "RETRY_DELAY", 0)
    monkeypatch.setattr(
        f.requests, "request", lambda *a, **k: _throttled_response()
    )
    with pytest.raises(f.SearchThrottled):
        f.request_with_keys("GET", "u")
    assert f.stop_reason["status"] == "search_throttled"
    # 限流不该退役任何 key
    assert f.key_pool.exhausted_count() == 0


def test_throttled_search_is_not_counted_as_search_failed(sandbox, monkeypatch):
    """被限流的集"压根没查成"，与"SubDL 库里没有这部剧"完全不同。"""
    def boom(*_a, **_k):
        raise f.SearchThrottled("throttled")

    monkeypatch.setattr(f, "search_subtitles", boom)
    _, result = f.download_one(_entry())
    assert result["status"] == "search_throttled"
    assert result["missing"] == []


# ------------------------------------------------------------ 脱敏

@pytest.mark.parametrize("text", [
    "GET https://api.subdl.com/x?api_key=subdl_secret&type=tv",
    "error: api_key=abc123",
])
def test_redact_removes_api_key(text):
    out = f._redact(text)
    assert "subdl_secret" not in out and "abc123" not in out
    assert "api_key=REDACTED" in out


def test_redact_keeps_the_rest_of_the_message_intact():
    out = f._redact("HTTPError 429 for url ...?api_key=K&type=tv")
    assert "429" in out and "type=tv" in out


def test_search_failure_log_does_not_leak_the_key(sandbox, monkeypatch):
    """异常消息里带完整 url，key 就在 query 里——日志常被贴出来排查。"""
    def boom(*_a, **_k):
        raise RuntimeError("failed for url: https://x?api_key=subdl_secret")

    monkeypatch.setattr(f, "search_subtitles", boom)
    _, result = f.download_one(_entry())
    assert "subdl_secret" not in result["error"]


@pytest.mark.parametrize("url,expected", [
    ("/sub/a.zip?api_key=OLD", "/sub/a.zip"),
    ("/sub/a.zip", "/sub/a.zip"),
])
def test_strip_api_key_leaves_only_the_path(url, expected):
    """不剥掉自带的旧 key，换 key 会静默失效（一直在撞同一堵墙）。"""
    assert f._strip_api_key(url) == expected


def test_download_uses_the_pool_key_not_the_one_in_the_url(sandbox, monkeypatch):
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["CURRENT"]))
    seen = {}

    class _Ok(_FakeZipResp):
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, params=None, **kwargs):
        seen["url"] = url
        seen["key"] = params["api_key"]
        return _Ok(_zip_bytes({"Show.S01E03.srt": _SRT}))

    monkeypatch.setattr(f.requests, "request", fake_request)
    target = sandbox / "out"
    target.mkdir()
    f._download_candidate(
        {"url": "/sub/a.zip?api_key=STALE"}, "en", 1, 3, str(target)
    )
    assert seen["key"] == "CURRENT"
    assert "STALE" not in seen["url"]


# ------------------------------------------------------------ 语种码映射

@pytest.mark.parametrize("lang,expected", [
    ("en", "EN"), ("zh", "ZH"), ("zh-cn", "ZH"), ("zh_CN", "ZH"),
    ("zh-tw", "ZH"), ("pt-br", "BR_PT"), ("en-us", "EN"),
    ("", None), ("x", None), ("123", None),
])
def test_subdl_language_code_mapping(lang, expected):
    assert f.subdl_language_code(lang) == expected


def test_unsupported_language_is_never_queried(sandbox, monkeypatch):
    """未知码 SubDL 不报错而是返回全语种结果，必须在进 pending 前拦下。"""
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en", "bogus!"])
    assert f.unsupported_languages() == ["bogus!"]
    asked = {}

    def fake_search(tid, season, episode, languages=None):
        asked["languages"] = list(languages or [])
        return []

    monkeypatch.setattr(f, "search_subtitles", fake_search)
    _, result = f.download_one(_entry())
    assert asked["languages"] == ["en"]
    assert "bogus!" not in result["missing"]


# ------------------------------------------------------------ 缺口台账

def test_ledger_merges_multiple_lines_by_union(sandbox):
    with open(f.LEDGER_LOG, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"key": "55_S01E03", "missing": ["zh"]}) + "\n")
        fh.write(json.dumps({"key": "55_S01E03", "missing": ["EN"]}) + "\n")
        fh.write("garbage\n")
    assert f.load_ledger() == {"55_S01E03": {"zh", "en"}}


def test_ledger_tolerates_missing_file(sandbox):
    """台账是省配额的优化，不是正确性的一环：读不到就当空的。"""
    assert f.load_ledger() == {}


def test_confirmed_gap_language_is_not_asked_again(sandbox, monkeypatch):
    """源站已确认没有的语种不再问第二遍 —— 这是省配额的关键。"""
    ledger = {"55_S01E03": {"zh"}}
    asked = {}

    def fake_search(tid, season, episode, languages=None):
        asked["languages"] = list(languages or [])
        return []

    monkeypatch.setattr(f, "search_subtitles", fake_search)
    f.download_one(_entry(), ledger)
    assert asked["languages"] == ["en"]


def test_episode_with_all_gaps_confirmed_costs_no_quota(sandbox, monkeypatch):
    """所缺语种全在台账里：整集跳过，一个请求都不发。"""
    ledger = {"55_S01E03": {"en", "zh"}}
    monkeypatch.setattr(
        f, "search_subtitles", lambda *a, **k: pytest.fail("不该发起查询")
    )
    _, result = f.download_one(_entry(), ledger)
    assert result["status"] == "gap_confirmed"
    assert result["givenUp"] == ["en", "zh"]


def test_fetch_failure_never_enters_the_ledger(sandbox, monkeypatch):
    """取回失败 != 源站没有：混进台账会让一次网络抖动永久放弃该语种。"""
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en"])
    monkeypatch.setattr(
        f, "search_subtitles",
        lambda *a, **k: [{"language": "EN", "season": 1, "episode": 3,
                          "url": "/en"}],
    )

    def boom(*_a, **_k):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(f, "request_with_keys", boom)
    _, result = f.download_one(_entry())
    assert result["fetch_failed"] == ["en"]
    assert result["missing"] == []       # 不进台账


def test_source_confirmed_absence_is_recorded(sandbox, monkeypatch):
    """源站确认没有才记台账。"""
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en"])
    monkeypatch.setattr(f, "search_subtitles", lambda *a, **k: [])
    _, result = f.download_one(_entry())
    assert result["missing"] == ["en"]

    f.record_gap(_entry(), result["missing"])
    assert f.load_ledger() == {"55_S01E03": {"en"}}


def test_not_in_subdl_records_every_pending_language(sandbox, monkeypatch):
    """库里压根没有这部剧是确定性结论，对所有语种都成立。"""
    def boom(*_a, **_k):
        raise RuntimeError(f"error: {f.NOT_FOUND_MARKER}")

    monkeypatch.setattr(f, "search_subtitles", boom)
    _, result = f.download_one(_entry())
    assert result["status"] == "not_in_subdl"
    assert sorted(result["missing"]) == ["en", "zh"]


def test_ledger_write_failure_does_not_break_the_run(sandbox, monkeypatch, capsys):
    monkeypatch.setattr(f, "LEDGER_LOG", str(sandbox / "nodir" / "g.jsonl"))
    f.record_gap(_entry(), ["en"])
    assert "缺口台账写入失败" in capsys.readouterr().out


# ------------------------------------------------------------ 收尾提示

def test_finish_message_differs_from_quota_exhausted_message(
    sandbox, monkeypatch, capsys,
):
    """"跑完了"与"没额度了"是两种收场，日志必须说得明显不同。"""
    monkeypatch.setattr(f, "SUCCESS_LOG", str(sandbox / "success.jsonl"))
    with open(f.SUCCESS_LOG, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "55", "season": 1, "episode": 3,
                             "year": 2000}) + "\n")
    monkeypatch.setattr(
        f, "download_one",
        lambda e, ledger=None: (e["key"], {"status": "ok", "saved": [],
                                           "missing": []}),
    )
    f._run()
    out = capsys.readouterr().out
    assert "全部待补剧集已处理完毕" in out
    assert "因配额耗尽而中断" not in out


def test_quota_stop_message_is_loud_and_tells_next_step(
    sandbox, monkeypatch, capsys,
):
    monkeypatch.setattr(f, "SUCCESS_LOG", str(sandbox / "success.jsonl"))
    with open(f.SUCCESS_LOG, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "55", "season": 1, "episode": 3,
                             "year": 2000}) + "\n")
    monkeypatch.setattr(
        f, "download_one",
        lambda e, ledger=None: (e["key"], {
            "status": "quota_exhausted", "error": "no quota",
            "saved": [], "missing": [],
            "quotaResetAt": "2026-09-13T00:00:00Z",
        }),
    )
    f._run()
    out = capsys.readouterr().out
    assert "因配额耗尽而中断" in out
    assert "2026-09-13T00:00:00Z" in out
    assert "再跑一次本脚本" in out
