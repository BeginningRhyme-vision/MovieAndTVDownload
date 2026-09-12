"""fetch_subtitles.py 的离线用例。

核心约束：**字幕是"可有可无"的附属物** —— 取不到、出错、缺配置都不能让
流程失败或以非零码退出。这些用例专门锁死这条原则，防止日后改动把它破坏。
"""

import io
import json
import os
import shutil
import threading
import zipfile

import pytest
import requests

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


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """把落盘目录指到 tmp_path，并给定固定的语种/格式。

    默认关掉 REMOTE_MODE：这批用例验的是抓取/转换/容错本身，走本地落盘最直接。
    R2 模式（生产默认）的专属行为在 remote_sandbox 那组用例里单独锁。
    """
    monkeypatch.setattr(f.dm, "BASE_DIR", str(tmp_path / "downloads"))
    monkeypatch.setattr(f.dm, "FOLDER_PREFIX", "movies")
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en", "zh"])
    monkeypatch.setattr(f, "SUBTITLE_FORMATS", ["vtt", "srt"])
    monkeypatch.setattr(f, "SUBDL_API_KEYS", ["TESTKEY"])
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["TESTKEY"]))
    monkeypatch.setattr(f, "STATE_LOG", str(tmp_path / "subtitles.jsonl"))
    monkeypatch.setattr(f, "LEDGER_LOG", str(tmp_path / "subtitles_gaps.jsonl"))
    monkeypatch.setattr(f, "REMOTE_MODE", False)
    # 停止闸门与其原因都是模块级共享状态。不换新的话，一旦某个用例把它置位，
    # 后面所有用例的 download_one 都会直接返回跳过 —— 用例间互相污染，
    # 且失败原因极难看出。
    monkeypatch.setattr(f, "stop_fetching", threading.Event())
    monkeypatch.setattr(f, "stop_reason", {})
    # 限流冷却是真 sleep，用例里一律归零；冷却窗口也要重置，免得串台。
    monkeypatch.setattr(f, "THROTTLE_COOLDOWN", 0)
    monkeypatch.setattr(f, "_throttle_until", 0.0)
    # 本地模式只给"影片目录还在"的片补字幕；用例里常用的几个 id 先把目录造出来。
    for tid in ("1", "2", "55", "56"):
        os.makedirs(f.dm.movie_dir(tid, 2000), exist_ok=True)
    return tmp_path


# ------------------------------------------------ 目录结构与下载侧保持同构

def test_subs_dir_sits_next_to_the_video(sandbox):
    """字幕必须落在影片目录下的 subs/，与视频、meta.json 同级。"""
    assert f.subs_dir("55", 2000) == os.path.join(
        f.dm.BASE_DIR, "movies", "2000", "55", "subs"
    )


def test_subs_dir_uses_same_year_fallback_as_downloader(sandbox):
    """year 缺失时两侧必须落到同一个 unknown_year，否则字幕与视频分家。"""
    assert f.subs_dir("55", None).startswith(f.dm.movie_dir("55", None))
    assert "unknown_year" in f.subs_dir("55", None)


def test_local_mode_skips_deleted_movie_dir(sandbox, monkeypatch):
    """影片目录已被用户删掉：不该凭空造出一个只有 subs/ 的孤立目录。"""
    shutil.rmtree(f.dm.movie_dir("55", 2000))
    called = []
    monkeypatch.setattr(f, "search_subtitles",
                        lambda *a, **k: (called.append(1), [])[1])
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "local_missing"
    assert called == []
    assert not os.path.exists(f.dm.movie_dir("55", 2000))


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
    """SubDL 查询失败只标记该片，不抛异常。"""
    def boom(*_a, **_k):
        raise RuntimeError("SubDL 503")

    monkeypatch.setattr(f, "search_subtitles", boom)
    tmdb_id, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert tmdb_id == "55"
    assert result["status"] == "search_failed"


def test_one_language_failure_keeps_the_other(sandbox, monkeypatch):
    """单语种下载失败不影响另一语种。

    🔑 网络失败归 fetch_failed 而**不是** missing：missing 会进缺口台账、
    从此不再尝试，一次网络抖动就把该语种永久判死了。
    """
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
        {"language": "ZH", "url": "/zh.zip"},
    ])

    def fake_request(method, url, **kwargs):
        if "/en.zip" in url:
            raise OSError("network down")
        return _FakeZipResp(_zip_bytes({"movie.srt": _SRT}))

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "ok"
    assert result["fetch_failed"] == ["en"]
    assert result["missing"] == [], "取回失败不是'源站没有'，不能进台账"
    assert sorted(result["saved"]) == ["zh.srt", "zh.vtt"]


def test_no_subtitle_found_is_not_an_error(sandbox, monkeypatch):
    """源站没有该片字幕是常态，状态仍是 ok、只记 missing。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [])
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "ok"
    assert sorted(result["missing"]) == ["en", "zh"]
    assert result["saved"] == []


def test_worker_exception_does_not_kill_the_batch(sandbox, monkeypatch, capsys):
    """某片抛异常时整批必须继续跑完。"""
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "1", "title": "A", "year": 2000},
        {"tmdbId": "2", "title": "B", "year": 2000},
    ])

    def flaky(entry, ledger=None):
        if entry["tmdbId"] == "1":
            raise RuntimeError("boom")
        return "2", {"status": "ok", "saved": ["en.vtt"], "missing": []}

    monkeypatch.setattr(f, "download_one", flaky)
    f.main()
    out = capsys.readouterr().out
    assert "异常（已跳过）" in out
    assert "本次新增字幕文件 1 个" in out


def test_state_log_failure_is_swallowed(sandbox, monkeypatch, capsys):
    """状态日志写不进去也不能影响主流程。"""
    monkeypatch.setattr(f, "STATE_LOG", "/nonexistent-dir/subtitles.jsonl")
    f.write_state({"tmdbId": "55"})
    assert "状态日志写入失败" in capsys.readouterr().out


# ------------------------------------------------------ 只补缺口、不重复抓

def test_languages_already_present_are_skipped(sandbox, monkeypatch):
    """下载侧已取到的语种直接跳过，不再请求 SubDL。"""
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    for name in ("en.vtt", "en.srt", "zh.vtt", "zh.srt"):
        open(os.path.join(target, name), "w").close()

    def boom(*_a, **_k):
        raise AssertionError("已有字幕不该再查 SubDL")

    monkeypatch.setattr(f, "search_subtitles", boom)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "skipped"


def test_only_missing_language_is_fetched(sandbox, monkeypatch):
    """只补缺的那个语种：en 已存在时只查 zh。"""
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    open(os.path.join(target, "en.vtt"), "w").close()

    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "ZH", "url": "/zh.zip"},
    ])

    monkeypatch.setattr(f, "request_with_retry",
                        lambda *a, **k: _FakeZipResp(
                            _zip_bytes({"movie.srt": _SRT})))
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    # en 已有 -> 不在 pending；zh 被补齐
    assert sorted(result["saved"]) == ["zh.srt", "zh.vtt"]
    assert result["missing"] == []


# -------------------------------------------------------- 落盘格式与下载侧一致

def test_srt_source_is_written_as_both_formats(sandbox):
    """SubDL 给的 srt 要同时落 vtt + srt，与下载侧口径一致。"""
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    saved = f._write_variants(target, "en", _SRT, "srt")
    assert sorted(saved) == ["en.srt", "en.vtt"]
    vtt = open(os.path.join(target, "en.vtt")).read()
    assert vtt.startswith("WEBVTT")
    assert "00:00:01.000 --> 00:00:03.500" in vtt


def test_ass_is_not_written(sandbox):
    """🔑 ass/ssa 不落盘、返回 []。

    前端 <track> 只认 vtt。历史版本曾原样存成 en.ass，结果该语种被一个用不了
    的文件"锁死"——_language_of 认为已有，永不再补。
    """
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    saved = f._write_variants(target, "en", "[Script Info]\n", "ass")
    assert saved == []
    assert os.listdir(target) == []


def test_stale_ass_file_does_not_count_as_existing(sandbox):
    """R2/本地上残留的旧 en.ass 不能算"en 已有"，否则该语种永远补不上。"""
    assert f._language_of("en.ass") is None
    assert f._language_of("movies/2000/55/subs/zh.ssa") is None
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    open(os.path.join(target, "en.ass"), "w").close()
    assert f._existing_languages_local(target) == set()


def test_ass_candidate_is_skipped_for_the_next_one(sandbox, monkeypatch):
    """第一条候选解出来是 ASS 时换下一条，拿到 srt 才算数。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/ass.zip"},
        {"language": "EN", "url": "/srt.zip"},
    ])

    def fake_request(method, url, **kwargs):
        if "/ass.zip" in url:
            return _FakeZipResp(_zip_bytes({"m.ass": _ASS}))
        return _FakeZipResp(_zip_bytes({"m.srt": _SRT}))

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert sorted(result["saved"]) == ["en.srt", "en.vtt"]
    assert result["missing"] == ["zh"]
    assert result["fetch_failed"] == []


def test_only_ass_candidates_become_missing_not_fetch_failed(sandbox, monkeypatch):
    """候选全是 ASS：结论是确定性的（下次还是这批 zip），归 missing 进台账。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/a.zip"},
        {"language": "EN", "url": "/b.zip"},
        {"language": "EN", "url": "/c.zip"},
    ])
    fetched = []

    def fake_request(method, url, **kwargs):
        fetched.append(url)
        return _FakeZipResp(_zip_bytes({"m.ass": _ASS}))

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["saved"] == []
    assert "en" in result["missing"]
    assert result["fetch_failed"] == []
    assert len(fetched) == f.MAX_CANDIDATES_PER_LANGUAGE, "每试一条都烧额度，必须封顶"


def test_extract_prefers_srt_over_bigger_ass_in_same_zip():
    """同一 zip 里 srt 与 ass 并存时选 srt，哪怕 ass 体积更大。"""
    content, ext = f.extract_srt(_zip_bytes({
        "m.ass": _ASS * 20,
        "m.srt": _SRT,
    }))
    assert ext == ".srt"
    assert content.decode() == _SRT


def test_oversized_zip_is_rejected(sandbox, monkeypatch):
    """源站返回异常大的内容时拒收，且**下载过程中**就中断。"""
    delivered = []

    class CountingResp(_FakeZipResp):
        def iter_content(self, chunk_size=65536):
            for i in range(0, 100, 8):
                delivered.append(i)
                yield b"x" * 8

    monkeypatch.setattr(f, "MAX_ZIP_BYTES", 10)
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(f, "request_with_retry", lambda *a, **k: CountingResp())
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert "en" in result["fetch_failed"], "拒收不等于源站没有，不该进台账"
    assert "en" not in result["missing"]
    assert result["saved"] == []
    assert len(delivered) <= 2, "超限后必须立刻停止拉取"


def test_pick_best_matches_language_case_insensitively():
    """内部用小写代码，SubDL 返回大写，比较必须不区分大小写。"""
    subs = [{"language": "EN", "url": "/a.zip"}]
    assert f.pick_best(subs, "en") is not None
    assert f.pick_best(subs, "zh") is None


def test_pick_best_skips_full_season_packs():
    subs = [
        {"language": "EN", "url": "/season.zip", "full_season": True},
        {"language": "EN", "url": "/movie.zip"},
    ]
    assert f.pick_best(subs, "en")["url"] == "/movie.zip"


def test_pick_candidates_is_capped_and_ordered():
    """候选按 SubDL 顺序返回，且封顶——每试一条都烧一次下载额度。"""
    subs = [{"language": "EN", "url": f"/{i}.zip"} for i in range(5)]
    picked = f.pick_candidates(subs, "en")
    assert [p["url"] for p in picked] == ["/0.zip", "/1.zip"]
    assert len(picked) == f.MAX_CANDIDATES_PER_LANGUAGE


def test_pick_candidates_uses_mapped_language_code():
    """config 写 pt-br，SubDL 返回的是 BR_PT——必须经映射比较，否则永远比不上。"""
    subs = [{"language": "BR_PT", "url": "/a.zip"}]
    assert f.pick_candidates(subs, "pt-br") == subs
    assert f.pick_candidates([{"language": "ZH", "url": "/z.zip"}], "zh-cn")


# ------------------------------------------------------------- 语种码映射/校验
# 2026-09-12 实测：SubDL 对未知语种码**不报错**而是返回全语种结果，光看
# "查到了东西"发现不了配置写错；且 PT_BR 的结果 language 字段是 BR_PT。

@pytest.mark.parametrize("lang,expected", [
    ("en", "EN"),
    ("zh", "ZH"),
    ("zh-cn", "ZH"),
    ("zh-TW", "ZH"),
    ("pt-br", "BR_PT"),
    ("en-US", "EN"),
    ("fre", "FRE"),
    ("", None),
    (None, None),
    ("chinese", None),
    ("zh cn", None),
    ("x", None),
])
def test_subdl_language_code_mapping(lang, expected):
    assert f.subdl_language_code(lang) == expected


def test_unsupported_language_is_warned_and_never_queried(
    sandbox, monkeypatch, capsys
):
    """映射不出的语种：启动告警、不进 pending、不进台账，其余语种照常补。"""
    monkeypatch.setattr(f, "SUBTITLE_LANGUAGES", ["en", "chinese"])
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "55", "title": "A", "year": 2000},
    ])
    asked = []

    def fake_search(tmdb_id, languages=None, **_k):
        asked.append(list(languages))
        return []

    monkeypatch.setattr(f, "search_subtitles", fake_search)
    f.main()

    out = capsys.readouterr().out
    assert "['chinese'] 无法映射为 SubDL 语种码" in out
    assert asked == [["en"]], "不合法语种不该被查"
    assert f.load_ledger().get("55") == {"en"}, "不合法语种不该进台账"


def test_search_sends_mapped_codes_and_media_type(sandbox, monkeypatch):
    """搜索请求带的是映射后的 SubDL 码与 type 参数，重复码只查一次。"""
    sent = []

    class _Resp:
        def json(self):
            return {"status": True, "subtitles": []}

    def fake_keys(method, url, params=None, **_k):
        sent.append(dict(params))
        return _Resp()

    monkeypatch.setattr(f, "request_with_keys", fake_keys)
    f.search_subtitles("55", ["zh", "zh-cn", "pt-br"], media_type="tv")
    assert [p["languages"] for p in sent] == ["ZH", "BR_PT"]
    assert {p["type"] for p in sent} == {"tv"}


# --------------------------------------------------------- movie / tv 类型
# SubDL 用 type 决定拿 tmdb_id 查哪张表，电影与剧集是两个独立编号空间。

@pytest.mark.parametrize("title_type,expected", [
    ("movie", "movie"),
    ("tvSeries", "tv"),
    ("tvMiniSeries", "tv"),
    ("TVSPECIAL", "tv"),
    # tvMovie 在 TMDB 落电影编号空间（见 test_fetch_movie_metadata 的 KEEP_TYPES 契约），
    # 必须按 movie 查，否则会 not_in_subdl 进台账永久放弃。
    ("tvMovie", "movie"),
    (None, "movie"),
    ("", "movie"),
])
def test_media_type_follows_title_type(title_type, expected):
    assert f._media_type_of(title_type) == expected


def test_download_one_passes_media_type_to_search(sandbox, monkeypatch):
    seen = {}

    def fake_search(tmdb_id, languages=None, media_type="movie"):
        seen["type"] = media_type
        return []

    monkeypatch.setattr(f, "search_subtitles", fake_search)
    f.download_one({"tmdbId": "55", "year": 2000, "title_type": "tvSeries"})
    assert seen["type"] == "tv"
    f.download_one({"tmdbId": "56", "year": 2000})
    assert seen["type"] == "movie", "老记录缺 title_type 时按电影处理"


def test_load_entries_dedupes_and_keeps_year(sandbox, monkeypatch):
    """success.jsonl 同片多条时取最后一条；year 必须带出来（拼目录要用）。"""
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": "55", "title": "A", "year": 1999}) + "\n"
        + json.dumps({"tmdbId": "55", "title": "A2", "year": 2000}) + "\n"
        + json.dumps({"title": "无 id"}) + "\n"
        + "坏行不是 json\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(f, "SUCCESS_LOG", str(path))
    entries = f.load_entries()
    assert entries == [
        {"tmdbId": "55", "title": "A2", "year": 2000, "title_type": None}
    ]


def test_load_entries_keeps_title_type(sandbox, monkeypatch):
    """带 title_type 的记录要把类型带出来，剧集才能按 type=tv 去查。"""
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": "1", "year": 2000, "title_type": "tvSeries"}) + "\n"
        + json.dumps({"tmdbId": "2", "year": 2000, "titleType": "movie"}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(f, "SUCCESS_LOG", str(path))
    types = {e["tmdbId"]: e["title_type"] for e in f.load_entries()}
    assert types == {"1": "tvSeries", "2": "movie"}


# ==================================================== R2 模式（生产默认路径）
#
# 下载侧上传成功后会把整个影片目录删掉，所以"本地没有字幕目录"是最正常的
# 状态，绝不能据此判定该片缺字幕。这组用例锁死：已有语种问 R2、新字幕传 R2、
# 本地不留残留、meta.json 同步更新。

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
        self.upload_error = None
        self.etag = '"v1"'
        self.on_get = None      # 每次 get_object 之后调用，用于插入并发修改
        self.get_calls = 0
        self.put_calls = 0
        self.conflicts = 0      # 实际抛出 412 的次数

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
    monkeypatch.setattr(f.dm, "S3_BUCKET", "test-bucket")
    monkeypatch.setattr(f.dm, "S3_PREFIX", "")
    # R2 模式下本地影片目录在上传成功那一刻就删了：把 sandbox 预建的目录清掉，
    # 才能验"本地不留任何残留"。
    shutil.rmtree(f.dm.BASE_DIR, ignore_errors=True)
    fake = _FakeS3()
    monkeypatch.setattr(f.dm, "get_s3_client", lambda: fake)
    monkeypatch.setattr(
        f.dm, "upload_to_r2",
        lambda local, key: (fake.uploaded.append((local, key)), (True, None))[1],
    )
    return fake


def test_remote_existing_languages_come_from_r2(remote_sandbox, monkeypatch):
    """R2 上已有 en，就只补 zh —— 哪怕本地目录根本不存在。"""
    remote_sandbox.keys = [
        "movies/2000/55/subs/en.srt", "movies/2000/55/subs/en.vtt",
    ]
    asked = []
    monkeypatch.setattr(
        f, "search_subtitles", lambda i, *_a, **_k: (asked.append(i), [])[1]
    )
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "ok"
    # 只有 zh 进了缺口，en 被 R2 上的已有文件挡住
    assert result["missing"] == ["zh"]
    assert asked == ["55"]


def test_remote_all_languages_present_skips_subdl(remote_sandbox, monkeypatch):
    """R2 上两种语言都齐了就直接跳过，一次 SubDL 请求都不发。"""
    remote_sandbox.keys = [
        "movies/2000/55/subs/en.srt", "movies/2000/55/subs/zh.vtt",
    ]
    monkeypatch.setattr(f, "search_subtitles", _never_called)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "skipped"


def _never_called(*_args, **_kwargs):
    raise AssertionError("不该请求 SubDL")


def test_remote_list_failure_skips_the_movie(remote_sandbox, monkeypatch, capsys):
    """列举 R2 失败时跳过该片，而不是当成'没有字幕'去重抓一遍。"""
    remote_sandbox.list_error = RuntimeError("R2 503")
    monkeypatch.setattr(f, "search_subtitles", _never_called)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "list_failed"
    assert "列举 R2 字幕失败" in capsys.readouterr().out


def test_remote_uploads_subtitles_and_leaves_no_local_files(
    remote_sandbox, monkeypatch
):
    """字幕必须进 R2，且本地不留任何残留（临时目录要删干净）。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(
        f, "request_with_retry",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"m.srt": _SRT})),
    )
    _, result = f.download_one({"tmdbId": "55", "year": 2000})

    assert result["status"] == "ok"
    assert sorted(result["saved"]) == ["en.srt", "en.vtt"]
    keys = sorted(k for _, k in remote_sandbox.uploaded)
    assert keys == ["movies/2000/55/subs/en.srt", "movies/2000/55/subs/en.vtt"]
    # 本地影片目录不该被重新造出来
    assert not os.path.exists(f.dm.movie_dir("55", 2000))
    # 临时目录也必须清掉
    for local, _ in remote_sandbox.uploaded:
        assert not os.path.exists(local)


def test_remote_upload_failure_is_not_reported_as_saved(
    remote_sandbox, monkeypatch
):
    """上传失败的字幕不能算已保存——它并没有进 R2。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(
        f, "request_with_retry",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"m.srt": _SRT})),
    )
    monkeypatch.setattr(f.dm, "upload_to_r2", lambda l, k: (False, "403"))
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "upload_failed"
    assert result["saved"] == []


def test_remote_meta_json_gets_the_new_subtitles(remote_sandbox, monkeypatch):
    """新补的字幕要并进 R2 上 meta.json 的 subtitles[]，否则前端索引不到。"""
    remote_sandbox.meta = {
        "tmdbId": "55",
        "subtitles": [{"language": "fr", "format": "vtt",
                       "path": "subs/fr.vtt"}],
    }
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(
        f, "request_with_retry",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"m.srt": _SRT})),
    )
    _, result = f.download_one({"tmdbId": "55", "year": 2000})

    assert result["metaUpdated"] is True
    written = json.loads(remote_sandbox.put_objects["movies/2000/55/meta.json"])
    paths = sorted(e["path"] for e in written["subtitles"])
    # 原有的 fr 保留，新增 en 的两种格式
    assert paths == ["subs/en.srt", "subs/en.vtt", "subs/fr.vtt"]
    assert "subtitlesUpdatedAt" in written


def test_remote_meta_update_does_not_duplicate_entries(remote_sandbox):
    """重复跑不该在 subtitles[] 里堆出重复条目。"""
    remote_sandbox.meta = {
        "subtitles": [{"language": "en", "format": "srt",
                       "path": "subs/en.srt"}],
    }
    assert f._update_remote_meta("55", 2000, ["en.srt"]) == (True, 0)
    assert remote_sandbox.put_objects == {}


def test_remote_missing_meta_does_not_break_subtitles(
    remote_sandbox, monkeypatch, capsys
):
    """meta.json 不存在时，字幕上传本身仍然算成功。"""
    remote_sandbox.meta = None     # get_object 会抛 NoSuchKey
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(
        f, "request_with_retry",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"m.srt": _SRT})),
    )
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "ok"
    assert sorted(result["saved"]) == ["en.srt", "en.vtt"]
    assert result["metaUpdated"] is False
    assert "读取 meta.json 失败" in capsys.readouterr().out


# ------------------------------- 只给"已进 R2"的片补字幕（避免畸形目录）

def test_remote_skips_movies_not_yet_uploaded(sandbox, monkeypatch, capsys):
    """uploaded=false 的片还没进 R2，给它传字幕会造出「有字幕没视频」的目录。

    下载侧对上传失败/槽位超时降级的片也会写 success.jsonl(uploaded=false)，
    等 reupload 补传。字幕要等它真正进了 R2 再补。
    """
    monkeypatch.setattr(f, "REMOTE_MODE", True)
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": "1", "title": "已上传", "year": 2000,
                    "uploaded": True}) + "\n"
        + json.dumps({"tmdbId": "2", "title": "待补传", "year": 2000,
                      "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(f, "SUCCESS_LOG", str(path))

    entries = f.load_entries()
    assert [e["tmdbId"] for e in entries] == ["1"]
    assert "跳过 1 部尚未上传 R2 的影片" in capsys.readouterr().out


def test_local_mode_still_takes_every_downloaded_movie(sandbox, monkeypatch):
    """纯本地模式没有 R2 的概念，uploaded 字段不该影响取数。"""
    path = sandbox / "success.jsonl"
    path.write_text(
        json.dumps({"tmdbId": "1", "year": 2000, "uploaded": False}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(f, "SUCCESS_LOG", str(path))
    # sandbox fixture 已把 REMOTE_MODE 置 False
    assert [e["tmdbId"] for e in f.load_entries()] == ["1"]


@pytest.mark.parametrize("name,expected", [
    ("en.srt", "en"),
    ("movies/2000/55/subs/zh.vtt", "zh"),
    ("zh-CN.srt", "zh-cn"),          # 带地区码统一小写
    ("EN.VTT", "en"),
    ("noextension", None),           # 畸形残留，忽略
])
def test_language_extraction_is_shared_by_both_modes(name, expected):
    """R2 与本地两侧必须用同一套语种判据，否则同一部片在两种模式下结论不同。"""
    assert f._language_of(name) == expected


def test_vtt_named_file_holding_srt_is_converted(sandbox):
    """zip 里名为 .vtt 但正文是 SRT 的文件，必须按内容转换而非原样存。

    与 download_movies._fetch_caption_text 同款防御：扩展名只是提示，
    缺 WEBVTT 头的 .vtt 会被浏览器 <track> 直接拒绝加载。
    """
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    f._write_variants(target, "en", _SRT, "vtt")   # 谎称 vtt

    vtt = open(os.path.join(target, "en.vtt"), encoding="utf-8").read()
    srt = open(os.path.join(target, "en.srt"), encoding="utf-8").read()
    assert vtt.startswith("WEBVTT")
    assert "00:00:01.000" in vtt      # 点号
    assert "00:00:01,000" in srt      # 逗号


_ASS = (
    "[Script Info]\nTitle: x\n\n[Events]\n"
    "Dialogue: 0,0:00:01.00,0:00:03.50,Default,,0,0,0,,Hi\n"
)
_VTT = "WEBVTT\n\n1\n00:00:01.000 --> 00:00:03.500\nHi\n"


@pytest.mark.parametrize("text,declared,expected", [
    # 正文说了算，声明一律让路
    (_SRT, "vtt", "srt"),
    (_SRT, "ass", "srt"),
    (_SRT, "srt", "srt"),
    (_VTT, "srt", "vtt"),
    (_VTT, "vtt", "vtt"),
    (_ASS, "srt", "ass"),      # ASS 被谎称 srt：必须识破
    (_ASS, "ass", "ass"),
    (_ASS, "ssa", "ssa"),      # 保留 ssa 以便区分
    # 正文认不出时才退回声明值
    ("看不懂的内容", "srt", "srt"),
])
def test_sniff_format_trusts_content_over_extension(text, declared, expected):
    assert f._sniff_format(text, declared) == expected


def test_ass_body_declared_as_srt_is_not_written_as_srt(sandbox):
    """ASS 正文若被当成 srt 处理，会连 .srt 带 .vtt 一起写废。

    ASS 是带样式的富文本，srt_to_vtt 对它毫无意义 —— 只会把
    "[Script Info]" 这种节标题原样搬进两个文件，两份全不可用。
    """
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    saved = f._write_variants(target, "en", _ASS, "srt")   # 谎称 srt

    assert saved == [], "必须识破，且 ASS 不落盘"
    assert not os.path.exists(os.path.join(target, "en.srt"))
    assert not os.path.exists(os.path.join(target, "en.vtt"))
    assert not os.path.exists(os.path.join(target, "en.ass"))


def test_remote_meta_ignores_non_track_formats(remote_sandbox):
    """meta.json 的 subtitles[] 只索引 srt/vtt：前端 <track> 不认 ass。"""
    remote_sandbox.meta = {"tmdbId": "55", "subtitles": []}
    assert f._update_remote_meta("55", 2000, ["en.ass", "en.srt"]) == (True, 1)
    written = json.loads(remote_sandbox.put_objects["movies/2000/55/meta.json"])
    assert [e["path"] for e in written["subtitles"]] == ["subs/en.srt"]


# ------------------------------------------------------- API Key 的读取来源

def test_config_api_key_is_read_when_env_is_absent(tmp_path, monkeypatch):
    """.env 没配时回退读 config.yaml 的 fetch_subtitles.subdl_api_key。"""
    (tmp_path / "config.yaml").write_text(
        "fetch_subtitles:\n  subdl_api_key: \"from-config\"\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(f, "_SCRIPT_DIR", tmp_path)
    assert f._config_api_key() == "from-config"


@pytest.mark.parametrize("content", [
    "fetch_subtitles:\n  subdl_api_key: \"\"\n",   # 留空（推荐写法）
    "fetch_subtitles: {}\n",                        # 没有该键
    "download_movies:\n  base_dir: x\n",            # 整段都不存在
    "这不是 : : 合法的 yaml : [\n",                  # 坏文件
])
def test_config_api_key_degrades_quietly(tmp_path, monkeypatch, content):
    """配置缺失/损坏一律当没配，绝不抛异常 —— 字幕是可有可无的步骤。"""
    (tmp_path / "config.yaml").write_text(content, encoding="utf-8")
    monkeypatch.setattr(f, "_SCRIPT_DIR", tmp_path)
    assert f._config_api_key() == ""


def test_missing_config_file_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(f, "_SCRIPT_DIR", tmp_path / "nowhere")
    assert f._config_api_key() == ""


# --------------------------------------------- SubDL 当日下载额度耗尽（50/天）
# 2026-09-12 真机教训：一次运行 101 部、0 产出，日志里每部都写
# 「已保存 [] 缺失 ['zh']」——与"源站没有中文字幕"完全同形，被误判成功能坏了。
# 实际是免费额度 50/天 用尽：{"error":"api_download_limit_exceeded",...}。
# 这组用例锁住三件事：识别、不重试、不污染 missing。

def _quota_response():
    """构造一个 SubDL 额度耗尽的 429 响应。"""
    class _Resp:
        status_code = 429

        def json(self):
            return {
                "error": "api_download_limit_exceeded",
                "message": "Free daily download limit reached (50/day).",
                "limit": 50,
                "retryAfterSeconds": 13728,
                "resetAt": "2026-09-12T00:00:00.000Z",
            }

        def close(self):
            pass

    return _Resp()


def test_quota_error_is_recognized_from_the_response_body():
    """靠 error 码认，不靠状态码猜。"""
    exc = f._parse_quota_error(_quota_response())
    assert isinstance(exc, f.QuotaExhausted)
    assert exc.retry_after == 13728
    assert exc.reset_at == "2026-09-12T00:00:00.000Z"


def test_plain_429_is_not_treated_as_quota_exhaustion():
    """🔑 普通 429（短时并发限流）等一会儿就恢复，必须走正常重试。

    把它误判成"额度耗尽"会让整批运行直接停摆——一次偶发抖动就报废全场。
    """
    class _Resp:
        status_code = 429

        def json(self):
            return {"error": "rate_limited"}

        def close(self):
            pass

    assert f._parse_quota_error(_Resp()) is None


def test_non_json_429_is_not_treated_as_quota_exhaustion():
    class _Resp:
        status_code = 429

        def json(self):
            raise ValueError("not json")

        def close(self):
            pass

    assert f._parse_quota_error(_Resp()) is None


def test_quota_exhaustion_is_not_retried(sandbox, monkeypatch):
    """额度耗尽是确定性失败，重试 3 次纯属白等 9 秒。"""
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(url)
        return _quota_response()

    monkeypatch.setattr(f.requests, "request", fake_request)
    monkeypatch.setattr(f.time, "sleep", lambda _: pytest.fail("不该重试等待"))

    with pytest.raises(f.QuotaExhausted):
        f.request_with_retry("GET", "https://dl.subdl.com/x.zip")
    assert len(calls) == 1, "只该发一次请求"


def test_quota_exhaustion_does_not_pollute_missing(sandbox, monkeypatch):
    """🔑 额度耗尽绝不能记进 missing。

    missing 的语义是"源站没有这个语种"。把配额问题混进去，下次运行会以为
    已经查过了（实际一次都没查成），这些片就永远补不上字幕了。
    """
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
        {"language": "ZH", "url": "/zh.zip"},
    ])

    def fake_request(method, url, **kwargs):
        raise f.QuotaExhausted("额度用尽", 13728, "2026-09-12T00:00:00.000Z")

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})

    assert result["status"] == "quota_exhausted"
    assert result["missing"] == [], "配额问题不是源站缺字幕"
    assert sorted(result["unattempted"]) == ["en", "zh"]
    assert result["quotaResetAt"] == "2026-09-12T00:00:00.000Z"


def test_quota_exhaustion_stops_remaining_movies(sandbox, monkeypatch):
    """撞上额度耗尽后，剩下的片直接跳过，不再发注定失败的请求。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    attempts = []

    def fake_request(method, url, **kwargs):
        attempts.append(url)
        raise f.QuotaExhausted("额度用尽", 13728, "2026-09-12T00:00:00.000Z")

    monkeypatch.setattr(f, "request_with_retry", fake_request)

    _, first = f.download_one({"tmdbId": "1", "year": 2000})
    assert first["status"] == "quota_exhausted"
    assert len(attempts) == 1

    # 第二部：标志已置位，必须一个请求都不发
    _, second = f.download_one({"tmdbId": "2", "year": 2000})
    assert second["status"] == "quota_exhausted"
    assert len(attempts) == 1, "额度耗尽后不该再发请求"


def test_quota_exhaustion_keeps_already_saved_subtitles(sandbox, monkeypatch):
    """额度在中途用尽时，前面已经拿到的语种必须照常保留、照常落盘。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
        {"language": "ZH", "url": "/zh.zip"},
    ])

    def fake_request(method, url, **kwargs):
        if "/en.zip" in url:
            return _FakeZipResp(_zip_bytes({"m.srt": _SRT}))
        raise f.QuotaExhausted("额度用尽", 13728, None)

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})

    assert result["status"] == "quota_exhausted"
    assert sorted(result["saved"]) == ["en.srt", "en.vtt"]
    assert result["unattempted"] == ["zh"]
    assert result["missing"] == []


def test_quota_message_is_loud_and_counted(sandbox, monkeypatch, capsys):
    """日志必须明确区分"没额度"与"源站没字幕"，并在收尾单独结账。"""
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "1", "title": "A", "year": 2000},
        {"tmdbId": "2", "title": "B", "year": 2000},
    ])

    def fake_download(entry, ledger=None):
        return entry["tmdbId"], {
            "status": "quota_exhausted", "saved": [], "missing": [],
            "error": "Free daily download limit reached (50/day).",
            "quotaResetAt": "2026-09-12T00:00:00.000Z",
        }

    monkeypatch.setattr(f, "download_one", fake_download)
    f.main()

    out = capsys.readouterr().out
    assert "当日下载额度均已用尽" in out
    assert "2026-09-12T00:00:00.000Z" in out
    assert "这不是「源站没有字幕」" in out
    assert "因配额耗尽而中断" in out
    assert "2 部影片本次未处理" in out
    assert "当日额度耗尽" in out
    assert "已处理完毕" not in out, "被迫中断不能报成圆满完成"


# ------------------------------------------------------ 下载 url 的参数拼装

@pytest.mark.parametrize("url,expected", [
    ("/subtitle/1-2.zip?api_key=K", "/subtitle/1-2.zip"),
    ("/subtitle/1-2.zip", "/subtitle/1-2.zip"),
    ("/subtitle/1-2.zip?api_key=K&x=1", "/subtitle/1-2.zip"),
])
def test_strip_api_key_leaves_only_the_path(url, expected):
    """SubDL 返回的 url 自带 api_key，必须剥掉。

    🔑 多 key 场景下这是**正确性**问题而非美观问题：自带的是搜索时那个 key，
    不剥的话即便 KeyPool 已切到 key2，请求带的仍是耗尽的 key1 —— 轮换静默失效。
    """
    assert f._strip_api_key(url) == expected


def test_download_uses_the_pool_key_not_the_one_in_the_url(sandbox, monkeypatch):
    """🔑 端到端护栏：请求用的必须是池子当前的 key，且只出现一次。"""
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["POOLKEY"]))
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/subtitle/1-2.zip?api_key=STALEKEY"},
    ])
    seen = {}

    def fake_request(method, url, **kwargs):
        seen["url"] = url
        seen["params"] = kwargs.get("params")
        return _FakeZipResp(_zip_bytes({"m.srt": _SRT}))

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    f.download_one({"tmdbId": "55", "year": 2000})

    assert "api_key" not in seen["url"], "url 里不该再有 key"
    assert seen["params"]["api_key"] == "POOLKEY"
    assert "STALEKEY" not in str(seen), "绝不能用搜索结果里那个过期的 key"


# ------------------------------------- 搜索接口被限流 ≠ 源站没有这个 tmdb_id
# 2026-09-12 真机续集：下载额度耗尽后 api.subdl.com 也开始返 429。它走
# raise_for_status，于是和真正的「can't find movie or tv」一起被归进
# search_failed —— 那个统计数字从此不可信（真查不到的该剔除待办，被限流的
# 下次还得再试）。且还白重试了 3 次。

def _throttled_response():
    """搜索接口的裸 429：没有 JSON 错误码，只有 HTTP 状态。"""
    class _Resp:
        status_code = 429

        def json(self):
            raise ValueError("not json")

        def raise_for_status(self):
            raise requests.HTTPError(
                "429 Client Error: Too Many Requests for url: "
                "https://api.subdl.com/api/v1/subtitles?api_key=subdl_SECRET"
            )

        def close(self):
            pass

    return _Resp()


def test_persistent_429_becomes_search_throttled(sandbox, monkeypatch):
    """重试用尽仍 429 -> SearchThrottled，而不是笼统的 HTTPError。"""
    monkeypatch.setattr(
        f.requests, "request", lambda *a, **k: _throttled_response()
    )
    monkeypatch.setattr(f.time, "sleep", lambda _: None)

    with pytest.raises(f.SearchThrottled):
        f.request_with_retry("GET", "https://api.subdl.com/api/v1/subtitles")


def test_non_429_failure_still_raises_original_error(sandbox, monkeypatch):
    """🔑 只有 429 才算限流。别的错误必须原样上抛，否则排查时看不到真因。
    失败的响应还得 close 掉：stream=True 的连接不关会一直占着。"""
    closed = []

    class _Resp:
        status_code = 500

        def json(self):
            raise ValueError("not json")

        def raise_for_status(self):
            raise requests.HTTPError("500 Server Error")

        def close(self):
            closed.append(1)

    monkeypatch.setattr(f.requests, "request", lambda *a, **k: _Resp())
    monkeypatch.setattr(f.time, "sleep", lambda _: None)

    with pytest.raises(requests.HTTPError, match="500"):
        f.request_with_retry("GET", "https://api.subdl.com/api/v1/subtitles")
    assert len(closed) == f.RETRY_MAX


def test_throttled_search_is_not_counted_as_search_failed(sandbox, monkeypatch):
    """🔑 被限流的片必须与"SubDL 库里没有这个片"分开统计。

    混进 search_failed 的后果：那个数字里混着两类完全不同的片，既没法据此
    剔除真的查不到的，也看不出有多少片其实只是没查成、下次该重试。
    """
    def throttled(*_a, **_k):
        raise f.SearchThrottled("429 Too Many Requests")

    monkeypatch.setattr(f, "search_subtitles", throttled)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})

    assert result["status"] == "search_throttled"
    assert sorted(result["unattempted"]) == ["en", "zh"]
    assert "missing" not in result or result["missing"] == []


def test_throttled_search_stops_remaining_movies(sandbox, monkeypatch):
    """限流是全局状态，剩下的片不该一部部撞死在同一面墙上。"""
    calls = []

    def throttled(tmdb_id, *_a, **_k):
        calls.append(tmdb_id)
        raise f.SearchThrottled("429 Too Many Requests")

    monkeypatch.setattr(f, "search_subtitles", throttled)

    _, first = f.download_one({"tmdbId": "1", "year": 2000})
    assert first["status"] == "search_throttled"
    assert len(calls) == 1

    _, second = f.download_one({"tmdbId": "2", "year": 2000})
    assert second["status"] == "search_throttled"
    assert len(calls) == 1, "闸门落下后不该再查"


def test_real_not_found_is_recorded_in_the_ledger(sandbox, monkeypatch):
    """🔑 SubDL 库里没有这个 tmdb_id：确定性结论，全部语种进台账。

    早先它走 search_failed 不进台账，每次运行都把库里没有的片重搜一遍，
    库大了以后纯属噪音。与 search_throttled（要重试）必须分开。
    """
    def not_found(*_a, **_k):
        raise RuntimeError("can't find movie or tv")

    monkeypatch.setattr(f, "search_subtitles", not_found)
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "55", "title": "A", "year": 2000},
    ])
    f.main()

    assert f.load_ledger()["55"] == {"en", "zh"}
    assert not f.stop_fetching.is_set(), "查不到一部片不该拦下后面的片"


def test_other_search_errors_still_stay_out_of_the_ledger(sandbox, monkeypatch):
    """回归护栏：只有那条固定错误消息才进台账，其它查询失败仍是 search_failed。"""
    def boom(*_a, **_k):
        raise RuntimeError("SubDL 503")

    monkeypatch.setattr(f, "search_subtitles", boom)
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "55", "title": "A", "year": 2000},
    ])
    f.main()
    assert f.load_ledger() == {}


def test_skipped_movies_report_the_actual_stop_reason(sandbox, monkeypatch):
    """被闸门拦下的片要报真实原因，不能把限流笼统说成额度耗尽。"""
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: (_ for _ in ()).throw(
        f.SearchThrottled("429 Too Many Requests")
    ))
    f.download_one({"tmdbId": "1", "year": 2000})

    _, second = f.download_one({"tmdbId": "2", "year": 2000})
    assert second["status"] == "search_throttled"


def test_throttle_message_distinguishes_itself_from_quota(
    sandbox, monkeypatch, capsys
):
    """限流的收尾提示不能说"额度重置"——它跟额度没关系，稍后重试就行。"""
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "1", "title": "A", "year": 2000},
    ])
    monkeypatch.setattr(f, "download_one", lambda e, ledger=None: (e["tmdbId"], {
        "status": "search_throttled", "error": "429 Too Many Requests",
        "unattempted": ["en", "zh"],
    }))
    f.main()

    out = capsys.readouterr().out
    assert "持续限流" in out
    assert "这不是「源站没有字幕」" in out
    assert "1 部影片本次未处理" in out
    assert "额度重置" not in out, "限流与额度是两件事，不能混说"


# --------------------------------------------------- 日志不得泄露 API Key
# requests 的 HTTPError 消息里带完整请求 url，而搜索接口的 key 就在 query 里。
# 日志经常要贴出来排查，原样打印等于泄露凭证。

@pytest.mark.parametrize("text,must_not_contain", [
    ("429 for url: https://api.subdl.com/x?api_key=subdl_SECRET", "subdl_SECRET"),
    ("...?api_key=subdl_SECRET&tmdb_id=9794", "subdl_SECRET"),
    ('{"url": "https://dl.subdl.com/a.zip?api_key=subdl_SECRET"}', "subdl_SECRET"),
])
def test_redact_removes_api_key(text, must_not_contain):
    cleaned = f._redact(text)
    assert must_not_contain not in cleaned
    assert "api_key=REDACTED" in cleaned


def test_redact_keeps_the_rest_of_the_message_intact():
    """只抹 key，别的信息一个字都不能少——否则排查时失去线索。"""
    cleaned = f._redact(
        "429 Client Error: Too Many Requests for url: "
        "https://api.subdl.com/api/v1/subtitles?api_key=subdl_X&tmdb_id=9794"
    )
    assert "429 Client Error: Too Many Requests" in cleaned
    assert "tmdb_id=9794" in cleaned


def test_search_failure_log_does_not_leak_the_key(sandbox, monkeypatch, capsys):
    """端到端护栏：查询失败这条日志里不能出现明文 key。"""
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "1", "title": "A", "year": 2000},
    ])

    def leaky(*_a, **_k):
        raise requests.HTTPError(
            "429 for url: https://api.subdl.com/x?api_key=subdl_SECRET"
        )

    monkeypatch.setattr(f, "search_subtitles", leaky)
    f.main()

    out = capsys.readouterr().out
    assert "subdl_SECRET" not in out
    assert "api_key=REDACTED" in out


# ========================================== 多 api_key 轮换（额度按账号叠加）
# 2026-09-12 实测确认额度按**账号**计而非按 IP 计：同一台机器、同一条 url，
# 旧 key 429 / 新 key 200 / 旧 key 复核仍 429。故多 key 轮换真能叠加额度。
# 额度是这里最稀缺的资源，**一次都不能浪费**，这组用例就是守住这一点。

def test_parse_keys_handles_separators_and_dedupes():
    assert f._parse_keys("a,b,c") == ["a", "b", "c"]
    assert f._parse_keys(" a , b \n c ") == ["a", "b", "c"]
    assert f._parse_keys("a,a,b") == ["a", "b"], "重复的 key 不该占位置"
    assert f._parse_keys("") == []
    assert f._parse_keys(None) == []


def test_parse_keys_preserves_order():
    """保序：用户把额度多的 key 放前面时，应当先用它。"""
    assert f._parse_keys("z,a,m") == ["z", "a", "m"]


def test_plural_and_singular_env_vars_are_merged(monkeypatch):
    """🔑 复数与单数都配时要**合并**，不是二选一。

    早先写的是 `复数 or 单数`，用户加了 SUBDL_API_KEYS 却忘了删旧的
    SUBDL_API_KEY，那个账号一整天的额度就白扔了 —— 额度是这里最稀缺的资源。
    复用 _parse_keys 的去重，两处填了同一个 key 也不会占两个位置。
    """
    merged = f._parse_keys(" ".join(filter(None, ["k1,k2", "k3", ""])))
    assert merged == ["k1", "k2", "k3"]

    # 单数与复数重复填同一个 key：只算一个
    deduped = f._parse_keys(" ".join(filter(None, ["k1,k2", "k1", ""])))
    assert deduped == ["k1", "k2"]


def test_pool_hands_out_keys_in_order():
    pool = f.KeyPool(["k1", "k2"])
    assert pool.current() == "k1"
    assert pool.retire("k1", "quota") == "k2"
    assert pool.current() == "k2"


def test_pool_returns_none_when_all_exhausted():
    pool = f.KeyPool(["k1"])
    assert pool.retire("k1", "quota") is None
    assert pool.current() is None
    assert pool.all_exhausted() is True


def test_pool_ignores_stale_reports_from_other_threads():
    """🔑 核心并发保护：晚到的重复报告绝不能多跳一个 key。

    4 个线程共用当前 key，k1 耗尽时它们会各自撞 429、各自要求换人。
    若无脑 `index += 1`，一次就从 k1 跳到 k4 —— k2/k3 两个账号的额度
    **原封不动地被扔掉**。额度正是这里最稀缺的东西。
    """
    pool = f.KeyPool(["k1", "k2", "k3", "k4"])

    assert pool.retire("k1", "quota") == "k2"      # 第一个线程：真正推进
    assert pool.retire("k1", "quota") == "k2"      # 晚到的报告：忽略
    assert pool.retire("k1", "quota") == "k2"
    assert pool.retire("k1", "quota") == "k2"

    assert pool.current() == "k2", "k2 必须还在，绝不能被跳过"
    assert pool.exhausted_count() == 1


def test_pool_is_thread_safe_under_real_contention():
    """真并发：8 个线程同时报告同一个 key 失效，只能前进一格。"""
    pool = f.KeyPool([f"k{i}" for i in range(10)])
    barrier = threading.Barrier(8)
    errors = []

    def report():
        try:
            barrier.wait(timeout=5)
            pool.retire("k0", "quota")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=report) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors
    assert pool.current() == "k1", "8 个线程报同一个 key，只能前进一格"
    assert pool.exhausted_count() == 1


def test_request_switches_key_on_quota_and_succeeds(sandbox, monkeypatch):
    """🔑 第一个 key 撞额度时应换人重试，而不是让整批停摆。"""
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1", "k2"]))
    monkeypatch.setattr(f, "stop_fetching", threading.Event())
    used = []

    def fake_retry(method, url, params=None, **kwargs):
        used.append(params["api_key"])
        if params["api_key"] == "k1":
            raise f.QuotaExhausted("k1 用尽", reset_at="2026-09-13T00:00:00Z")
        return "OK"

    monkeypatch.setattr(f, "request_with_retry", fake_retry)
    assert f.request_with_keys("GET", "https://x") == "OK"
    assert used == ["k1", "k2"]
    assert not f.stop_fetching.is_set(), "还有 key 可用时不该落闸"


def test_stop_only_after_every_key_is_exhausted(sandbox, monkeypatch):
    """只有**所有** key 都用尽才落全局闸门 —— 这是与单 key 时代的关键差别。"""
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1", "k2", "k3"]))
    monkeypatch.setattr(f, "stop_fetching", threading.Event())
    monkeypatch.setattr(f, "stop_reason", {})
    used = []

    def always_exhausted(method, url, params=None, **kwargs):
        used.append(params["api_key"])
        raise f.QuotaExhausted("用尽")

    monkeypatch.setattr(f, "request_with_retry", always_exhausted)
    with pytest.raises(f.QuotaExhausted):
        f.request_with_keys("GET", "https://x")

    assert used == ["k1", "k2", "k3"], "每个 key 都要真正试过一次"
    assert f.stop_fetching.is_set()
    assert f.stop_reason["status"] == "quota_exhausted"


def test_throttled_key_is_not_retired(sandbox, monkeypatch):
    """🔑 短时限流不换 key：冷却后用**同一个** key 重试成功。

    早先 throttled 也退役 key。4 个线程并发打搜索接口，一次短时限流就能把
    所有 key 串烧光、报"全部 key 被限流"停跑——其实每个 key 额度都还在。
    """
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1", "k2"]))
    monkeypatch.setattr(f, "THROTTLE_COOLDOWN", 0)
    used = []

    def fake_retry(method, url, params=None, **kwargs):
        used.append(params["api_key"])
        if len(used) == 1:
            raise f.SearchThrottled("429")
        return "OK"

    monkeypatch.setattr(f, "request_with_retry", fake_retry)
    assert f.request_with_keys("GET", "https://x") == "OK"
    assert used == ["k1", "k1"], "限流后必须还是 k1"
    assert f.key_pool.exhausted_count() == 0, "限流不能烧掉 key"
    assert not f.stop_fetching.is_set()


def test_throttle_cooldown_is_global_and_bounded(sandbox, monkeypatch):
    """冷却走全局窗口；连续 THROTTLE_MAX_ROUNDS 轮仍 429 才落闸上抛。"""
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1"]))
    slept = []
    monkeypatch.setattr(f.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(f, "_throttle_until", 0.0)
    monkeypatch.setattr(f, "THROTTLE_COOLDOWN", 7)
    calls = []

    def always_throttled(method, url, params=None, **kwargs):
        calls.append(params["api_key"])
        raise f.SearchThrottled("429")

    monkeypatch.setattr(f, "request_with_retry", always_throttled)
    with pytest.raises(f.SearchThrottled):
        f.request_with_keys("GET", "https://x")

    assert len(calls) == f.THROTTLE_MAX_ROUNDS + 1
    assert len(slept) == f.THROTTLE_MAX_ROUNDS
    assert all(0 < s <= 7 for s in slept)
    assert f.key_pool.current() == "k1", "key 仍然在，没被退役"
    assert f.stop_reason["status"] == "search_throttled"


def test_concurrent_throttle_waits_share_one_window(sandbox, monkeypatch):
    """两个线程几乎同时撞 429 时只推一次窗口，第二个不再叠加一轮。"""
    monkeypatch.setattr(f, "_throttle_until", 0.0)
    monkeypatch.setattr(f, "THROTTLE_COOLDOWN", 5)
    slept = []
    monkeypatch.setattr(f.time, "sleep", lambda s: slept.append(s))

    first = f._wait_for_throttle()
    second = f._wait_for_throttle()
    assert first == 5
    assert second <= first, "第二个只等到同一时刻，不再叠加"


def test_threads_bail_out_once_throttle_stop_is_signalled(sandbox, monkeypatch):
    """别的线程已判定限流不可恢复时，本线程不再各自冷却一轮，直接上抛。"""
    monkeypatch.setattr(f, "key_pool", f.KeyPool(["k1"]))
    f._signal_stop("search_throttled", "429")
    monkeypatch.setattr(f, "request_with_retry", _never_called)
    with pytest.raises(f.SearchThrottled):
        f.request_with_keys("GET", "https://x")


def test_finish_message_differs_from_quota_exhausted_message(
    sandbox, monkeypatch, capsys
):
    """🔑 "跑完了"与"没额度了"必须说得明显不同。

    两者的下一步完全相反：前者无需再跑，后者明天还得来。
    混为一谈的话，用户看完日志不知道到底还要不要再来一趟。
    """
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "1", "title": "A", "year": 2000},
    ])
    monkeypatch.setattr(f, "download_one", lambda e: (e["tmdbId"], {
        "status": "ok", "saved": ["en.srt"], "missing": [],
    }))
    f.main()

    out = capsys.readouterr().out
    assert "全部待补影片已处理完毕" in out
    assert "无需今天再跑" in out
    assert "因配额耗尽而中断" not in out


def test_finish_message_still_reports_keys_burned_along_the_way(
    sandbox, monkeypatch, capsys
):
    """跑完了 ≠ 一路顺风：中途烧掉的 key 要如实说。

    真机实测出过这个假象：日志里明明有一次 key 耗尽切换，收尾却报
    "没有触发任何配额限制"。用户据此会以为 key 够用，实际已经烧掉一个了。
    """
    monkeypatch.setattr(f, "SUBDL_API_KEYS", ["k1", "k2"])
    pool = f.KeyPool(["k1", "k2"])
    pool.retire("k1", "quota")          # 模拟中途烧掉一个
    monkeypatch.setattr(f, "key_pool", pool)
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "1", "title": "A", "year": 2000},
    ])
    monkeypatch.setattr(f, "download_one", lambda e: (e["tmdbId"], {
        "status": "ok", "saved": ["en.srt"], "missing": [],
    }))
    f.main()

    out = capsys.readouterr().out
    assert "全部待补影片已处理完毕" in out
    assert "期间用尽了 1/2 个" in out
    assert "未触发任何配额限制" not in out, "烧掉了 key 就不能说没触发限制"


# ===================================== meta.json 乐观锁（与下载侧并行的前提）
# 本函数是"读-改-写"，下载侧上传成品时会整份覆盖同一个 key。裸写的时序：
#   本进程 get(v1) -> 下载侧 put(v2) -> 本进程 put(v1+字幕)   ← v2 被静默吃掉
# 带 If-Match 就能发现冲突并重读重试。

def test_meta_update_sends_if_match(remote_sandbox):
    """必须带上读取时拿到的 ETag，否则乐观锁形同虚设。"""
    remote_sandbox.meta = {"tmdbId": "55"}
    captured = {}
    original = remote_sandbox.put_object

    def spy(Bucket=None, Key=None, Body=None, **kwargs):
        captured.update(kwargs)
        return original(Bucket=Bucket, Key=Key, Body=Body, **kwargs)

    remote_sandbox.put_object = spy
    assert f._update_remote_meta("55", 2000, ["en.srt"]) == (True, 1)
    assert captured.get("IfMatch") == '"v1"'


def test_meta_update_retries_and_preserves_concurrent_change(remote_sandbox):
    """🔑 下载侧在我们读完之后改了 meta —— 它的改动一个字段都不能丢。"""
    remote_sandbox.meta = {"tmdbId": "55", "title": "旧"}
    state = {"done": False}

    def downloader_writes(stub):
        # 只在第一次 get 之后插一刀，模拟下载侧刚好此时覆盖了 meta.json
        if state["done"]:
            return
        state["done"] = True
        stub.meta = {"tmdbId": "55", "title": "旧", "posterPath": "/p.jpg"}
        stub.etag = '"v2"'

    remote_sandbox.on_get = downloader_writes
    assert f._update_remote_meta("55", 2000, ["en.srt"]) == (True, 1)

    assert remote_sandbox.conflicts == 1, "第一次写应当被 412 挡下"
    assert remote_sandbox.get_calls == 2, "冲突后必须重读"
    final = remote_sandbox.meta
    assert final["posterPath"] == "/p.jpg", "下载侧写的字段不能被覆盖掉"
    assert [e["path"] for e in final["subtitles"]] == ["subs/en.srt"]


def test_meta_update_gives_up_after_repeated_conflicts(remote_sandbox, capsys):
    """一直冲突时要有尽头，且字幕本身仍算上传成功。"""
    remote_sandbox.meta = {"tmdbId": "55"}

    def always_change(stub):
        stub.etag = f'"v{stub.get_calls + 100}"'   # 每次读完就变，必然冲突

    remote_sandbox.on_get = always_change
    assert f._update_remote_meta("55", 2000, ["en.srt"]) == (False, 0)
    assert remote_sandbox.conflicts == f.META_UPDATE_RETRIES
    assert "字幕已上传" in capsys.readouterr().out


def test_meta_update_does_not_retry_on_non_412(remote_sandbox, capsys):
    """非 412 的失败重试多少次都一样，当场放弃比空转三轮更诚实。"""
    remote_sandbox.meta = {"tmdbId": "55"}

    def boom(**_kwargs):
        raise RuntimeError("AccessDenied")

    remote_sandbox.put_object = boom
    assert f._update_remote_meta("55", 2000, ["en.srt"]) == (False, 0)
    assert remote_sandbox.get_calls == 1, "不该重读"


@pytest.mark.parametrize("exc,expected", [
    (_PreconditionFailed(), True),
    (RuntimeError("boom"), False),
])
def test_precondition_detector(exc, expected):
    assert f._is_precondition_failed(exc) is expected


# ---------------------------------------------------------------------------
# 缺口台账：源站确认没有的语种不再重复问（省配额）
# ---------------------------------------------------------------------------
# 背景：实测 101 部里 zh 缺口 57 部、en 缺口仅 5 部，SubDL 中文库很薄。
# 没有台账时每次重跑都把这 57 部的 zh 重查一遍，每次都空手而归。

def test_ledger_merges_multiple_lines_by_union(sandbox):
    """纯追加文件，同一片多行按并集合并（读取端聚合范式）。"""
    with open(f.LEDGER_LOG, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"tmdbId": "55", "missing": ["zh"]}) + "\n")
        fh.write(json.dumps({"tmdbId": "55", "missing": ["en"]}) + "\n")
        fh.write(json.dumps({"tmdbId": "66", "missing": ["ZH"]}) + "\n")

    ledger = f.load_ledger()
    assert ledger["55"] == {"en", "zh"}
    assert ledger["66"] == {"zh"}, "语种要归一成小写，否则匹配不上"


def test_ledger_tolerates_missing_file_and_bad_lines(sandbox):
    """🔑 台账是省配额的优化，不是正确性的一环——读不了就当空的，绝不能抛。"""
    assert f.load_ledger() == {}, "文件不存在应返回空台账"

    with open(f.LEDGER_LOG, "w", encoding="utf-8") as fh:
        fh.write("{ 这不是 json\n")
        fh.write(json.dumps({"missing": ["zh"]}) + "\n")          # 缺 tmdbId
        fh.write(json.dumps({"tmdbId": "77", "missing": "zh"}) + "\n")  # 非列表
        fh.write(json.dumps({"tmdbId": "88", "missing": ["zh"]}) + "\n")
    assert f.load_ledger() == {"88": {"zh"}}


def test_confirmed_gap_language_is_not_asked_again(sandbox, monkeypatch):
    """🔴 台账里已确认没有的语种，不能再发起搜索——这正是省配额的地方。"""
    f.record_gap("55", ["zh"])
    asked = []
    monkeypatch.setattr(f, "search_subtitles", lambda i, *_a, **_k: (
        asked.append(i), [{"language": "EN", "url": "/en.zip"}]
    )[1])
    monkeypatch.setattr(
        f, "request_with_retry",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"m.srt": _SRT})),
    )

    _, result = f.download_one(
        {"tmdbId": "55", "year": 2000}, ledger=f.load_ledger()
    )
    assert sorted(result["saved"]) == ["en.srt", "en.vtt"]
    assert "zh" not in result["missing"], "zh 压根没被尝试，不该再记一次"


def test_movie_with_all_gaps_confirmed_costs_no_quota(sandbox, monkeypatch):
    """所有缺口都已确认没有时整片跳过，一次请求都不发。"""
    f.record_gap("55", ["en", "zh"])
    called = []
    monkeypatch.setattr(f, "search_subtitles", lambda i, *_a, **_k: called.append(i))

    _, result = f.download_one(
        {"tmdbId": "55", "year": 2000}, ledger=f.load_ledger()
    )
    assert result["status"] == "gap_confirmed"
    assert called == [], "不能发任何请求"


def test_fetch_failure_never_enters_the_ledger(sandbox, monkeypatch):
    """🔴 本组最关键的一条：取回失败**绝不能**进台账。

    否则一次网络抖动就让该片该语种被永久放弃，而日志上看起来只是
    "源站没有"，无从察觉。台账只接受"源站确认没有"。
    """
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "55", "title": "A", "year": 2000},
    ])
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
        {"language": "ZH", "url": "/zh.zip"},
    ])

    def fake_request(method, url, **kwargs):
        if "/en.zip" in url:
            raise OSError("network down")     # en 取回失败
        return _FakeZipResp(_zip_bytes({"m.srt": _SRT}))

    monkeypatch.setattr(f, "request_with_retry", fake_request)
    f.main()

    ledger = f.load_ledger()
    assert "en" not in ledger.get("55", set()), "取回失败被错记进台账"


def test_source_confirmed_absence_is_recorded(sandbox, monkeypatch):
    """反过来：源站确认没有的，必须记进台账，否则省不下配额。"""
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "55", "title": "A", "year": 2000},
    ])
    # 只有 EN，zh 是真·源站没有
    monkeypatch.setattr(f, "search_subtitles", lambda *_a, **_k: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(
        f, "request_with_retry",
        lambda *a, **k: _FakeZipResp(_zip_bytes({"m.srt": _SRT})),
    )
    f.main()

    assert f.load_ledger()["55"] == {"zh"}


def test_ledger_write_failure_does_not_break_the_run(sandbox, monkeypatch, capsys):
    """写不进台账也只是少省点配额，不能影响主流程。"""
    monkeypatch.setattr(f, "LEDGER_LOG", "/nonexistent-dir/gaps.jsonl")
    f.record_gap("55", ["zh"])
    assert "缺口台账写入失败" in capsys.readouterr().out


def test_ledger_skipped_movies_are_counted_and_explained(
    sandbox, monkeypatch, capsys,
):
    """🔑 被台账跳过的片必须在收尾里说清楚。

    否则日志会宣称"当前 success.jsonl 里的片都已尝试过"，而那批片这次
    一个请求都没发 —— 日后排查"某片为什么始终没字幕"会白跑一轮。
    """
    f.record_gap("55", ["en", "zh"])
    monkeypatch.setattr(f, "load_entries", lambda: [
        {"tmdbId": "55", "title": "A", "year": 2000},
    ])
    called = []
    monkeypatch.setattr(f, "search_subtitles", lambda i, *_a, **_k: called.append(i))

    f.main()

    out = capsys.readouterr().out
    assert called == [], "台账命中的片不该发任何请求"
    assert "gap_confirmed" in out, "统计里要有这一类"
    assert "按缺口台账直接跳过" in out
    assert "都已尝试过" not in out, "这批片并没有被尝试，措辞不能这么写"
