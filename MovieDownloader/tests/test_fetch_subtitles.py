"""fetch_subtitles.py 的离线用例。

核心约束：**字幕是"可有可无"的附属物** —— 取不到、出错、缺配置都不能让
流程失败或以非零码退出。这些用例专门锁死这条原则，防止日后改动把它破坏。
"""

import io
import json
import os
import re
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
    monkeypatch.setattr(f, "SUBDL_API_KEY", "TESTKEY")
    monkeypatch.setattr(f, "STATE_LOG", str(tmp_path / "subtitles.jsonl"))
    monkeypatch.setattr(f, "REMOTE_MODE", False)
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


# ---------------------------------------------- 「可有可无」：不因字幕而失败

def test_missing_api_key_exits_quietly(monkeypatch, capsys):
    """没配 API Key 只跳过，不能抛 SystemExit —— 否则 cron/&& 串联会中断。"""
    monkeypatch.setattr(f, "SUBDL_API_KEY", "")
    f.main()   # 不抛异常即通过
    assert "跳过字幕补全" in capsys.readouterr().out


def test_missing_success_log_returns_empty(sandbox, monkeypatch, capsys):
    """success.jsonl 不存在是正常状态（还没下过片），不是错误。"""
    monkeypatch.setattr(f, "SUCCESS_LOG", str(sandbox / "nope.jsonl"))
    assert f.load_entries() == []
    assert "无需补字幕" in capsys.readouterr().out


def test_search_failure_does_not_raise(sandbox, monkeypatch):
    """SubDL 查询失败只标记该片，不抛异常。"""
    def boom(_):
        raise RuntimeError("SubDL 503")

    monkeypatch.setattr(f, "search_subtitles", boom)
    tmdb_id, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert tmdb_id == "55"
    assert result["status"] == "search_failed"


def test_one_language_failure_keeps_the_other(sandbox, monkeypatch):
    """单语种下载失败不影响另一语种。"""
    monkeypatch.setattr(f, "search_subtitles", lambda _: [
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
    assert result["missing"] == ["en"]
    assert sorted(result["saved"]) == ["zh.srt", "zh.vtt"]


def test_no_subtitle_found_is_not_an_error(sandbox, monkeypatch):
    """源站没有该片字幕是常态，状态仍是 ok、只记 missing。"""
    monkeypatch.setattr(f, "search_subtitles", lambda _: [])
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

    def flaky(entry):
        if entry["tmdbId"] == "1":
            raise RuntimeError("boom")
        return "2", {"status": "ok", "saved": ["en.vtt"], "missing": []}

    monkeypatch.setattr(f, "download_one", flaky)
    f.main()
    out = capsys.readouterr().out
    assert "异常（已跳过）" in out
    assert "完成。字幕文件 1 个" in out


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

    def boom(_):
        raise AssertionError("已有字幕不该再查 SubDL")

    monkeypatch.setattr(f, "search_subtitles", boom)
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert result["status"] == "skipped"


def test_only_missing_language_is_fetched(sandbox, monkeypatch):
    """只补缺的那个语种：en 已存在时只查 zh。"""
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    open(os.path.join(target, "en.vtt"), "w").close()

    monkeypatch.setattr(f, "search_subtitles", lambda _: [
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


def test_ass_is_kept_as_is(sandbox):
    """ass/ssa 的结构与 srt/vtt 完全不同，不做转换、原样保存。

    ⚠️ 参数必须与生产调用一致：download_one 传的是 `extension.lstrip(".")`，
    即**不带点**的扩展名。早先本用例传 ".ass"（带点），与 srt 用例传 "srt"
    （不带点）自相矛盾，掩盖了拼出 "enass"（无扩展名）的真实 bug。
    """
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    saved = f._write_variants(target, "en", "[Script Info]\n", "ass")
    assert saved == ["en.ass"]
    assert os.path.isfile(os.path.join(target, "en.ass"))


def test_ass_filename_matches_skip_pattern(sandbox):
    """原样保存的文件名必须能被"已存在语种"正则认出。

    否则每次运行都会重新抓一遍并再写一个同样的坏文件名。
    """
    target = f.subs_dir("55", 2000)
    os.makedirs(target)
    f._write_variants(target, "en", "[Script Info]\n", "ass")
    names = os.listdir(target)
    assert any(re.fullmatch(r"en\.\w+", n) for n in names), names


def test_oversized_zip_is_rejected(sandbox, monkeypatch):
    """源站返回异常大的内容时拒收，且**下载过程中**就中断。"""
    delivered = []

    class CountingResp(_FakeZipResp):
        def iter_content(self, chunk_size=65536):
            for i in range(0, 100, 8):
                delivered.append(i)
                yield b"x" * 8

    monkeypatch.setattr(f, "MAX_ZIP_BYTES", 10)
    monkeypatch.setattr(f, "search_subtitles", lambda _: [
        {"language": "EN", "url": "/en.zip"},
    ])
    monkeypatch.setattr(f, "request_with_retry", lambda *a, **k: CountingResp())
    _, result = f.download_one({"tmdbId": "55", "year": 2000})
    assert "en" in result["missing"]
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
    assert entries == [{"tmdbId": "55", "title": "A2", "year": 2000}]


# ==================================================== R2 模式（生产默认路径）
#
# 下载侧上传成功后会把整个影片目录删掉，所以"本地没有字幕目录"是最正常的
# 状态，绝不能据此判定该片缺字幕。这组用例锁死：已有语种问 R2、新字幕传 R2、
# 本地不留残留、meta.json 同步更新。

class _FakeS3:
    """最小 S3 桩：记录 upload/put，按预置对象列表回答 list/get。"""

    def __init__(self, keys=None, meta=None):
        self.keys = list(keys or [])
        self.meta = meta
        self.uploaded = []      # [(local_path, key)]
        self.put_objects = {}   # key -> bytes
        self.list_error = None
        self.upload_error = None

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
        return {"Body": io.BytesIO(json.dumps(self.meta).encode("utf-8"))}

    def put_object(self, Bucket=None, Key=None, Body=None, **kwargs):
        self.put_objects[Key] = Body


@pytest.fixture
def remote_sandbox(sandbox, monkeypatch):
    """R2 模式：REMOTE_MODE=True，S3 client 换成可断言的假对象。"""
    monkeypatch.setattr(f, "REMOTE_MODE", True)
    monkeypatch.setattr(f.dm, "S3_BUCKET", "test-bucket")
    monkeypatch.setattr(f.dm, "S3_PREFIX", "")
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
        f, "search_subtitles", lambda i: (asked.append(i), [])[1]
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
    monkeypatch.setattr(f, "search_subtitles", lambda _: [
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
    monkeypatch.setattr(f, "search_subtitles", lambda _: [
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
    monkeypatch.setattr(f, "search_subtitles", lambda _: [
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
    assert f._update_remote_meta("55", 2000, ["en.srt"]) is False
    assert remote_sandbox.put_objects == {}


def test_remote_missing_meta_does_not_break_subtitles(
    remote_sandbox, monkeypatch, capsys
):
    """meta.json 不存在时，字幕上传本身仍然算成功。"""
    remote_sandbox.meta = None     # get_object 会抛 NoSuchKey
    monkeypatch.setattr(f, "search_subtitles", lambda _: [
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

    assert saved == ["en.ass"], "必须识破并按 ass 原样保存"
    assert not os.path.exists(os.path.join(target, "en.srt"))
    assert not os.path.exists(os.path.join(target, "en.vtt"))
