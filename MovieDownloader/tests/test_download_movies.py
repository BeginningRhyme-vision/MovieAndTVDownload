"""download_movies.py 多源下载改动的离线用例。

覆盖本轮扩源在下载侧的改动：urls 条目归一（兼容旧的裸字符串形态）、
mp4 直链请求头、失败分类与被拒原因归类。全部为纯函数级测试，不联网。
"""

import pytest

import download_movies as d


# -------------------------------------------------------------- urls 条目归一

def test_plain_string_url_is_treated_as_vidup_m3u8():
    """旧 results.jsonl 的 urls 是裸字符串数组。归一后必须还能消费，
    否则换代码后历史数据全部读不动。"""
    node = d._normalize_url_entry("https://a/master.m3u8")
    assert node == {
        "url": "https://a/master.m3u8", "provider": "vidup", "type": "m3u8",
        "headers": {}, "quality": None, "size": None,
    }


def test_dict_entry_keeps_provider_headers_quality_size():
    node = d._normalize_url_entry({
        "url": "https://cdn/1080.mp4", "provider": "vidlink", "type": "mp4",
        "headers": {"User-Agent": "okhttp/4.9.3"}, "quality": 1080, "size": 1234,
    })
    assert node["type"] == "mp4"
    assert node["provider"] == "vidlink"
    assert node["headers"] == {"User-Agent": "okhttp/4.9.3"}
    assert node["quality"] == 1080 and node["size"] == 1234


@pytest.mark.parametrize("item", [
    None, 123, [], "", "   ",
    {"url": ""},
    {"url": None},
    {"url": "https://a/x", "type": "torrent"},   # 未知 type 无法下载
])
def test_invalid_entries_are_dropped(item):
    # 非法条目返回 None 由调用方跳过，不能让一条脏数据整片失败
    assert d._normalize_url_entry(item) is None


def test_entry_type_defaults_to_m3u8_and_is_case_insensitive():
    assert d._normalize_url_entry({"url": "https://a/x"})["type"] == "m3u8"
    assert d._normalize_url_entry({"url": "https://a/x", "type": "MP4"})["type"] == "mp4"


def test_non_dict_headers_fall_back_to_empty():
    node = d._normalize_url_entry({"url": "https://a/x", "headers": "oops"})
    assert node["headers"] == {}


@pytest.mark.parametrize("field", ["quality", "size"])
@pytest.mark.parametrize("bad", [0, -1, -1080, "0", "-5"])
def test_non_positive_quality_and_size_become_none(field, bad):
    """results.jsonl 是跨进程的不可信输入。负数/0 会造成实质损害：
    quality<=0 让节点被判定性淘汰；size<=0 作为总长兜底会算出空区间。
    归 None 即"未声明"，交由下游实测，是安全的退化方向。"""
    node = d._normalize_url_entry({"url": "https://a/x", field: bad})
    assert node[field] is None


def test_positive_quality_and_size_survive():
    node = d._normalize_url_entry({"url": "https://a/x", "quality": 1080, "size": 123})
    assert node["quality"] == 1080 and node["size"] == 123


# ------------------------------------------------------------ mp4 直链请求头

def test_mp4_headers_strip_referer_and_xhr():
    """vidlink CDN 带任何 Referer 都会 429、浏览器 UA 无 Referer 会 428，
    只有 okhttp UA + 无 Referer 能拿到 206。None 值用于让 requests 删掉 Session 默认头。"""
    headers = d._mp4_request_headers({"User-Agent": "okhttp/4.9.3"})
    assert headers["Referer"] is None
    assert headers["X-Requested-With"] is None
    assert headers["User-Agent"] == "okhttp/4.9.3"


def test_mp4_headers_work_without_node_headers():
    headers = d._mp4_request_headers(None)
    assert headers["Referer"] is None


# ------------------------------------------------------------------ parse_int

@pytest.mark.parametrize("value,expected", [
    (1080, 1080), ("1080", 1080), ("  1080  ", 1080),
    (True, None),      # bool 是 int 子类，size=true 不能变成 1
    (False, None),
    (1080.0, None),    # 严格解析：float 走 str() 后是 "1080.0"，int() 抛错
    (None, None), ("", None), ("abc", None), ([], None),
])
def test_parse_int_rejects_bool_and_garbage(value, expected):
    assert d.parse_int(value) == expected


# ------------------------------------------------------------------ 失败分类

def test_direct_link_expiry_is_permanent_for_this_script():
    """vidlink 直链带时效签名，403/410 表示这条 url 过期了。

    本脚本读的是固化的 results.jsonl，没有重新取流的能力——多轮重投拿到的还是
    同一条过期 url，必然再挂。故这里判死、只留 failed.jsonl，靠重跑
    tmdb_ids_to_links.py 换一条新直链来修复。
    """
    msg = f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: https://cdn/a.mp4"
    assert d._classify_failure(msg) is False


def test_generic_http_403_does_not_kill_the_whole_movie():
    """对比上一条：非直链的 403 只用于"层内短路"（尽快换下一个节点），
    不能让整片判死——它常常只是源站临时风控，冷却一轮往往就恢复。"""
    assert d._HTTP_PERMANENT_MARKER not in d._PERMANENT_FAILURE_MARKERS


@pytest.mark.parametrize("msg", [
    "分辨率 640x360 低于红线",
    "码率未达到门槛：500 kbps < 1480 kbps",
    "缺少 tmdbId 或 urls",
])
def test_quality_failures_are_permanent(msg):
    # 真不达标的片重试多少轮都一样，不能浪费代理流量
    assert d._classify_failure(msg) is False


@pytest.mark.parametrize("msg", [
    "HTTP 500 Server Error",
    "Read timed out",
    "本轮候选流无一入选（各流原因见上方日志），下一轮重采",
])
def test_transient_failures_are_retriable(msg):
    assert d._classify_failure(msg) is True


def test_reject_reason_rules_are_ordered_before_generic_ones():
    """归类是按顺序首个命中，直链专属类目必须排在通用的"超时/SSL"之前，
    否则含 timeout 字样的直链错误会被归错桶，统计失真。"""
    reason = d.classify_reject_reason(
        f"直链已失效（HTTP 403），{d._NEEDS_REFETCH_MARKER}: https://cdn/a.mp4")
    assert "重新取流" in reason
