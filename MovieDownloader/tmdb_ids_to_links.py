import random
from curl_cffi import requests
import re
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import yaml


def load_config():
    """读取同目录 config.yaml 中本脚本对应的配置段；缺失时返回空字典。"""
    cfg_path = Path(__file__).with_name("config.yaml")
    if not cfg_path.exists():
        return {}
    with open(cfg_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("tmdb_ids_to_links", {}) or {}


_CFG = load_config()
_PROXY_CFG = _CFG.get("proxy", {}) or {}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Referer": "https://vidup.to/",
    "X-Requested-With": "XMLHttpRequest"
}

# 抓取 vidup.to 页面时不能带 X-Requested-With，否则会被 Cloudflare 拦成 403
PAGE_HEADERS = {k: v for k, v in HEADERS.items() if k != "X-Requested-With"}

API = _CFG.get("api", "https://enc-dec.app/api")
MAX_RETRIES = _CFG.get("max_retries", 3)
RETRY_DELAY = _CFG.get("retry_delay", 2)  # 秒
TIMEOUT = _CFG.get("timeout", 20)  # 单个 HTTP 请求超时（秒）


# 代理开关：设为 True 时启用下方代理，False 则直连
# 注意：vidup.to 有 Cloudflare 机房 IP 拦截，直连会返回 403，必须走住宅代理
USE_PROXY = _PROXY_CFG.get("enabled", True)
PROXY_HOST = _PROXY_CFG.get("host", "unmetered.residential.proxyrack.net")
PROXY_USER = _PROXY_CFG.get("user", "daran")
PROXY_PASS = _PROXY_CFG.get("password", "TYQSUK9-3VKDM4M-MH365O9-HPDIYIG-O9YHYN9-FXCSKNO-QMS83CJ")
PROXY_PORT_RANGE = tuple(_PROXY_CFG.get("port_range", (9000, 9050)))  # 每次随机取一个端口，换一个出口 IP


def build_proxy():
    """返回 requests 用的 proxies；未启用代理时返回 None 表示直连。"""
    if not USE_PROXY:
        return None
    port = random.randint(*PROXY_PORT_RANGE)
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{port}"
    return {"http": proxy_url, "https": proxy_url}


# ---------- 辅助函数 ----------
def validate(data, path):
    if data.get("status") != 200:
        error_msg = data.get("error", "unknown")
        raise Exception(f"API Error at {path}: status={data.get('status')}, error={error_msg}")
    return data["result"]


def _is_retriable(exc):
    """判断异常是否值得重试：只对瞬时错误（网络/SSL/超时/连接中断/5xx/403）重试。
    真无源的干净 404（All servers failed / stream 取流 404）不重试——重试也还是 404，纯浪费。
    """
    msg = str(exc)
    # 取流阶段所有 server 都失败：只有当 Last error 是干净 404（=vidup 真无源）才不重试；
    # 若是瞬时错误（SSL/超时等）导致的 All servers failed，仍需重试，避免误丢可成功的片。
    if "All servers failed" in msg:
        return "404" not in msg
    # 明确的瞬时错误特征：连接中断、SSL、超时、代理错误、连接被拒
    retriable_markers = (
        "curl: (35)", "SSL", "timed out", "Timeout", "timeout",
        "Connection", "connection", "Failed to perform", "Proxy",
        "Could not resolve", "Recv failure", "Send failure",
        "403",  # 偶发 Cloudflare 限流，换 IP 重试可能过
        "500", "502", "503", "504",
        "API Error",  # enc/dec 服务瞬时非200，换次重试可能过
        "No servers found",  # 偶发 server 列表空，重试可能拿到
        "Extract failed (retriable)",  # 页面200但提取不到加密文本：可能撞Cloudflare挑战页，换IP重试
    )
    return any(m in msg for m in retriable_markers)


def process_tmdb_id(tmdb_id):
    """处理单个 TMDB ID，仅对瞬时错误重试。

    返回 (status, 结果字典或None)，status 三态供多轮捞回区分：
      - "ok"    成功，附结果字典 {urls, tmdbId, title}
      - "dead"  真无源的干净404 → 永久排除，绝不再抓
      - "retry" 瞬时错误（含页面提取不到加密文本）换 IP 重试 MAX_RETRIES 次仍失败 → 下一轮重跑
    """
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        # 每个 id、每次重试用一个独立 Session：
        # - session 内部复用 TCP/TLS 连接，减少同一 id 内多次请求的握手开销；
        # - session 绑定本次 build_proxy() 的随机出口 IP，作用域仅限本次尝试，
        #   结束即关闭，因此“同一 id 同 IP、不同 id 换 IP”的轮换粒度保持不变。
        with requests.Session(impersonate="chrome") as session:
            proxy = build_proxy()
            if proxy:
                session.proxies = proxy
            try:
                # 1. 获取页面，提取加密文本
                base_url = f"https://vidup.to/movie/{tmdb_id}/"
                resp = session.get(base_url, timeout=TIMEOUT, headers=PAGE_HEADERS)
                resp.raise_for_status()
                html = resp.text

                match_1 = re.search(r'\\"en\\":\\"(.*?)\\"', html)
                match = re.search(r'\\"token\\":\\"(.*?)\\"', html)
                if match_1:
                    text = match_1.group(1)
                else:
                    if match:
                        text = match.group(1)
                    else:
                        # 页面 200 但提取不到加密文本：可能真无源，也可能是本次代理 IP
                        # 撞上 Cloudflare 挑战页/半截 HTML。抛可重试异常走换 IP 重试通道，
                        # 换几个出口 IP 仍提取不到才由重试耗尽逻辑收敛（更可能是真无源）。
                        raise Exception(f"Extract failed (retriable) for {tmdb_id}")

                # 2. 调用 enc-vidup 获取 parts
                enc_vidup = f"{API}/enc-vidup?text={text}"
                resp = session.get(enc_vidup, timeout=TIMEOUT, headers=HEADERS)
                resp.raise_for_status()
                data = resp.json()
                parts = validate(data, enc_vidup)
                servers = parts['servers']
                stream = parts['stream']
                token = parts['token']

                headers_with_token = HEADERS.copy()
                headers_with_token["X-CSRF-Token"] = token

                # 3. 获取加密的服务器列表
                resp = session.post(servers, headers=headers_with_token, timeout=TIMEOUT)
                servers_encrypted = resp.text

                # 4. 解密服务器列表
                dec_vidup = f"{API}/dec-vidup"
                resp = session.post(dec_vidup, json={"text": servers_encrypted}, timeout=TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                servers_decrypted = validate(data, dec_vidup)

                if not servers_decrypted:
                    raise Exception("No servers found")

                # ========== 方案C：遍历所有服务器，收集全部可用 url ==========
                last_server_error = None
                urls = []
                result_tmdb_id = None
                result_title = None
                for server in servers_decrypted:
                    try:
                        data_val = server['data']
                        # 5. 获取加密的流数据
                        stream_url = f"{stream}/{data_val}"
                        resp = session.post(stream_url, headers=headers_with_token, timeout=TIMEOUT)
                        resp.raise_for_status()
                        stream_encrypted = resp.text

                        # 6. 解密流数据
                        resp = session.post(dec_vidup, json={"text": stream_encrypted}, timeout=TIMEOUT)
                        resp.raise_for_status()
                        data = resp.json()
                        stream_decrypted = validate(data, dec_vidup)

                        url = stream_decrypted.get("url")
                        tid = stream_decrypted.get("tmdbId")
                        if url and tid:
                            if url not in urls:
                                urls.append(url)
                            # tmdbId/title 各 server 一致，取首个成功的即可
                            if result_tmdb_id is None:
                                result_tmdb_id = tid
                                result_title = stream_decrypted.get("title")
                        else:
                            last_server_error = "Missing url or tmdbId in decrypted data"
                    except Exception as e:
                        last_server_error = e
                        server_name = server.get('name', 'unknown')
                        print(f"  Server '{server_name}' failed: {e}")
                        continue

                if urls and result_tmdb_id:
                    # 收集到至少一个可用节点，返回全部备用 url
                    return "ok", {
                        "urls": urls,
                        "tmdbId": result_tmdb_id,
                        "title": result_title,
                    }

                # 所有服务器都尝试失败
                raise Exception(f"All servers failed for {tmdb_id}. Last error: {last_server_error}")

            except Exception as e:
                last_exception = e
                # 真无源的干净 404 不重试，直接失败退出，省掉 2/3 的无用请求与 sleep
                if not _is_retriable(e):
                    print(f"  [无源 404] {tmdb_id}: {e}")
                    return "dead", None
                print(f"  [Attempt {attempt}/{MAX_RETRIES}] 瞬时错误 for {tmdb_id}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"  All {MAX_RETRIES} attempts failed for ID {tmdb_id}")

    # 瞬时错误重试耗尽 → 交给多轮捞回，下一轮重跑
    return "retry", None


def load_processed_ids(results_file, fail_file):
    processed = set()
    if results_file.exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        tid = obj.get('tmdbId')
                        if tid is not None:
                            processed.add(str(tid))
                    except:
                        pass
    if fail_file.exists():
        with open(fail_file, 'r', encoding='utf-8') as f:
            for line in f:
                tid = line.strip()
                if tid:
                    processed.add(tid)
    return processed


def run_batch(to_process, results_file, fail_file, max_workers):
    """并发处理一批 ID，实时落盘 ok/dead，返回本批“瞬时耗尽”待重跑的 ID 列表。

      - "ok"    → 追加写 results_file（output）
      - "dead"  → 追加写 fail_file，永久排除
      - "retry" → 不落盘，收集返回，交给上层多轮循环下一轮重跑
    """
    lock = threading.Lock()
    ok_count = 0
    dead_count = 0
    retry_ids = []

    def process_one(tid):
        status, result = process_tmdb_id(tid)
        with lock:
            if status == "ok" and result:
                with open(results_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                print(f"✅ SUCCESS: {result.get('title')} ({result.get('tmdbId')})")
            elif status == "retry":
                retry_ids.append(tid)
                print(f"🔁 RETRY-LATER: {tid}")
            else:  # "dead" 或异常兜底
                with open(fail_file, 'a', encoding='utf-8') as f:
                    f.write(f"{tid}\n")
                print(f"❌ DEAD: {tid}")
        return status

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, tid): tid for tid in to_process}
        for future in as_completed(futures):
            tid = futures[future]
            try:
                status = future.result()
                if status == "ok":
                    ok_count += 1
                elif status == "retry":
                    pass  # 已收集进 retry_ids
                else:
                    dead_count += 1
            except Exception as e:
                print(f"⚠️  Unexpected exception for {tid}: {e}")
                with lock:
                    retry_ids.append(tid)

    print(f"\n本批完成 | 成功 {ok_count} | 真无源 {dead_count} | 待重跑 {len(retry_ids)}")
    return retry_ids


def main():
    ids_file = Path(_CFG.get("input", "ids.txt"))
    results_file = Path(_CFG.get("output", "results.jsonl"))
    fail_file = Path(_CFG.get("fail_file", "fail.txt"))

    if not ids_file.exists():
        print("ids.txt not found!")
        return

    with open(ids_file, 'r', encoding='utf-8') as f:
        ids = [line.strip() for line in f if line.strip()]

    processed = load_processed_ids(results_file, fail_file)
    to_process = [tid for tid in ids if tid not in processed]
    print(f"Total IDs: {len(ids)}, Already processed: {len(processed)}, To process: {len(to_process)}")

    if not to_process:
        print("All IDs processed.")
        return

    max_workers = _CFG.get("max_workers", 20)
    max_rounds = _CFG.get("max_rounds", 8)

    # ---- 内嵌多轮捞回：首轮跑全部，之后每轮只重跑上一轮“瞬时耗尽”的 ID ----
    # ok 累加进 output、dead 累加进 fail_file 均在 run_batch 内实时落盘，
    # 故 output 执行完即为“原结果 + 捞回结果”的合并（原 total_results.jsonl）。
    pending = to_process
    round_no = 0
    while pending:
        round_no += 1
        print(f"\n{'=' * 70}")
        print(f"==> 第 {round_no}/{max_rounds} 轮 | 待处理 {len(pending)} 个 ID")
        print(f"{'=' * 70}")

        retry_ids = run_batch(pending, results_file, fail_file, max_workers)

        if not retry_ids:
            print("\n==> 瞬时失败已清零，所有有源 ID 已捞干净，正常结束。")
            break
        if round_no >= max_rounds:
            # 达上限仍未捞回的，写入 fail_file 归档（视为难以捞回）
            with open(fail_file, 'a', encoding='utf-8') as f:
                for tid in retry_ids:
                    f.write(f"{tid}\n")
            print(f"\n==> 已达最大轮数 {max_rounds}，剩余 {len(retry_ids)} 个瞬时失败 ID 归入 fail_file。")
            break
        pending = retry_ids

    print(f"\nAll done. 共跑 {round_no} 轮，结果已合并写入 {results_file}")


if __name__ == "__main__":
    main()
