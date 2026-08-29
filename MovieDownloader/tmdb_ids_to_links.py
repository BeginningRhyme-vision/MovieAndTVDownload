import random
import requests
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


def process_tmdb_id(tmdb_id):
    """处理单个 TMDB ID，内部自动重试，返回 (成功标志, 结果字典或None)"""
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        # 每个 id、每次重试用一个独立 Session：
        # - session 内部复用 TCP/TLS 连接，减少同一 id 内多次请求的握手开销；
        # - session 绑定本次 build_proxy() 的随机出口 IP，作用域仅限本次尝试，
        #   结束即关闭，因此“同一 id 同 IP、不同 id 换 IP”的轮换粒度保持不变。
        with requests.Session() as session:
            proxy = build_proxy()
            if proxy:
                session.proxies.update(proxy)
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
                        print(f'Failed to extract encrypted text from page for: {tmdb_id}')
                        return None, None

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

                # ========== 关键改动：遍历所有服务器，逐个尝试 ==========
                last_server_error = None
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

                        result = {
                            "url": stream_decrypted.get("url"),
                            "tmdbId": stream_decrypted.get("tmdbId"),
                            "title": stream_decrypted.get("title")
                        }
                        if result["url"] and result["tmdbId"]:
                            # 成功获取数据，返回结果
                            return True, result
                        else:
                            # 数据不完整，视为失败，继续尝试下一个服务器
                            last_server_error = "Missing url or tmdbId in decrypted data"
                            continue
                    except Exception as e:
                        last_server_error = e
                        server_name = server.get('name', 'unknown')
                        print(f"  Server '{server_name}' failed: {e}")
                        continue

                # 所有服务器都尝试失败
                raise Exception(f"All servers failed for {tmdb_id}. Last error: {last_server_error}")

            except Exception as e:
                last_exception = e
                print(f"  [Attempt {attempt}/{MAX_RETRIES}] Error for {tmdb_id}: {e}")
                if attempt < MAX_RETRIES:
                    print(f"  Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"  All {MAX_RETRIES} attempts failed for ID {tmdb_id}")

    return False, None


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

    # 多线程配置
    MAX_WORKERS = _CFG.get("max_workers", 20)  # 并发线程数，可调整
    lock = threading.Lock()  # 文件写入锁
    success_count = 0
    fail_count = 0

    def process_one(tid):
        """线程执行的包装函数，负责调用处理并写入结果"""
        success, result = process_tmdb_id(tid)
        with lock:
            if success and result:
                with open(results_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                print(f"✅ SUCCESS: {result.get('title')} ({result.get('tmdbId')})")
                return True
            else:
                with open(fail_file, 'a', encoding='utf-8') as f:
                    f.write(f"{tid}\n")
                print(f"❌ FAILED: {tid}")
                return False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_one, tid): tid for tid in to_process}
        for future in as_completed(futures):
            tid = futures[future]
            try:
                ok = future.result()
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                print(f"⚠️  Unexpected exception for {tid}: {e}")
                with lock:
                    with open(fail_file, 'a', encoding='utf-8') as f:
                        f.write(f"{tid}\n")
                fail_count += 1

    print(f"\nDone. Success: {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    main()
