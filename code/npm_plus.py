#!/usr/bin/env python3
import os
import json
import time
import logging
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Set

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException
from urllib3.util.retry import Retry
from tqdm import tqdm

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# 설정
MAX_WORKERS = 40
CHUNK_SIZE = 100000
NAMES_FILE = "../data/all-names.txt"
OUTPUT_DIR = "../result/npm"
# (신규) 에러 로그 파일 경로 정의
ERROR_LOG_FILE = os.path.join(OUTPUT_DIR, "error_log.csv")

# (신규) 스레드 세이프한 파일 쓰기를 위한 Lock 객체
log_lock = threading.Lock()

# 전역 세션
SESSION = requests.Session()
retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 504])
adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retry)
SESSION.mount('https://', adapter)
SESSION.mount('http://', adapter)
SESSION.headers.update({
    "Accept-Encoding": "gzip",
    "npm-replication-opt-in": "true"
})

# (신규) 에러를 CSV 파일에 기록하는 함수
def log_error(pkg_name: str, error_type: str, details: str = ""):
    """에러 정보를 스레드에 안전한 방식으로 CSV 파일에 기록합니다."""
    with log_lock:
        # 파일이 없으면 헤더를 추가
        file_exists = os.path.exists(ERROR_LOG_FILE)
        
        with open(ERROR_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "package_name", "error_type", "details"])
            
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([timestamp, pkg_name, error_type, details])

# ---- load_processed_names 함수 수정 ----
def load_processed_names(output_dir: str) -> Set[str]:
    """성공한 JSON 파일과 에러 로그 CSV에서 이미 처리된 모든 패키지 이름을 로드합니다."""
    processed_names = set()
    
    # 1. 성공한 패키지 로드 (기존 로직)
    if os.path.exists(output_dir):
        logging.info(f"Scanning for successfully processed packages in {output_dir}...")
        for filename in os.listdir(output_dir):
            if filename.startswith("npm_data_") and filename.endswith(".json"):
                file_path = os.path.join(output_dir, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for item in data:
                            if 'name' in item and 'error' not in item:
                                processed_names.add(item['name'])
                except (json.JSONDecodeError, IOError) as e:
                    logging.warning(f"Could not read or parse {file_path}, skipping: {e}")
    
    # 2. 에러난 패키지 로드 (추가된 로직)
    if os.path.exists(ERROR_LOG_FILE):
        logging.info(f"Scanning for failed packages in {ERROR_LOG_FILE}...")
        try:
            with open(ERROR_LOG_FILE, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # 헤더 스킵
                for row in reader:
                    if len(row) > 1:
                        processed_names.add(row[1]) # 패키지 이름은 2번째 열
        except Exception as e:
            logging.warning(f"Could not read or parse {ERROR_LOG_FILE}, skipping: {e}")

    if processed_names:
        logging.info(f"Found {len(processed_names)} previously processed (success or failed) packages.")
    return processed_names

def load_names_from_file(path: str) -> List[str]:
    # (이하 동일, 생략하지 않음)
    logging.info(f"Loading package names from file: {path}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            names = [line.strip() for line in f if line.strip()]
        logging.info(f"Loaded {len(names)} package names")
        return names
    except FileNotFoundError:
        logging.error(f"Names file not found: {path}")
        return []

def fetch_meta(pkg_name: str) -> Dict[str, Any]:
    # (이하 동일, 생략하지 않음)
    url = f"https://registry.npmjs.org/{pkg_name}"
    resp = SESSION.get(url, timeout=30)
    if resp.status_code == 404:
        # 404 에러는 예외 대신 에러 객체를 반환하도록 수정
        return {"name": pkg_name, "error": "Package not found", "status_code": 404}
    resp.raise_for_status()
    data = resp.json()
    latest_version_tag = data.get("dist-tags", {}).get("latest")
    if not latest_version_tag:
        return {"name": pkg_name, "error": "No latest version found"}
    ver_data = data.get("versions", {}).get(latest_version_tag, {})
    return {
        "name": pkg_name,
        "latest": latest_version_tag,
        "license": ver_data.get("license", ""),
        "dependencies": ver_data.get("dependencies", {})
    }

def save_chunk_json(data: List[Dict[str, Any]], index: int, start_num: int):
    # (이하 동일, 생략하지 않음)
    if not data:
        logging.warning(f"No data to save for chunk {index}.")
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_index = (start_num // CHUNK_SIZE) + index
    file_path = os.path.join(OUTPUT_DIR, f"npm_data_{file_index:04d}.json")
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved chunk {index} with {len(data)} items to {file_path}")
    except IOError as e:
        logging.error(f"Failed to save chunk {index} to {file_path}: {e}")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    processed_names_set = load_processed_names(OUTPUT_DIR)
    all_names = load_names_from_file(NAMES_FILE)
    if not all_names:
        return
    names_to_process = [name for name in all_names if name not in processed_names_set]
    total_to_process = len(names_to_process)
    if total_to_process == 0:
        logging.info("All packages have already been processed. Nothing to do.")
        return
    logging.info(f"Total packages to process: {total_to_process} (out of {len(all_names)} total)")
    chunks = [names_to_process[i:i + CHUNK_SIZE] for i in range(0, total_to_process, CHUNK_SIZE)]
    logging.info(f"Split into {len(chunks)} chunk(s) of up to {CHUNK_SIZE} names each")
    start_num = len(processed_names_set)

    for idx, chunk_names in enumerate(chunks, start=1):
        logging.info(f"Processing chunk {idx}/{len(chunks)} with {len(chunk_names)} packages")
        results = []
        start_time = time.time()
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                future_to_name = {pool.submit(fetch_meta, name): name for name in chunk_names}
                
                # ---- main 루프 예외 처리 수정 ----
                for future in tqdm(as_completed(future_to_name), total=len(chunk_names), desc=f"Chunk {idx}"):
                    pkg_name = future_to_name[future]
                    try:
                        meta = future.result()
                        # fetch_meta가 반환한 에러 객체(e.g. 404) 처리
                        if 'error' in meta:
                            log_error(pkg_name, meta['error'], str(meta.get('status_code', '')))
                        else:
                            results.append(meta)
                    except HTTPError as e:
                        log_error(pkg_name, "HTTP Error", str(e.response.status_code))
                    except RequestException as e:
                        log_error(pkg_name, "Request Exception", e.__class__.__name__)
                    except Exception as e:
                        log_error(pkg_name, "Unexpected Exception", str(e))
        except KeyboardInterrupt:
            logging.warning("Process interrupted by user (Ctrl+C). Saving fetched results before exit.")
            if results: save_chunk_json(results, idx, start_num)
            logging.info("Exiting.")
            return
        elapsed = time.time() - start_time
        logging.info(f"Fetched {len(results)}/{len(chunk_names)} successful metas in {elapsed:.2f}s")
        if results: save_chunk_json(results, idx, start_num)
    logging.info("All chunks processed successfully.")

if __name__ == "__main__":
    main()