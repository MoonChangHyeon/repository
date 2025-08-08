#!/usr/bin/env python3
"""
npm 전체 패키지 이름 수집 스크립트
- CouchDB _all_docs + startkey/limit 방식
- 작성: 2025-08-07
"""
import json, os, sys, time
from urllib.parse import quote
import requests

REGISTRY_URL = "https://replicate.npmjs.com/registry/_all_docs"
PAGE_LIMIT    = 10_000               # 1회 호출에서 가져올 행 수
OUT_FILE      = "all-names.txt"      # 결과 저장 파일
PAUSE_SEC     = 1.0                  # 호출 간 지연(친화적 접근)

def fetch_page(startkey_json: str | None):
    params = {"limit": PAGE_LIMIT}
    if startkey_json:
        params["startkey"] = startkey_json

    r = requests.get(
        REGISTRY_URL,
        params=params,
        headers={"Accept-Encoding": "gzip"}  # 그대로 둬도 자동 해제
    )
    r.raise_for_status()
    return r.json()          # 바로 JSON 파싱 → dict


def main():
    # ① 이어받기 지원: 기존 파일이 있으면 마지막 줄을 startkey 로
    start = None
    if os.path.isfile(OUT_FILE):
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            try:
                last_name = f.readlines()[-1].strip()
                if last_name:
                    # CouchDB startkey 는 JSON 인코딩된 문자열이어야 함 ⇒ "\"foo\""
                    start = json.dumps(last_name)
                    print(f"[resume] last={last_name!r} → startkey={start}")
            except IndexError:
                pass

    session = requests.Session()  # 재사용 커넥션

    while True:
        try:
            page = fetch_page(start)
        except Exception as e:
            print("⚠️  fetch error:", e, file=sys.stderr)
            print("⏸  10 초 후 재시도…", file=sys.stderr)
            time.sleep(10)
            continue

        rows = page.get("rows", [])
        if not rows:
            print("✅  끝! 더 받을 데이터가 없습니다.")
            break

        # ② 파일 append
        with open(OUT_FILE, "a", encoding="utf-8") as f:
            for row in rows:
                name = row["key"]          # key == id (패키지명)
                f.write(name + "\n")

        # ③ 다음 페이지 준비 (startkey 는 ‘포함’이라 중복 한 줄 제거 필요)
        last_name = rows[-1]["key"]
        start = json.dumps(last_name)      # JSON 문자열로 인코딩
        print(f"📦  {len(rows):>5}개 저장, 마지막 = {last_name!r}")
        time.sleep(PAUSE_SEC)

if __name__ == "__main__":
    main()
