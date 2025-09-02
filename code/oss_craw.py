from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
import time
import json
import os
from datetime import datetime
import argparse
import concurrent.futures
import random


# 마지막 페이지 하드코딩 제거: 빈 페이지를 만나면 자동 종료

class SimpleOSSIndexScraper:
    def __init__(self, headless=True):
        self.setup_driver(headless)
        self.wait = WebDriverWait(self.driver, 15)
    
    def setup_driver(self, headless=True):
        """Chrome 드라이버 설정"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
    
    def get_component_list(self, query="a", component_type="cargo", page: int = 0):
        """Component 리스트만 간단하게 가져오기

        매개변수
        - query: 검색어(기본 a)
        - component_type: 생태계 타입(cargo/npm/pypi 등)
        - page: 페이지 번호(0 기반)
        """
        components = []
        
        try:
            # 검색 페이지로 이동
            url = f"https://ossindex.sonatype.org/search?type={component_type}&q={query}&page={page}"
            print(f"페이지 접속: {url}")
            self.driver.get(url)
            
            # 페이지 로딩 대기
            print("페이지 로딩 중...")
            time.sleep(5)  # 충분한 로딩 시간
            
            # Component 리스트 추출
            print("Component 리스트 추출 중...")
            components = self.extract_components()
            
            if not components:
                # 디버깅: 현재 페이지 상태 확인
                print("컴포넌트를 찾을 수 없습니다. 페이지 상태 확인 중...")
                self.debug_current_page()
                
        except Exception as e:
            print(f"오류 발생: {e}")
        
        return components
    
    def extract_components(self):
        """페이지에서 컴포넌트 리스트 추출"""
        components = []
        
        # 가능한 컴포넌트 선택자들 (우선순위별)
        selectors = [
            # 테이블 기반
            "table tbody tr",
            ".table tbody tr",
            
            # 카드/리스트 기반  
            "[data-testid*='component']",
            ".component-item", 
            ".component-card",
            ".search-result",
            ".result-item",
            ".card",
            
            # 일반적인 리스트
            "ul li",
            ".list-item",
            
            # div 기반
            "div[class*='component']",
            "div[class*='result']"
        ]
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    print(f"'{selector}' 선택자로 {len(elements)}개 요소 발견")
                    
                    for element in elements:
                        text = element.text.strip()
                        if text and len(text) > 10:  # 의미있는 텍스트만
                            # 링크가 있으면 가져오기
                            link = None
                            try:
                                link_element = element.find_element(By.TAG_NAME, "a")
                                link = link_element.get_attribute("href")
                            except:
                                pass
                            
                            component = {
                                'text': text,
                                'link': link
                            }
                            components.append(component)
                    
                    if components:  # 컴포넌트를 찾았으면 중단
                        break
                        
            except Exception as e:
                continue
        
        return components
    
    def debug_current_page(self):
        """현재 페이지 디버깅 정보"""
        try:
            print(f"현재 URL: {self.driver.current_url}")
            print(f"페이지 제목: {self.driver.title}")
            
            # body 텍스트 일부 출력
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            print(f"페이지 내용 (첫 500자):\n{body_text[:500]}...")
            
            # 주요 element 확인
            important_elements = ["table", "ul", "div[class*='component']", "div[class*='result']"]
            for elem in important_elements:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, elem)
                    if elements:
                        print(f"{elem}: {len(elements)}개 발견")
                except:
                    pass
                    
        except Exception as e:
            print(f"디버깅 중 오류: {e}")
    
    def save_components(self, components, filename="components.json"):
        """컴포넌트 리스트 저장"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(components, f, ensure_ascii=False, indent=2)
            print(f"컴포넌트 리스트를 {filename}에 저장했습니다.")
        except Exception as e:
            print(f"저장 실패: {e}")

    def save_links_only(self, components, out_dir="result", filename: str | None = None,
                        query: str = "a", component_type: str = "cargo", page: int = 0):
        """컴포넌트에서 링크만 추출하여 ./result 폴더에 JSON 리스트로 저장

        파일명 기본 규칙: ossindex_{type}_q-{query}_p-{page}_{ts}.json
        """
        try:
            os.makedirs(out_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            if filename is None:
                safe_q = str(query).replace('/', '_')
                filename = os.path.join(out_dir, f"ossindex_{component_type}_q-{safe_q}_p-{page}_{ts}.json")

            links = [c.get('link') for c in components if c.get('link')]
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(links, f, ensure_ascii=False, indent=2)
            print(f"링크 리스트를 {filename}에 저장했습니다. (총 {len(links)}건)")
        except Exception as e:
            print(f"링크 저장 실패: {e}")

    def crawl_until_empty(self, query: str = "a", component_type: str = "cargo",
                          start_page: int = 1, out_dir: str = "result",
                          sleep_sec: float = 1.5, max_pages: int = 1000) -> dict:
        """start_page부터 결과가 없는 페이지를 만날 때까지 순차 수집

        동작
        - 각 페이지마다 링크 JSON을 `ossindex_{type}_q-{query}_p-{page}_*.json`으로 저장
        - 수집된 모든 링크를 통합하여 `ossindex_{type}_q-{query}_p-{start}_to_{end}_*.json`으로 저장
        - 빈 페이지를 만나면 중단
        - 안전장치로 max_pages를 두어 무한 순회를 방지
        """
        os.makedirs(out_dir, exist_ok=True)
        all_links: list[str] = []
        pages_crawled = 0
        last_page = start_page - 1
        for i in range(max_pages):
            p = start_page + i
            print(f"\n=== 페이지 {p} 수집 시작 ===")
            comps = self.get_component_list(query=query, component_type=component_type, page=p)
            if not comps:
                print(f"페이지 {p}에서 컴포넌트를 찾지 못했습니다. 순회를 종료합니다.")
                break
            # 페이지별 링크 저장
            self.save_links_only(comps, out_dir=out_dir, query=query, component_type=component_type, page=p)
            # 통합 링크 적재(중복 방지)
            page_links = [c.get('link') for c in comps if c.get('link')]
            all_links.extend(page_links)
            pages_crawled += 1
            last_page = p
            # 예의상 대기
            time.sleep(sleep_sec)

        # 통합본 저장
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_q = str(query).replace('/', '_')
        combined_path = os.path.join(out_dir, f"ossindex_{component_type}_q-{safe_q}_p-{start_page}_to_{last_page}_{ts}.json")
        unique_links = sorted(set([lnk for lnk in all_links if lnk]))
        with open(combined_path, 'w', encoding='utf-8') as f:
            json.dump(unique_links, f, ensure_ascii=False, indent=2)
        print(f"\n통합 링크 리스트 저장: {combined_path} (총 {len(unique_links)}건)")
        return {
            "start_page": start_page,
            "last_page": last_page,
            "pages_crawled": pages_crawled,
            "total_links": len(unique_links),
            "combined_path": combined_path,
            "links": unique_links,
        }

    def crawl_az(self, component_type: str = "cargo", from_letter: str = "a", to_letter: str = "z",
                 start_page: int = 1, out_dir: str = "result", sleep_sec: float = 1.5,
                 max_pages: int = 1000) -> dict:
        """query를 a~z로 바꿔가며 순차 수집

        - 각 알파벳에 대해 start_page부터 빈 페이지가 나올 때까지 수집
        - 페이지별/쿼리별 JSON 저장 + 전체 통합본 저장
        """
        letters = [chr(c) for c in range(ord(from_letter.lower()), ord(to_letter.lower()) + 1)]
        all_links: list[str] = []
        per_query_summary = []
        for idx, q in enumerate(letters):
            sp = start_page if idx == 0 else 1
            print(f"\n##### 쿼리 '{q}' 수집 시작 (start_page={sp}) #####")
            res = self.crawl_until_empty(query=q, component_type=component_type, start_page=sp,
                                         out_dir=out_dir, sleep_sec=sleep_sec, max_pages=max_pages)
            all_links.extend(res.get("links", []))
            per_query_summary.append({
                "query": q,
                "pages_crawled": res.get("pages_crawled", 0),
                "total_links": res.get("total_links", 0),
                "combined_path": res.get("combined_path"),
            })

        # 전체 통합본 저장
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        combined_all = os.path.join(out_dir, f"ossindex_{component_type}_az_{from_letter}_{to_letter}_{ts}.json")
        unique_all = sorted(set(all_links))
        with open(combined_all, 'w', encoding='utf-8') as f:
            json.dump(unique_all, f, ensure_ascii=False, indent=2)
        print(f"\n전체 통합 링크 리스트 저장: {combined_all} (총 {len(unique_all)}건)")
        return {
            "from_letter": from_letter,
            "to_letter": to_letter,
            "total_links": len(unique_all),
            "combined_all": combined_all,
            "queries": per_query_summary,
        }

    def crawl_until_empty_parallel(self, query: str = "a", component_type: str = "cargo",
                                   start_page: int = 1, out_dir: str = "result",
                                   concurrency: int = 4) -> dict:
        """start_page부터 결과가 없는 페이지를 만날 때까지 병렬 수집(바운디드 동시성)

        전략
        - 최초에 `concurrency` 개의 페이지를 제출하고, 각 결과가 끝날 때마다 다음 페이지를 하나씩 제출
        - 빈 페이지가 관찰되면 추가 제출을 중단하고, 진행 중인 것만 수집
        - 각 작업은 독립적인 headless Chrome 인스턴스를 사용(리소스 고려 필요)
        """
        os.makedirs(out_dir, exist_ok=True)
        all_links: list[str] = []
        submitted: dict[concurrent.futures.Future, int] = {}
        next_page = start_page
        stop = False
        pages_done = set()

        def fetch_one(page_no: int):
            inst = SimpleOSSIndexScraper(headless=True)
            try:
                comps = inst.get_component_list(query=query, component_type=component_type, page=page_no)
            finally:
                inst.close()
            return page_no, comps

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
            # 초기 제출
            for _ in range(concurrency):
                fut = ex.submit(fetch_one, next_page)
                submitted[fut] = next_page
                next_page += 1

            while submitted:
                done, _pending = concurrent.futures.wait(submitted.keys(), return_when=concurrent.futures.FIRST_COMPLETED)
                for fut in done:
                    page_no, comps = fut.result()
                    submitted.pop(fut, None)
                    pages_done.add(page_no)

                    if comps:
                        # 저장 및 누적
                        self.save_links_only(comps, out_dir=out_dir, query=query, component_type=component_type, page=page_no)
                        page_links = [c.get('link') for c in comps if c.get('link')]
                        all_links.extend(page_links)
                    else:
                        # 빈 페이지 → 더 이상 제출하지 않음
                        stop = True

                    # 다음 페이지 제출(중단 플래그가 없는 경우에만)
                    if not stop:
                        fut2 = ex.submit(fetch_one, next_page)
                        submitted[fut2] = next_page
                        next_page += 1

        # 통합 저장
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_q = str(query).replace('/', '_')
        end_page = max(pages_done) if pages_done else start_page - 1
        combined_path = os.path.join(out_dir, f"ossindex_{component_type}_q-{safe_q}_p-{start_page}_to_{end_page}_{ts}.json")
        unique_links = sorted(set([lnk for lnk in all_links if lnk]))
        with open(combined_path, 'w', encoding='utf-8') as f:
            json.dump(unique_links, f, ensure_ascii=False, indent=2)
        print(f"\n[병렬] 통합 링크 리스트 저장: {combined_path} (총 {len(unique_links)}건)")
        return {
            "start_page": start_page,
            "end_page": end_page,
            "total_links": len(unique_links),
            "combined_path": combined_path,
        }
    
    def close(self):
        """드라이버 종료"""
        if hasattr(self, 'driver'):
            self.driver.quit()

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OSS Index 검색 → 링크 JSON 저장 (단일/연속/알파벳/병렬)")
    p.add_argument("--query", "-q", default="a", help="검색어(기본: a)")
    p.add_argument("--type", "-t", default="cargo", help="생태계 타입(cargo/npm/pypi 등)")
    p.add_argument("--page", "-p", type=int, default=0, help="페이지 번호(0 기반, 기본: 0)")
    p.add_argument("--headless", action="store_true", help="헤드리스 모드(브라우저 창 숨김)")
    # 연속 수집 옵션(빈 페이지를 만날 때까지)
    p.add_argument("--all", action="store_true", help="시작 페이지부터 결과가 없을 때까지 순차 수집")
    p.add_argument("--from-page", type=int, default=1, help="시작 페이지(기본: 1)")
    p.add_argument("--sleep", type=float, default=1.5, help="페이지 간 대기(초), 기본 1.5")
    p.add_argument("--max-pages", type=int, default=1000, help="안전장치용 최대 페이지 수(기본: 1000)")
    # 알파벳 시리즈 옵션
    p.add_argument("--az", action="store_true", help="query를 a~z까지 바꿔가며 연속 수집")
    p.add_argument("--from-letter", default="a", help="시작 글자(기본: a)")
    p.add_argument("--to-letter", default="z", help="끝 글자(기본: z)")
    # 병렬 옵션
    p.add_argument("--parallel", action="store_true", help="연속 수집을 병렬(바운디드 동시성)로 수행")
    p.add_argument("--concurrency", type=int, default=4, help="동시 크롤링 드라이버 수(기본: 4)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    scraper = None
    try:
        # 기본은 headless=False였으나, 옵션으로 제어
        scraper = SimpleOSSIndexScraper(headless=args.headless)
        if args.az:
            scraper.crawl_az(component_type=args.type,
                             from_letter=args.from_letter, to_letter=args.to_letter,
                             start_page=max(1, args.from_page), out_dir="result",
                             sleep_sec=args.sleep, max_pages=args.max_pages)
        elif args.all and args.parallel:
            scraper.crawl_until_empty_parallel(query=args.query, component_type=args.type,
                                               start_page=max(1, args.from_page), out_dir="result",
                                               concurrency=max(1, args.concurrency))
        elif args.all:
            scraper.crawl_until_empty(query=args.query, component_type=args.type,
                                      start_page=max(1, args.from_page), out_dir="result",
                                      sleep_sec=args.sleep, max_pages=args.max_pages)
        else:
            # 단일 페이지 수집
            components = scraper.get_component_list(query=args.query, component_type=args.type, page=args.page)

            if components:
                print(f"\n=== 총 {len(components)}개 Component 발견 ===")
                for i, component in enumerate(components, 1):
                    print(f"{i}. {component['text'][:100]}...")
                    if component['link']:
                        print(f"   링크: {component['link']}")
                    print("-" * 50)

                # 전체 저장(현재 폴더)
                scraper.save_components(components)
                # 링크만 저장(./result 폴더, 파일명에 type/query/page 포함)
                scraper.save_links_only(components, query=args.query, component_type=args.type, page=args.page)
            else:
                print("Component를 찾을 수 없습니다.")
    except Exception as e:
        print(f"실행 중 오류: {e}")
        return 1
    finally:
        if scraper:
            scraper.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
설치 필요:
pip install selenium

Chrome 드라이버 설치:
- https://chromedriver.chromium.org/ 에서 Chrome 버전에 맞는 드라이버 다운로드
- PATH에 추가하거나 같은 폴더에 위치
"""
