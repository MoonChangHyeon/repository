#!/usr/bin/env python3
"""
생태계 JSON 파서 with 재시작 북마크 기능
- result/Json 디렉토리의 모든 생태계 파일을 순차 처리
- 중단점에서 재시작 가능한 북마크 시스템
- 진행상황 자동 저장 및 복원
- 작성: 2025-08-11
"""
import json
import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from pathlib import Path
from tqdm import tqdm
import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# 설정
JSON_INPUT_DIR = "../../result/Json"
BOOKMARK_FILE = "ecosystem_parser_bookmark.json"
MAX_WORKERS = 4
BATCH_SIZE = 1000
PROGRESS_INTERVAL = 10000
AUTO_SAVE_INTERVAL = 50000  # 5만개마다 북마크 저장

# MariaDB 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'database': 'package_parser_db',
    'user': 'fortify',
    'password': 'Fortify!234',
    'charset': 'utf8mb4',
    'autocommit': False
}

# 생태계별 테이블 매핑
ECOSYSTEM_TABLES = {
    'npm': {'packages': 'npm_packages', 'versions': 'npm_package_versions'},
    'pypi': {'packages': 'pypi_packages', 'versions': 'pypi_package_versions'},
    'maven': {'packages': 'maven_packages', 'versions': 'maven_package_versions'},
    'nuget': {'packages': 'nuget_packages', 'versions': 'nuget_package_versions'},
    'go': {'packages': 'go_packages', 'versions': 'go_package_versions'},
    'rubygems': {'packages': 'rubygems_packages', 'versions': 'rubygems_package_versions'},
    'cargo': {'packages': 'cargo_packages', 'versions': 'cargo_package_versions'}
}

# 글로벌 변수
current_bookmark = {}
db_lock = threading.Lock()

@contextmanager
def get_db_connection():
    """데이터베이스 연결을 안전하게 관리합니다."""
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        yield connection
    except Error as e:
        logging.error(f"Database connection error: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()

def load_bookmark() -> Dict[str, Any]:
    """저장된 북마크를 로드합니다."""
    if os.path.exists(BOOKMARK_FILE):
        try:
            with open(BOOKMARK_FILE, 'r', encoding='utf-8') as f:
                bookmark = json.load(f)
                logging.info(f"📖 북마크 로드됨: {bookmark.get('last_saved', 'Unknown')}")
                logging.info(f"   마지막 처리: {bookmark.get('last_ecosystem', 'None')}")
                logging.info(f"   마지막 파일: {bookmark.get('last_file', 'None')}")
                logging.info(f"   처리된 패키지: {bookmark.get('total_processed', 0):,}개")
                return bookmark
        except Exception as e:
            logging.warning(f"북마크 로드 실패: {e}")
    
    return {
        'completed_ecosystems': [],
        'completed_files': [],
        'current_ecosystem': None,
        'current_file': None,
        'current_file_position': 0,
        'total_processed': 0,
        'total_saved': 0,
        'start_time': time.time(),
        'last_saved': time.strftime('%Y-%m-%d %H:%M:%S')
    }

def save_bookmark(bookmark: Dict[str, Any]):
    """현재 진행상황을 북마크로 저장합니다."""
    global current_bookmark
    bookmark['last_saved'] = time.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        with open(BOOKMARK_FILE, 'w', encoding='utf-8') as f:
            json.dump(bookmark, f, ensure_ascii=False, indent=2)
        current_bookmark = bookmark.copy()
        logging.info(f"💾 북마크 저장됨: {bookmark['total_processed']:,}개 처리됨")
    except Exception as e:
        logging.error(f"북마크 저장 실패: {e}")

def should_skip_ecosystem(ecosystem: str, bookmark: Dict[str, Any]) -> bool:
    """이미 완료된 생태계인지 확인합니다."""
    return ecosystem in bookmark.get('completed_ecosystems', [])

def should_skip_file(file_path: str, bookmark: Dict[str, Any]) -> bool:
    """이미 완료된 파일인지 확인합니다."""
    return file_path in bookmark.get('completed_files', [])

def detect_ecosystem(filename: str) -> str:
    """파일명에서 생태계를 추출합니다."""
    filename_lower = filename.lower()
    
    if 'npm' in filename_lower:
        return 'npm'
    elif 'pypi' in filename_lower:
        return 'pypi'
    elif 'maven' in filename_lower:
        return 'maven'
    elif 'nuget' in filename_lower:
        return 'nuget'
    elif 'go' in filename_lower:
        return 'go'
    elif 'rubygems' in filename_lower:
        return 'rubygems'
    elif 'cargo' in filename_lower:
        return 'cargo'
    else:
        return 'unknown'

def find_json_files() -> Dict[str, List[str]]:
    """Json 디렉토리에서 생태계별 파일들을 찾습니다."""
    input_path = Path(JSON_INPUT_DIR)
    
    if not input_path.exists():
        logging.error(f"Input directory not found: {JSON_INPUT_DIR}")
        return {}
    
    ecosystem_files = {}
    
    for file_path in input_path.glob("*.json"):
        ecosystem = detect_ecosystem(file_path.name)
        if ecosystem != 'unknown':
            ecosystem_files.setdefault(ecosystem, []).append(str(file_path))
    
    # 파일 크기 순으로 정렬 (작은 것부터)
    for ecosystem in ecosystem_files:
        ecosystem_files[ecosystem].sort(key=lambda x: os.path.getsize(x))
    
    total_files = sum(len(files) for files in ecosystem_files.values())
    logging.info(f"Found {total_files} JSON files across {len(ecosystem_files)} ecosystems")
    
    for ecosystem, files in ecosystem_files.items():
        total_size = sum(os.path.getsize(f) for f in files)
        logging.info(f"  {ecosystem.upper()}: {len(files)} files ({total_size/1024/1024:.1f} MB)")
    
    return ecosystem_files

def parse_json_file_with_bookmark(file_path: str, ecosystem: str, bookmark: Dict[str, Any]) -> Dict[str, Any]:
    """북마크 지원으로 JSON 파일을 파싱합니다."""
    try:
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        start_position = 0
        
        # 북마크에서 시작 위치 확인
        if (bookmark.get('current_file') == file_path and 
            bookmark.get('current_ecosystem') == ecosystem):
            start_position = bookmark.get('current_file_position', 0)
            if start_position > 0:
                logging.info(f"📍 재시작: {file_name} (위치: {start_position:,})")
        
        logging.info(f"Processing {file_name} ({file_size / 1024 / 1024:.1f} MB)")
        
        packages = []
        processed_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            # 큰 파일의 경우 한 줄씩 읽기
            if file_size > 100 * 1024 * 1024:  # 100MB 이상
                logging.info(f"Large file detected, reading line by line: {file_name}")
                
                for line_num, line in enumerate(f):
                    # 시작 위치 스킵
                    if line_num < start_position:
                        continue
                        
                    line = line.strip()
                    if line and not line.startswith('[') and not line.startswith(']'):
                        if line.endswith(','):
                            line = line[:-1]
                        try:
                            package = json.loads(line)
                            packages.append(package)
                            processed_count += 1
                            
                            # 자동 저장 간격마다 북마크 업데이트
                            if processed_count % AUTO_SAVE_INTERVAL == 0:
                                bookmark.update({
                                    'current_ecosystem': ecosystem,
                                    'current_file': file_path,
                                    'current_file_position': line_num + 1,
                                    'total_processed': bookmark.get('total_processed', 0) + processed_count
                                })
                                save_bookmark(bookmark)
                                
                                # 메모리 관리를 위해 배치 저장
                                if packages:
                                    saved_count = save_packages_to_db(packages, ecosystem)
                                    bookmark['total_saved'] = bookmark.get('total_saved', 0) + saved_count
                                    packages = []  # 메모리 해제
                                    logging.info(f"  💾 중간 저장: {saved_count:,}개 (누적: {bookmark['total_saved']:,}개)")
                            
                            if processed_count % PROGRESS_INTERVAL == 0:
                                logging.info(f"  Processed {processed_count:,} packages from {file_name}")
                                
                        except json.JSONDecodeError:
                            continue
            else:
                # 작은 파일은 일반적인 방식으로 읽기
                content = f.read().strip()
                if content.startswith('[') and content.endswith(']'):
                    packages = json.loads(content)
                else:
                    for line in content.split('\n'):
                        line = line.strip()
                        if line and line != ',' and not line.startswith('[') and not line.startswith(']'):
                            if line.endswith(','):
                                line = line[:-1]
                            try:
                                package = json.loads(line)
                                packages.append(package)
                            except json.JSONDecodeError:
                                continue
        
        logging.info(f"Parsed {len(packages):,} packages from {file_name}")
        
        return {
            'file_path': file_path,
            'file_name': file_name,
            'ecosystem': ecosystem,
            'package_count': len(packages),
            'packages': packages,
            'success': True
        }
        
    except Exception as e:
        error_msg = f"Error parsing {file_path}: {str(e)}"
        logging.error(error_msg)
        return {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'ecosystem': ecosystem,
            'error': error_msg,
            'success': False
        }

def save_packages_to_db(packages_data: List[Dict[str, Any]], ecosystem: str) -> int:
    """패키지 데이터를 데이터베이스에 저장합니다."""
    if not packages_data or ecosystem not in ECOSYSTEM_TABLES:
        return 0
    
    tables = ECOSYSTEM_TABLES[ecosystem]
    packages_table = tables['packages']
    versions_table = tables['versions']
    
    saved_count = 0
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            for i in range(0, len(packages_data), BATCH_SIZE):
                batch = packages_data[i:i + BATCH_SIZE]
                
                for package in batch:
                    try:
                        name = package.get('Name', '').strip()
                        versions = package.get('Versions', [])
                        
                        if not name or not versions:
                            continue
                        
                        # 패키지 삽입/업데이트
                        if ecosystem == 'maven':
                            parts = name.split(':')
                            if len(parts) >= 2:
                                group_id = parts[0]
                                artifact_id = parts[1]
                                
                                package_sql = f"""
                                INSERT INTO {packages_table} (name, group_id, artifact_id)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
                                """
                                cursor.execute(package_sql, (name, group_id, artifact_id))
                            else:
                                continue
                        else:
                            package_sql = f"""
                            INSERT INTO {packages_table} (name)
                            VALUES (%s)
                            ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
                            """
                            cursor.execute(package_sql, (name,))
                        
                        # 패키지 ID 가져오기
                        package_id = cursor.lastrowid
                        if not package_id:
                            cursor.execute(f"SELECT id FROM {packages_table} WHERE name = %s", (name,))
                            result = cursor.fetchone()
                            package_id = result[0] if result else None
                        
                        if package_id and versions:
                            cursor.execute(f"DELETE FROM {versions_table} WHERE package_id = %s", (package_id,))
                            
                            version_data = [
                                (package_id, str(version)[:500])  # 버전 길이 제한
                                for version in versions
                                if version and str(version).strip()
                            ]
                            
                            if version_data:
                                version_sql = f"INSERT INTO {versions_table} (package_id, version) VALUES (%s, %s)"
                                cursor.executemany(version_sql, version_data)
                        
                        saved_count += 1
                        
                    except Error as e:
                        logging.warning(f"Failed to save package {package.get('Name', 'unknown')} in {ecosystem}: {e}")
                        continue
                
                conn.commit()
            
    except Error as e:
        logging.error(f"Database error while saving {ecosystem} packages: {e}")
        
    return saved_count

def process_ecosystem_with_bookmark(ecosystem: str, file_paths: List[str], bookmark: Dict[str, Any]) -> Dict[str, Any]:
    """북마크 지원으로 특정 생태계의 모든 파일을 처리합니다."""
    if should_skip_ecosystem(ecosystem, bookmark):
        logging.info(f"⏭️  {ecosystem.upper()} 생태계 이미 완료됨 - 스킵")
        return {'ecosystem': ecosystem, 'skipped': True}
    
    logging.info(f"\n🌐 Processing {ecosystem.upper()} ecosystem ({len(file_paths)} files)...")
    
    start_time = time.time()
    total_packages = 0
    total_saved = 0
    processed_files = 0
    failed_files = 0
    
    for file_path in file_paths:
        if should_skip_file(file_path, bookmark):
            logging.info(f"⏭️  {os.path.basename(file_path)} 이미 완료됨 - 스킵")
            continue
            
        try:
            # 파일 파싱
            result = parse_json_file_with_bookmark(file_path, ecosystem, bookmark)
            
            if result['success']:
                packages = result['packages']
                if packages:
                    saved_count = save_packages_to_db(packages, ecosystem)
                    total_saved += saved_count
                    total_packages += len(packages)
                
                processed_files += 1
                
                # 파일 완료 후 북마크 업데이트
                bookmark.setdefault('completed_files', []).append(file_path)
                bookmark['total_processed'] = bookmark.get('total_processed', 0) + len(packages)
                bookmark['total_saved'] = bookmark.get('total_saved', 0) + saved_count
                bookmark['current_file_position'] = 0  # 파일 완료 시 위치 리셋
                save_bookmark(bookmark)
                
                logging.info(f"  ✅ {result['file_name']}: {len(packages):,} packages → {saved_count:,} saved")
            else:
                failed_files += 1
                logging.error(f"  ❌ {result['file_name']}: {result.get('error', 'Unknown error')}")
                
        except KeyboardInterrupt:
            logging.info(f"\n⚠️  사용자 중단 요청 - 북마크 저장 중...")
            save_bookmark(bookmark)
            logging.info(f"💾 진행상황이 저장되었습니다. 다음 실행 시 이어서 계속됩니다.")
            raise
        except Exception as e:
            failed_files += 1
            logging.error(f"  ❌ {os.path.basename(file_path)}: {str(e)}")
    
    # 생태계 완료 후 북마크 업데이트
    bookmark.setdefault('completed_ecosystems', []).append(ecosystem)
    save_bookmark(bookmark)
    
    elapsed_time = time.time() - start_time
    
    summary = {
        'ecosystem': ecosystem,
        'processed_files': processed_files,
        'failed_files': failed_files,
        'total_packages': total_packages,
        'total_saved': total_saved,
        'processing_time': elapsed_time,
        'success_rate': (total_saved / total_packages * 100) if total_packages > 0 else 0
    }
    
    logging.info(f"🎯 {ecosystem.upper()} Summary:")
    logging.info(f"  Files: {processed_files}/{len(file_paths)} processed")
    logging.info(f"  Packages: {total_saved:,}/{total_packages:,} saved ({summary['success_rate']:.1f}%)")
    logging.info(f"  Time: {elapsed_time:.1f}s")
    
    return summary

def main():
    """메인 실행 함수"""
    global current_bookmark
    
    logging.info("🚀 Starting Ecosystem Parser with Resume Support...")
    
    # 북마크 로드
    bookmark = load_bookmark()
    current_bookmark = bookmark
    
    # 사용자에게 재시작 옵션 제공
    if bookmark.get('total_processed', 0) > 0:
        print(f"\n📖 이전 진행상황 발견:")
        print(f"   처리된 패키지: {bookmark['total_processed']:,}개")
        print(f"   저장된 패키지: {bookmark.get('total_saved', 0):,}개")
        print(f"   완료된 생태계: {len(bookmark.get('completed_ecosystems', []))}개")
        print(f"   마지막 저장: {bookmark.get('last_saved', 'Unknown')}")
        
        choice = input("\n계속 진행하시겠습니까? (y/n, 기본값=y): ").strip().lower()
        if choice == 'n':
            # 새로 시작
            bookmark = {
                'completed_ecosystems': [],
                'completed_files': [],
                'current_ecosystem': None,
                'current_file': None,
                'current_file_position': 0,
                'total_processed': 0,
                'total_saved': 0,
                'start_time': time.time(),
                'last_saved': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            save_bookmark(bookmark)
            logging.info("🔄 새로운 파싱 세션 시작...")
    
    try:
        # JSON 파일들 찾기
        ecosystem_files = find_json_files()
        if not ecosystem_files:
            logging.error("No JSON files found. Exiting.")
            return
        
        overall_start_time = time.time()
        all_summaries = []
        
        # 생태계별 순차 처리
        for ecosystem, file_paths in ecosystem_files.items():
            try:
                summary = process_ecosystem_with_bookmark(ecosystem, file_paths, bookmark)
                if not summary.get('skipped'):
                    all_summaries.append(summary)
            except KeyboardInterrupt:
                logging.info("\n⚠️  프로그램이 중단되었습니다.")
                return
            except Exception as e:
                logging.error(f"Failed to process {ecosystem}: {e}")
                continue
        
        # 전체 완료 시 북마크 삭제
        if os.path.exists(BOOKMARK_FILE):
            os.remove(BOOKMARK_FILE)
            logging.info("🗑️  모든 작업 완료 - 북마크 파일 삭제됨")
        
        # 전체 요약
        overall_time = time.time() - overall_start_time
        total_files = sum(s.get('processed_files', 0) + s.get('failed_files', 0) for s in all_summaries)
        total_packages = sum(s.get('total_packages', 0) for s in all_summaries)
        total_saved = sum(s.get('total_saved', 0) for s in all_summaries)
        
        logging.info(f"\n" + "="*60)
        logging.info(f"🎉 ECOSYSTEM PARSING COMPLETE!")
        logging.info(f"="*60)
        logging.info(f"📊 Overall Statistics:")
        logging.info(f"  Ecosystems: {len(all_summaries)}")
        logging.info(f"  Files: {total_files}")
        logging.info(f"  Packages: {total_saved:,}/{total_packages:,} saved ({total_saved/total_packages*100:.1f}%)")
        logging.info(f"  Time: {overall_time:.1f}s")
        
        logging.info(f"\n📋 Ecosystem Breakdown:")
        for summary in sorted(all_summaries, key=lambda x: x.get('total_saved', 0), reverse=True):
            logging.info(f"  {summary['ecosystem'].upper()}: {summary.get('total_saved', 0):,} packages")
    
    except KeyboardInterrupt:
        logging.info(f"\n⚠️  프로그램이 사용자에 의해 중단되었습니다.")
        logging.info(f"💾 다음 실행 시 '{BOOKMARK_FILE}' 파일로부터 이어서 계속됩니다.")

if __name__ == "__main__":
    main()