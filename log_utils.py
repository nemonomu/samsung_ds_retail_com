"""
로그 저장 유틸리티
- 스크래퍼 실행 시작 시 setup_log() 호출 → 임시 파일에 로그 기록
- 스크래퍼 종료 시 save_log() 호출 → 종료 시각 기준 파일명으로 저장
"""
import logging
import os
import shutil
from datetime import datetime


_log_handlers = {}  # scraper_name → FileHandler
_log_temp_files = {}  # scraper_name → temp file path

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')


def setup_log(scraper_name):
    """스크래퍼 시작 시 호출. 임시 로그 파일에 기록 시작."""
    os.makedirs(LOG_DIR, exist_ok=True)

    temp_path = os.path.join(LOG_DIR, f'{scraper_name}_latest.log')
    handler = logging.FileHandler(temp_path, encoding='utf-8', mode='w')
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logging.getLogger().addHandler(handler)
    _log_handlers[scraper_name] = handler
    _log_temp_files[scraper_name] = temp_path


def save_log(scraper_name):
    """스크래퍼 종료 시 호출. 종료 시각 기준 파일명으로 로그 저장."""
    handler = _log_handlers.pop(scraper_name, None)
    temp_path = _log_temp_files.pop(scraper_name, None)

    if handler:
        handler.flush()
        handler.close()
        logging.getLogger().removeHandler(handler)

    if temp_path and os.path.exists(temp_path):
        end_time = datetime.now().strftime('%Y%m%d%H%M')
        final_path = os.path.join(LOG_DIR, f'{end_time}_{scraper_name}.log')
        shutil.copy2(temp_path, final_path)
        print(f"[LOG] 로그 저장: {final_path}")
