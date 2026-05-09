"""
BestBuy 자동 복구 스크립트 (B RDP 전용)
- A RDP에서 bestbuy_v2.py 실행 중 일부만 수집된 경우
- B RDP에서 이 스크립트 실행 → 가장 최근 세션 자동 감지 → 누락 URL/NULL 자동 재수집
- 메뉴 인터랙션 없음 (대상/세션/파일명 모두 자동)

자동화 규칙:
- 대상: bestbuy 고정
- 세션: bestbuy_price_crawl_tbl_usa_v2의 가장 최근 세션 (DATE+HOUR 기준)
- 파일명 일시: 파일서버 최근 usa_bestbuy.zip 업로드 시각 + 30분
  (예: 20260426_090057 → 20260426_093057)

사용법:
    python recovery_bestbuy.py
"""

import os
import sys
import logging
import re
from datetime import datetime, timedelta

import pandas as pd
import paramiko
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

from config import FILE_SERVER_CONFIG
from recovery import RecoveryManager, TARGET_CONFIG
from alert_monitor import monitor_and_alert

TARGET = 'bestbuy'


def get_latest_session(manager):
    """bestbuy의 가장 최근 세션 (DATE+HOUR 기준) 반환

    Returns:
        (session_start: str, total_count: int) 또는 None
    """
    config = TARGET_CONFIG[TARGET]
    table = config['table']

    query = f"""
    SELECT
        DATE_FORMAT(MIN(kr_crawl_datetime), '%%Y-%%m-%%d %%H:%%i:%%s') as session_start,
        COUNT(*) as total_count
    FROM {table}
    GROUP BY DATE(kr_crawl_datetime), HOUR(kr_crawl_datetime)
    ORDER BY MIN(kr_crawl_datetime) DESC
    LIMIT 1
    """
    df = pd.read_sql(query, manager.db_engine)
    if df.empty:
        return None
    return df.iloc[0]['session_start'], int(df.iloc[0]['total_count'])


def get_latest_uploaded_dt():
    """파일서버에서 가장 최근 usa_bestbuy.zip 업로드 시각 반환 (YYYYMMDD_HHMMSS)

    경로: {upload_path}/usa/{YYYYMMDD}/{YYYYMMDD_HHMMSS}_usa_bestbuy.zip
    """
    transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
    transport.connect(
        username=FILE_SERVER_CONFIG['username'],
        password=FILE_SERVER_CONFIG['password']
    )
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/usa"

        # 날짜 폴더 (YYYYMMDD)
        date_folders = []
        for entry in sftp.listdir(country_dir):
            if len(entry) == 8 and entry.isdigit():
                date_folders.append(entry)
        if not date_folders:
            return None
        date_folders.sort(reverse=True)

        # 최근 날짜 폴더부터 usa_bestbuy.zip 검색
        for date_folder in date_folders:
            date_dir = f"{country_dir}/{date_folder}"
            files = sftp.listdir(date_dir)
            bestbuy_zips = [f for f in files if f.endswith('_usa_bestbuy.zip')]
            if bestbuy_zips:
                bestbuy_zips.sort(reverse=True)
                match = re.match(r'(\d{8}_\d{6})_usa_bestbuy\.zip', bestbuy_zips[0])
                if match:
                    return match.group(1)
        return None
    finally:
        sftp.close()
        transport.close()


def calculate_next_filename_dt(latest_dt_str):
    """latest_dt_str (YYYYMMDD_HHMMSS) + 30분 → YYYYMMDD_HHMMSS"""
    dt = datetime.strptime(latest_dt_str, '%Y%m%d_%H%M%S')
    new_dt = dt + timedelta(minutes=30)
    return new_dt.strftime('%Y%m%d_%H%M%S')


def _get_master_count(manager, config):
    """samsung_price_tracking_list에서 마스터 URL 개수 조회"""
    tracking_country = config.get('tracking_country')
    tracking_mall_name = config.get('tracking_mall_name')
    if not tracking_country or not tracking_mall_name:
        return 0
    try:
        count_query = """
        SELECT COUNT(*) as cnt
        FROM samsung_price_tracking_list
        WHERE country = :country AND mall_name = :mall_name AND is_active = TRUE
        """
        count_df = pd.read_sql(text(count_query), manager.db_engine,
                              params={'country': tracking_country, 'mall_name': tracking_mall_name})
        return int(count_df['cnt'][0])
    except Exception:
        return 0


def _upload_and_alert(manager, config, results_df, master_count, session_start, custom_filename,
                     recovery_prefix='', recovery_suffix='', error_message=None, error_logs=None):
    """파일서버 업로드 + 메일 알림 (항상 호출)"""
    alert_code = config.get('alert_code', TARGET)

    # 파일서버 업로드 (results_df가 있고 비어있지 않으면)
    if results_df is not None and not results_df.empty:
        logger.info(f"파일서버 업로드 대상: {len(results_df)}개 레코드")
        upload_success = manager.generate_and_upload_file(
            TARGET, results_df, session_start, custom_filename
        )
        if upload_success:
            logger.info("파일서버 업로드 완료")
        else:
            logger.error("파일서버 업로드 실패")
    else:
        logger.warning("업로드할 레코드 없음 → 파일 생성 건너뜀")

    # 메일 알림 (항상)
    session_date_str = str(session_start)[:10].replace('-', '') if session_start else None
    monitor_and_alert(
        alert_code, master_count, results_df,
        fs_country_code=config['country_code'],
        file_prefix=config['file_prefix'],
        skip_date=session_date_str,
        recovery_prefix=recovery_prefix,
        recovery_suffix=recovery_suffix,
        error_message=error_message,
        error_logs=error_logs
    )
    logger.info("메일 알림 발송 완료")


def run_auto_recovery():
    """bestbuy 자동 복구 메인 (recovery.py의 run_recovery 로직 복제, 인터랙션 제거, 항상 메일 발송)"""
    config = TARGET_CONFIG[TARGET]
    alert_code = config.get('alert_code', TARGET)

    print("\n" + "=" * 60)
    print(f"  BestBuy 자동 복구 (B RDP) - {config['name']}")
    print("=" * 60)

    # DB 연결 (실패 시 알림조차 일부만 가능)
    manager = RecoveryManager()
    if manager.db_engine is None:
        logger.error("DB 연결 실패. 종료.")
        monitor_and_alert(alert_code, 0, None, error_message="recovery_bestbuy: DB 연결 실패")
        return

    # 1. 최근 세션 자동 감지
    sess_result = get_latest_session(manager)
    if sess_result is None:
        logger.error("세션을 찾을 수 없습니다.")
        master_count = _get_master_count(manager, config)
        monitor_and_alert(alert_code, master_count, None,
                         error_message="recovery_bestbuy: 최근 세션 없음")
        return
    session_start, session_count = sess_result
    logger.info(f"감지된 세션: {session_start} ({session_count}개 레코드)")

    # 2. 파일명 일시 자동 계산 (파일서버 최근 업로드 + 30분)
    latest_dt = get_latest_uploaded_dt()
    if latest_dt is None:
        logger.error("파일서버에서 usa_bestbuy 파일을 찾을 수 없습니다.")
        master_count = _get_master_count(manager, config)
        monitor_and_alert(alert_code, master_count, None,
                         error_message="recovery_bestbuy: 파일서버 최근 업로드 없음")
        return
    next_dt = calculate_next_filename_dt(latest_dt)
    custom_filename = f"{next_dt}_{config['file_prefix']}"
    logger.info(f"최근 업로드: {latest_dt} → 새 파일명: {custom_filename}")

    # 3. 복구 전 전체 세션 레코드 미리 조회 (파일 생성용)
    all_records = manager.get_session_all_records(TARGET, session_start)
    if all_records is None or all_records.empty:
        logger.error("전체 세션 레코드 조회 실패")
        master_count = _get_master_count(manager, config)
        monitor_and_alert(alert_code, master_count, None,
                         error_message="recovery_bestbuy: 전체 세션 레코드 조회 실패")
        return
    logger.info(f"전체 세션 레코드: {len(all_records)}개 (미리 조회 완료)")

    # 4. 마스터 카운트 조회
    master_count = _get_master_count(manager, config)

    logger.info(f"\n{'='*60}")
    logger.info(f"복구 시작: {config['name']}")
    logger.info(f"세션: {session_start}")
    logger.info(f"{'='*60}")

    # 5. NULL 레코드 / 누락 URL 조회
    null_records = manager.get_null_records(TARGET, session_start)
    has_null = null_records is not None and not null_records.empty

    missing_urls = manager.get_missing_urls(TARGET, session_start)
    has_missing = not missing_urls.empty if missing_urls is not None else False

    if has_null:
        logger.info(f"NULL 복구 대상: {len(null_records)}개")
    if has_missing:
        logger.info(f"누락 URL 대상: {len(missing_urls)}개")

    # 6. 복구 대상 없음 → 1차 결과 그대로 업로드 + 알림
    if not has_null and not has_missing:
        logger.info("복구 대상 레코드가 없습니다 → 세션 결과 그대로 업로드")
        _upload_and_alert(manager, config, all_records, master_count, session_start, custom_filename,
                         recovery_suffix='no recovery needed')
        return

    expected_total = len(all_records) + (len(missing_urls) if has_missing else 0)
    logger.info(f"파일서버 업로드 예상: {expected_total}개 (세션 {len(all_records)}개 + 누락 {len(missing_urls) if has_missing else 0}개)")

    # 7. 스크래퍼 로드
    scraper = manager.load_scraper(TARGET)
    if scraper is None:
        logger.error("스크래퍼 로드 실패 → 1차 결과 그대로 업로드")
        _upload_and_alert(manager, config, all_records, master_count, session_start, custom_filename,
                         recovery_prefix='[RE] ', recovery_suffix='scraper load failed')
        return

    # 8. 복구 루프
    success_count = 0
    fail_count = 0
    recovered_results = {}
    missing_results = []
    error_logs = []

    try:
        # NULL 복구
        if has_null:
            # 1차 수집 NULL 스크린샷 삭제용 헬퍼 (실패해도 무시)
            try:
                from null_screenshot import RETAILER_NAME_BY_TARGET_KEY, delete_screenshots_for_sku
                _retailer_name = RETAILER_NAME_BY_TARGET_KEY.get(TARGET)
            except Exception:
                _retailer_name = None
                delete_screenshots_for_sku = None

            logger.info(f"\n--- NULL 복구 시작 ({len(null_records)}개) ---")
            for i, (idx, row) in enumerate(null_records.iterrows()):
                url = row['producturl']
                original_kr_crawl_datetime = row['kr_crawl_datetime']
                logger.info(f"\n[{i+1}/{len(null_records)}] 재크롤링: {url[:60]}...")

                # 1차 수집 NULL 스크린샷 삭제 (recovery 진입 시 일괄 제거)
                # 재크롤링 결과가 NULL이면 extract_product_info 내부에서 새로 업로드함
                if _retailer_name and delete_screenshots_for_sku:
                    try:
                        sku = row.get('retailersku', '')
                        original_date = str(original_kr_crawl_datetime)[:10].replace('-', '')  # 'YYYYMMDD'
                        if sku and len(original_date) == 8:
                            delete_screenshots_for_sku(_retailer_name, sku, original_date)
                    except Exception as e:
                        logger.debug(f"1차 스크린샷 삭제 시도 중 무시된 오류: {e}")

                result = manager.recrawl_url(scraper, url, row, TARGET)

                if result and (result.get('title') is not None or result.get('retailprice') is not None):
                    result['producturl'] = url
                    if manager.update_db_record(TARGET, original_kr_crawl_datetime, result):
                        logger.info(f"  -> 성공: title={str(result.get('title', ''))[:30]}, price={result.get('retailprice')}")
                        success_count += 1
                        recovered_results[url] = result
                    else:
                        logger.warning("  -> DB UPDATE 실패")
                        fail_count += 1
                        error_logs.append(f"[NULL UPDATE 실패] {url}")
                else:
                    logger.warning("  -> 재크롤링 실패 (여전히 NULL 또는 price=0)")
                    fail_count += 1
                    error_logs.append(f"[NULL 재크롤링 실패] {url}")

        # 누락 URL 크롤링
        if has_missing:
            missing_success = 0
            missing_fail = 0
            logger.info(f"\n--- 누락 URL 크롤링 시작 ({len(missing_urls)}개) ---")
            for i, (idx, row) in enumerate(missing_urls.iterrows()):
                url = row['url']
                logger.info(f"\n[누락 {i+1}/{len(missing_urls)}] 크롤링: {url[:60]}...")

                result = manager.recrawl_url(scraper, url, row, TARGET)

                if result and (result.get('title') is not None or result.get('retailprice') is not None):
                    result['producturl'] = url
                    if manager.insert_missing_record(TARGET, result):
                        logger.info(f"  -> INSERT 성공: title={str(result.get('title', ''))[:30]}, price={result.get('retailprice')}")
                        missing_success += 1
                        missing_results.append(result)
                    else:
                        logger.warning("  -> DB INSERT 실패")
                        missing_fail += 1
                        error_logs.append(f"[누락 INSERT 실패] {url}")
                else:
                    logger.warning("  -> 크롤링 실패")
                    missing_fail += 1
                    error_logs.append(f"[누락 크롤링 실패] {url}")

            logger.info(f"누락 URL 결과: 성공 {missing_success}개, 실패 {missing_fail}개")
            success_count += missing_success
            fail_count += missing_fail

    except Exception as e:
        logger.error(f"복구 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        error_logs.append(f"[치명적 오류] {str(e)}")

    finally:
        # 브라우저 종료
        if scraper:
            try:
                if hasattr(scraper, 'driver') and scraper.driver:
                    scraper.driver.quit()
                elif hasattr(scraper, 'page') and scraper.page:
                    scraper.page.quit()
                logger.info("브라우저 종료")
            except Exception:
                pass

    logger.info(f"\n{'='*60}")
    logger.info(f"복구 완료: 성공 {success_count}개, 실패 {fail_count}개")
    logger.info(f"{'='*60}")

    # 9. 복구 결과를 all_records에 merge (성공분만 반영)
    for url, r in recovered_results.items():
        mask = all_records['producturl'] == url
        if mask.any():
            for col in ['title', 'imageurl', 'retailprice', 'ships_from', 'sold_by',
                        'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime']:
                if col in r:
                    all_records.loc[mask, col] = r.get(col)
            logger.info(f"  merge 완료: {url[:60]}")

    # 누락 URL 결과 추가
    if missing_results:
        missing_df = pd.DataFrame(missing_results)
        all_records = pd.concat([all_records, missing_df], ignore_index=True)
        logger.info(f"  누락 URL {len(missing_results)}개 추가")

    # 10. 파일서버 업로드 + 메일 알림 (항상)
    if fail_count == 0 and success_count > 0:
        suffix = 'update complete'
    elif success_count > 0:
        suffix = f'partial recovery ({success_count}/{success_count + fail_count})'
    else:
        suffix = 'recovery failed (all)'

    _upload_and_alert(manager, config, all_records, master_count, session_start, custom_filename,
                     recovery_prefix='[RE] ', recovery_suffix=suffix,
                     error_logs=error_logs if error_logs else None)


if __name__ == "__main__":
    run_auto_recovery()
