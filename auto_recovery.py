"""
자동 복구 모듈
- 각 스크래퍼 크롤링 완료 후 자동 호출
- 성공/실패 판정 → 실패 시 자동 recovery → 파일서버 업로드 → 메일 알림
"""

import logging
import sys
import pandas as pd
import pytz
from datetime import datetime
from sqlalchemy import text

from config import DB_CONFIG_V2 as DB_CONFIG
from recovery import RecoveryManager, TARGET_CONFIG
from alert_monitor import monitor_and_alert

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ships_from/sold_by 복구 대상
SHIPS_FROM_TARGETS = {'de', 'es', 'fr', 'in', 'it', 'jp', 'gb', 'nl', 'usa'}


def _load_title_null_thresholds(db_engine):
    """DB에서 title NULL 임계값 로드"""
    try:
        df = pd.read_sql(text("SELECT target_key, title_null_threshold FROM auto_recovery_config"),
                        db_engine)
        return dict(zip(df['target_key'], df['title_null_threshold']))
    except Exception as e:
        logger.warning(f"임계값 DB 로드 실패, 기본값 0 사용: {e}")
        return {}


def auto_recovery_run(target_key, results_df, target_count, error_logs=None):
    """
    스크래퍼 크롤링 완료 후 자동 복구 + 파일 업로드 + 메일 알림

    Args:
        target_key: TARGET_CONFIG 키 (예: 'bestbuy', 'de', 'currys')
        results_df: 1차 크롤링 결과 DataFrame
        target_count: tracking list 개수
        error_logs: 에러 로그 리스트
    """
    config = TARGET_CONFIG.get(target_key)
    if not config:
        logger.error(f"TARGET_CONFIG에 '{target_key}' 없음")
        return

    logger.info(f"\n{'='*60}")
    logger.info(f"자동 복구 시작: {config['name']}")
    logger.info(f"{'='*60}")

    manager = RecoveryManager()
    if manager.db_engine is None:
        logger.error("DB 연결 실패")
        monitor_and_alert(config.get('alert_code', target_key), target_count, results_df,
                         error_message="auto_recovery DB 연결 실패", error_logs=error_logs)
        return

    # results_df가 None이면 1차 크롤링 자체 실패
    if results_df is None or results_df.empty:
        logger.error("1차 크롤링 결과 없음")
        monitor_and_alert(config.get('alert_code', target_key), target_count, None,
                         error_message="크롤링 결과 없음", error_logs=error_logs)
        return

    # 컬럼명 소문자 통일
    results_df_lower = results_df.copy()
    results_df_lower.columns = results_df_lower.columns.str.lower()

    # 세션 범위 결정 (kr_crawl_datetime의 min/max)
    session_start = str(results_df_lower['kr_crawl_datetime'].min())
    session_end = str(results_df_lower['kr_crawl_datetime'].max())
    crawled_count = len(results_df_lower)

    logger.info(f"1차 수집: {crawled_count}개 / 대상: {target_count}개")
    logger.info(f"세션 범위: {session_start} ~ {session_end}")

    # === 1. 성공/실패 판정 ===
    title_null_count = results_df_lower['title'].isna().sum() if 'title' in results_df_lower.columns else 0
    thresholds = _load_title_null_thresholds(manager.db_engine)
    threshold = thresholds.get(target_key, 0)
    missing_count = target_count - crawled_count

    # 판매자 있는데 가격 없는 경우 카운트 (ships_from/sold_by 컬럼이 있는 target만)
    price_null_with_seller = 0
    if 'retailprice' in results_df_lower.columns:
        has_ships = results_df_lower['ships_from'].notna() & (results_df_lower['ships_from'] != '') if 'ships_from' in results_df_lower.columns else pd.Series(False, index=results_df_lower.index)
        has_sold = results_df_lower['sold_by'].notna() & (results_df_lower['sold_by'] != '') if 'sold_by' in results_df_lower.columns else pd.Series(False, index=results_df_lower.index)
        has_seller = has_ships | has_sold
        price_null = results_df_lower['retailprice'].isna()
        price_null_with_seller = (has_seller & price_null).sum()

    if target_key == 'bestbuy':
        needs_recovery = missing_count > 0 or title_null_count > threshold
    else:
        needs_recovery = missing_count > 0 or title_null_count > threshold or price_null_with_seller > 0

    logger.info(f"누락: {missing_count}개, title NULL: {title_null_count}개 (임계값: {threshold}개), 판매자有/가격無: {price_null_with_seller}개")
    logger.info(f"복구 필요: {'예' if needs_recovery else '아니오'}")

    # xkom 예외: 봇감지 수동 필요 → recovery 건너뜀
    if config.get('needs_manual_check') and needs_recovery:
        logger.warning(f"{target_key}: 봇감지 수동 체크 필요, auto recovery 건너뜀")
        needs_recovery = False

    # === 2-A. 성공 시: 파일 업로드 + 일반 메일 ===
    if not needs_recovery:
        logger.info("1차 수집 성공 → 파일서버 업로드")
        _upload_and_alert(manager, config, target_key, results_df, target_count,
                         session_start, error_logs=error_logs)
        return

    # === 2-B. 실패 시: 자동 recovery ===

    logger.info(f"\n--- 자동 복구 시작 ---")

    # 누락 URL 조회 (BETWEEN 방식)
    missing_urls = _get_missing_urls_between(manager, target_key, session_start, session_end)

    # title NULL 레코드 조회 (BETWEEN 방식)
    null_records = _get_null_records_between(manager, target_key, session_start, session_end)

    has_missing = missing_urls is not None and not missing_urls.empty
    has_null = null_records is not None and not null_records.empty

    if has_missing:
        logger.info(f"누락 URL: {len(missing_urls)}개")
    if has_null:
        logger.info(f"NULL 복구 대상: {len(null_records)}개")

    if not has_missing and not has_null:
        logger.info("복구 대상 없음 (DB 기준) → 1차 결과 그대로 업로드")
        _upload_and_alert(manager, config, target_key, results_df, target_count,
                         session_start, error_logs=error_logs)
        return

    # 스크래퍼 로드
    scraper = manager.load_scraper(target_key)
    if scraper is None:
        logger.error("스크래퍼 로드 실패 → 1차 결과 그대로 업로드")
        _upload_and_alert(manager, config, target_key, results_df, target_count,
                         session_start, error_logs=error_logs,
                         recovery_prefix='[RE] ', recovery_suffix='scraper load failed')
        return

    # recovery 크롤링 (메모리에만 보관, DB 미반영)
    recovered_results = {}   # url → result dict (NULL 복구)
    missing_results = []     # result dict 리스트 (누락 URL)
    MAX_CONSECUTIVE_FAILS = 5  # 연속 실패 N회 시 즉시 중단

    try:
        consecutive_fails = 0

        # NULL 복구 크롤링
        if has_null:
            logger.info(f"\n--- NULL 복구 크롤링 ({len(null_records)}개) ---")
            for i, (idx, row) in enumerate(null_records.iterrows()):
                url = row['producturl']
                logger.info(f"[{i+1}/{len(null_records)}] 재크롤링: {url[:60]}...")
                result = manager.recrawl_url(scraper, url, row, target_key)
                if result and (result.get('title') is not None or result.get('retailprice') is not None):
                    result['producturl'] = url
                    recovered_results[url] = result
                    consecutive_fails = 0
                    logger.info(f"  -> 성공: title={str(result.get('title', ''))[:30]}, price={result.get('retailprice')}")
                else:
                    consecutive_fails += 1
                    logger.warning(f"  -> 실패 (연속 {consecutive_fails}회)")
                    if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                        logger.error(f"연속 {MAX_CONSECUTIVE_FAILS}회 실패 → recovery 중단")
                        break

        # 누락 URL 크롤링
        if has_missing and consecutive_fails < MAX_CONSECUTIVE_FAILS:
            logger.info(f"\n--- 누락 URL 크롤링 ({len(missing_urls)}개) ---")
            for i, (idx, row) in enumerate(missing_urls.iterrows()):
                url = row['url']
                logger.info(f"[누락 {i+1}/{len(missing_urls)}] 크롤링: {url[:60]}...")
                result = manager.recrawl_url(scraper, url, row, target_key)
                if result and (result.get('title') is not None or result.get('retailprice') is not None):
                    result['producturl'] = url
                    missing_results.append(result)
                    consecutive_fails = 0
                    logger.info(f"  -> 성공: title={str(result.get('title', ''))[:30]}, price={result.get('retailprice')}")
                else:
                    consecutive_fails += 1
                    logger.warning(f"  -> 실패 (연속 {consecutive_fails}회)")
                    if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                        logger.error(f"연속 {MAX_CONSECUTIVE_FAILS}회 실패 → recovery 중단")
                        break

    except Exception as e:
        logger.error(f"복구 크롤링 중 오류: {e}")
        import traceback
        traceback.print_exc()

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

    # === 3. 복구 결과 비교 ===
    is_different = _compare_with_first_crawl(
        target_key, results_df_lower, recovered_results, missing_results
    )

    recovery_success = len(recovered_results) + len(missing_results)
    total_collected = crawled_count + len(missing_results)
    collection_rate = (total_collected / target_count * 100) if target_count > 0 else 0

    logger.info(f"\n복구 결과: 성공 {recovery_success}개")
    logger.info(f"총 수집: {total_collected}/{target_count}개 ({collection_rate:.1f}%)")
    logger.info(f"데이터 변경: {'있음' if is_different else '없음'}")

    # 수집률 50% 미만이면 가격 이상 감지 건너뜀 (수집 실패를 가격 이상으로 오인 방지)
    skip_price_anomaly = collection_rate < 50
    if skip_price_anomaly:
        logger.warning(f"수집률 {collection_rate:.1f}% (50% 미만) → 가격 이상 감지 건너뜀")

    if not is_different:
        # === 3-A. same data: 1차 결과 그대로 업로드 ===
        logger.info("복구 전후 동일 → 1차 결과 그대로 업로드")
        _upload_and_alert(manager, config, target_key, results_df, target_count,
                         session_start, error_logs=error_logs,
                         recovery_prefix='[RE] ', recovery_suffix='same data',
                         skip_price_anomaly=skip_price_anomaly)
    else:
        # === 3-B. update complete: DB 반영 + 복구 데이터로 업로드 ===
        logger.info("복구 전후 다름 → DB 반영 + 복구 데이터 업로드")

        # DB UPDATE (NULL 복구)
        for url, result in recovered_results.items():
            original_kr = null_records[null_records['producturl'] == url]['kr_crawl_datetime'].iloc[0]
            manager.update_db_record(target_key, original_kr, result)

        # DB INSERT (누락 URL)
        for result in missing_results:
            manager.insert_missing_record(target_key, result)

        # 1차 결과에 복구 결과 merge
        merged_df = _merge_recovery_results(results_df_lower, recovered_results, missing_results)

        _upload_and_alert(manager, config, target_key, merged_df, target_count,
                         session_start, error_logs=error_logs,
                         recovery_prefix='[RE] ', recovery_suffix='update complete',
                         skip_price_anomaly=skip_price_anomaly)

    logger.info(f"\n{'='*60}")
    logger.info(f"자동 복구 완료: {config['name']}")
    logger.info(f"{'='*60}")


def _get_missing_urls_between(manager, target_key, session_start, session_end):
    """BETWEEN 방식으로 누락 URL 조회"""
    config = TARGET_CONFIG[target_key]
    table = config['table']
    tracking_country = config.get('tracking_country')
    tracking_mall_name = config.get('tracking_mall_name')

    if not tracking_country or not tracking_mall_name:
        return pd.DataFrame()

    try:
        # 마스터 리스트
        master_query = """
        SELECT *
        FROM samsung_price_tracking_list
        WHERE country = :country AND mall_name = :mall_name AND is_active = TRUE
        """
        master_df = pd.read_sql(text(master_query), manager.db_engine,
                                params={'country': tracking_country, 'mall_name': tracking_mall_name})
        if master_df.empty:
            return pd.DataFrame()

        # 세션 내 크롤링된 URL
        session_query = f"""
        SELECT producturl
        FROM {table}
        WHERE kr_crawl_datetime BETWEEN :start AND :end
        """
        session_df = pd.read_sql(text(session_query), manager.db_engine,
                                 params={'start': session_start, 'end': session_end})
        crawled_urls = set(session_df['producturl'].tolist()) if not session_df.empty else set()

        # 비교
        missing = master_df[~master_df['url'].isin(crawled_urls)]
        if not missing.empty:
            logger.info(f"누락 URL: {len(missing)}개 (마스터 {len(master_df)}개 - 크롤 {len(crawled_urls)}개)")
        return missing

    except Exception as e:
        logger.error(f"누락 URL 조회 실패: {e}")
        return pd.DataFrame()


def _get_null_records_between(manager, target_key, session_start, session_end):
    """BETWEEN 방식으로 NULL 레코드 조회"""
    config = TARGET_CONFIG[target_key]
    table = config['table']

    if target_key == 'danawa':
        null_condition = "(title IS NULL OR imageurl IS NULL OR imageurl = '' OR retailprice IS NULL OR retailprice = 0)"
    else:
        null_condition = "(title IS NULL OR imageurl IS NULL OR imageurl = '' OR retailprice IS NULL)"

    try:
        query = f"""
        SELECT *
        FROM {table}
        WHERE kr_crawl_datetime BETWEEN :start AND :end
          AND {null_condition}
        """
        df = pd.read_sql(text(query), manager.db_engine,
                         params={'start': session_start, 'end': session_end})
        df = df.drop_duplicates(subset=['producturl'], keep='last')
        return df

    except Exception as e:
        logger.error(f"NULL 레코드 조회 실패: {e}")
        return None


def _compare_with_first_crawl(target_key, first_crawl_df, recovered_results, missing_results):
    """1차 크롤링 결과와 복구 결과 비교. 다르면 True 반환."""
    if not recovered_results and not missing_results:
        return False

    # 누락 URL이 1개라도 성공하면 데이터 다름
    if missing_results:
        return True

    # NULL 복구 결과 비교
    fields = ['title', 'retailprice', 'imageurl']
    if target_key in SHIPS_FROM_TARGETS:
        fields.extend(['ships_from', 'sold_by'])

    for url, new_result in recovered_results.items():
        old_rows = first_crawl_df[first_crawl_df['producturl'] == url]
        if old_rows.empty:
            return True  # 1차에 없던 URL이 복구됨

        old_row = old_rows.iloc[0]
        for field in fields:
            old_val = old_row.get(field)
            new_val = new_result.get(field)

            old_is_null = old_val is None or pd.isna(old_val)
            new_is_null = new_val is None or pd.isna(new_val)

            if old_is_null and new_is_null:
                continue
            if old_is_null != new_is_null:
                return True
            if str(old_val) != str(new_val):
                return True

    return False


def _merge_recovery_results(first_crawl_df, recovered_results, missing_results):
    """1차 크롤링 DataFrame에 복구 결과를 merge"""
    merged = first_crawl_df.copy()

    # NULL 복구 결과 merge
    for url, result in recovered_results.items():
        mask = merged['producturl'] == url
        if mask.any():
            for col in ['title', 'imageurl', 'retailprice', 'ships_from', 'sold_by',
                        'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime']:
                if col in result and col in merged.columns:
                    merged.loc[mask, col] = result.get(col)

    # 누락 URL 결과 추가
    if missing_results:
        missing_df = pd.DataFrame(missing_results)
        missing_df.columns = missing_df.columns.str.lower()
        merged = pd.concat([merged, missing_df], ignore_index=True)

    return merged


def _upload_and_alert(manager, config, target_key, results_df, target_count,
                      session_start, error_logs=None,
                      recovery_prefix='', recovery_suffix='',
                      skip_price_anomaly=False):
    """파일서버 업로드 + 메일 알림"""
    alert_code = config.get('alert_code', target_key)

    # 세션 시작 시간을 현지시간으로 변환 (폴더명/파일명 기준)
    korea_tz = pytz.timezone('Asia/Seoul')
    local_tz = pytz.timezone(config['local_tz'])
    session_dt_kr = korea_tz.localize(datetime.strptime(session_start[:19], '%Y-%m-%d %H:%M:%S'))
    session_dt_local = session_dt_kr.astimezone(local_tz)
    date_str = session_dt_local.strftime('%Y%m%d')
    time_str = session_dt_local.strftime('%H%M%S')
    custom_filename = f"{date_str}_{time_str}_{config['file_prefix']}"

    # 현지시간 기준 session_start (generate_and_upload_file이 폴더 날짜로 사용)
    local_session_start = session_dt_local.strftime('%Y-%m-%d %H:%M:%S')

    # 파일서버 업로드
    logger.info(f"파일서버 업로드 대상: {len(results_df)}개 레코드")
    upload_success = manager.generate_and_upload_file(
        target_key, results_df, local_session_start, custom_filename
    )
    if upload_success:
        logger.info("파일서버 업로드 완료")
    else:
        logger.error("파일서버 업로드 실패")

    # 메일 알림 (skip_price_anomaly=True이면 가격 이상 감지 건너뜀)
    monitor_and_alert(
        alert_code, target_count, results_df,
        error_logs=error_logs,
        fs_country_code=None if skip_price_anomaly else config['country_code'],
        file_prefix=None if skip_price_anomaly else config['file_prefix'],
        skip_date=date_str,
        recovery_prefix=recovery_prefix,
        recovery_suffix=recovery_suffix
    )
