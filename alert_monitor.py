"""
Amazon 크롤링 모니터링 및 알림 모듈
- 크롤링 결과 분석
- 이상 감지 시 이메일 알림 발송
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pytz
import logging
import pandas as pd

from config import EMAIL_CONFIG

logger = logging.getLogger(__name__)

# 국가별 이름 매핑 (본문용)
COUNTRY_NAMES = {
    'usa': '미국 Amazon (USA)',
    'gb': '영국 Amazon (GB)',
    'de': '독일 Amazon (DE)',
    'fr': '프랑스 Amazon (FR)',
    'es': '스페인 Amazon (ES)',
    'it': '이탈리아 Amazon (IT)',
    'jp': '일본 Amazon (JP)',
    'in': '인도 Amazon (IN)',
    'nl': '네덜란드 Amazon (NL)',
    'usa_bestbuy': '미국 BestBuy',
    'gb_currys': '영국 Currys',
    'de_mediamarkt': '독일 MediaMarkt',
    'nl_coolblue': '네덜란드 Coolblue',
    'pl_xkom': '폴란드 X-kom',
    'kr_danawa': '한국 다나와',
    'fr_fnac': '프랑스 Fnac',
    'au_centrecom': '호주 Centrecom'
}

# 제목용 간결한 이름 매핑
COUNTRY_SHORT_NAMES = {
    'usa': 'usa_amazon',
    'gb': 'uk_amazon',
    'de': 'de_amazon',
    'fr': 'fr_amazon',
    'es': 'es_amazon',
    'it': 'it_amazon',
    'jp': 'jp_amazon',
    'in': 'in_amazon',
    'nl': 'nl_amazon',
    'usa_bestbuy': 'bestbuy',
    'gb_currys': 'currys',
    'de_mediamarkt': 'mediamarkt',
    'nl_coolblue': 'coolblue',
    'pl_xkom': 'xkom',
    'kr_danawa': 'danawa',
    'fr_fnac': 'fnac',
    'au_centrecom': 'centrecom'
}


def detect_price_anomalies(current_df, country_code, file_prefix, threshold=20, skip_date=None):
    """
    직전 파일서버 데이터 대비 가격 이상 감지

    Args:
        current_df: 현재 크롤링 결과 DataFrame
        country_code: 국가 코드 (파일서버 디렉토리명, 예: 'usa')
        file_prefix: 파일 접두사 (예: 'usa_bestbuy')
        threshold: 가격 변동 임계값 (%, 기본 20)
        skip_date: 추가로 건너뛸 날짜 문자열 (예: '20260403', recovery 시 세션 날짜)

    Returns:
        list: 이상 항목 리스트
    """
    try:
        import paramiko
        import zipfile
        import tempfile
        import os
        from config import FILE_SERVER_CONFIG

        if current_df is None or current_df.empty:
            return []

        # 현재 결과에서 컬럼명 소문자 통일
        curr = current_df.copy()
        curr.columns = curr.columns.str.lower()

        if 'producturl' not in curr.columns or 'retailprice' not in curr.columns:
            return []

        # 1. 파일서버 접속
        transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
        transport.connect(
            username=FILE_SERVER_CONFIG['username'],
            password=FILE_SERVER_CONFIG['password']
        )
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            # 2. 날짜 폴더 목록 조회 (내림차순 정렬)
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{country_code}"
            try:
                date_folders = sorted(sftp.listdir(country_dir), reverse=True)
            except FileNotFoundError:
                return []

            # 3. 날짜 폴더만 필터링 (backup 등 제외), 오늘 제외하고 가장 최근 폴더에서 매칭되는 zip 찾기
            today_str = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')
            date_folders = [f for f in date_folders if f.isdigit() and len(f) == 8]
            prev_zip_path = None

            for folder in date_folders:
                if folder == today_str or folder == skip_date:
                    continue
                # 해당 폴더에서 file_prefix 매칭되는 zip 찾기
                folder_path = f"{country_dir}/{folder}"
                try:
                    files = sftp.listdir(folder_path)
                    for f in files:
                        if f.endswith('.zip') and file_prefix in f:
                            prev_zip_path = f"{folder_path}/{f}"
                            break
                except Exception:
                    continue
                if prev_zip_path:
                    break

            if not prev_zip_path:
                logger.info(f"직전 파일 없음: {country_code}/{file_prefix}")
                return []

            logger.info(f"직전 파일 발견: {prev_zip_path}")

            # 4. zip 다운로드 → csv 읽기
            prev_df = None
            with tempfile.TemporaryDirectory() as tmpdir:
                local_zip = os.path.join(tmpdir, 'prev.zip')
                sftp.get(prev_zip_path, local_zip)

                with zipfile.ZipFile(local_zip, 'r') as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
                    if csv_names:
                        with zf.open(csv_names[0]) as csv_file:
                            prev_df = pd.read_csv(csv_file)

        finally:
            sftp.close()
            transport.close()

        if prev_df is None or prev_df.empty:
            return []

        prev_df.columns = prev_df.columns.str.lower()

        # 가격 컬럼 숫자 변환 (콤마 포함 문자열 대응)
        for df in [curr, prev_df]:
            if df['retailprice'].dtype == object:
                df['retailprice'] = pd.to_numeric(df['retailprice'].astype(str).str.replace(',', ''), errors='coerce')

        # 5. 비교 (outer merge로 한쪽에만 있는 URL도 포함, indicator로 소속 판별)
        curr_cols = curr[['producturl', 'retailprice', 'title']]
        prev_cols = prev_df[['producturl', 'retailprice', 'title']]
        merged = pd.merge(prev_cols, curr_cols,
                         on='producturl', suffixes=('_prev', '_curr'), how='outer', indicator=True)

        if merged.empty:
            return []

        result = []

        for _, row in merged.iterrows():
            prev_price = row.get('retailprice_prev')
            curr_price = row.get('retailprice_curr')
            prev_is_null = prev_price is None or pd.isna(prev_price)
            curr_is_null = curr_price is None or pd.isna(curr_price)
            title_prev = row.get('title_prev')
            title_curr = row.get('title_curr')
            title = str(title_curr if title_curr and not pd.isna(title_curr) else (title_prev if title_prev and not pd.isna(title_prev) else ''))[:50]

            merge_source = row['_merge']  # 'left_only', 'right_only', 'both'

            # Case 0a: URL이 어제 있었는데 오늘 없음
            if merge_source == 'left_only':
                result.append({
                    'url': row['producturl'],
                    'title': title,
                    'prev_price': float(prev_price) if not prev_is_null else None,
                    'curr_price': None,
                    'pct_change': None,
                    'anomaly_type': 'url_removed'
                })
                continue
            # Case 0b: URL이 어제 없었는데 오늘 생김
            if merge_source == 'right_only':
                result.append({
                    'url': row['producturl'],
                    'title': title,
                    'prev_price': None,
                    'curr_price': float(curr_price) if not curr_is_null else None,
                    'pct_change': None,
                    'anomaly_type': 'url_added'
                })
                continue

            # Case 1: 가격이 있다가 없어진 경우
            if not prev_is_null and curr_is_null:
                result.append({
                    'url': row['producturl'],
                    'title': title,
                    'prev_price': float(prev_price),
                    'curr_price': None,
                    'pct_change': None,
                    'anomaly_type': 'price_disappeared'
                })
            # Case 2: 가격이 없다가 생긴 경우
            elif prev_is_null and not curr_is_null:
                result.append({
                    'url': row['producturl'],
                    'title': title,
                    'prev_price': None,
                    'curr_price': float(curr_price),
                    'pct_change': None,
                    'anomaly_type': 'price_appeared'
                })
            # Case 3: 양쪽 다 가격이 있고, 변동률 임계값 초과
            elif not prev_is_null and not curr_is_null and float(prev_price) > 0:
                pct = abs(float(curr_price) - float(prev_price)) / float(prev_price) * 100
                if pct >= threshold:
                    result.append({
                        'url': row['producturl'],
                        'title': title,
                        'prev_price': float(prev_price),
                        'curr_price': float(curr_price),
                        'pct_change': round(pct, 1),
                        'anomaly_type': 'price_change'
                    })

        # 변동률 큰 순 정렬 (None은 뒤로)
        result.sort(key=lambda x: x['pct_change'] if x['pct_change'] is not None else -1, reverse=True)

        if result:
            logger.info(f"가격 이상 감지: {len(result)}개 (임계값 {threshold}%)")

        return result

    except Exception as e:
        logger.error(f"가격 이상 감지 실패: {e}")
        return []


def analyze_crawl_results(country_code, target_count, results_df, error_logs=None, blocked_page_failures=0, all_null_failures=0, title_null_failures=0, price_anomalies=None, recovery_prefix='', recovery_suffix=''):
    """
    크롤링 결과 분석

    Args:
        country_code: 국가 코드 (예: 'jp', 'it')
        target_count: tracking list의 총 개수 (수집 시도해야 할 개수)
        results_df: 크롤링 결과 DataFrame
        error_logs: 크롤링 중 발생한 에러 로그 리스트 (선택)
        blocked_page_failures: 차단 페이지로 인한 최종 실패 개수 (선택)
        all_null_failures: title, imageurl, retailprice 모두 NULL인 최종 실패 개수 (선택)
        title_null_failures: title이 NULL인 최종 실패 개수 (선택)
        price_anomalies: 가격 이상 항목 리스트 (선택, detect_price_anomalies 결과)

    Returns:
        dict: 분석 결과
    """
    analysis = {
        'country_code': country_code,
        'country_name': COUNTRY_NAMES.get(country_code, country_code.upper()),
        'target_count': target_count,
        'crawled_count': len(results_df) if results_df is not None else 0,
        'alerts': [],
        'is_critical': False,
        'has_price_error': False,  # ships_from/sold_by 있는데 price 없는 경우
        'blocked_page_failures': blocked_page_failures,  # 차단 페이지로 인한 최종 실패 개수
        'all_null_failures': all_null_failures,  # title, imageurl, retailprice 모두 NULL인 최종 실패 개수
        'title_null_failures': title_null_failures,  # title이 NULL인 최종 실패 개수
        'field_stats': {},
        'error_logs': error_logs or [],
        'price_anomalies': price_anomalies or [],
        'recovery_prefix': recovery_prefix,
        'recovery_suffix': recovery_suffix
    }

    # 크롤링 자체가 실패한 경우
    if results_df is None or len(results_df) == 0:
        analysis['alerts'].append({
            'type': 'CRITICAL',
            'message': '크롤링 실행 실패 - 결과 데이터 없음 (chromedriver 오류 가능성)'
        })
        analysis['is_critical'] = True
        return analysis

    crawled_count = len(results_df)

    # 수집 시도 누락 확인
    if crawled_count < target_count:
        missing_count = target_count - crawled_count
        analysis['alerts'].append({
            'type': 'CRITICAL',
            'message': f'수집 시도 누락: {target_count}개 중 {crawled_count}개만 시도 ({missing_count}개 누락)'
        })
        analysis['is_critical'] = True

    # 각 필드별 빈 값 비율 분석
    fields_to_check = ['retailprice', 'imageurl', 'ships_from', 'sold_by', 'title']
    field_names = {
        'retailprice': '가격',
        'imageurl': '이미지 URL',
        'ships_from': '배송지',
        'sold_by': '판매자',
        'title': '제목'
    }

    for field in fields_to_check:
        if field in results_df.columns:
            # None, NaN, 빈 문자열 모두 빈 값으로 처리
            try:
                # 숫자 타입인 경우 isna()만 사용
                if results_df[field].dtype in ['float64', 'int64', 'float', 'int']:
                    empty_count = results_df[field].isna().sum()
                else:
                    # 문자열 타입인 경우 빈 문자열도 체크
                    empty_count = results_df[field].isna().sum() + (results_df[field] == '').sum()
            except Exception:
                empty_count = results_df[field].isna().sum()
            empty_rate = (empty_count / crawled_count) * 100 if crawled_count > 0 else 0

            analysis['field_stats'][field] = {
                'name': field_names.get(field, field),
                'empty_count': int(empty_count),
                'total_count': crawled_count,
                'empty_rate': round(empty_rate, 1)
            }

            # 50% 이상 빈 값이면 긴급 알림
            if empty_rate >= 50:
                analysis['alerts'].append({
                    'type': 'CRITICAL',
                    'message': f'{field_names.get(field, field)} 빈 값 {empty_rate:.1f}% ({empty_count}/{crawled_count})'
                })
                analysis['is_critical'] = True

    # ships_from 또는 sold_by가 있는데 retailprice가 없는 경우 감지
    if results_df is not None and len(results_df) > 0:
        if 'retailprice' in results_df.columns and ('ships_from' in results_df.columns or 'sold_by' in results_df.columns):
            for idx, row in results_df.iterrows():
                # retailprice가 None, NaN, 또는 빈 문자열인지 확인
                price_val = row['retailprice'] if 'retailprice' in row.index else None
                price_is_empty = price_val is None or pd.isna(price_val) or (isinstance(price_val, str) and price_val == '')

                # ships_from이 유효한 값인지 확인
                ships_from_val = row['ships_from'] if 'ships_from' in row.index else None
                ships_from_exists = ships_from_val is not None and not pd.isna(ships_from_val) and ships_from_val != ''

                # sold_by가 유효한 값인지 확인
                sold_by_val = row['sold_by'] if 'sold_by' in row.index else None
                sold_by_exists = sold_by_val is not None and not pd.isna(sold_by_val) and sold_by_val != ''

                if price_is_empty and (ships_from_exists or sold_by_exists):
                    analysis['has_price_error'] = True
                    break

    return analysis


def send_alert_email(analysis, error_message=None):
    """
    분석 결과를 이메일로 발송

    Args:
        analysis: analyze_crawl_results()의 반환값
        error_message: 추가 에러 메시지 (선택)

    Returns:
        bool: 발송 성공 여부
    """
    try:
        korea_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_tz)

        # 이메일 제목 생성
        country_name = analysis['country_name']

        # 가격 미수집 개수 및 비율 확인
        price_stats = analysis['field_stats'].get('retailprice', {})
        price_empty_count = price_stats.get('empty_count', 0)
        price_empty_rate = price_stats.get('empty_rate', 0)

        # price error 접두사 (ships_from/sold_by 있는데 price 없는 경우)
        price_error_prefix = "price error " if analysis.get('has_price_error', False) else ""

        # title 누락 개수 확인
        title_stats = analysis['field_stats'].get('title', {})
        title_empty_count = title_stats.get('empty_count', 0)

        # 차단 페이지 실패 개수 확인
        blocked_failures = analysis.get('blocked_page_failures', 0)

        # title, imageurl, retailprice 모두 NULL인 최종 실패 개수 확인
        all_null_failures = analysis.get('all_null_failures', 0)

        # title NULL인 최종 실패 개수 확인
        title_null_failures = analysis.get('title_null_failures', 0)

        # Failed 접두사 (우선순위: title_null > all_null_failures > blocked_failures > title_empty > price_empty)
        if title_null_failures > 0:
            failed_prefix = f"title null {title_null_failures} "
        elif all_null_failures > 0:
            failed_prefix = f"{all_null_failures} failed "
        elif blocked_failures > 0:
            failed_prefix = f"Failed {blocked_failures} sku "
        elif title_empty_count > 0:
            failed_prefix = f"Failed {title_empty_count} sku "
        elif price_empty_rate >= 20:
            failed_prefix = "Failed "
        else:
            failed_prefix = ""

        # 가격 이상 접두사
        price_anomalies = analysis.get('price_anomalies', [])
        price_anomaly_prefix = f"price anomaly {len(price_anomalies)} " if price_anomalies else ""

        # recovery 접두사/접미사
        recovery_prefix = analysis.get('recovery_prefix', '')
        recovery_suffix = analysis.get('recovery_suffix', '')
        recovery_suffix_str = f" - {recovery_suffix}" if recovery_suffix else ""

        # 제목 생성 (간결하게: [DS] + [RE] + Failed X sku + 사이트명 + suffix)
        short_name = COUNTRY_SHORT_NAMES.get(analysis['country_code'], analysis['country_code'])
        subject = f"[DS] {recovery_prefix}{failed_prefix}{price_error_prefix}{price_anomaly_prefix}{short_name}{recovery_suffix_str}"

        # 이메일 본문 생성 (HTML)
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Malgun Gothic', Arial, sans-serif; }}
                .critical {{ color: #dc3545; font-weight: bold; }}
                .warning {{ color: #ffc107; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .header {{ background-color: #333; color: white; padding: 15px; }}
                .section {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Amazon {country_name} 크롤링 모니터링 리포트</h2>
                <p>시간: {now.strftime('%Y-%m-%d %H:%M:%S')} (KST)</p>
            </div>

            <div class="section">
                <h3>수집 현황</h3>
                <table>
                    <tr>
                        <th>항목</th>
                        <th>값</th>
                    </tr>
                    <tr>
                        <td>대상 제품 수 (Tracking List)</td>
                        <td>{analysis['target_count']}개</td>
                    </tr>
                    <tr>
                        <td>수집 시도 수</td>
                        <td>{analysis['crawled_count']}개</td>
                    </tr>
                </table>
            </div>
        """

        # 알림 메시지 섹션
        if analysis['alerts'] or error_message:
            html_content += """
            <div class="section">
                <h3>알림 내용</h3>
                <ul>
            """

            if error_message:
                html_content += f'<li class="critical">[CRITICAL] {error_message}</li>'

            for alert in analysis['alerts']:
                alert_class = 'critical' if alert['type'] == 'CRITICAL' else 'warning'
                html_content += f'<li class="{alert_class}">[{alert["type"]}] {alert["message"]}</li>'

            html_content += """
                </ul>
            </div>
            """

        # 필드별 통계 섹션 (가격, 제목만 표시)
        if analysis['field_stats']:
            html_content += """
            <div class="section">
                <h3>필드별 빈 값 현황</h3>
                <table>
                    <tr>
                        <th>필드명</th>
                        <th>빈 값 개수</th>
                        <th>총 개수</th>
                        <th>빈 값 비율</th>
                        <th>상태</th>
                    </tr>
            """

            # 가격, 제목 필드만 표시 (순서대로)
            priority_fields = ['retailprice', 'title']
            for field in priority_fields:
                if field in analysis['field_stats']:
                    stats = analysis['field_stats'][field]
                    # 20% 이상이면 ERROR, 그렇지 않으면 정상
                    if stats['empty_rate'] >= 20:
                        status = '<span class="critical">ERROR</span>'
                    else:
                        status = '<span style="color: #28a745;">정상</span>'
                    html_content += f"""
                    <tr>
                        <td>{stats['name']}</td>
                        <td>{stats['empty_count']}</td>
                        <td>{stats['total_count']}</td>
                        <td>{stats['empty_rate']}%</td>
                        <td>{status}</td>
                    </tr>
                """

            html_content += """
                </table>
            </div>
            """

        # 가격 이상 / URL 변동 섹션 (항상 표시)
        price_items = [a for a in price_anomalies if a.get('anomaly_type') in ('price_change', 'price_disappeared', 'price_appeared')]
        url_items = [a for a in price_anomalies if a.get('anomaly_type') in ('url_removed', 'url_added')]

        # 가격 이상 섹션
        if price_items:
            html_content += f"""
            <div class="section">
                <h3 class="critical">전일 대비 가격 이상 감지 ({len(price_items)}건)</h3>
                <table>
                    <tr><th>제품</th><th>전일 가격</th><th>금일 가격</th><th>상태</th><th>URL</th></tr>
            """
            for item in price_items:
                atype = item.get('anomaly_type')
                if atype == 'price_disappeared':
                    status = '<span class="critical">가격 사라짐</span>'
                    prev_str = f"{item['prev_price']}"
                    curr_str = '-'
                elif atype == 'price_appeared':
                    status = '<span style="color: #ffc107; font-weight: bold;">가격 신규</span>'
                    prev_str = '-'
                    curr_str = f"{item['curr_price']}"
                else:
                    status = f'<span class="critical">{item["pct_change"]}% 변동</span>'
                    prev_str = f"{item['prev_price']}"
                    curr_str = f"{item['curr_price']}"
                html_content += f"""
                    <tr>
                        <td>{item['title']}</td><td>{prev_str}</td><td>{curr_str}</td>
                        <td>{status}</td><td><a href="{item['url']}">{item['url'][-30:]}</a></td>
                    </tr>"""
            html_content += "</table></div>"
        else:
            html_content += """
            <div class="section">
                <h3 style="color: #28a745;">전일 대비 가격 이상 없음</h3>
            </div>"""

        # URL 변동 섹션
        if url_items:
            html_content += f"""
            <div class="section">
                <h3 class="critical">URL 변동 감지 ({len(url_items)}건)</h3>
                <table>
                    <tr><th>제품</th><th>가격</th><th>상태</th><th>URL</th></tr>
            """
            for item in url_items:
                atype = item.get('anomaly_type')
                if atype == 'url_removed':
                    status = '<span class="critical">삭제됨 (전일 존재 → 금일 없음)</span>'
                    price_str = f"{item['prev_price']}" if item['prev_price'] is not None else '-'
                else:
                    status = '<span style="color: #17a2b8; font-weight: bold;">추가됨 (전일 없음 → 금일 존재)</span>'
                    price_str = f"{item['curr_price']}" if item['curr_price'] is not None else '-'
                html_content += f"""
                    <tr>
                        <td>{item['title']}</td><td>{price_str}</td>
                        <td>{status}</td><td><a href="{item['url']}">{item['url'][-30:]}</a></td>
                    </tr>"""
            html_content += "</table></div>"
        else:
            html_content += """
            <div class="section">
                <h3 style="color: #28a745;">전일 대비 URL 변동 없음</h3>
            </div>"""

        # 에러 로그 섹션
        if analysis.get('error_logs'):
            html_content += """
            <div class="section">
                <h3>에러 로그</h3>
                <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 4px; padding: 10px; max-height: 300px; overflow-y: auto;">
                    <pre style="margin: 0; font-size: 12px; white-space: pre-wrap; word-wrap: break-word;">"""

            for error_log in analysis['error_logs'][-20:]:  # 최근 20개만
                html_content += f"{error_log}\n"

            if len(analysis['error_logs']) > 20:
                html_content += f"\n... 외 {len(analysis['error_logs']) - 20}개 에러"

            html_content += """</pre>
                </div>
            </div>
            """

        html_content += """
            <div class="section">
                <p style="color: #666; font-size: 12px;">
                    이 메일은 자동 발송되었습니다. 문제가 지속되면 HTML/XPath 변경 여부를 확인해주세요.
                </p>
            </div>
        </body>
        </html>
        """

        # 이메일 생성
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']

        msg.attach(MIMEText(html_content, 'html'))

        # 이메일 발송
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['receiver_email'],
                msg.as_string()
            )

        logger.info(f"알림 이메일 발송 완료: {subject}")
        return True

    except Exception as e:
        logger.error(f"이메일 발송 실패: {e}")
        return False


def monitor_and_alert(country_code, target_count, results_df, error_message=None, error_logs=None, blocked_page_failures=0, all_null_failures=0, title_null_failures=0, fs_country_code=None, file_prefix=None, skip_date=None, recovery_prefix='', recovery_suffix=''):
    """
    크롤링 결과 모니터링 및 알림 (메인 함수)

    각 크롤러에서 이 함수를 호출하면 됩니다.

    Args:
        country_code: 국가 코드
        target_count: tracking list 총 개수
        results_df: 크롤링 결과 DataFrame (실패 시 None)
        error_message: 추가 에러 메시지 (선택)
        error_logs: 크롤링 중 발생한 에러 로그 리스트 (선택)
        blocked_page_failures: 차단 페이지로 인한 최종 실패 개수 (선택)
        all_null_failures: title, imageurl, retailprice 모두 NULL인 최종 실패 개수 (선택)
        title_null_failures: title이 NULL인 최종 실패 개수 (선택)
        fs_country_code: 파일서버 국가 디렉토리명 (가격 이상 감지용, 선택. 예: 'usa')
        file_prefix: 파일서버 파일 접두사 (가격 이상 감지용, 선택. 예: 'usa_bestbuy')
        skip_date: 건너뛸 날짜 (recovery 시 세션 날짜, 선택. 예: '20260403')

    Returns:
        bool: 알림 발송 성공 여부

    사용 예시:
        from alert_monitor import monitor_and_alert

        # 크롤링 완료 후 (가격 이상 감지 포함)
        monitor_and_alert('usa_bestbuy', len(urls_data), results_df,
                         fs_country_code='usa', file_prefix='usa_bestbuy')

        # 에러 발생 시 (가격 비교 불필요)
        monitor_and_alert('usa_bestbuy', len(urls_data), None, error_message="DB 연결 실패")
    """
    try:
        # 가격 이상 감지 (파일서버 정보가 있을 때만)
        price_anomalies = []
        if fs_country_code and file_prefix and results_df is not None:
            price_anomalies = detect_price_anomalies(results_df, fs_country_code, file_prefix, skip_date=skip_date)

        # 결과 분석
        analysis = analyze_crawl_results(country_code, target_count, results_df, error_logs, blocked_page_failures, all_null_failures, title_null_failures, price_anomalies=price_anomalies, recovery_prefix=recovery_prefix, recovery_suffix=recovery_suffix)

        # 항상 이메일 발송 (일일 리포트)
        return send_alert_email(analysis, error_message)

    except Exception as e:
        logger.error(f"모니터링 중 오류: {e}")
        return False
