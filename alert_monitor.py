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

from config import EMAIL_CONFIG

logger = logging.getLogger(__name__)

# 국가별 이름 매핑
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
    'fr_fnac': '프랑스 Fnac'
}


def analyze_crawl_results(country_code, target_count, results_df, error_logs=None):
    """
    크롤링 결과 분석

    Args:
        country_code: 국가 코드 (예: 'jp', 'it')
        target_count: tracking list의 총 개수 (수집 시도해야 할 개수)
        results_df: 크롤링 결과 DataFrame
        error_logs: 크롤링 중 발생한 에러 로그 리스트 (선택)

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
        'field_stats': {},
        'error_logs': error_logs or []
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
            empty_count = results_df[field].isna().sum() + (results_df[field] == '').sum()
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
        if analysis['is_critical'] or error_message:
            subject = f"[CRITICAL] {country_name} 크롤링 알림 - {now.strftime('%Y-%m-%d %H:%M')}"
        elif analysis['alerts']:
            subject = f"[WARNING] {country_name} 크롤링 알림 - {now.strftime('%Y-%m-%d %H:%M')}"
        else:
            subject = f"[OK] {country_name} 크롤링 리포트 - {now.strftime('%Y-%m-%d %H:%M')}"

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

        # 필드별 통계 섹션
        if analysis['field_stats']:
            html_content += """
            <div class="section">
                <h3>필드별 빈 값 현황</h3>
                <table>
                    <tr>
                        <th>필드</th>
                        <th>빈 값 개수</th>
                        <th>총 개수</th>
                        <th>빈 값 비율</th>
                        <th>상태</th>
                    </tr>
            """

            for field, stats in analysis['field_stats'].items():
                status = '<span class="critical">위험</span>' if stats['empty_rate'] >= 50 else '정상'
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


def monitor_and_alert(country_code, target_count, results_df, error_message=None, error_logs=None):
    """
    크롤링 결과 모니터링 및 알림 (메인 함수)

    각 크롤러에서 이 함수를 호출하면 됩니다.

    Args:
        country_code: 국가 코드
        target_count: tracking list 총 개수
        results_df: 크롤링 결과 DataFrame (실패 시 None)
        error_message: 추가 에러 메시지 (선택)
        error_logs: 크롤링 중 발생한 에러 로그 리스트 (선택)

    Returns:
        bool: 알림 발송 성공 여부

    사용 예시:
        from alert_monitor import monitor_and_alert

        # 크롤링 완료 후
        monitor_and_alert('jp', len(urls_data), results_df)

        # 에러 발생 시
        monitor_and_alert('jp', len(urls_data), None, error_message="ChromeDriver 초기화 실패")

        # 에러 로그 포함
        monitor_and_alert('jp', len(urls_data), results_df, error_logs=error_list)
    """
    try:
        # 결과 분석
        analysis = analyze_crawl_results(country_code, target_count, results_df, error_logs)

        # 항상 이메일 발송 (일일 리포트)
        return send_alert_email(analysis, error_message)

    except Exception as e:
        logger.error(f"모니터링 중 오류: {e}")
        return False
