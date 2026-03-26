"""
X-kom 크롤러 V2 (타임존 분리 버전)
원본 xkom.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간(폴란드)과 한국시간 분리 저장
- 새 데이터베이스 사용 (DB_CONFIG_V2)
- 1회 크롤링 실행
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import paramiko
import time
import random
import re
from datetime import datetime, timedelta
import pytz
import logging
import os
import traceback
import json
import zipfile
import hashlib
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('xkom_infinite.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert

class XKomScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.sftp_client = None
        self.is_logged_in = False
        self.crawl_count = 0
        self.country_code = 'pl'
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Warsaw')  # X-KOM 폴란드 현지 시간
        self.start_time = datetime.now(self.korea_tz)
        self.saved_cookies = []

        # 이메일 설정 (Windows 환경변수 또는 직접 설정)
        self.email_config = {
            'smtp_server': os.environ.get('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': int(os.environ.get('SMTP_PORT', '587')),
            'sender_email': os.environ.get('SMTP_EMAIL', 'unsan010@gmail.com'),
            'sender_password': os.environ.get('SMTP_PASSWORD', 'wxzj osxb ommz pkts'),
            'receiver_email': os.environ.get('ALERT_EMAIL', 'unsan010@gmail.com')
        }
        
        # DB 연결 설정
        self.setup_db_connection()
        
        # DB에서 XPath 로드
        self.load_xpaths_from_db()
        
    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            logger.info("✅ DB 연결 설정 완료")
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            self.db_engine = None
    
    def load_xpaths_from_db(self):
        """DB에서 X-kom용 선택자 로드"""
        try:
            # X-kom 선택자 확인
            check_query = """
            SELECT COUNT(*) as count
            FROM mall_selectors
            WHERE mall_name = 'x-kom' 
              AND country_code = 'pl'
              AND is_active = TRUE
            """
            
            check_df = pd.read_sql(check_query, self.db_engine)
            
            if check_df['count'][0] > 0:
                # X-kom 선택자가 있으면 로드
                query = """
                SELECT element_type, selector_value, priority
                FROM mall_selectors
                WHERE mall_name = 'x-kom' 
                  AND country_code = 'pl'
                  AND is_active = TRUE
                ORDER BY element_type, priority DESC
                """
                
                df = pd.read_sql(query, self.db_engine)
                
                # element_type별로 그룹화
                self.XPATHS = {}
                for element_type in df['element_type'].unique():
                    type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    self.XPATHS[element_type] = type_selectors
                
                logger.info(f"✅ DB에서 X-kom 선택자 로드 완료: {len(df)}개")
                
            else:
                # X-kom 선택자가 없으면 빈 값 사용 (DB에서 관리)
                logger.warning("⚠️ DB에 X-kom 선택자가 없습니다. DB에 선택자를 추가해주세요.")
                self.XPATHS = {
                    'price': [],
                    'title': [],
                    'imageurl': [],
                    'availability': []
                }
                
        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            self.XPATHS = {
                'price': [],
                'title': [],
                'imageurl': [],
                'availability': []
            }
    
    def get_crawl_targets(self, limit=None):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'pl' 
              AND mall_name = 'x-kom'
              AND is_active = TRUE
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"✅ 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("🔧 Chrome 드라이버 설정 중...")
        
        try:
            options = uc.ChromeOptions()

            # 페이지 로드 전략: eager (DOM 로드 완료 시 즉시 반환)
            options.page_load_strategy = 'eager'

            # 기본 옵션
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--window-size=1920,1080')
            
            # 폴란드 설정
            options.add_argument('--lang=pl-PL')
            options.add_experimental_option('prefs', {
                "intl.accept_languages": "pl-PL,pl"
            })
            
            # 설치된 Chrome 버전 감지하여 맞는 드라이버 사용
            chrome_version = None
            try:
                import subprocess
                result = subprocess.run(
                    ['reg', 'query', 'HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon', '/v', 'version'],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if 'version' in line.lower():
                            version_str = line.strip().split()[-1]
                            chrome_version = int(version_str.split('.')[0])
                            logger.info(f"감지된 Chrome 버전: {chrome_version}")
                            break
            except Exception as e:
                logger.warning(f"Chrome 버전 감지 실패, 자동 매칭 시도: {e}")

            self.driver = uc.Chrome(options=options, version_main=chrome_version)
            self.driver.maximize_window()
            self.driver.set_page_load_timeout(30)

            logger.info("✅ 드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def initial_manual_login(self):
        """초기 수동 로그인 - Cloudflare 통과"""
        logger.info("\n" + "="*60)
        logger.info("🔐 === 초기 수동 로그인 ===")
        logger.info("="*60)

        try:
            # X-kom 메인 페이지 접속
            logger.info("X-kom 접속 중...")
            self.driver.get("https://www.x-kom.pl")

            logger.info("\n📋 다음 단계를 수행해주세요:")
            logger.info("1. Cloudflare 챌린지가 나타나면 해결하세요")
            logger.info("2. 쿠키 동의 팝업이 나타나면 수락하세요")
            logger.info("3. 사이트가 완전히 로드될 때까지 기다리세요")

            input("\n✅ 모든 작업이 완료되면 Enter를 누르세요...")
            
            # 현재 상태 확인
            current_url = self.driver.current_url
            if "x-kom.pl" in current_url and not self.check_cloudflare_challenge():
                self.is_logged_in = True
                logger.info("✅ 로그인 성공! 무한 크롤링을 시작합니다.")
                
                # 쿠키 저장
                try:
                    self.saved_cookies = self.driver.get_cookies()
                    logger.info(f"💾 쿠키 {len(self.saved_cookies)}개 저장")
                    
                    # 파일로도 저장
                    with open('xkom_cookies.json', 'w') as f:
                        json.dump(self.saved_cookies, f)
                except Exception as e:
                    logger.error(f"쿠키 저장 실패: {e}")
                
                return True
            else:
                logger.error("❌ 로그인 실패 - Cloudflare를 통과하지 못했습니다.")
                return False
                
        except Exception as e:
            logger.error(f"초기 로그인 오류: {e}")
            return False
    
    def check_cloudflare_challenge(self):
        """Cloudflare 챌린지 페이지인지 확인"""
        try:
            indicators = [
                "Verifying you are human",
                "cf-challenge",
                "Just a moment",
                "Checking your browser",
                "DDoS protection"
            ]
            
            page_source = self.driver.page_source.lower()
            page_title = self.driver.title.lower()
            
            for indicator in indicators:
                if indicator.lower() in page_source or indicator.lower() in page_title:
                    return True
                    
            return False
            
        except Exception:
            return False
    
    def keep_session_alive(self):
        """세션 유지를 위한 활동 (더 안전하게)"""
        try:
            # 브라우저 상태만 확인
            try:
                current_url = self.driver.current_url
                logger.debug(f"현재 URL: {current_url}")
            except:
                logger.warning("⚠️ 브라우저 응답 없음")
                self.is_logged_in = False
                return
            
            # 너무 자주 이동하지 않음
            if "x-kom.pl" in current_url:
                # 현재 페이지에서 작은 액션만
                try:
                    # 작은 스크롤
                    self.driver.execute_script("window.scrollBy(0, 100)")
                    time.sleep(1)
                    self.driver.execute_script("window.scrollBy(0, -100)")
                    
                    logger.info("💓 세션 keep-alive 완료 (가벼운 액션)")
                except:
                    pass
            else:
                # X-kom 페이지가 아니면 홈으로
                try:
                    self.driver.get("https://www.x-kom.pl")
                    time.sleep(3)
                    
                    # Cloudflare 체크
                    if self.check_cloudflare_challenge():
                        logger.error("❌ Keep-alive 중 Cloudflare 감지!")
                        self.is_logged_in = False
                        return
                    
                    logger.info("💓 세션 keep-alive 완료 (홈 방문)")
                except Exception as e:
                    logger.error(f"Keep-alive 오류: {e}")
                    
        except Exception as e:
            logger.error(f"Keep-alive 오류: {e}")
    
    def restart_browser(self):
        """브라우저 재시작 및 재로그인"""
        try:
            logger.info("🔄 브라우저 재시작 중...")
            
            # 기존 드라이버 종료
            try:
                self.driver.quit()
            except:
                pass
            
            time.sleep(5)
            
            # 드라이버 재설정
            if not self.setup_driver():
                return False
            
            # 자동 재로그인 시도 (저장된 쿠키 사용)
            if self.saved_cookies:
                try:
                    self.driver.get("https://www.x-kom.pl")
                    time.sleep(3)
                    
                    # 쿠키 복원
                    for cookie in self.saved_cookies:
                        try:
                            self.driver.add_cookie(cookie)
                        except:
                            pass
                    
                    # 페이지 새로고침
                    self.driver.refresh()
                    time.sleep(5)
                    
                    # Cloudflare 체크
                    if not self.check_cloudflare_challenge():
                        logger.info("✅ 쿠키로 자동 재로그인 성공")
                        self.is_logged_in = True
                        return True
                except:
                    pass
            
            # 쿠키 복원 실패 시 수동 로그인 필요
            logger.warning("⚠️ 자동 재로그인 실패. 수동 로그인이 필요합니다.")
            return self.initial_manual_login()
            
        except Exception as e:
            logger.error(f"브라우저 재시작 실패: {e}")
            return False
    
    def send_email_alert(self, subject, message):
        """이메일 알림 전송"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            # 이메일 설정 확인
            if self.email_config['sender_email'] == 'your_email@gmail.com':
                logger.warning("이메일 설정이 되어있지 않습니다. 환경변수를 설정하세요.")
                logger.warning("set SMTP_EMAIL=your_email@gmail.com")
                logger.warning("set SMTP_PASSWORD=your_app_password")
                logger.warning("set ALERT_EMAIL=receiver@gmail.com")
                return False
            
            # 이메일 구성
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = self.email_config['receiver_email']
            msg['Subject'] = f"[X-kom 크롤러] {subject}"
            
            body = f"""
X-kom 크롤러 알림

{message}

=== 상세 정보 ===
시간: {datetime.now(self.korea_tz).strftime('%Y-%m-%d %H:%M:%S')}
서버: {os.environ.get('COMPUTERNAME', 'Windows EC2')}
크롤링 횟수: {self.crawl_count}
가동 시간: {datetime.now(self.korea_tz) - self.start_time}
Python 버전: {os.sys.version.split()[0]}

로그 위치: {os.getcwd()}\\xkom_infinite.log
            """
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            # 이메일 전송
            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                server.starttls()
                server.login(self.email_config['sender_email'], self.email_config['sender_password'])
                server.send_message(msg)
            
            logger.info(f"📧 이메일 알림 전송 완료: {self.email_config['receiver_email']}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 실패: {e}")
            
            # 대안: AWS SNS 사용 (EC2에서 IAM 역할 있는 경우)
            try:
                import boto3
                sns = boto3.client('sns', region_name='ap-northeast-2')
                
                # SNS 주제 생성 또는 기존 주제 사용
                response = sns.create_topic(Name='Xkom-Crawler-Alerts')
                topic_arn = response['TopicArn']
                
                # 메시지 발행
                sns.publish(
                    TopicArn=topic_arn,
                    Subject=f"[X-kom] {subject}",
                    Message=f"{message}\n\n시간: {datetime.now()}"
                )
                logger.info("📱 AWS SNS 알림 전송 완료")
                return True
            except Exception as sns_error:
                logger.error(f"SNS 알림도 실패: {sns_error}")
                return False
    
    def check_browser_health(self):
        """브라우저 상태 확인"""
        try:
            result = self.driver.execute_script("return document.readyState")
            return result == "complete"
        except:
            return False
    
    def extract_product_info(self, url, row_data):
        """제품 정보 추출"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                logger.info(f"🔍 페이지 접속: {url}" + (f" (재시도 {attempt + 1}/{max_retries})" if attempt > 0 else ""))
                self.driver.get(url)

                # 페이지 로드 대기
                time.sleep(random.uniform(3, 5))
                break  # 성공 시 루프 탈출

            except Exception as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg or "renderer" in error_msg:
                    logger.warning(f"⚠️ 타임아웃 발생 (시도 {attempt + 1}/{max_retries}): {e}")

                    if attempt < max_retries - 1:
                        # 브라우저 새로고침 시도
                        try:
                            logger.info("🔄 브라우저 새로고침 시도...")
                            self.driver.refresh()
                            time.sleep(5)
                        except:
                            pass
                        continue
                    else:
                        logger.error(f"❌ 최대 재시도 횟수 초과: {url}")
                        return None
                else:
                    # 타임아웃이 아닌 다른 에러
                    logger.error(f"❌ 페이지 접속 오류: {e}")
                    return None

        try:
            
            # Cloudflare 체크
            if self.check_cloudflare_challenge():
                logger.error("❌ Cloudflare 챌린지 감지! 세션이 만료되었습니다.")
                self.is_logged_in = False
                return None
            
            # V2: 타임존 분리
            now_time = datetime.now(self.korea_tz)
            local_time = datetime.now(self.local_tz)


            # ISO 8601 형식

            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")

            tz_offset = local_time.strftime("%z")

            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"

            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"


            # 기본 결과 구조
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'pl'),
                'ships_from': 'PL',
                'channel_name': 'xkom',
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': 'X-kom',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }
            
            # 가격 추출
            try:
                price_found = False
                
                # DB에서 가져온 선택자로 시도
                for selector in self.XPATHS.get('price', []):
                    try:
                        if selector.startswith('//'):
                            # XPath인 경우
                            price_elements = self.driver.find_elements(By.XPATH, selector)
                        elif selector.startswith('meta'):
                            # meta 태그인 경우
                            elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                            price_text = elem.get_attribute('content')
                            if price_text:
                                price_match = re.search(r'(\d+)[,.]?(\d*)', price_text)
                                if price_match:
                                    price = price_match.group(1)
                                    if price_match.group(2):
                                        price += '.' + price_match.group(2)
                                    result['retailprice'] = float(price)
                                    logger.info(f"✅ 가격 추출 성공 (meta): {result['retailprice']} PLN")
                                    price_found = True
                                    break
                            continue
                        else:
                            # CSS 선택자인 경우
                            price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        
                        for price_element in price_elements:
                            price_text = price_element.text.strip()
                            
                            if price_text:
                                # PLN 가격 추출 (다양한 형식 지원)
                                # 예: "899 zł", "899,00 zł", "899", "zł 899"
                                price_text = price_text.replace('zł', '').replace('PLN', '').replace(' ', '').strip()
                                price_match = re.search(r'(\d+)[,.]?(\d*)', price_text)
                                if price_match:
                                    price = price_match.group(1)
                                    if price_match.group(2):
                                        price += '.' + price_match.group(2)
                                    result['retailprice'] = float(price)
                                    logger.info(f"✅ 가격 추출 성공: {result['retailprice']} PLN (선택자: {selector})")
                                    price_found = True
                                    break
                        
                        if price_found:
                            break
                            
                    except Exception as e:
                        logger.debug(f"선택자 {selector} 실패: {e}")
                        continue
                
                if not price_found:
                    logger.warning("❌ DB 선택자로 가격을 찾을 수 없습니다")
                    
            except Exception as e:
                logger.warning(f"가격 추출 실패: {e}")
            
            # 제목 추출
            try:
                for selector in self.XPATHS.get('title', []):
                    try:
                        if selector.startswith('//'):
                            title_element = self.driver.find_element(By.XPATH, selector)
                        elif selector.startswith('meta'):
                            elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                            result['title'] = elem.get_attribute('content')
                            logger.info(f"제목: {result['title']}")
                            break
                        else:
                            title_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목: {result['title']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")
            
            # 이미지 URL 추출
            try:
                for selector in self.XPATHS.get('imageurl', []):
                    try:
                        if selector.startswith('//'):
                            image_element = self.driver.find_element(By.XPATH, selector)
                        elif selector.startswith('meta'):
                            elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                            result['imageurl'] = elem.get_attribute('content')
                            logger.info(f"이미지 URL: {result['imageurl']}")
                            break
                        else:
                            image_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        result['imageurl'] = image_element.get_attribute('src')
                        logger.info(f"이미지 URL: {result['imageurl']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            return None
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # xkom_price_crawl_tbl_pl_v2 테이블에 저장
            df.to_sql('xkom_price_crawl_tbl_pl_v2', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'pl',
                    'url': row['producturl'],
                    'status': 'success' if row['retailprice'] is not None else 'failed',
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })
            
            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            return False
    
    def upload_to_file_server(self, local_file_path, date_folder):
        """파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            # 국가별 디렉토리 경로
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"

            # 국가 디렉토리가 없으면 생성
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"📁 국가 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)

            # 날짜별 디렉토리 경로
            date_dir = f"{country_dir}/{date_folder}"

            # 날짜 디렉토리가 없으면 생성
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"📁 날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)

            # 업로드 경로
            remote_filename = os.path.basename(local_file_path)
            remote_path = f"{date_dir}/{remote_filename}"

            # 파일 업로드
            sftp.put(local_file_path, remote_path)
            logger.info(f"✅ 파일서버 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()

            return True
        except Exception as e:
            logger.error(f"❌ 파일서버 업로드 실패: {e}")
            return False
    def save_results(self, df, save_db=True):
        """결과를 DB와 파일서버에 저장"""
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        time_str = now.strftime("%H%M%S")
        base_filename = f"{date_str}_{time_str}_pl_xkom"

        results = {'db_saved': False, 'server_uploaded': False}

        # DB 저장
        if save_db:
            results['db_saved'] = self.save_to_db(df)

        # 파일서버 업로드
        try:
            # 1. CSV 파일 생성
            csv_filename = f'{base_filename}.csv'
            # Header를 대문자로 변환
            df.columns = df.columns.str.upper()
            df.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')

            # 2. CSV를 ZIP으로 압축
            zip_filename = f'{base_filename}.zip'
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(csv_filename, arcname=csv_filename)

            # 3. MD5 계산
            def calculate_md5(filename):
                md5 = hashlib.md5()
                with open(filename, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        md5.update(chunk)
                return md5.hexdigest()

            csv_md5 = calculate_md5(csv_filename)
            zip_md5 = calculate_md5(zip_filename)

            # 4. MD5 파일 생성 (정합성 확인)
            md5_filename = f'{base_filename}.md5'
            with open(md5_filename, 'w', encoding='utf-8') as f:
                f.write(f"{os.path.basename(zip_filename)} {zip_md5}\n")
                f.write(f"{os.path.basename(csv_filename)} {csv_md5}\n")

            # 5. ZIP과 MD5를 날짜 폴더에 업로드
            if self.upload_to_file_server(zip_filename, date_str):
                if self.upload_to_file_server(md5_filename, date_str):
                    results['server_uploaded'] = True

            # 6. 로컬 임시 파일 삭제
            for temp_file in [csv_filename, zip_filename, md5_filename]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            logger.info("임시 파일 삭제 완료")
        except Exception as e:
            logger.error(f"파일 저장 실패: {e}")

        return results
    
    def crawl_once(self):
        """1회 크롤링 실행"""
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 크롤링 라운드 {self.crawl_count + 1} 시작")
        logger.info(f"시작 시간: {datetime.now()}")
        logger.info(f"{'='*60}")
        
        # DB에서 URL 목록 조회
        urls_data = self.get_crawl_targets()
        
        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            return
        
        logger.info(f"📊 총 {len(urls_data)}개 제품 처리 예정")
        
        results = []
        success_count = 0
        
        for idx, row in enumerate(urls_data):
            # 세션 확인
            if not self.is_logged_in:
                logger.error("❌ 세션이 만료되었습니다. 재로그인이 필요합니다.")
                break
            
            logger.info(f"\n진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
            
            # URL 추출
            url = row.get('url')
            
            # 제품 정보 추출
            result = self.extract_product_info(url, row)
            
            if result:
                results.append(result)
                if result['retailprice'] is not None:
                    success_count += 1
            
            # 5개마다 keep-alive
            if (idx + 1) % 5 == 0:
                # 브라우저 상태 확인
                if not self.check_browser_health():
                    logger.warning("⚠️ 브라우저 상태 이상 감지")
                    if self.restart_browser():
                        logger.info("✅ 브라우저 재시작 완료")
                    else:
                        logger.error("❌ 브라우저 재시작 실패. 크롤링 중단")
                        self.is_logged_in = False
                        break
                else:
                    self.keep_session_alive()
                
                # 중간 저장
                if results:
                    interim_df = pd.DataFrame(results[-5:])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('xkom_price_crawl_tbl_pl_v2', self.db_engine, 
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 5개 레코드")
                        except:
                            pass
            
            # 다음 요청 전 대기
            if idx < len(urls_data) - 1:
                wait_time = random.uniform(5, 10)
                time.sleep(wait_time)
                
                # 25개마다 긴 휴식
                if (idx + 1) % 25 == 0:
                    logger.info("☕ 25개 처리 완료, 30초 휴식...")
                    time.sleep(30)
        
        # 마지막 남은 데이터 DB 저장 (5개 단위 중간 저장에 포함되지 않은 나머지)
        remainder = len(results) % 5
        if remainder > 0 and self.db_engine:
            try:
                remainder_df = pd.DataFrame(results[-remainder:])
                remainder_df.to_sql('xkom_price_crawl_tbl_pl_v2', self.db_engine,
                                    if_exists='append', index=False)
                logger.info(f"💾 마지막 저장: {remainder}개 레코드")
            except Exception as e:
                logger.error(f"마지막 배치 저장 실패: {e}")

        # 결과 저장 (중간저장+마지막저장으로 DB 완료, 파일서버 업로드만 수행)
        if results:
            df = pd.DataFrame(results)
            save_results = self.save_results(df, save_db=False)

            # 통계
            logger.info(f"\n📊 === 크롤링 라운드 {self.crawl_count + 1} 완료 ===")
            logger.info(f"전체 제품: {len(results)}개")
            logger.info(f"가격 추출 성공: {success_count}개")
            logger.info(f"성공률: {success_count/len(results)*100:.1f}%")
            logger.info(f"DB 저장: ✅ (중간저장 완료)")
            logger.info(f"파일서버 업로드: {'✅' if save_results['server_uploaded'] else '❌'}")

            # 알림 발송
            monitor_and_alert('pl_xkom', len(urls_data), df)
        else:
            # 결과 없음
            monitor_and_alert('pl_xkom', len(urls_data), None, error_message="크롤링 결과 없음")

        self.crawl_count += 1

    def start(self):
        """메인 시작 함수"""
        logger.info("\n🚀 X-kom 크롤러 시작")
        logger.info("="*60)
        
        # 드라이버 설정
        if not self.setup_driver():
            logger.error("드라이버 설정 실패로 종료합니다.")
            return
        
        try:
            # 초기 수동 로그인
            if not self.initial_manual_login():
                logger.error("초기 로그인 실패로 종료합니다.")
                return
            
            # 1회 크롤링 실행
            self.crawl_once()
            
        except Exception as e:
            logger.error(f"치명적 오류: {e}")
            logger.error(traceback.format_exc())
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔧 드라이버 종료")

def main():
    """메인 실행 함수"""
    print("\n🚀 X-kom 크롤러")
    print("="*60)
    print("초기에 수동으로 Cloudflare를 통과한 후")
    print("1회 크롤링이 시작됩니다.")
    print("="*60)
    
    # 스크래퍼 생성 및 실행
    scraper = XKomScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('pl_xkom', 0, None, error_message="DB 연결 실패")
        return

    # 시작
    scraper.start()

if __name__ == "__main__":
    # 필요한 패키지 확인
    required_packages = [
        'undetected-chromedriver',
        'selenium',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'paramiko',
        'openpyxl'
    ]
    
    print("📦 필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print()
    
    main()