"""
Fnac 가격 추출 시스템 v3 - DrissionPage 기반
fnac_v2.py 기반 - Playwright를 DrissionPage로 변경 (봇 감지 우회)
DB에서 URL 읽어와서 크롤링 후 결과 저장
파일명 형식: {수집일자}_{수집시간}_{국가코드}_{쇼핑몰}.csv
"""
import os

from DrissionPage import ChromiumPage, ChromiumOptions
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import paramiko
import time
import random
import re
from datetime import datetime
import pytz
import logging
import json
from io import StringIO
import zipfile
import hashlib

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert

class FnacScraperV3:
    def __init__(self):
        self.page = None
        self.db_engine = None
        self.sftp_client = None
        self.country_code = 'fr'
        self.is_logged_in = False
        self.saved_cookies = []
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Paris')  # Fnac 프랑스 현지 시간

        # DB 연결 설정
        self.setup_db_connection()

        # DB에서 XPath 로드
        self.load_xpaths_from_db()

    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            # SQLAlchemy 엔진 생성
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            logger.info("DB 연결 설정 완료")

        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            self.db_engine = None

    def load_xpaths_from_db(self):
        """DB에서 Fnac용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'fnac'
              AND country_code = 'fr'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """

            df = pd.read_sql(query, self.db_engine)

            # element_type별로 그룹화
            self.XPATHS = {}
            for element_type in df['element_type'].unique():
                type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                self.XPATHS[element_type] = type_selectors

            logger.info(f"DB에서 선택자 로드 완료: {len(df)}개")

            # 기본값 설정 (DB에 없는 경우) - DB에서 관리되므로 빈 배열로 설정
            if not self.XPATHS:
                logger.warning("DB에 선택자가 없습니다. DB에 선택자를 추가해주세요.")
                self.XPATHS = {
                    'price': [],
                    'title': [],
                    'imageurl': []
                }

        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 오류 시 빈 배열 - DB에서 관리되므로
            self.XPATHS = {
                'price': [],
                'title': [],
                'imageurl': []
            }

    def get_crawl_targets(self, limit=None, include_failed=False):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            if include_failed:
                # 최근 실패한 URL도 포함 (24시간 이내 실패 3회 미만)
                query = """
                WITH failed_counts AS (
                    SELECT url, COUNT(*) as fail_count
                    FROM amazon_crawl_logs
                    WHERE retailprice IS NULL
                      AND crawl_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      AND country_code = 'fr'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'fr'
                  AND t.mall_name = 'fnac'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC  -- 실패한 것 우선
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'fr'
                  AND mall_name = 'fnac'
                  AND is_active = TRUE
                """

            if limit:
                query += f" LIMIT {limit}"

            df = pd.read_sql(query, self.db_engine)
            logger.info(f"크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')

        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []

    def setup_browser(self):
        """DrissionPage 브라우저 설정"""
        logger.info("DrissionPage 브라우저 설정 중...")

        try:
            # ChromiumOptions 설정
            co = ChromiumOptions()

            # 기본 옵션
            co.set_argument('--no-sandbox')
            co.set_argument('--disable-dev-shm-usage')
            co.set_argument('--disable-blink-features=AutomationControlled')
            co.set_argument('--disable-web-security')
            co.set_argument('--disable-features=IsolateOrigins,site-per-process')
            co.set_argument('--window-size=1920,1080')

            # 프랑스 언어/로케일 설정
            co.set_argument('--lang=fr-FR')
            co.set_pref('intl.accept_languages', 'fr-FR,fr')

            # ChromiumPage 초기화
            self.page = ChromiumPage(co)

            # 타임아웃 설정
            self.page.set.timeouts(page_load=30)

            logger.info("DrissionPage 브라우저 설정 완료")
            return True

        except Exception as e:
            logger.error(f"브라우저 설정 실패: {e}")
            return False

    def check_bot_challenge(self):
        """봇 감지/Cloudflare 챌린지 페이지인지 확인"""
        try:
            indicators = [
                "Verifying you are human",
                "cf-challenge",
                "Just a moment",
                "Checking your browser",
                "DDoS protection",
                "Access denied",
                "blocked"
            ]

            page_source = self.page.html.lower()
            page_title = self.page.title.lower()

            for indicator in indicators:
                if indicator.lower() in page_source or indicator.lower() in page_title:
                    return True

            return False

        except Exception:
            return False

    def handle_cookie_popup(self):
        """쿠키 팝업 처리"""
        try:
            logger.info("쿠키 팝업 확인 중...")
            time.sleep(1)  # 팝업이 나타날 시간 대기

            # 쿠키 버튼 선택자들 (v2: onetrust 선택자 추가)
            cookie_selectors = [
                "css:#onetrust-accept-btn-handler",  # OneTrust 쿠키 배너
                "xpath://button[contains(text(), \"J'accepte\")]",
                "xpath://button[contains(text(), 'accepte')]",
                "css:[class*='accept' i]",
                "css:[id*='accept' i]",
                "css:button[class*='cookie']",
                "css:.didomi-button"
            ]

            cookie_found = False
            for selector in cookie_selectors:
                try:
                    button = self.page.ele(selector, timeout=2)
                    if button:
                        button.click()
                        logger.info(f"쿠키 동의 팝업 처리 완료 (선택자: {selector})")
                        time.sleep(2)
                        cookie_found = True
                        break
                except Exception as e:
                    continue

            if not cookie_found:
                logger.info("쿠키 팝업이 없거나 이미 처리됨")

        except Exception as e:
            logger.debug(f"쿠키 팝업 처리 중 오류 (무시): {e}")

    def initialize_session(self):
        """Fnac 세션 초기화"""
        logger.info("Fnac 세션 초기화...")

        try:
            # Fnac 메인 페이지 접속
            self.page.get("https://www.fnac.com")
            logger.info("페이지 로드 완료")
            time.sleep(2)

            # 봇 감지 체크
            if self.check_bot_challenge():
                logger.warning("봇 감지 챌린지 페이지 감지!")
                logger.info("\n다음 단계를 수행해주세요:")
                logger.info("1. 봇 감지/Cloudflare 챌린지가 나타나면 해결하세요")
                logger.info("2. 쿠키 동의 팝업이 나타나면 수락하세요")
                logger.info("3. 사이트가 완전히 로드될 때까지 기다리세요")

                input("\n모든 작업이 완료되면 Enter를 누르세요...")

                # 재확인
                if self.check_bot_challenge():
                    logger.error("봇 감지 통과 실패")
                    return False

                # 쿠키 저장 (재시작 시 활용)
                try:
                    self.saved_cookies = self.page.cookies()
                    logger.info(f"쿠키 {len(self.saved_cookies)}개 저장")
                    with open('fnac_cookies.json', 'w') as f:
                        json.dump(self.saved_cookies, f)
                except Exception as e:
                    logger.error(f"쿠키 저장 실패: {e}")
            else:
                # 쿠키 팝업 처리
                self.handle_cookie_popup()

            # 세션이 제대로 설정되었는지 확인
            title = self.page.title
            if "fnac" in title.lower():
                self.is_logged_in = True
                logger.info("Fnac 세션 초기화 완료")
                return True
            else:
                logger.warning("세션 초기화 부분 성공")
                self.is_logged_in = True
                return True

        except Exception as e:
            logger.error(f"세션 초기화 실패: {e}")
            return False

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=10):
        """제품 정보 추출 (재시도 로직)"""
        try:
            logger.info(f"페이지 접속: {url} (시도: {retry_count + 1}/{max_retries})")
            self.page.get(url)

            # 페이지 로드 대기
            time.sleep(random.uniform(3, 5))

            # 봇 감지 체크
            if self.check_bot_challenge():
                logger.error("봇 감지 챌린지 감지! 세션이 만료되었습니다.")
                self.is_logged_in = False
                raise Exception("Bot challenge detected")

            # 쿠키 팝업 처리 (제품 페이지에서도 나타날 수 있음)
            self.handle_cookie_popup()

            # 현재 시간
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
                'country_code': row_data.get('country', 'fr'),
                'ships_from': 'FR',
                'channel_name': 'fnac',
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
                'sold_by': 'Fnac',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }

            # 제목 추출
            title_extracted = False
            try:
                for selector in self.XPATHS.get('title', []):
                    try:
                        if selector.startswith('//'):
                            elem = self.page.ele(f'xpath:{selector}', timeout=5)
                        else:
                            elem = self.page.ele(f'css:{selector}', timeout=5)

                        if elem:
                            title_text = elem.text

                            if title_text and title_text.strip():
                                result['title'] = title_text.strip()
                                logger.info(f"제목: {result['title']}")
                                title_extracted = True
                                break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")

            # 제목 추출 실패 시 재시도 로직
            if not title_extracted:
                logger.warning("제목 추출 실패 - 차단 페이지 가능성")

                # 1단계: 새로고침 시도
                logger.info("새로고침 시도...")
                self.page.refresh()
                time.sleep(random.uniform(3, 5))

                # 제목 재추출 시도
                for selector in self.XPATHS.get('title', []):
                    try:
                        if selector.startswith('//'):
                            elem = self.page.ele(f'xpath:{selector}', timeout=5)
                        else:
                            elem = self.page.ele(f'css:{selector}', timeout=5)

                        if elem:
                            title_text = elem.text

                            if title_text and title_text.strip():
                                result['title'] = title_text.strip()
                                logger.info(f"새로고침 후 제목 추출 성공: {result['title']}")
                                title_extracted = True
                                break
                    except:
                        continue

                # 2단계: fnac.com 접속 후 재시도
                if not title_extracted:
                    logger.warning("새로고침 후에도 실패 - fnac.com 접속 후 재시도")

                    self.page.get("https://www.fnac.com")
                    time.sleep(random.uniform(2, 4))

                    logger.info(f"원래 URL 재접속: {url}")
                    self.page.get(url)
                    time.sleep(random.uniform(3, 5))

                    for selector in self.XPATHS.get('title', []):
                        try:
                            if selector.startswith('//'):
                                elem = self.page.ele(f'xpath:{selector}', timeout=5)
                            else:
                                elem = self.page.ele(f'css:{selector}', timeout=5)

                            if elem:
                                title_text = elem.text

                                if title_text and title_text.strip():
                                    result['title'] = title_text.strip()
                                    logger.info(f"재접속 후 제목 추출 성공: {result['title']}")
                                    title_extracted = True
                                    break
                        except:
                            continue

                    if not title_extracted:
                        logger.error("모든 재시도 후에도 제목 추출 실패")
                        raise Exception("Title extraction failed - possible blocked page")

            # 품절 체크 (가격 추출 전)
            is_out_of_stock = False
            try:
                oos_elem = self.page.ele('css:[data-automation-id="product-availability"]', timeout=2)
                if oos_elem:
                    oos_text = oos_elem.text
                    if 'épuisé' in oos_text.lower():
                        is_out_of_stock = True
                        result['_out_of_stock'] = True
                        logger.info(f"품절 감지: {oos_text.strip()}")
            except Exception:
                pass

            # 가격 추출 (품절이 아닌 경우만)
            if is_out_of_stock:
                logger.info("품절 상품 → 가격 추출 건너뜀")
            else:
                try:
                    price_found = False
                    logger.info("가격 추출 시도...")

                    for selector in self.XPATHS.get('price', []):
                        try:
                            if selector.startswith('//'):
                                elem = self.page.ele(f'xpath:{selector}', timeout=5)
                            else:
                                elem = self.page.ele(f'css:{selector}', timeout=5)

                            if elem:
                                price_text = elem.text

                                if price_text and price_text.strip():
                                    # Fnac 프랑스 가격 형식: "419,99 €" 또는 "1 234,56 €"
                                    price_text_clean = price_text.replace(',', '.').replace('€', '').replace('\xa0', '').replace(' ', '').strip()
                                    price_match = re.search(r'(\d+\.?\d*)', price_text_clean)
                                    if price_match:
                                        price_number = price_match.group(1)
                                        result['retailprice'] = float(price_number)
                                        logger.info(f"가격 추출 성공: {result['retailprice']}")
                                        price_found = True
                                        break

                        except Exception as e:
                            continue

                    # JavaScript로 가격 찾기 (최후 수단)
                    if not price_found:
                        try:
                            js_result = self.page.run_js("""
                                var priceSelectors = [
                                    '.f-faPriceBox__price',
                                    '[class*="price"]',
                                    'span[class*="Price"]'
                                ];

                                for (var i = 0; i < priceSelectors.length; i++) {
                                    var elements = document.querySelectorAll(priceSelectors[i]);
                                    for (var j = 0; j < elements.length; j++) {
                                        var text = elements[j].textContent || elements[j].innerText;
                                        if (text && /\\d/.test(text) && text.includes('€')) {
                                            return text.trim();
                                        }
                                    }
                                }
                                return null;
                            """)

                            if js_result:
                                price_text_clean = js_result.replace(',', '.').replace('€', '').replace('\xa0', '').replace(' ', '').strip()
                                price_match = re.search(r'(\d+\.?\d*)', price_text_clean)
                                if price_match:
                                    price_number = price_match.group(1)
                                    result['retailprice'] = float(price_number)
                                    logger.info(f"가격 추출 성공 (JS): {result['retailprice']}")
                                    price_found = True
                        except Exception as e:
                            logger.debug(f"JavaScript 가격 추출 실패: {e}")

                    if not price_found:
                        logger.warning("모든 가격 추출 방법 실패")

                except Exception as e:
                    logger.warning(f"가격 추출 실패: {e}")

            # 이미지 URL 추출
            try:
                image_found = False

                for selector in self.XPATHS.get('imageurl', []):
                    try:
                        if selector.startswith('//'):
                            elem = self.page.ele(f'xpath:{selector}', timeout=5)
                        else:
                            elem = self.page.ele(f'css:{selector}', timeout=5)

                        if elem:
                            src = elem.attr('src')

                            if src and 'fnac-static.com' in src:
                                result['imageurl'] = src
                                logger.info(f"이미지 URL: {result['imageurl']}")
                                image_found = True
                                break
                    except:
                        continue

                # JavaScript로 이미지 찾기
                if not image_found:
                    try:
                        js_result = self.page.run_js("""
                            var imgs = document.querySelectorAll('img');
                            for (var i = 0; i < imgs.length; i++) {
                                var src = imgs[i].src || imgs[i].getAttribute('data-src');
                                if (src && src.includes('fnac-static.com')) {
                                    return src;
                                }
                            }
                            return null;
                        """)

                        if js_result:
                            result['imageurl'] = js_result
                            logger.info(f"이미지 URL (JS): {result['imageurl']}")
                            image_found = True
                    except:
                        pass

                if not image_found:
                    logger.warning("이미지 URL 추출 실패")

            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")

            return result

        except Exception as e:
            logger.error(f"페이지 처리 오류: {e}")

            # 재시도 로직 (고정 10초 대기, 최대 10회)
            if retry_count < max_retries - 1:
                wait_time = 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 2}/{max_retries})")
                time.sleep(wait_time)
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)

            # 최대 재시도 횟수 초과 시 기본값 반환
            logger.error(f"최대 재시도 횟수 초과: {url}")
            now_time = datetime.now(self.korea_tz)
            local_time = datetime.now(self.local_tz)

            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'fr'),
                'ships_from': 'FR',
                'channel_name': 'fnac',
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
                'sold_by': 'Fnac',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }

    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결이 없어 DB 저장을 건너뜁니다")
            return False

        try:
            # fnac_price_crawl_tbl_fr 테이블에 저장
            df.to_sql('fnac_price_crawl_tbl_fr', self.db_engine, if_exists='append', index=False)
            logger.info(f"DB 저장 완료: {len(df)}개 레코드")

            # 크롤링 로그 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'fr',
                    'url': row['producturl'],
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })

            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"크롤링 로그 저장 완료: {len(log_records)}개")

            return True

        except Exception as e:
            logger.error(f"DB 저장 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
                logger.info(f"국가 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)

            # 날짜별 디렉토리 경로
            date_dir = f"{country_dir}/{date_folder}"

            # 날짜 디렉토리가 없으면 생성
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)

            # 업로드 경로
            remote_filename = os.path.basename(local_file_path)
            remote_path = f"{date_dir}/{remote_filename}"

            # 파일 업로드
            sftp.put(local_file_path, remote_path)
            logger.info(f"파일서버 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()

            return True
        except Exception as e:
            logger.error(f"파일서버 업로드 실패: {e}")
            return False

    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        date_str = now.strftime('%Y%m%d')
        time_str = now.strftime('%H%M%S')
        base_filename = f"{date_str}_{time_str}_fr_fnac"

        results = {'db_saved': False, 'server_uploaded': False}

        if save_db:
            results['db_saved'] = self.save_to_db(df)

        if upload_server:
            try:
                # 1. CSV 파일 생성
                csv_filename = f'{base_filename}.csv'
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

                # 4. MD5 파일 생성
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

    def scrape_urls(self, urls_data, max_items=None):
        """여러 URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]

        logger.info(f"총 {len(urls_data)}개 제품 처리 시작")

        results = []
        failed_urls = []
        blocked_page_failures = []
        out_of_stock_urls = []

        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

                url = row.get('url')
                result = self.extract_product_info(url, row)

                # 품절 플래그 처리 (DB 저장 전에 제거)
                is_oos = result.pop('_out_of_stock', False)
                if is_oos:
                    out_of_stock_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'title': result.get('title', '')
                    })

                if result['retailprice'] is None and result['title'] is None:
                    # 차단 페이지로 인한 실패
                    blocked_page_failures.append({
                        'url': url,
                        'row_data': row,
                        'item': row.get('item', '')
                    })
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'reason': '차단 페이지'
                    })
                elif result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'reason': '가격 추출 실패'
                    })

                results.append(result)

                # 10개마다 DB에 중간 저장
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('fnac_price_crawl_tbl_fr', self.db_engine,
                                            if_exists='append', index=False)
                            logger.info(f"중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")

                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(2, 5)
                    logger.info(f"{wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    if (idx + 1) % 10 == 0:
                        logger.info("10개 처리 완료, 30초 휴식...")
                        time.sleep(30)

            # 마지막으로 저장되지 않은 나머지 데이터 저장
            remainder = len(results) % 10
            if remainder > 0 and self.db_engine:
                try:
                    remainder_df = pd.DataFrame(results[-remainder:])
                    remainder_df.to_sql('fnac_price_crawl_tbl_fr', self.db_engine, if_exists='append', index=False)
                    logger.info(f"마지막 저장: {remainder}개 레코드")
                except Exception as e:
                    logger.error(f"마지막 저장 실패: {e}")

            # 차단 페이지로 인한 실패 목록 재시도 (10회씩)
            if blocked_page_failures:
                logger.info("=" * 60)
                logger.info(f"차단 페이지 실패 {len(blocked_page_failures)}개 재시도 시작")
                logger.info("=" * 60)

                final_blocked_failures = []

                for fail_idx, fail_item in enumerate(blocked_page_failures):
                    url = fail_item['url']
                    row_data = fail_item['row_data']
                    logger.info(f"재시도 진행: {fail_idx + 1}/{len(blocked_page_failures)} - {fail_item['item']}")

                    retry_success = False
                    for retry in range(10):
                        logger.info(f"차단 페이지 재시도 {retry + 1}/10: {url}")
                        result = self.extract_product_info(url, row_data, retry_count=0, max_retries=1)
                        result.pop('_out_of_stock', False)

                        if result['title'] is not None or result['retailprice'] is not None:
                            logger.info(f"재시도 성공! title={result['title']}, price={result['retailprice']}")
                            for i, r in enumerate(results):
                                if r['producturl'] == url:
                                    results[i] = result
                                    break
                            retry_success = True
                            break
                        else:
                            logger.warning(f"재시도 {retry + 1}/10 실패")
                            if retry < 9:
                                time.sleep(10)

                    if not retry_success:
                        logger.error(f"최종 실패 (10회 재시도 후에도 실패): {url}")
                        final_blocked_failures.append({
                            'url': url,
                            'item': fail_item['item'],
                            'reason': '차단 페이지로 인한 최종 실패 (10회 재시도 후)'
                        })

                if final_blocked_failures:
                    logger.warning(f"차단 페이지 최종 실패 {len(final_blocked_failures)}개:")
                    for fail in final_blocked_failures:
                        logger.warning(f"  - {fail['item']}: {fail['reason']}")
                else:
                    logger.info("모든 차단 페이지 실패 항목 재시도 성공!")

                failed_urls.extend(final_blocked_failures)
                blocked_page_failures = final_blocked_failures

        except Exception as e:
            logger.error(f"스크래핑 중 오류: {e}")

        finally:
            if failed_urls:
                logger.warning(f"\n가격 추출 실패한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail.get('reason', '')}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")

            if self.page:
                try:
                    self.page.quit()
                    logger.info("브라우저 종료")
                except Exception:
                    pass

        return pd.DataFrame(results), blocked_page_failures, out_of_stock_urls

    def analyze_results(self, df):
        """결과 분석"""
        logger.info("\n=== 결과 분석 ===")

        if df is None or df.empty:
            logger.warning("분석할 데이터가 없습니다.")
            return

        if 'retailprice' not in df.columns:
            logger.warning("'retailprice' 컬럼이 없습니다.")
            return

        total = len(df)
        with_price = df['retailprice'].notna().sum()
        without_price = df['retailprice'].isna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0

        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")

        if with_price > 0:
            price_df = df[df['retailprice'].notna()].copy()
            price_df['numeric_price'] = price_df['retailprice']

            logger.info(f"\n가격 통계:")
            logger.info(f"평균가: {price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: {price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: {price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: {price_df['numeric_price'].median():.2f}")


def main():
    """메인 실행 함수"""
    from log_utils import setup_log, save_log
    setup_log('fr_fnac')

    print("\nFnac 가격 추출 시스템 v3 - DrissionPage 기반")
    print("="*60)

    scraper = FnacScraperV3()

    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('fr_fnac', 0, None, error_message="DB 연결 실패")
        return

    # 테스트 모드
    test_mode = os.getenv("TEST_MODE", "false").lower()

    if test_mode in ["true", "1", "yes"]:
        logger.info("테스트 모드 실행")

        if not scraper.setup_browser():
            return

        if not scraper.initialize_session():
            return

        # DB에서 첫 번째 제품 가져오기
        test_products = scraper.get_crawl_targets(limit=1)
        if test_products:
            test_row = test_products[0]
            test_url = test_row.get('url')
            test_result = scraper.extract_product_info(test_url, test_row)

            logger.info("추출된 정보:")
            logger.info(f"  - 상품명: {test_result['title']}")
            logger.info(f"  - 가격: {test_result['retailprice']}")
            logger.info(f"  - 이미지: {'추출됨' if test_result['imageurl'] else '없음'}")

        if scraper.page:
            try:
                scraper.page.quit()
            except Exception:
                pass
        return

    # 실제 크롤링
    logger.info("\n실제 크롤링 시작")

    if not scraper.setup_browser():
        return

    if not scraper.initialize_session():
        return

    urls_data = scraper.get_crawl_targets()

    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        monitor_and_alert('fr_fnac', 0, None, error_message="크롤링 대상 URL이 없습니다")
        return

    logger.info(f"크롤링 대상: {len(urls_data)}개")

    start_time = datetime.now(scraper.korea_tz)
    results_df, blocked_failures, out_of_stock_urls = scraper.scrape_urls(urls_data)

    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        monitor_and_alert('fr_fnac', len(urls_data), None, error_message="크롤링 결과가 없습니다")
        return

    end_time = datetime.now(scraper.korea_tz)

    logger.info("\n최종 결과 저장")

    success_count = results_df['retailprice'].notna().sum()
    failed_count = results_df['retailprice'].isna().sum()
    success_rate = (success_count / len(results_df) * 100) if len(results_df) > 0 else 0

    logger.info(f"\n=== 최종 결과 ===")
    logger.info(f"전체: {len(results_df)}개")
    logger.info(f"성공: {success_count}개")
    logger.info(f"실패: {failed_count}개")
    logger.info(f"성공률: {success_rate:.1f}%")
    logger.info(f"소요 시간: {round((end_time - start_time).total_seconds() / 60, 2)} 분")

    # 중간 저장에서 이미 DB 저장했으므로, 여기서는 파일 업로드만 수행
    save_results = scraper.save_results(
        results_df,
        save_db=False,
        upload_server=False  # auto_recovery에서 처리
    )

    scraper.analyze_results(results_df)
    logger.info("Fnac 크롤링 완료!")

    # 자동 복구 + 파일 업로드 + 메일 알림
    from auto_recovery import auto_recovery_run
    auto_recovery_run(
        target_key='fnac',
        results_df=results_df,
        target_count=len(urls_data),
        error_logs=None,
        out_of_stock_urls=out_of_stock_urls
    )

    save_log('fr_fnac')


if __name__ == "__main__":
    print("필요한 패키지:")
    print("pip install DrissionPage pandas pymysql sqlalchemy paramiko")
    print()

    main()
