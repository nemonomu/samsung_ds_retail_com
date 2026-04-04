"""
BestBuy 가격 추출 시스템 - DB 기반 버전
DB에서 URL 읽어와서 크롤링 후 결과 저장
파일명 형식: {수집일자}{수집시간}_{국가코드}_{쇼핑몰}.csv
"""
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
import os
import sys
import zipfile
import hashlib

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert

class BestBuyScraper:
    def __init__(self):
        self.page = None
        self.db_engine = None
        self.sftp_client = None
        self.country_code = 'usa'
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('America/New_York')  # BestBuy 현지 시간 (워싱턴)

        # 에러 로그 수집용 리스트
        self.error_logs = []

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
            self.db_engine = create_engine(
                connection_string,
                pool_pre_ping=True,    # 쿼리 전 연결 상태 확인 (끊겼으면 자동 재연결)
                pool_recycle=3600      # 1시간마다 커넥션 재활용
            )
            logger.info("✅ DB 연결 설정 완료")
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            self.db_engine = None
    
    def load_xpaths_from_db(self):
        """DB에서 BestBuy용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'bestbuy' 
              AND country_code = 'usa'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """
            
            df = pd.read_sql(query, self.db_engine)
            
            # element_type별로 그룹화 (모든 타입 포함)
            self.XPATHS = {}
            for element_type in df['element_type'].unique():
                type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                self.XPATHS[element_type] = type_selectors

            logger.info(f"✅ DB에서 선택자 로드 완료: {len(df)}개")
            
            # 기본값 설정 (DB에 없는 경우 - price 제외)
            if not self.XPATHS.get('title'):
                logger.warning("⚠️ DB에 선택자가 없어 기본값 사용")
                self.XPATHS.update({
                    'title': [
                        '/html/body/div[6]/div[5]/div[1]/div/div[2]/h1',
                        '/html/body/div[5]/div[4]/div[1]/div/h1',
                        '/html/body/div[5]/div[4]/div[2]/div/h1',
                        '//h1[@class="sku-title"]',
                        '//div[@class="sku-title"]//h1'
                    ],
                    'imageurl': [
                        '/html/body/div[6]/div[5]/div[2]/div/div[2]/div[2]/div/div[2]/div/button/img',
                        '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button/img',
                        '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
                        '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button/img',
                        '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
                        '//img[@class="primary-image"]',
                        '//div[@class="media-gallery"]//img'
                    ],
                    'imageurl_fallback': [
                        '/html/body/div[5]/div[3]/div[1]/div/div[1]/img'
                    ],
                    'stock_flag': [
                        'Out of Stock', 'Sold Out', 'Currently unavailable',
                        'Coming Soon', 'Temporarily out of stock'
                    ],
                    'country_select': [
                        '/html/body/div[2]/div/div/div/div[1]/div[2]/a[2]'
                    ],
                    'no_longer_available_flag': [
                        "//div[contains(@class, 'text-danger') and contains(., 'no longer available')]",
                        "/html/body/div[6]/div[4]/div[2]/div/div/div[2]/div[4]"
                    ],
                    'no_longer_available_title': [
                        '/html/body/div[6]/div[3]/div[1]/div/div[2]/h1',
                        '/html/body/div[6]/div[4]/div[2]/div/div/div[1]/h1'
                    ],
                    'no_longer_available_imageurl': [
                        '/html/body/div[6]/div[3]/div[1]/div/div[1]/img',
                        '/html/body/div[6]/div[4]/div[2]/div/div/div[1]/img'
                    ]
                })
                
                # fallback 이미지 선택자 추가
                if 'imageurl_fallback' not in self.XPATHS:
                    self.XPATHS['imageurl_fallback'] = ['/html/body/div[5]/div[3]/div[1]/div/div[1]/img']
                
        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.XPATHS = {
                'price': [
                    '/html/body/div[6]/div[5]/div[1]/div/div[4]/div/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                    '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]',
                    '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
                    '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                    '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span'
                ],
                'title': [],
                'imageurl': [],
                'imageurl_fallback': ['/html/body/div[5]/div[3]/div[1]/div/div[1]/img'],
                'stock_flag': [],
                'country_select': []
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
                      AND country_code = 'usa'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'usa' 
                  AND t.mall_name = 'bestbuy'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'usa' 
                  AND mall_name = 'bestbuy'
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
        """DrissionPage 브라우저 설정"""
        logger.info("🔧 DrissionPage 브라우저 설정 중...")

        try:
            co = ChromiumOptions()
            co.auto_port()  # 매번 새 포트로 독립 브라우저 실행 (기존 Chrome 세션에 연결 방지)
            co.no_imgs(True)
            self.page = ChromiumPage(co)

            logger.info("✅ 브라우저 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 브라우저 설정 실패: {e}")
            return False

    def _warmup_with_different_page(self):
        """차단 후 세션 워밍업 - SSD/메모리가 아닌 다른 카테고리 상품 페이지 접속"""
        warmup_urls = [
            'https://www.bestbuy.com/site/apple-macbook-air-13-inch-laptop-m4-chip-16gb-memory-256gb/6604203.p',
            'https://www.bestbuy.com/site/sony-wh-1000xm5-wireless-noise-canceling-over-the-ear-headphones/6505727.p',
            'https://www.bestbuy.com/site/dyson-v15-detect-extra-cordless-vacuum/6539767.p',
            'https://www.bestbuy.com/site/nintendo-switch-oled-model-w-white-joy-con/6470923.p',
            'https://www.bestbuy.com/site/apple-ipad-10th-generation-with-wi-fi-64gb/4901809.p',
            'https://www.bestbuy.com/site/bose-quietcomfort-ultra-headphones/6554461.p',
        ]
        try:
            url = random.choice(warmup_urls)
            logger.info(f"세션 워밍업: {url[:60]}...")
            self.page.get(url)
            time.sleep(random.uniform(5, 10))
            try:
                self.page.scroll.down(300)
                time.sleep(random.uniform(2, 4))
                self.page.scroll.down(300)
                time.sleep(random.uniform(2, 3))
            except Exception:
                pass
            logger.info("세션 워밍업 완료")
        except Exception as e:
            logger.warning(f"세션 워밍업 실패: {e}")

    def close_driver(self):
        """브라우저 안전 종료"""
        try:
            if self.page:
                self.page.quit()
                self.page = None
                logger.info("브라우저 종료 완료")
        except Exception as e:
            logger.warning(f"브라우저 종료 오류: {e}")
            self.page = None

    def restart_driver(self):
        """브라우저 종료 후 재시작"""
        logger.info("🔧 브라우저 재시작 중...")
        self.close_driver()
        time.sleep(3)
        return self.setup_driver()

    def wait_for_price_elements(self, max_wait=10):
        """가격 요소들이 실제로 로드될 때까지 스마트 대기"""

        # 1단계: 기본 페이지 구조 대기
        try:
            self.page.ele('tag:body', timeout=10)
        except:
            pass

        # 2단계: 가격 관련 컨테이너들 대기 (여러 후보 중 하나라도 나타나면 OK)
        price_containers = [
            "//div[contains(@class, 'pricing')]",
            "//div[contains(@class, 'price')]",
            "//span[contains(@class, 'current-price')]",
            "//div[contains(@data-testid, 'pricing')]"
        ]

        for container in price_containers:
            try:
                self.page.ele(f'xpath:{container}', timeout=5)
                break
            except:
                continue

        # 3단계: 실제 가격 값이 들어올 때까지 대기
        start_time = time.time()
        while time.time() - start_time < max_wait:
            for xpath in self.XPATHS.get('price', []):
                try:
                    element = self.page.ele(f'xpath:{xpath}', timeout=2)
                    text = element.text.strip()
                    if text and ('$' in text or text.replace(',', '').replace('.', '').isdigit()):
                        logger.info(f"✅ 가격 요소 로딩 완료: {text}")
                        return True
                except:
                    continue
            time.sleep(1)  # 1초마다 재확인

        logger.warning("⚠️ 가격 요소 로딩 시간 초과")
        return False

    def wait_for_network_idle(self, idle_time=2):
        """네트워크 요청이 완료될 때까지 대기"""
        try:
            # Performance API 사용해서 네트워크 요청 완료 확인
            script = """
            return window.performance.getEntriesByType('navigation')[0].loadEventEnd > 0
            """
            
            start_time = time.time()
            while time.time() - start_time < 10:  # 최대 10초
                if self.page.run_js(script):
                    time.sleep(idle_time)  # 추가 안정화 시간
                    return True
                time.sleep(0.5)
        except:
            pass
        return False

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=2):
        """제품 정보 추출 (재시도 로직 포함)"""
        try:
            logger.info(f"🔍 페이지 접속: {url} (시도: {retry_count + 1}/{max_retries + 1})")
            
            self.page.get(url)

            # ERR_HTTP2_PROTOCOL_ERROR 감지
            try:
                page_source = self.page.html or ''
                page_title = self.page.title or ''
                current_url = self.page.url or ''

                # 1. 직접 텍스트 감지
                blocked_texts = ['ERR_HTTP2_PROTOCOL_ERROR', "This site can't be reached",
                                 'ERR_CONNECTION_RESET', 'ERR_CONNECTION_REFUSED',
                                 'ERR_NAME_NOT_RESOLVED', 'chrome-error://']
                for blocked_text in blocked_texts:
                    if blocked_text in page_source or blocked_text in page_title or blocked_text in current_url:
                        logger.warning(f"🚫 차단 감지 ({blocked_text}) - Best Buy 차단")
                        return {'_blocked': True}

                # 2. 페이지 소스가 비정상적으로 짧으면 에러 페이지로 판단
                if len(page_source) < 500 and 'bestbuy' not in page_source.lower():
                    logger.warning(f"🚫 비정상 페이지 감지 (page_source 길이: {len(page_source)}) - Best Buy 차단 의심")
                    return {'_blocked': True}

                # 3. chrome-error 페이지 감지
                if 'chrome-error' in current_url:
                    logger.warning(f"🚫 Chrome 에러 페이지 감지 - Best Buy 차단")
                    return {'_blocked': True}
            except Exception as e:
                logger.warning(f"차단 감지 중 오류: {e}")

            # 스마트 대기 전략 적용
            logger.info("⏳ 페이지 로딩 대기 중...")

            # 1. 네트워크 완료 대기
            self.wait_for_network_idle()

            # 2. 가격 요소들 로딩 대기
            if not self.wait_for_price_elements():
                logger.warning("가격 요소 로딩 실패, 그래도 추출 시도")

            # 3. 추가 안정화 시간
            time.sleep(random.uniform(1, 2))

            # 차단 감지
            title = self.page.title or ''
            blocked_patterns = ["Access Denied", "Blocked", "Robot", "Captcha", "Sorry", "Error"]
            for pattern in blocked_patterns:
                if pattern.lower() in title.lower():
                    logger.warning(f"⚠️ 차단 감지: {pattern}")
                    self.error_logs.append(f"[차단 감지] URL: {url} | 패턴: {pattern}")
                    raise Exception(f"Blocked: {pattern}")
            
            # 현재 시간
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
                'country_code': row_data.get('country', 'usa'),
                'ships_from': 'usa',
                'channel_name': 'bestbuy',
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
                'sold_by': 'BestBuy',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }
            
            # 에러 페이지 감지 (retry 하지 않고 다음 제품으로)
            try:
                error_element = self.page.ele("xpath://h1[contains(@class, 'VPT-title')]", timeout=2)
                if error_element and "something went wrong" in error_element.text.lower():
                    logger.warning(f"⚠️ 에러 페이지 감지: '{error_element.text}' - retry 없이 다음 제품으로")
                    self.error_logs.append(f"[에러 페이지] URL: {url} | 메시지: {error_element.text}")
                    return result  # retry 없이 바로 반환
            except:
                pass  # 에러 요소 없으면 정상 페이지

            # "no longer available in new condition" 상태 감지
            is_no_longer_available = False
            for flag_xpath in self.XPATHS.get('no_longer_available_flag', []):
                try:
                    no_longer_element = self.page.ele(f'xpath:{flag_xpath}', timeout=2)
                    if no_longer_element:
                        logger.info("ℹ️ 'No longer available in new condition' 상태 감지")
                        is_no_longer_available = True

                        # 해당 상태용 title 추출
                        logger.info(f"🔍 no_longer_available title xpath 목록: {self.XPATHS.get('no_longer_available_title', [])}")
                        for title_xpath in self.XPATHS.get('no_longer_available_title', []):
                            try:
                                logger.info(f"  시도 중: {title_xpath}")
                                title_element = self.page.ele(f'xpath:{title_xpath}', timeout=2)
                                result['title'] = title_element.text.strip()
                                logger.info(f"제목 (no longer available): {result['title'][:50]}...")
                                break
                            except Exception as e:
                                logger.warning(f"  실패: {e}")
                                continue

                        # 해당 상태용 imageurl 추출
                        logger.info(f"🔍 no_longer_available imageurl xpath 목록: {self.XPATHS.get('no_longer_available_imageurl', [])}")
                        for img_xpath in self.XPATHS.get('no_longer_available_imageurl', []):
                            try:
                                logger.info(f"  시도 중: {img_xpath}")
                                image_element = self.page.ele(f'xpath:{img_xpath}', timeout=2)
                                result['imageurl'] = image_element.attr('src')
                                logger.info(f"이미지 URL (no longer available): {result['imageurl'][:50]}...")
                                break
                            except Exception as e:
                                logger.warning(f"  실패: {e}")
                                continue

                        # retailprice는 None으로 유지, no_longer_available 표시 후 반환
                        result['no_longer_available'] = True
                        return result
                except:
                    continue

            # 가격 추출
            price_found = False
            
            # 1단계: 기존 선택자로 시도
            logger.info("💰 기존 가격 선택자로 시도 중...")
            for xpath in self.XPATHS.get('price', []):
                try:
                    price_element = self.page.ele(f'xpath:{xpath}', timeout=2)
                    price_text = price_element.text.strip()

                    logger.info(f"🔍 선택자: {xpath}")
                    logger.info(f"📝 추출된 텍스트: '{price_text}'")
                    
                    if price_text:
                        # BestBuy는 주로 $ 사용
                        price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
                        if price_match:
                            price_number = price_match.group(1).replace(',', '')
                            result['retailprice'] = float(price_number)
                            logger.info(f"✅ 가격 추출 성공: ${result['retailprice']} (선택자: {xpath})")
                            price_found = True
                            break
                        else:
                            logger.info(f"❌ 가격 패턴 매칭 실패: '{price_text}'")
                    else:
                        logger.info("❌ 빈 텍스트")
                except Exception as xe:
                    logger.info(f"❌ 선택자 실행 실패: {xe}")
                    continue
            
            # 2단계: CSS 선택자로 재시도
            if not price_found:
                logger.info("💰 CSS 선택자로 재시도 중...")
                try:
                    css_xpath = "//span[@class='sr-only' and contains(text(), 'current price')]"
                    price_element = self.page.ele(f'xpath:{css_xpath}', timeout=2)
                    price_text = price_element.text
                    if '$' in price_text:
                        price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
                        if price_match:
                            result['retailprice'] = float(price_match.group(1).replace(',', ''))
                            logger.info(f"✅ 가격 추출 성공 (CSS): ${result['retailprice']}")
                            price_found = True
                except Exception as ex:
                    logger.info("CSS 선택자도 실패")
            
            if not price_found:
                logger.warning("모든 가격 추출 방법 실패")
                self.error_logs.append(f"[가격 추출 실패] URL: {url}")
            
            # 제목 추출 (명시적 대기 후 시도)
            is_soldout_fallback = False  # 품절 fallback 여부 플래그
            try:
                # title 요소 로딩 대기 (최대 10초)
                title_loaded = False
                for xpath in self.XPATHS.get('title', []):
                    try:
                        self.page.ele(f'xpath:{xpath}', timeout=10)
                        title_loaded = True
                        logger.info(f"✅ title 요소 로딩 완료: {xpath}")
                        break
                    except:
                        continue

                if not title_loaded:
                    logger.warning("⚠️ title 요소 로딩 대기 시간 초과, 그래도 추출 시도")

                for xpath in self.XPATHS.get('title', []):
                    try:
                        title_element = self.page.ele(f'xpath:{xpath}', timeout=2)
                        result['title'] = title_element.text.strip()
                        if result['title']:  # 빈 문자열이 아닌 경우만 성공
                            logger.info(f"제목: {result['title'][:50]}...")
                            break
                    except:
                        continue

                # 제목 추출 실패 시 품절 상품용 fallback 시도
                if not result['title']:
                    try:
                        title_element = self.page.ele('xpath:/html/body/div[5]/div[3]/div[1]/div/div[2]/h1', timeout=2)
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목 (품절 fallback): {result['title'][:50]}...")
                        is_soldout_fallback = True  # 품절 fallback으로 제목 추출됨
                    except:
                        pass
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")

            # 이미지 URL 추출
            try:
                for xpath in self.XPATHS.get('imageurl', []):
                    try:
                        image_element = self.page.ele(f'xpath:{xpath}', timeout=2)
                        result['imageurl'] = image_element.attr('src')
                        logger.info(f"이미지 URL: {result['imageurl'][:50]}...")
                        break
                    except:
                        continue

                # 이미지 추출 실패 시 품절 상품용 fallback 시도
                if not result['imageurl']:
                    try:
                        image_element = self.page.ele('xpath:/html/body/div[5]/div[3]/div[1]/div/div[1]/img', timeout=2)
                        result['imageurl'] = image_element.attr('src')
                        logger.info(f"이미지 URL (품절 fallback): {result['imageurl'][:50]}...")
                    except:
                        pass
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")

            # 가격 추출 실패 시 재시도 (exception 없이 price가 None인 경우)
            # 단, 품절 fallback으로 제목을 추출한 경우 재시도하지 않음 (품절 상품은 가격 없는 것이 정상)
            if result['retailprice'] is None and retry_count < max_retries and not is_soldout_fallback:
                wait_time = 1
                logger.warning(f"⚠️ 가격 추출 실패, {wait_time}초 후 재시도... (재시도 {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)

                # 드라이버 새로고침
                try:
                    self.page.refresh()
                except:
                    logger.info("🔧 드라이버 재시작 중...")
                    try:
                        self.page.quit()
                    except:
                        pass
                    self.setup_driver()

                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)

            # 품절 fallback으로 제목 추출된 경우 로그 출력
            if is_soldout_fallback and result['retailprice'] is None:
                logger.info("ℹ️ 품절 상품으로 판단, 가격 재시도 생략")

            return result

        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            self.error_logs.append(f"[페이지 처리 오류] URL: {url} | 오류: {str(e)}")

            # 재시도 로직
            if retry_count < max_retries:
                wait_time = 1
                logger.info(f"🔄 {wait_time}초 후 재시도합니다... (재시도 {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)

                # 드라이버 새로고침 또는 재시작
                try:
                    self.page.refresh()
                except:
                    # 드라이버가 죽었으면 재시작
                    logger.info("🔧 드라이버 재시작 중...")
                    try:
                        self.page.quit()
                    except:
                        pass
                    self.setup_driver()

                # 재귀 호출로 재시도
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)

            # 최대 재시도 횟수 초과 시 기본값 반환
            logger.error(f"❌ 최대 재시도 횟수 초과: {url}")
            # V2: 타임존 분리

            now_time = datetime.now(self.korea_tz)

            local_time = datetime.now(self.local_tz)

            # ISO 8601 형식
            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'usa'),
                'ships_from': 'usa',
                'channel_name': 'bestbuy',
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
                'sold_by': 'BestBuy',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }

    def extract_with_retry(self, url, row_data):
        """제품 정보 추출 (extract_product_info 내부에 재시도 로직 포함)"""
        return self.extract_product_info(url, row_data)
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # bestbuy_price_crawl_tbl_usa_v2 테이블에 저장
            df.to_sql('bestbuy_price_crawl_tbl_usa_v2', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그를 pandas DataFrame으로 만들어서 한번에 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'usa',
                    'url': row['producturl'],
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })
            
            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"✅ 크롤링 로그 저장 완료: {len(log_records)}개")
            
            # 저장된 데이터 확인
            with self.db_engine.connect() as conn:
                count_query = "SELECT COUNT(*) FROM bestbuy_price_crawl_tbl_usa_v2 WHERE DATE(crawl_datetime) = CURDATE()"
                result = conn.execute(count_query)
                today_count = result.scalar()
                logger.info(f"📊 오늘 저장된 총 레코드: {today_count}개")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
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
    def save_results(self, df, save_db=True, upload_server=True, custom_filename=None, custom_date_folder=None):
        """결과 저장 (파일명은 미국 현지 시간 기준)"""
        local_time = datetime.now(self.local_tz)  # 미국 현지 시간 (crawl_datetime 기준)
        date_str = custom_date_folder or local_time.strftime('%Y%m%d')
        base_filename = custom_filename or f"{date_str}_{local_time.strftime('%H%M%S')}_usa_bestbuy"

        results = {'db_saved': False, 'server_uploaded': False}

        if save_db:
            results['db_saved'] = self.save_to_db(df)

        if upload_server:
            try:
                # 1. CSV 파일 생성 (컬럼 순서 고정)
                csv_filename = f'{base_filename}.csv'
                column_order = [
                    'retailerid', 'country_code', 'ships_from', 'channel_name', 'channel',
                    'retailersku', 'brand', 'brand_eng', 'form_factor',
                    'segment_lv1', 'segment_lv2', 'segment_lv3', 'capacity', 'item',
                    'retailprice', 'sold_by', 'imageurl', 'producturl',
                    'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime',
                    'title', 'vat'
                ]
                df_csv = df.copy()
                df_csv.columns = df_csv.columns.str.lower()
                existing_cols = [c for c in column_order if c in df_csv.columns]
                df_csv = df_csv[existing_cols]
                df_csv.columns = df_csv.columns.str.upper()
                df_csv.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')

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
    def test_connection(self):
        """연결 테스트 및 세션 초기화"""
        logger.info("=== BestBuy 세션 초기화 및 테스트 ===")
        
        if not self.setup_driver():
            return False
        
        try:
            # 1단계: Google 연결 테스트
            logger.info("1단계: Google 연결 테스트...")
            self.page.get("https://www.google.com")
            time.sleep(2)
            google_title = self.page.title or ''

            if "Google" in google_title:
                logger.info("✅ Google 접속 성공")
            else:
                logger.warning("⚠️ Google 접속 이상")
            
            # 2단계: 테스트 상품 페이지 접속
            logger.info("3단계: 테스트 상품 페이지 접속...")
            test_url = "https://www.bestbuy.com/site/samsung-9100-pro-1tb-internal-ssd-pcie-gen-5x4-nvme-speeds-up-to-14700-mb-s/6618929.p?skuId=6618929"
            
            test_row = {
                'url': test_url,
                'brand': 'Samsung',
                'item': 'Test Item',
                'country': 'usa'
            }
            
            test_result = self.extract_product_info(test_url, test_row)

            # 차단된 경우 테스트 건너뛰고 진행 (scrape_urls에서 auto-retry 처리)
            if isinstance(test_result, dict) and test_result.get('_blocked'):
                logger.warning("⚠️ 테스트 페이지 차단됨 - 테스트 건너뛰고 크롤링 진행 (auto-retry 적용)")
            else:
                logger.info("추출된 정보:")
                logger.info(f"  - 상품명: {test_result['title'][:50] if test_result['title'] else 'None'}...")
                logger.info(f"  - 가격: {test_result['retailprice']}")
                logger.info(f"  - 이미지: {'추출됨' if test_result['imageurl'] else '없음'}")

            # 4단계: 파일서버 연결 테스트
            logger.info("4단계: 파일서버 연결 테스트...")
            try:
                transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
                transport.connect(
                    username=FILE_SERVER_CONFIG['username'],
                    password=FILE_SERVER_CONFIG['password']
                )
                transport.close()
                logger.info("✅ 파일서버 연결 성공")
            except Exception as e:
                logger.warning(f"⚠️ 파일서버 연결 실패: {e}")

            return True  # 테스트 차단 여부와 관계없이 진행 (auto-retry가 처리)
                
        except Exception as e:
            logger.error(f"❌ 테스트 실패: {e}")
            return False
    
    def scrape_urls(self, urls_data, max_items=None, save_interim=True, start_from=0):
        """여러 URL 스크래핑

        Args:
            urls_data: 크롤링 대상 URL 목록
            max_items: 최대 처리 개수 (None이면 전체)
            save_interim: 중간 저장 여부 (재시도 시에는 False로 설정)
            start_from: 시작 인덱스 (0부터 시작, resume용)
        """
        if max_items:
            urls_data = urls_data[:max_items]

        if start_from > 0:
            logger.info(f"📊 총 {len(urls_data)}개 중 {start_from + 1}번째부터 재개")
        else:
            logger.info(f"📊 총 {len(urls_data)}개 제품 처리 시작")

        results = []
        failed_urls = []

        # 자동 재시도 설정
        MAX_RETRIES = 5          # 최대 재시도 횟수
        INITIAL_WAIT = 1200      # 대기 시간 (20분)
        retry_count = 0          # 현재 재시도 횟수
        unsaved_results = []     # 중간 저장 안 된 결과

        i = start_from
        while i < len(urls_data):
            row = urls_data[i]
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {i + 1}/{len(urls_data)} ({(i + 1)/len(urls_data)*100:.1f}%)")

                # URL 추출
                url = row.get('url')

                # 제품 정보 추출 (재시도 로직 적용)
                result = self.extract_with_retry(url, row)

                # ERR_HTTP2_PROTOCOL_ERROR 차단 감지
                if isinstance(result, dict) and result.get('_blocked'):
                    retry_count += 1
                    if retry_count > MAX_RETRIES:
                        logger.error(f"🛑 최대 재시도 횟수({MAX_RETRIES}) 초과. 중단합니다.")
                        logger.info(f"📌 {i + 1}번째 항목부터 미처리")
                        break

                    if retry_count == 1:
                        logger.info(f"\n{'='*50}")
                        logger.info(f"🔄 [RETRY {retry_count}/{MAX_RETRIES}] Best Buy 차단 감지. 워밍업 후 재시도...")
                        logger.info(f"{'='*50}")
                    else:
                        wait_time = min(INITIAL_WAIT * (retry_count - 1), 1200)
                        logger.info(f"\n{'='*50}")
                        logger.info(f"🔄 [RETRY {retry_count}/{MAX_RETRIES}] Best Buy 차단 감지. {wait_time // 60}분 대기...")
                        logger.info(f"{'='*50}")
                        time.sleep(wait_time)

                    self._warmup_with_different_page()

                    # 같은 URL 다시 시도 (i 증가 안 함)
                    continue

                # 실패 여부 확인 - 가격+제목 모두 없으면 차단 의심
                if result.get('retailprice') is None and result.get('title') is None:
                    retry_count += 1
                    if retry_count > MAX_RETRIES:
                        logger.error(f"🛑 최대 재시도 횟수({MAX_RETRIES}) 초과. 중단합니다.")
                        logger.info(f"📌 {i + 1}번째 항목부터 미처리")
                        break

                    if retry_count == 1:
                        logger.info(f"\n{'='*50}")
                        logger.info(f"🔄 [RETRY {retry_count}/{MAX_RETRIES}] 가격+제목 추출 실패. 차단 의심. 워밍업 후 재시도...")
                        logger.info(f"{'='*50}")
                    else:
                        wait_time = min(INITIAL_WAIT * (retry_count - 1), 1200)
                        logger.info(f"\n{'='*50}")
                        logger.info(f"🔄 [RETRY {retry_count}/{MAX_RETRIES}] 가격+제목 추출 실패. 차단 의심. {wait_time // 60}분 대기...")
                        logger.info(f"{'='*50}")
                        time.sleep(wait_time)

                    self._warmup_with_different_page()

                    # 같은 URL 다시 시도
                    continue

                # 성공 (가격 또는 제목 있음)
                retry_count = 0

                if result.get('retailprice') is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', '')
                    })

                results.append(result)
                unsaved_results.append(result)

                # 10개마다 DB에 중간 저장 (save_interim=True일 때만)
                if save_interim and len(unsaved_results) >= 10:
                    interim_df = pd.DataFrame(unsaved_results[:10])
                    # no_longer_available 컬럼 제거 (DB 테이블에 없음)
                    if 'no_longer_available' in interim_df.columns:
                        interim_df = interim_df.drop(columns=['no_longer_available'])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('bestbuy_price_crawl_tbl_usa_v2', self.db_engine,
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                    unsaved_results = unsaved_results[10:]

                # 다음 요청 전 대기
                if i < len(urls_data) - 1:
                    delay = random.uniform(5, 10)
                    logger.info(f"⏳ {delay:.1f}초 대기 중...")
                    time.sleep(delay)

                i += 1

            except Exception as e:
                logger.error(f"❌ 스크래핑 중 오류 (URL: {row.get('url', 'unknown')}): {e}")
                self.error_logs.append(f"[스크래핑 오류] URL: {row.get('url', 'unknown')} | 오류: {str(e)}")
                i += 1
                continue

        # 남은 항목 저장
        if save_interim and unsaved_results:
            remaining_df = pd.DataFrame(unsaved_results)
            # no_longer_available 컬럼 제거 (DB 테이블에 없음)
            if 'no_longer_available' in remaining_df.columns:
                remaining_df = remaining_df.drop(columns=['no_longer_available'])
            if self.db_engine:
                try:
                    remaining_df.to_sql('bestbuy_price_crawl_tbl_usa_v2', self.db_engine,
                                       if_exists='append', index=False)
                    logger.info(f"💾 남은 {len(unsaved_results)}개 레코드 DB 저장")
                except Exception as e:
                    logger.error(f"남은 항목 저장 실패: {e}")

        # 정리
        if failed_urls:
            logger.warning(f"\n⚠️ 가격 추출 실패한 URL {len(failed_urls)}개:")
            for fail in failed_urls[:5]:
                logger.warning(f"  - {fail['brand']} {fail['item']}: {fail['url']}")
            if len(failed_urls) > 5:
                logger.warning(f"  ... 외 {len(failed_urls) - 5}개")

        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("\n📊 === 결과 분석 ===")
        
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
            price_df['numeric_price'] = pd.to_numeric(price_df['retailprice'], errors='coerce')
            
            logger.info(f"\n💰 가격 통계:")
            logger.info(f"평균가: ${price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: ${price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: ${price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: ${price_df['numeric_price'].median():.2f}")
            
            # 브랜드별 통계
            if 'brand' in df.columns:
                brand_stats = price_df['brand'].value_counts()
                logger.info(f"\n📈 브랜드별 성공:")
                for brand, count in brand_stats.items():
                    logger.info(f"  {brand}: {count}개")
            
            # 용량별 평균 가격
            if 'capacity' in df.columns:
                capacity_stats = price_df.groupby('capacity')['numeric_price'].agg(['mean', 'count'])
                logger.info(f"\n💾 용량별 평균 가격:")
                for capacity, stats in capacity_stats.iterrows():
                    logger.info(f"  {capacity}: ${stats['mean']:.2f} ({int(stats['count'])}개)")

def get_db_history(engine, days=7):
    """DB에서 최근 기록 조회"""
    try:
        query = f"""
        SELECT DATE(crawl_datetime) as date, 
               COUNT(*) as total_count,
               SUM(CASE WHEN retailprice IS NOT NULL THEN 1 ELSE 0 END) as with_price,
               COUNT(DISTINCT brand) as brands,
               COUNT(DISTINCT item) as items
        FROM bestbuy_price_crawl_tbl_usa_v2
        WHERE crawl_datetime >= DATE_SUB(NOW(), INTERVAL {days} DAY)
        GROUP BY DATE(crawl_datetime)
        ORDER BY date DESC
        """
        
        df = pd.read_sql(query, engine)
        logger.info(f"\n📅 최근 {days}일 크롤링 기록:")
        if not df.empty:
            print(df.to_string(index=False))
        else:
            logger.info("최근 크롤링 기록이 없습니다.")
        
    except Exception as e:
        logger.error(f"DB 조회 오류: {e}")

def main():
    """메인 실행 함수"""
    print("\n🚀 BestBuy 가격 추출 시스템 - DB 기반 버전")
    print("="*60)
    
    # 스크래퍼 초기화
    scraper = BestBuyScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('usa_bestbuy', 0, None, error_message="DB 연결 실패")
        return

    # 최근 크롤링 기록 확인
    get_db_history(scraper.db_engine, 7)

    # resume 지원: 커맨드라인 인자로 시작 번호 지정 (예: python bestbuy_v2.py 51)
    start_from = 0
    if len(sys.argv) > 1:
        try:
            start_from = int(sys.argv[1]) - 1  # 사용자 입력은 1부터, 내부 인덱스는 0부터
            if start_from < 0:
                start_from = 0
            logger.info(f"⏭️ resume 모드: {start_from + 1}번째부터 시작")
        except ValueError:
            logger.warning(f"⚠️ 잘못된 시작 번호: {sys.argv[1]} - 처음부터 시작합니다")
            start_from = 0

    # 브라우저 설정
    if not scraper.setup_driver():
        logger.error("브라우저 설정 실패로 종료합니다.")
        return
    
    # 변수 초기화 (except 블록에서 사용하기 위해)
    urls_data = []
    results_df = None

    try:
        # 1단계: 전체 크롤링 실행
        logger.info("\n📊 1단계: 전체 크롤링 시작")
        urls_data = scraper.get_crawl_targets()

        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            monitor_and_alert('usa_bestbuy', 0, None, error_message="크롤링 대상 URL이 없습니다")
            return

        logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

        # 크롤링 실행 (10개마다 DB 중간 저장)
        first_results_df = scraper.scrape_urls(urls_data, save_interim=True, start_from=start_from)

        if first_results_df is None or first_results_df.empty:
            logger.error("크롤링 결과가 없습니다.")
            monitor_and_alert('usa_bestbuy', len(urls_data), None, error_message="크롤링 결과가 없습니다")
            return

        # 1단계 결과 분석
        logger.info("\n📊 1단계 결과:")
        first_failed = first_results_df['retailprice'].isna().sum()
        first_success = first_results_df['retailprice'].notna().sum()
        logger.info(f"성공: {first_success}개, 실패: {first_failed}개")

        # 2단계: 실패한 URL 재시도 (실패가 있는 경우만, no_longer_available 제외)
        final_results_df = first_results_df.copy()

        # no_longer_available 컬럼이 없으면 False로 채움
        if 'no_longer_available' not in final_results_df.columns:
            final_results_df['no_longer_available'] = False
        final_results_df['no_longer_available'] = final_results_df['no_longer_available'].fillna(False)

        # 재시도 대상: retailprice가 None이고 no_longer_available이 아닌 것
        retry_candidates = final_results_df[
            (final_results_df['retailprice'].isna()) &
            (final_results_df['no_longer_available'] == False)
        ]
        no_longer_count = final_results_df['no_longer_available'].sum()

        if no_longer_count > 0:
            logger.info(f"ℹ️ no_longer_available 상품 {no_longer_count}개는 재시도 제외")

        if len(retry_candidates) > 0:
            logger.info(f"\n🔄 2단계: 실패한 {len(retry_candidates)}개 URL 재시도")
            logger.info("5초 대기 후 재시도합니다...")
            time.sleep(5)

            # 실패한 URL들만 추출 (no_longer_available 제외)
            failed_product_urls = retry_candidates['producturl'].tolist()

            # 원본 urls_data에서 실패한 URL에 해당하는 데이터만 추출
            failed_urls_data = [
                row for row in urls_data
                if row.get('url') in failed_product_urls
            ]

            if failed_urls_data:
                logger.info(f"재시도 대상: {len(failed_urls_data)}개 (실패한 URL만)")

                # 새 드라이버로 재시도
                if scraper.page:
                    try:
                        scraper.page.quit()
                    except:
                        pass
                    scraper.page = None

                # 새 브라우저 세팅 후 재시도
                retry_results_df = None
                if not scraper.setup_driver():
                    logger.error("❌ 재시도용 브라우저 설정 실패")
                else:
                    retry_results_df = scraper.scrape_urls(failed_urls_data, save_interim=False)

                if retry_results_df is not None and not retry_results_df.empty:
                    # 재시도 결과 분석
                    retry_success = retry_results_df['retailprice'].notna().sum()
                    retry_failed = retry_results_df['retailprice'].isna().sum()
                    logger.info(f"\n📊 재시도 결과: 성공 {retry_success}개, 실패 {retry_failed}개")

                    # 기존 실패한 결과를 재시도 결과로 업데이트 (메모리 + DB)
                    for _, retry_row in retry_results_df.iterrows():
                        if retry_row['retailprice'] is not None:
                            # 메모리 데이터 업데이트
                            mask = final_results_df['producturl'] == retry_row['producturl']
                            if mask.any():
                                final_results_df.loc[mask, 'retailprice'] = retry_row['retailprice']
                                final_results_df.loc[mask, 'title'] = retry_row['title']
                                final_results_df.loc[mask, 'imageurl'] = retry_row['imageurl']
                                final_results_df.loc[mask, 'crawl_datetime'] = retry_row['crawl_datetime']
                                final_results_df.loc[mask, 'crawl_strdatetime'] = retry_row['crawl_strdatetime']

                            # DB 레코드 UPDATE (1차에서 저장된 레코드 덮어쓰기)
                            try:
                                from sqlalchemy import text
                                update_query = text("""
                                UPDATE bestbuy_price_crawl_tbl_usa_v2
                                SET retailprice = :retailprice, title = :title, imageurl = :imageurl,
                                    crawl_datetime = :crawl_datetime, crawl_strdatetime = :crawl_strdatetime
                                WHERE producturl = :producturl AND retailprice IS NULL
                                """)
                                with scraper.db_engine.connect() as conn:
                                    conn.execute(update_query, {
                                        'retailprice': retry_row['retailprice'],
                                        'title': retry_row['title'],
                                        'imageurl': retry_row['imageurl'],
                                        'crawl_datetime': retry_row['crawl_datetime'],
                                        'crawl_strdatetime': retry_row['crawl_strdatetime'],
                                        'producturl': retry_row['producturl']
                                    })
                                    conn.commit()
                                logger.info(f"✅ DB 업데이트: {retry_row['producturl'][:50]}...")
                            except Exception as e:
                                logger.error(f"DB 업데이트 실패: {e}")

        # 3단계: 최종 결과 저장
        logger.info("\n💾 3단계: 최종 결과 저장")

        # 최종 통계
        final_success = final_results_df['retailprice'].notna().sum()
        final_failed = final_results_df['retailprice'].isna().sum()
        success_rate = (final_success / len(final_results_df) * 100) if len(final_results_df) > 0 else 0

        logger.info(f"\n📊 === 최종 결과 ===")
        logger.info(f"전체: {len(final_results_df)}개")
        logger.info(f"성공: {final_success}개")
        logger.info(f"실패: {final_failed}개")
        logger.info(f"성공률: {success_rate:.1f}%")

        # 개선율 표시
        if first_failed > 0 and first_failed > final_failed:
            improvement = first_failed - final_failed
            logger.info(f"✨ 재시도로 {improvement}개 추가 성공!")

        # 상세 분석 (save_results 전에 실행 - 컬럼명 대문자 변환 전)
        scraper.analyze_results(final_results_df)

        # resume 시: DB에서 오늘 전체 레코드 조회하여 파일서버 업로드
        if start_from > 0:
            logger.info("resume 모드: DB에서 오늘 전체 레코드 조회하여 파일 생성")
            today_query = """
            SELECT * FROM bestbuy_price_crawl_tbl_usa_v2
            WHERE DATE(kr_crawl_datetime) = CURDATE()
            """
            try:
                upload_df = pd.read_sql(today_query, scraper.db_engine)
                upload_df = upload_df.drop_duplicates(subset=['producturl'], keep='last')
                logger.info(f"오늘 전체 레코드: {len(upload_df)}개")
            except Exception as e:
                logger.error(f"전체 레코드 조회 실패: {e}")
                upload_df = final_results_df
        else:
            upload_df = final_results_df

        # no_longer_available 컬럼 제거 (DB 저장용)
        if 'no_longer_available' in upload_df.columns:
            upload_df = upload_df.drop(columns=['no_longer_available'])

        # resume 시: 파일명/폴더 직접 입력
        custom_filename = None
        custom_date_folder = None
        if start_from > 0:
            print(f"\n업로드 폴더(날짜)를 입력하세요 (예: 20260227)")
            print("Enter를 누르면 현재 시간으로 자동 생성됩니다.")
            folder_input = input("폴더: ").strip()
            if folder_input:
                custom_date_folder = folder_input

            print(f"\n파일명 일시를 입력하세요 (예: 20260227_120000)")
            print("Enter를 누르면 현재 시간으로 자동 생성됩니다.")
            datetime_input = input("일시: ").strip()
            if datetime_input:
                custom_filename = f"{datetime_input}_usa_bestbuy"

        # DB와 파일서버에 최종 결과 저장
        save_results = scraper.save_results(
            upload_df,
            save_db=False,  # 중간 저장으로 이미 DB에 저장됨
            upload_server=True,
            custom_filename=custom_filename,
            custom_date_folder=custom_date_folder
        )

        # 저장 결과 출력
        logger.info("\n📊 저장 결과:")
        logger.info(f"DB 저장: {'✅ 성공' if save_results['db_saved'] else '❌ 실패'}")
        logger.info(f"파일서버 업로드: {'✅ 성공' if save_results['server_uploaded'] else '❌ 실패'}")

        # 실패한 URL 로그
        if final_failed > 0:
            logger.warning(f"\n⚠️ {final_failed}개 URL에서 가격 추출 실패")
            failed_items = final_results_df[final_results_df['retailprice'].isna()]
            logger.warning("실패 목록 (상위 5개):")
            for idx, row in failed_items.head().iterrows():
                logger.warning(f"  - {row['brand']} {row['item']}: {row['producturl'][:50]}...")

        logger.info("\n✅ 크롤링 프로세스 완료!")

        # 크롤링 완료 후 알림
        results_df = upload_df
        monitor_and_alert('usa_bestbuy', len(urls_data), upload_df, error_logs=scraper.error_logs)

    except Exception as e:
        # 예외 발생 시 알림
        logger.error(f"크롤링 중 예외 발생: {e}")
        import traceback
        error_detail = traceback.format_exc()
        scraper.error_logs.append(f"[치명적 오류] {str(e)}\n{error_detail}")
        monitor_and_alert('usa_bestbuy', len(urls_data), results_df,
                         error_message=str(e), error_logs=scraper.error_logs)

    finally:
        # 드라이버 종료
        if scraper.page:
            scraper.page.quit()
            logger.info("🔧 드라이버 종료")

if __name__ == "__main__":
    # 필요한 패키지 설치 확인
    required_packages = [
        'DrissionPage',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'paramiko',
        'openpyxl'
    ]
    
    print("📦 필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("\n⚠️ DB 설정을 먼저 확인하세요:")
    print("DB_CONFIG 딕셔너리의 user, password, host 정보를 실제 값으로 변경해야 합니다.")
    print()
    
    main()