"""
BestBuy 가격 추출 시스템 - GraphQL + ZenRows 버전 (v3)
- v2는 DrissionPage로 페이지를 1개씩 렌더링했지만,
  v3는 BestBuy gateway/graphql endpoint를 ZenRows로 호출해
  여러 SKU의 retailprice/title/imageurl을 1콜에 묶어 받는다.
- Batch 자동 fallback: 65 → 33 → 17 → 9 (실패한 chunk만 다음 size로 재분할)
- DB insert, SFTP 업로드는 v2 로직 그대로 재사용
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import zipfile
from datetime import datetime

import pandas as pd
import paramiko
import pymysql
import pytz
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# config 로드 (직접 파일 읽지 않고 import)
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG, ZENROWS_API_KEY


# =====================================================================
# 상수
# =====================================================================
BESTBUY_GRAPHQL_URL = "https://www.bestbuy.com/gateway/graphql"
ZENROWS_API_URL = "https://api.zenrows.com/v1/"

# Batch fallback 시퀀스
DEFAULT_BATCH_SIZES = [65, 33, 17, 9]

# GraphQL fragment.
# 케이스별 시그널:
#   - 정상: buyingOptions에 'New' 등 존재
#   - 단종: buyingOptions=[], bsinProduct.featuredSKU도 없음
#   - redirect: buyingOptions=[]이지만 bsinProduct.featuredSKU에 새 SKU/가격 들어옴
#                (또는 buyingOptions은 있지만 url.pdp가 BSIN 기반으로 바뀜)
# v2는 페이지 렌더링이라 자동 redirect로 새 가격을 잡았음. v3는 이 fragment 데이터로 재현.
PRICE_FRAGMENT = """
fragment SkuPrice on Product {
  skuId
  name { short }
  primaryImage { piscesHref }
  url {
    pdp
    relativePdp
    skuSpecificUrl
  }
  buyingOptions {
    __typename
    type
    pdpUrl
  }
  price(input: {salesChannel: "LargeView"}) {
    customerPrice
    regularPrice
    restrictedPriceDisplayMessage
  }
  bsinProduct {
    bsin
    featuredSKU(filter: {isBestbuyMember: false}) {
      product {
        skuId
        name { short }
        primaryImage { piscesHref }
        buyingOptions { type }
        price(input: {salesChannel: "LargeView"}) {
          customerPrice
          regularPrice
        }
      }
    }
  }
}
""".strip()

OPERATION_NAME = "GetMultiPrice"

# 호출 시 보낼 헤더 (DX 코드와 동일하게 4개로 단순화)
DEFAULT_BBY_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://www.bestbuy.com",
    "referer": "https://www.bestbuy.com/",
}


# =====================================================================
# 헬퍼
# =====================================================================
def parse_sku_from_url(url):
    """BestBuy URL에서 skuId 추출.
    우선 ?skuId= query param, fallback으로 path 끝의 /{digits}.p
    """
    if not url:
        return None
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None
    qs = urllib.parse.parse_qs(parsed.query or '')
    if 'skuId' in qs and qs['skuId']:
        return str(qs['skuId'][0]).strip()
    m = re.search(r'/(\d+)\.p(?:[/?#]|$)', parsed.path or '')
    if m:
        return m.group(1)
    return None


def build_batch_query(skus):
    """65개 SKU를 alias로 묶은 단일 GraphQL query 생성."""
    lines = [f"query {OPERATION_NAME} {{"]
    for sku in skus:
        # alias prefix 'p' + 숫자 SKU (GraphQL alias 첫 글자는 letter 필요)
        alias = f"p{sku}"
        lines.append(f'  {alias}: productBySkuId(skuId: "{sku}") {{ ...SkuPrice }}')
    lines.append("}")
    lines.append("")
    lines.append(PRICE_FRAGMENT)
    return "\n".join(lines)


def chunked(seq, size):
    """리스트를 size 단위로 자름."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# =====================================================================
# 메인 클래스
# =====================================================================
class BestBuyGraphQLScraper:
    def __init__(self):
        self.db_engine = None
        self.country_code = 'usa'
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('America/New_York')
        self.error_logs = []
        self.total_zenrows_calls = 0
        self.total_call_seconds = 0.0
        self.setup_db_connection()

    # -----------------------------------------------------------------
    # DB 연결 / 조회 (v2와 동일)
    # -----------------------------------------------------------------
    def setup_db_connection(self):
        try:
            if self.db_engine is not None:
                self.db_engine.dispose()
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            logger.info("✅ DB 연결 설정 완료")
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            self.db_engine = None

    def read_sql_with_retry(self, query, params=None, retries=3, delay=5):
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                if self.db_engine is None:
                    self.setup_db_connection()
                return pd.read_sql(query, self.db_engine, params=params)
            except (OperationalError, DBAPIError, pymysql.err.OperationalError) as e:
                last_error = e
                logger.warning(f"DB 쿼리 실패({attempt}/{retries}), 재연결 후 재시도: {e}")
                try:
                    if self.db_engine is not None:
                        self.db_engine.dispose()
                except Exception:
                    pass
                self.db_engine = None
                if attempt < retries:
                    time.sleep(delay * attempt)
                    self.setup_db_connection()
        raise last_error

    def get_crawl_targets(self, limit=None):
        """samsung_price_tracking_list에서 BestBuy USA 대상 조회."""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'usa'
              AND mall_name = 'bestbuy'
              AND is_active = TRUE
            """
            if limit:
                query += f" LIMIT {int(limit)}"
            df = self.read_sql_with_retry(query)
            logger.info(f"✅ 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []

    # -----------------------------------------------------------------
    # GraphQL 호출 (1 chunk = 1 ZenRows POST)
    # -----------------------------------------------------------------
    def call_graphql_batch(self, skus, timeout=240):
        """skus 리스트를 alias로 묶어 1콜 GraphQL POST.

        Returns:
            (ok: bool, parsed_json_data: dict | None, error_message: str | None)
            ok=True 일 때 parsed_json_data 는 GraphQL의 data 필드 (alias -> product dict)
        """
        if not skus:
            return True, {}, None

        query = build_batch_query(skus)
        payload = {
            "operationName": OPERATION_NAME,
            "variables": {},
            "query": query,
            "extensions": {
                "clientLibrary": {"name": "@apollo/client", "version": "4.1.6"}
            },
        }
        zr_params = {
            "url": BESTBUY_GRAPHQL_URL,
            "apikey": ZENROWS_API_KEY,
            "custom_headers": "true",
            "premium_proxy": "true",
            # premium_proxy 시 country 미지정이면 random IP라 BestBuy 차단 가능성↑
            "proxy_country": "us",
            # BestBuy 보안 정책상 js_render 필수 (DX 테스트 로그 기준
            # — js_render=false면 ZenRows HTTP 400 REQS002 발생)
            "js_render": "true",
        }
        body = json.dumps(payload)

        start = time.time()
        try:
            self.total_zenrows_calls += 1
            response = requests.post(
                ZENROWS_API_URL,
                params=zr_params,
                headers=DEFAULT_BBY_HEADERS,
                data=body,
                timeout=timeout,
            )
        except requests.RequestException as e:
            elapsed = time.time() - start
            self.total_call_seconds += elapsed
            return False, None, f"requests exception: {e}"

        elapsed = time.time() - start
        self.total_call_seconds += elapsed
        logger.info(
            f"🌐 ZenRows POST | skus={len(skus)} | status={response.status_code} | {elapsed:.1f}s"
        )

        if response.status_code != 200:
            snippet = (response.text or '')[:300]
            return False, None, f"HTTP {response.status_code}: {snippet}"

        try:
            j = response.json()
        except ValueError as e:
            snippet = (response.text or '')[:300]
            return False, None, f"json parse error: {e} | body: {snippet}"

        if not isinstance(j, dict):
            return False, None, f"unexpected json type: {type(j).__name__}"

        data = j.get('data')
        errors = j.get('errors')

        if data is None:
            err_snip = json.dumps(errors)[:300] if errors else 'no data, no errors'
            return False, None, f"no data field: {err_snip}"

        # data 있고, errors는 부분일 수 있음 (개별 alias가 null인 케이스). partial OK.
        if errors:
            logger.warning(f"⚠️ GraphQL partial errors: {json.dumps(errors)[:200]}")

        return True, data, None

    # -----------------------------------------------------------------
    # Batch fallback orchestrator
    # -----------------------------------------------------------------
    def collect_with_fallback(self, urls_data, batch_sizes=None, fixed_batch_size=None):
        """전체 SKU 리스트에 대해 batch fallback 수행.

        Args:
            urls_data: DB row dict 리스트 (각각 'url' 포함)
            batch_sizes: [65, 33, 17, 9] 형태 시퀀스 (None이면 default)
            fixed_batch_size: 지정 시 fallback 끄고 그 size로만 시도

        Returns:
            results: list[result_dict]
        """
        # SKU 파싱
        items = []
        for row in urls_data:
            sku = parse_sku_from_url(row.get('url'))
            if not sku:
                logger.warning(f"⚠️ skuId 파싱 실패, 결과 None으로 기록: {row.get('url')}")
                items.append((None, row))
            else:
                items.append((sku, row))

        valid_items = [it for it in items if it[0] is not None]
        invalid_items = [it for it in items if it[0] is None]

        results = []

        # 파싱 실패한 row는 즉시 fail result로
        for sku, row in invalid_items:
            results.append(self._build_result(row, None, None, None, error='sku parse failed'))

        if not valid_items:
            return results

        if fixed_batch_size:
            sizes = [int(fixed_batch_size)]
            logger.info(f"📦 batch size 고정: {fixed_batch_size} (fallback 비활성)")
        else:
            sizes = list(batch_sizes or DEFAULT_BATCH_SIZES)
            logger.info(f"📦 batch fallback 시퀀스: {sizes}")

        # failed_chunks: 다음 level에서 재시도할 chunk(=valid_items 부분리스트)
        failed_chunks = [valid_items]
        final_failed = []

        for level_idx, size in enumerate(sizes):
            level_no = level_idx
            logger.info(f"\n=== Level {level_no} (batch size={size}) 시작 ===")
            next_failed = []
            for chunk in failed_chunks:
                for sub_chunk in chunked(chunk, size):
                    ok, parsed_results = self._try_chunk(sub_chunk)
                    if ok:
                        results.extend(parsed_results)
                    else:
                        next_failed.append(sub_chunk)

            if not next_failed:
                logger.info(f"✅ Level {level_no}에서 모든 chunk 성공")
                failed_chunks = []
                break

            logger.warning(
                f"⚠️ Level {level_no} 실패 chunk {len(next_failed)}개"
                f" (실패 SKU 합계 {sum(len(c) for c in next_failed)}개)"
            )
            failed_chunks = next_failed
            # 마지막 level이면 더 줄일 수 없으니 종료
            if level_idx == len(sizes) - 1:
                final_failed = failed_chunks
                break

        # 마지막까지 실패한 chunk는 retailprice=None으로 기록
        for chunk in final_failed:
            for sku, row in chunk:
                results.append(self._build_result(row, sku, None, None, error='all batch levels failed'))
                self.error_logs.append(f"[GraphQL 전체 실패] SKU: {sku} | URL: {row.get('url')}")

        return results

    def _try_chunk(self, chunk_items):
        """chunk_items: [(sku, row), ...]
        GraphQL 호출 → 성공이면 [(result_dict)] 리턴, 실패면 (False, None).
        """
        skus = [sku for sku, _ in chunk_items]
        ok, data, err = self.call_graphql_batch(skus)
        if not ok:
            logger.warning(f"❌ chunk 실패 (skus={len(skus)}): {err}")
            self.error_logs.append(f"[batch 실패 skus={len(skus)}] {err}")
            return False, None

        # 각 sku alias 결과를 row와 join
        chunk_results = []
        for sku, row in chunk_items:
            alias = f"p{sku}"
            node = data.get(alias)
            if node is None:
                # 개별 SKU만 null (단종/제거)
                chunk_results.append(
                    self._build_result(row, sku, None, None, error='product not found')
                )
                continue

            name_short = None
            try:
                name_short = (node.get('name') or {}).get('short')
            except AttributeError:
                pass

            image_href = None
            try:
                image_href = (node.get('primaryImage') or {}).get('piscesHref')
            except AttributeError:
                pass

            customer_price = None
            regular_price = None
            restricted_msg = None
            try:
                price_obj = node.get('price') or {}
                customer_price = price_obj.get('customerPrice')
                regular_price = price_obj.get('regularPrice')
                restricted_msg = price_obj.get('restrictedPriceDisplayMessage')
            except AttributeError:
                pass

            buying_options = node.get('buyingOptions') or []

            # url, bsinProduct.featuredSKU 정보 추출 (redirect 케이스 식별용)
            url_pdp = None
            try:
                url_pdp = (node.get('url') or {}).get('pdp')
            except AttributeError:
                pass

            bsin = None
            featured_sku = None
            featured_cprice = None
            featured_rprice = None
            featured_bo_types = []
            featured_name = None
            featured_image = None
            try:
                bsin_obj = node.get('bsinProduct') or {}
                bsin = bsin_obj.get('bsin')
                featured = bsin_obj.get('featuredSKU') or {}
                featured_product = featured.get('product') or {}
                featured_sku = featured_product.get('skuId')
                fp_price = featured_product.get('price') or {}
                featured_cprice = fp_price.get('customerPrice')
                featured_rprice = fp_price.get('regularPrice')
                featured_bo_types = [bo.get('type') for bo in (featured_product.get('buyingOptions') or [])]
                featured_name = (featured_product.get('name') or {}).get('short')
                featured_image = (featured_product.get('primaryImage') or {}).get('piscesHref')
            except AttributeError:
                pass

            # 정책: featCprice (bsinProduct.featuredSKU.product.price.customerPrice) 우선.
            #   사용자 전수조사로 cprice != featCprice 인 17건 mismatch 케이스에서
            #   페이지 가격 = featCprice 검증됨. cprice == featCprice 인 나머지 케이스는
            #   어느 쪽 써도 같음. 단종 로직 제거 (사용자가 "이번엔 단종 없었어" 명시).
            #   featCprice가 null인 케이스만 cprice fallback (안전망).
            if featured_cprice is not None:
                customer_price = featured_cprice
                # regular_price 는 미검증이라 None 유지

            # mismatch 또는 missing 케이스만 디버그 로그
            if featured_cprice is None:
                logger.info(
                    f"  · sku={sku} | featCprice missing — cprice fallback "
                    f"({customer_price}) | bsin={bsin} url={url_pdp}"
                )

            chunk_results.append(
                self._build_result(row, sku, name_short, image_href, customer_price, regular_price)
            )

        return True, chunk_results

    # -----------------------------------------------------------------
    # Result dict 생성 (v2 스키마 그대로)
    # -----------------------------------------------------------------
    def _build_result(self, row, sku, name_short, image_href,
                      customer_price=None, regular_price=None, error=None):
        # retailprice 결정: customerPrice 우선, None이면 regularPrice
        if customer_price is not None:
            retailprice = customer_price
        elif regular_price is not None:
            retailprice = regular_price
        else:
            retailprice = None

        now_time = datetime.now(self.korea_tz)
        local_time = datetime.now(self.local_tz)
        crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
        tz_offset = local_time.strftime("%z")
        tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
        crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

        if error:
            logger.info(f"  · sku={sku} → {error}")

        return {
            'retailerid': row.get('retailerid', ''),
            'country_code': row.get('country', 'usa'),
            'ships_from': 'usa',
            'channel_name': 'bestbuy',
            'channel': row.get('channel', 'Online'),
            'retailersku': row.get('retailersku', sku or ''),
            'brand': row.get('brand', ''),
            'brand_eng': row.get('brand_eng', row.get('brand', '')),
            'form_factor': row.get('form_factor', ''),
            'segment_lv1': row.get('seg_lv1', ''),
            'segment_lv2': row.get('seg_lv2', ''),
            'segment_lv3': row.get('seg_lv3', ''),
            'capacity': row.get('capacity', ''),
            'item': row.get('item', ''),
            'retailprice': retailprice,
            'sold_by': 'BestBuy',
            'imageurl': image_href,
            'producturl': row.get('url', ''),
            'crawl_datetime': crawl_datetime_iso,
            'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
            'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
            'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
            'title': name_short,
            'vat': row.get('vat', 'x'),
        }

    # -----------------------------------------------------------------
    # DB 저장 / 파일서버 업로드 (v2 그대로)
    # -----------------------------------------------------------------
    def save_to_db(self, df):
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        try:
            df.to_sql('bestbuy_price_crawl_tbl_usa_v2', self.db_engine,
                      if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")

            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'usa',
                    'url': row['producturl'],
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': self.total_call_seconds / max(1, self.total_zenrows_calls),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime'],
                })
            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine,
                              if_exists='append', index=False)
                logger.info(f"✅ 크롤링 로그 저장 완료: {len(log_records)}개")

            with self.db_engine.connect() as conn:
                count_query = text(
                    "SELECT COUNT(*) FROM bestbuy_price_crawl_tbl_usa_v2 "
                    "WHERE DATE(crawl_datetime) = CURDATE()"
                )
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
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password'],
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"📁 국가 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)

            date_dir = f"{country_dir}/{date_folder}"
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"📁 날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)

            remote_filename = os.path.basename(local_file_path)
            remote_path = f"{date_dir}/{remote_filename}"
            sftp.put(local_file_path, remote_path)
            logger.info(f"✅ 파일서버 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()
            return True
        except Exception as e:
            logger.error(f"❌ 파일서버 업로드 실패: {e}")
            return False

    def save_results(self, df, save_db=True, upload_server=True,
                     custom_filename=None, custom_date_folder=None):
        local_time = datetime.now(self.local_tz)
        date_str = custom_date_folder or local_time.strftime('%Y%m%d')
        base_filename = custom_filename or f"{date_str}_{local_time.strftime('%H%M%S')}_usa_bestbuy"

        results = {'db_saved': False, 'server_uploaded': False}

        if save_db:
            results['db_saved'] = self.save_to_db(df)

        if upload_server:
            try:
                csv_filename = f'{base_filename}.csv'
                column_order = [
                    'retailerid', 'country_code', 'ships_from', 'channel_name', 'channel',
                    'retailersku', 'brand', 'brand_eng', 'form_factor',
                    'segment_lv1', 'segment_lv2', 'segment_lv3', 'capacity', 'item',
                    'retailprice', 'sold_by', 'imageurl', 'producturl',
                    'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime',
                    'title', 'vat',
                ]
                df_csv = df.copy()
                df_csv.columns = df_csv.columns.str.lower()
                existing_cols = [c for c in column_order if c in df_csv.columns]
                df_csv = df_csv[existing_cols]
                df_csv.columns = df_csv.columns.str.upper()
                # float_format='%g': 정수형 가격(879.0)을 "879"로, 소수(184.59)는 그대로.
                # v2 출력 형식과 일치시키기 위함.
                df_csv.to_csv(
                    csv_filename, index=False, encoding='utf-8',
                    lineterminator='\r\n', float_format='%g',
                )

                zip_filename = f'{base_filename}.zip'
                with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(csv_filename, arcname=csv_filename)

                def calculate_md5(filename):
                    md5 = hashlib.md5()
                    with open(filename, 'rb') as f:
                        for chunk in iter(lambda: f.read(4096), b''):
                            md5.update(chunk)
                    return md5.hexdigest()

                csv_md5 = calculate_md5(csv_filename)
                zip_md5 = calculate_md5(zip_filename)

                md5_filename = f'{base_filename}.md5'
                with open(md5_filename, 'w', encoding='utf-8') as f:
                    f.write(f"{os.path.basename(zip_filename)} {zip_md5}\n")
                    f.write(f"{os.path.basename(csv_filename)} {csv_md5}\n")

                if self.upload_to_file_server(zip_filename, date_str):
                    if self.upload_to_file_server(md5_filename, date_str):
                        results['server_uploaded'] = True

                for temp_file in [csv_filename, zip_filename, md5_filename]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                logger.info("임시 파일 삭제 완료")
            except Exception as e:
                logger.error(f"파일 저장 실패: {e}")

        return results

    # -----------------------------------------------------------------
    # 분석
    # -----------------------------------------------------------------
    def analyze_results(self, df):
        logger.info("\n📊 === 결과 분석 ===")
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        without_price = df['retailprice'].isna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        logger.info(f"ZenRows 호출 횟수: {self.total_zenrows_calls}회")
        logger.info(f"총 호출 시간: {self.total_call_seconds:.1f}초")


# =====================================================================
# main
# =====================================================================
def main():
    # Windows cp949 콘솔에서 이모지/한글 출력 시 UnicodeEncodeError 방지
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="BestBuy GraphQL price crawler (v3)")
    parser.add_argument('--limit', type=int, default=None,
                        help='크롤링 대상 개수 제한 (개발 검증용)')
    parser.add_argument('--batch-size', type=int, default=None,
                        help='고정 batch size. 미지정 시 65→33→17→9 자동 fallback')
    parser.add_argument('--dry-run', action='store_true',
                        help='GraphQL 호출만 수행, DB/SFTP 저장 안 함')
    args = parser.parse_args()

    # log_utils 있으면 사용, 없으면 skip
    try:
        from log_utils import setup_log, save_log
        setup_log('usa_bestbuy_v3')
        log_enabled = True
    except ImportError:
        log_enabled = False

    # alert_monitor 있으면 사용, dry-run 또는 모듈 없을 때는 no-op
    def _alert(target_count, results_df, error_message=None, error_logs=None):
        if args.dry_run:
            return
        try:
            from alert_monitor import monitor_and_alert
            monitor_and_alert(
                'usa_bestbuy_v3', target_count, results_df,
                error_message=error_message, error_logs=error_logs,
            )
        except ImportError:
            logger.warning("alert_monitor 모듈 없음 — 알림 건너뜀")
        except Exception as e:
            logger.warning(f"알림 전송 실패: {e}")

    print("\n🚀 BestBuy 가격 추출 시스템 - GraphQL+ZenRows 버전 (v3)")
    print("=" * 60)

    scraper = BestBuyGraphQLScraper()
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        _alert(0, None, error_message="DB 연결 실패")
        return

    urls_data = []
    results_df = None
    try:
        urls_data = scraper.get_crawl_targets(limit=args.limit)
        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            _alert(0, None, error_message="크롤링 대상 URL이 없습니다")
            return
        logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

        results = scraper.collect_with_fallback(
            urls_data,
            fixed_batch_size=args.batch_size,
        )
        if not results:
            logger.error("결과가 없습니다.")
            _alert(len(urls_data), None, error_message="크롤링 결과가 없습니다")
            return

        results_df = pd.DataFrame(results)
        scraper.analyze_results(results_df)

        if args.dry_run:
            logger.info("\n🟡 --dry-run: DB/SFTP 저장 건너뜀")
            preview_cols = ['retailersku', 'title', 'retailprice', 'imageurl']
            preview_cols = [c for c in preview_cols if c in results_df.columns]
            print(results_df[preview_cols].head(10).to_string(index=False))
        else:
            scraper.save_results(results_df, save_db=True, upload_server=True)
            # 정상 종료 알림 (실패 SKU 있더라도 process가 끝까지 갔으면 성공으로 봄 — v2와 동일)
            _alert(len(urls_data), results_df)

        logger.info("BestBuy v3 크롤링 완료!")
    except Exception as e:
        logger.error(f"크롤링 중 예외: {e}")
        import traceback
        error_detail = traceback.format_exc()
        logger.error(error_detail)
        scraper.error_logs.append(f"[치명적 오류] {str(e)}\n{error_detail}")
        _alert(len(urls_data), results_df,
               error_message=str(e), error_logs=scraper.error_logs)
    finally:
        if log_enabled:
            try:
                save_log('usa_bestbuy_v3')
            except Exception:
                pass


if __name__ == "__main__":
    main()
