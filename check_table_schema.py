# -*- coding: utf-8 -*-
"""
DB 테이블 스키마 확인 스크립트
"""
import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG

# 기존 DB 연결 (ssd_crawl_db)
old_connection_string = (
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/ssd_crawl_db"
)
old_engine = create_engine(old_connection_string)

print("=" * 80)
print("기존 DB 테이블 스키마 확인 (ssd_crawl_db)")
print("=" * 80)

# 일본 테이블 확인
table_name = 'amazon_price_crawl_tbl_jp'
engine = old_engine

try:
    # 테이블 구조 확인
    query = f"DESCRIBE {table_name}"
    df = pd.read_sql(query, engine)

    print(f"\n테이블: {table_name}")
    print("-" * 80)
    print(df.to_string(index=False))

    # retailprice 컬럼만 따로 확인
    print("\n" + "=" * 80)
    print("retailprice 컬럼 상세:")
    print("-" * 80)
    retailprice_info = df[df['Field'] == 'retailprice']
    if not retailprice_info.empty:
        for col in df.columns:
            print(f"{col}: {retailprice_info[col].values[0]}")
    else:
        print("retailprice 컬럼을 찾을 수 없습니다!")

    # 실제 데이터 샘플 확인
    print("\n" + "=" * 80)
    print("실제 데이터 샘플 (최근 5개):")
    print("-" * 80)
    sample_query = f"SELECT retailprice, brand, item, crawl_datetime FROM {table_name} ORDER BY crawl_datetime DESC LIMIT 5"
    sample_df = pd.read_sql(sample_query, engine)
    print(sample_df.to_string(index=False))

    # retailprice의 데이터 타입 확인
    print("\n" + "=" * 80)
    print("retailprice 값의 Python 타입:")
    print("-" * 80)
    for idx, row in sample_df.iterrows():
        price_value = row['retailprice']
        print(f"값: {price_value}, 타입: {type(price_value)}, repr: {repr(price_value)}")

except Exception as e:
    print(f"오류 발생: {e}")

print("\n" + "=" * 80)
