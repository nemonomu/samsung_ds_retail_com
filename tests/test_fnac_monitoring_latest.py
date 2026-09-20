"""Exercise real registration SQL in an isolated DB, with no production imports."""
import ast
from datetime import datetime, timezone
import logging
from pathlib import Path
import sqlite3
import types
import unittest

import pandas as pd


class Cursor:
    def __init__(self, db):
        self.inner = db.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.inner.close()

    def execute(self, sql, values):
        self.inner.execute(sql.replace('ssd_crawl_db.', '').replace('%s', '?'), values)

    def fetchone(self):
        return self.inner.fetchone()

    @property
    def lastrowid(self):
        return self.inner.lastrowid


class Connection:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return Cursor(self.db)

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def close(self):
        pass  # Keep the in-memory DB open for assertions.


class LatestMonitoringTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            CREATE TABLE ds_monitoring_file (
                file_id INTEGER PRIMARY KEY, file_name TEXT, file_path TEXT,
                file_size INTEGER, file_type TEXT, is_del INTEGER,
                created_at TEXT, created_id TEXT, updated_at TEXT, updated_id TEXT);
            CREATE TABLE ds_monitoring_report_anomaly (
                id INTEGER PRIMARY KEY, crawl_date TEXT, retailer_id INTEGER,
                country_code TEXT, title TEXT, retailprice REAL, ships_from TEXT,
                sold_by TEXT, imageurl TEXT, producturl TEXT, retailersku TEXT,
                screenshot_id INTEGER, cause TEXT, memo TEXT, is_del INTEGER,
                created_at TEXT, created_id TEXT, updated_at TEXT, updated_id TEXT);
            INSERT INTO ds_monitoring_report_anomaly
                (id, crawl_date, retailer_id, retailersku, title, retailprice,
                 ships_from, sold_by, imageurl, cause, memo, is_del)
            VALUES (1, '2026-09-18', 1, 'sample', 'Previous title', 300,
                    'FR', 'Fnac', 'old-image', 'User cause', 'User memo', 0);
        ''')
        path = Path(__file__).resolve().parents[1] / 'null_screenshot.py'
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        names = {'_insert_monitoring_file_and_anomaly', '_result_value',
                 '_normalize_retailer', '_file_sku'}
        body = [node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name in names]
        self.module = types.ModuleType('registration_test_subject')
        self.module.__dict__.update(
            pd=pd, datetime=datetime, KST=timezone.utc,
            MONITORING_CREATED_ID='offline-test', logger=logging.getLogger('offline.registration'),
            _get_db_connection=lambda: Connection(self.db),
            _get_monitoring_target=lambda cursor, retailer: (1, retailer, 'FR'))
        exec(compile(ast.Module(body=body, type_ignores=[]), str(path), 'exec'),
             self.module.__dict__)

    def register(self, data, retailer='fnac', day='2026-09-18'):
        result = self.module._insert_monitoring_file_and_anomaly(
            retailer, 'sample', 'https://example.test/product', 'sample.png',
            'offline/', 10, day, data)
        self.assertIsNotNone(result)
        return result

    def row(self, day='2026-09-18'):
        return self.db.execute('SELECT * FROM ds_monitoring_report_anomaly WHERE crawl_date = ?',
                               (day,)).fetchone()

    def test_latest_null_replaces_old_price_and_preserves_user_review(self):
        photo_id = self.register({'retailprice': None, 'title': 'Latest title'})
        row = self.row()
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['title'], 'Latest title')
        self.assertEqual(row['screenshot_id'], photo_id)
        self.assertEqual((row['cause'], row['memo']), ('User cause', 'User memo'))
        self.assertEqual(row['imageurl'], 'old-image')

    def test_latest_numeric_price_replaces_null(self):
        self.register({'retailprice': None})
        self.register({'retailprice': 200})
        self.assertEqual(self.row()['retailprice'], 200)

    def test_explicit_nulls_refresh_all_supplied_crawl_fields(self):
        fields = ('title', 'retailprice', 'imageurl', 'ships_from', 'sold_by')
        self.register(dict.fromkeys(fields))
        for field in fields:
            self.assertIsNone(self.row()[field], field)

    def test_nan_price_is_stored_as_null(self):
        self.register({'retailprice': float('nan')})
        self.assertIsNone(self.row()['retailprice'])

    def test_missing_result_or_price_keeps_existing_price(self):
        for data in (None, {}, {'title': 'Only title'}):
            with self.subTest(data=data):
                self.register(data)
                self.assertEqual(self.row()['retailprice'], 300)

    def test_zero_is_a_supplied_value(self):
        self.register({'retailprice': 0})
        self.assertEqual(self.row()['retailprice'], 0)

    def test_other_retailers_keep_previous_null_update_contract(self):
        self.register({'retailprice': None}, retailer='amazon_gb')
        self.assertEqual(self.row()['retailprice'], 300)

    def test_new_day_inserts_null_without_altering_previous_day(self):
        self.register({'retailprice': None}, day='2026-09-19')
        self.assertIsNone(self.row('2026-09-19')['retailprice'])
        self.assertEqual(self.row()['retailprice'], 300)


if __name__ == '__main__':
    unittest.main()
