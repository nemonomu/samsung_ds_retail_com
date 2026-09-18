"""Committed crawl -> report synchronization, using actual SQL and an offline DB."""
import ast
from datetime import date
import importlib.util
import logging
from pathlib import Path
import sqlite3
import sys
import types
import unittest
from unittest.mock import Mock, patch

import pandas as pd
from test_fnac_monitoring_latest import Connection, Cursor

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('offline_fnac_monitoring', ROOT / 'fnac_monitoring.py')
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class SyncCursor(Cursor):
    def execute(self, sql, values):
        sql = sql.replace('samsung_ds_retail_com.', '').replace('ssd_crawl_db.', '').replace('%s', '?')
        values = tuple(v.isoformat() if isinstance(v, date) else v for v in values)
        self.inner.execute(sql, values)

    def fetchall(self):
        return self.inner.fetchall()


class SyncConnection(Connection):
    def cursor(self):
        return SyncCursor(self.db)


class ReportSyncTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            CREATE TABLE fnac_price_crawl_tbl_fr (
                retailersku TEXT, country_code TEXT, title TEXT, retailprice REAL,
                ships_from TEXT, sold_by TEXT, imageurl TEXT, producturl TEXT, crawl_strdatetime TEXT);
            CREATE TABLE ds_monitoring_report_close (crawl_date TEXT, is_closed INTEGER);
            CREATE TABLE ds_monitoring_report_anomaly (
                id INTEGER PRIMARY KEY, crawl_date TEXT, retailer_id INTEGER, retailersku TEXT,
                country_code TEXT, title TEXT, retailprice REAL, ships_from TEXT, sold_by TEXT,
                imageurl TEXT, producturl TEXT, screenshot_id INTEGER, cause TEXT, memo TEXT,
                is_del INTEGER, created_at TEXT, created_id TEXT, updated_at TEXT, updated_id TEXT);
            CREATE TABLE ds_monitoring_report_daily (
                crawl_date TEXT, retailer_id INTEGER, is_del INTEGER,
                anomaly_total INTEGER, anomaly_title_null INTEGER, anomaly_image_null INTEGER,
                anomaly_partial_null INTEGER, anomaly_price_zero INTEGER, updated_at TEXT,
                updated_id TEXT, memo TEXT, total_count INTEGER);
            INSERT INTO ds_monitoring_report_daily
            VALUES ('2026-09-18',1,0,1,0,0,1,0,NULL,NULL,'Daily memo',31);
            INSERT INTO ds_monitoring_report_anomaly
                (crawl_date, retailer_id, retailersku, title, retailprice, ships_from,
                 sold_by, imageurl, screenshot_id, cause, memo, is_del)
            VALUES ('2026-09-18',1,'sample','Old',300,'FR','Fnac','https://example.test/img',7,'User cause','User memo',0);
        ''')
        helper = types.ModuleType('null_screenshot')
        helper._get_db_connection = lambda: SyncConnection(self.db)
        helper._get_monitoring_target = lambda cursor, retailer: (1, 'Fnac', 'FR')
        helper._result_value = lambda row, key: None if pd.isna(row.get(key)) else row.get(key)
        session = types.ModuleType('crawler_session')
        session.MONITORING_CREATED_ID = 'offline-test'
        self.patch = patch.dict(sys.modules, {'null_screenshot': helper, 'crawler_session': session})
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def save_raw(self, price=None, sku='sample', stamp='202609180900000000', **overrides):
        row = dict(retailersku=sku, country_code='fr', title='Latest', retailprice=price,
                   ships_from='FR', sold_by='Fnac', imageurl='https://example.test/img',
                   producturl='https://example.test/product', crawl_strdatetime=stamp)
        row.update(overrides)
        self.db.execute('INSERT INTO fnac_price_crawl_tbl_fr VALUES (?,?,?,?,?,?,?,?,?)', tuple(row.values()))
        self.db.commit()
        return row

    def report(self, sku='sample'):
        return self.db.execute('SELECT * FROM ds_monitoring_report_anomaly WHERE retailersku=? ORDER BY id DESC', (sku,)).fetchone()

    def daily(self):
        return self.db.execute('SELECT * FROM ds_monitoring_report_daily').fetchone()

    def sync(self, *rows):
        status = subject.sync_saved_fnac_results(rows)
        self.assertTrue(status['success'], status)
        return status

    def test_failed_photo_still_updates_price_to_null_preserving_notes(self):
        self.sync(self.save_raw())
        row = self.report()
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['title'], 'Latest')
        self.assertEqual((row['cause'], row['memo'], row['screenshot_id']), ('User cause', 'User memo', 7))
        self.assertEqual(self.daily()['anomaly_partial_null'], 1)

    def test_normal_recovery_retires_anomaly_and_updates_count(self):
        self.sync(self.save_raw(300))
        self.assertEqual((self.report()['is_del'], self.report()['retailprice']), (1, 300))
        self.assertEqual(self.daily()['anomaly_total'], 0)
        self.assertEqual((self.daily()['memo'], self.daily()['total_count']), ('Daily memo', 31))

    def test_new_null_without_photo_is_registered(self):
        self.sync(self.save_raw(sku='new'))
        self.assertIsNone(self.report('new')['screenshot_id'])
        self.assertIsNone(self.report('new')['retailprice'])
        self.assertEqual(self.report('new')['is_del'], 0)

    def test_uses_newest_committed_row_not_candidate_or_older_batch(self):
        first = self.save_raw(200)
        self.save_raw(None, stamp='202609181000000000')
        first['retailprice'] = 999
        self.sync(first)
        self.assertIsNone(self.report()['retailprice'])

    def test_partial_run_does_not_remove_unrelated_sku_or_other_dates(self):
        self.db.execute('''INSERT INTO ds_monitoring_report_anomaly
            (crawl_date,retailer_id,retailersku,title,retailprice,ships_from,sold_by,imageurl,is_del)
            VALUES ('2026-09-18',1,'untouched','Old',NULL,'FR','Fnac','https://example.test/img',0)''')
        self.db.execute("INSERT INTO ds_monitoring_report_anomaly (crawl_date,retailer_id,retailersku,is_del) VALUES ('2026-09-17',1,'yesterday',0)")
        self.sync(self.save_raw(300))
        self.assertEqual(self.report('untouched')['is_del'], 0)
        self.assertEqual(self.report('yesterday')['is_del'], 0)
        self.assertEqual(self.daily()['anomaly_total'], 1)

    def test_closed_report_remains_closed(self):
        self.db.execute("INSERT INTO ds_monitoring_report_close VALUES ('2026-09-18',1)")
        result = self.sync(self.save_raw())
        self.assertEqual(result['closed_dates'], 1)
        self.assertEqual(self.report()['retailprice'], 300)

    def test_repeated_sync_is_idempotent(self):
        row = self.save_raw(sku='new')
        self.sync(row)
        first_id = self.report('new')['id']
        self.sync(row)
        self.assertEqual(self.report('new')['id'], first_id)

    def test_missing_committed_rows_roll_back_report_changes(self):
        row = self.save_raw()
        missing = dict(row, retailersku='missing')
        result = subject.sync_saved_fnac_results([row, missing])
        self.assertFalse(result['success'])
        self.assertEqual(self.report()['retailprice'], 300)

    def test_image_missing_with_normal_price_remains_anomaly(self):
        self.sync(self.save_raw(300, imageurl=None))
        self.assertEqual(self.report()['is_del'], 0)
        self.assertEqual(self.report()['retailprice'], 300)
        self.assertEqual(self.daily()['anomaly_image_null'], 1)

    def test_normal_new_sku_does_not_create_anomaly(self):
        self.sync(self.save_raw(300, sku='new'))
        self.assertIsNone(self.report('new'))

    def test_report_failure_rolls_back_without_losing_saved_crawl(self):
        row = self.save_raw()
        self.db.execute('''CREATE TRIGGER fail_daily BEFORE UPDATE ON ds_monitoring_report_daily
                           BEGIN SELECT RAISE(ABORT, 'offline report failure'); END''')
        result = subject.sync_saved_fnac_results([row])
        self.assertFalse(result['success'])
        self.assertEqual(self.report()['retailprice'], 300)
        self.assertIsNone(self.db.execute('SELECT retailprice FROM fnac_price_crawl_tbl_fr').fetchone()[0])

    def test_other_retailer_is_untouched(self):
        self.db.execute('''INSERT INTO ds_monitoring_report_anomaly
            (crawl_date,retailer_id,retailersku,title,retailprice,is_del)
            VALUES ('2026-09-18',2,'sample','Other retailer',900,0)''')
        self.sync(self.save_raw())
        other = self.db.execute('SELECT retailprice,is_del FROM ds_monitoring_report_anomaly WHERE retailer_id=2').fetchone()
        self.assertEqual(tuple(other), (900, 0))


class SaveOrderingTests(unittest.TestCase):
    def setUp(self):
        source = (ROOT / 'fnac_v3.py').read_text(encoding='utf-8-sig')
        method = next(n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.FunctionDef) and n.name == 'save_to_db')
        namespace = {'pd': pd, 'FNAC_TABLE': 'fnac_price_crawl_tbl_fr', 'logger': logging.getLogger('test.save')}
        exec(compile(ast.Module(body=[method], type_ignores=[]), '<save_to_db>', 'exec'), namespace)
        self.method = namespace['save_to_db']
        self.owner = types.SimpleNamespace(db_engine=object(), error_logs=[], total_call_seconds=0, total_zenrows_calls=0)
        self.df = Mock()
        self.df.__len__ = Mock(return_value=1)
        self.df.iterrows.return_value = []
        self.df.to_dict.return_value = [{'retailersku': 'sample'}]
        self.sync = Mock(return_value={'success': True, 'updated': 1, 'closed_dates': 0})
        module = types.ModuleType('fnac_monitoring')
        module.sync_saved_fnac_results = self.sync
        patcher = patch.dict(sys.modules, {'fnac_monitoring': module})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_committed_insert_happens_before_report_sync(self):
        calls = []
        self.df.to_sql.side_effect = lambda *a, **k: calls.append('insert')
        self.sync.side_effect = lambda *a: calls.append('sync') or {'success': True, 'updated': 1, 'closed_dates': 0}
        self.assertTrue(self.method(self.owner, self.df))
        self.assertEqual(calls, ['insert', 'sync'])

    def test_crawl_save_failure_does_not_sync(self):
        self.df.to_sql.side_effect = RuntimeError('offline database failure')
        self.assertFalse(self.method(self.owner, self.df))
        self.sync.assert_not_called()

    def test_report_failure_does_not_claim_crawl_save_failed(self):
        self.sync.return_value = {'success': False}
        self.assertTrue(self.method(self.owner, self.df))
        self.assertEqual(self.owner.error_logs, ['FNAC monitoring report sync failed'])

    def test_no_db_connection_does_not_sync(self):
        self.owner.db_engine = None
        self.assertFalse(self.method(self.owner, self.df))
        self.sync.assert_not_called()


if __name__ == '__main__':
    unittest.main()
