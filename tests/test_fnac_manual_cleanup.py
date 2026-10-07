"""Photo-only cleanup and committed report sync, using an isolated SQLite DB."""
import ast
from datetime import date, datetime, timezone
import logging
from pathlib import Path
import types
import unittest
from unittest.mock import Mock

import test_fnac_report_sync as report_fixtures
from test_fnac_manual_recovery import load_manual

ROOT = Path(__file__).resolve().parents[1]


class CleanupCursor(report_fixtures.SyncCursor):
    @property
    def rowcount(self):
        return self.inner.rowcount


class CleanupConnection(report_fixtures.SyncConnection):
    def cursor(self):
        return CleanupCursor(self.db)


class ManualCleanupTests(unittest.TestCase):
    def setUp(self):
        self.case = report_fixtures.ReportSyncTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.db = self.case.db
        self.db.executescript('''
            CREATE TABLE ds_monitoring_file (
                file_id INTEGER PRIMARY KEY, file_path TEXT, file_name TEXT,
                is_del INTEGER, updated_at TEXT, updated_id TEXT);
            INSERT INTO ds_monitoring_file VALUES
                (7,'2026/202609/20260918/fnac/','fnac_sample_old.png',0,NULL,NULL);
        ''')
        tree = ast.parse((ROOT / 'null_screenshot.py').read_text(encoding='utf-8-sig'))
        nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name in {'_delete_monitoring_db_records', 'delete_screenshots_for_sku'}]
        self.helper = types.ModuleType('offline_photo_cleanup')
        self.helper.__dict__.update(
            datetime=datetime, KST=timezone.utc, S3_PATH_PREFIX='screenshots',
            MONITORING_CREATED_ID='offline-test', logger=logging.getLogger('test.cleanup'),
            _file_sku=str, _normalize_retailer=lambda value: value,
            _monitoring_file_path=lambda retailer, day: f'{day[:4]}/{day[:6]}/{day}/{retailer}/',
            _monitoring_date_parts=lambda day: (None, None, None, date.fromisoformat(f'{day[:4]}-{day[4:6]}-{day[6:]}')),
            _get_db_connection=lambda: CleanupConnection(self.db),
            _get_monitoring_target=lambda *_: (1, 'Fnac', 'FR'),
            _get_s3_client=Mock(return_value=Mock()),
            _get_s3_config=lambda: {'bucket_name': 'offline-bucket'},
            _delete_existing_screenshots=Mock(return_value=1),
        )
        exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
                     '<actual-cleanup-functions>', 'exec'), self.helper.__dict__)
        self.manual, _ = load_manual()
        self.manual.delete_screenshots_for_sku = self.helper.delete_screenshots_for_sku
        self.manual.sync_saved_fnac_results = report_fixtures.subject.sync_saved_fnac_results
        self.scraper = self.manual.FnacManualRecoveryScraper()

    def finalize(self, **overrides):
        result = self.case.save_raw(300, **overrides)
        result['kr_crawl_datetime'] = '2026-09-18 17:00:00'
        self.scraper.finalize_saved_result(result, '2026-09-18 08:00:00')
        return result

    def test_price_recovered_image_missing_preserves_same_anomaly_and_notes(self):
        old_id = self.case.report()['id']
        result = self.finalize(imageurl=None)
        row = self.case.report()
        self.assertEqual((row['id'], row['is_del']), (old_id, 0))
        self.assertEqual((row['cause'], row['memo']), ('User cause', 'User memo'))
        self.assertEqual(row['retailprice'], 300)
        self.assertIsNone(row['imageurl'])
        self.assertIsNone(row['screenshot_id'])
        self.assertEqual(self.case.daily()['anomaly_image_null'], 1)
        self.assertEqual(result['_s3_upload'], 'skip')
        self.assertEqual(self.db.execute('SELECT count(*) FROM ds_monitoring_report_anomaly').fetchone()[0], 1)
        self.manual.capture_and_upload.assert_not_called()

    def test_fully_normal_recovery_retires_anomaly_after_sync_and_updates_count(self):
        self.finalize()
        row = self.case.report()
        self.assertEqual(row['is_del'], 1)
        self.assertEqual((row['cause'], row['memo']), ('User cause', 'User memo'))
        self.assertIsNone(row['screenshot_id'])
        self.assertEqual(self.case.daily()['anomaly_total'], 0)

    def test_photo_only_cleanup_unlinks_photo_without_retiring_anomaly(self):
        self.helper.delete_screenshots_for_sku('fnac', 'sample', '20260918', preserve_anomaly=True)
        row = self.case.report()
        self.assertEqual(row['is_del'], 0)
        self.assertIsNone(row['screenshot_id'])
        self.assertEqual((row['cause'], row['memo']), ('User cause', 'User memo'))
        self.assertEqual(self.db.execute('SELECT is_del FROM ds_monitoring_file WHERE file_id=7').fetchone()[0], 1)

    def test_default_cleanup_retains_previous_behavior_for_other_callers(self):
        self.helper.delete_screenshots_for_sku('fnac', 'sample', '20260918')
        self.assertEqual(self.case.report()['is_del'], 1)
        self.assertIsNone(self.case.report()['screenshot_id'])

    def test_other_sku_date_and_retailer_are_untouched(self):
        self.db.executescript('''
            INSERT INTO ds_monitoring_report_anomaly
                (crawl_date,retailer_id,retailersku,screenshot_id,is_del,cause,memo)
            VALUES ('2026-09-18',1,'other-sku',8,0,'Other cause','Other memo'),
                   ('2026-09-17',1,'sample',9,0,'Old cause','Old memo'),
                   ('2026-09-18',2,'sample',10,0,'Other retailer','Other memo');
        ''')
        self.helper.delete_screenshots_for_sku('fnac', 'sample', '20260918', preserve_anomaly=True)
        others = self.db.execute('SELECT screenshot_id,is_del FROM ds_monitoring_report_anomaly WHERE screenshot_id IS NOT NULL ORDER BY screenshot_id').fetchall()
        self.assertEqual([tuple(row) for row in others], [(8, 0), (9, 0), (10, 0)])

    def test_db_cleanup_failure_rolls_back_photo_link_and_notes(self):
        self.db.execute('''CREATE TRIGGER fail_cleanup BEFORE UPDATE ON ds_monitoring_report_anomaly
                           BEGIN SELECT RAISE(ABORT, 'offline cleanup failure'); END''')
        deleted = self.helper._delete_monitoring_db_records('fnac', 'sample', '20260918', preserve_anomaly=True)
        self.assertEqual(deleted, 0)
        row = self.case.report()
        self.assertEqual((row['screenshot_id'], row['is_del']), (7, 0))
        self.assertEqual((row['cause'], row['memo']), ('User cause', 'User memo'))
        self.assertEqual(self.db.execute('SELECT is_del FROM ds_monitoring_file WHERE file_id=7').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
