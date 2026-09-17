"""No real configuration, DB, storage or browser: verify screenshot registration boundary."""
import ast
from datetime import datetime, timezone
import logging
from pathlib import Path
import types
import unittest
from unittest.mock import Mock


def load_uploader():
    path = Path(__file__).resolve().parents[1] / 'null_screenshot.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef)
            and n.name in ('capture_and_upload', '_delete_existing_screenshots')]
    module = types.ModuleType('offline_uploader')
    module.__dict__.update(datetime=datetime, KST=timezone.utc, logger=logging.getLogger('test.uploader'),
        _capture_bytes=Mock(return_value=b'image'), _is_blank_or_white_screenshot=Mock(return_value=False),
        _add_watermark=Mock(side_effect=lambda data, url: data), _normalize_retailer=lambda value: value,
        _file_sku=lambda value: value, _monitoring_file_path=lambda retailer, date: f'{date}/{retailer}/',
        _get_s3_client=Mock(return_value=Mock()), _get_s3_config=lambda: {'bucket_name': 'test'},
        _insert_monitoring_file_and_anomaly=Mock(return_value=123),
        _is_safe_screenshot_delete_prefix=Mock(return_value=True))
    exec(compile(ast.Module(body=body, type_ignores=[]), str(path), 'exec'), module.__dict__)
    return module


class MonitoringRegistrationTests(unittest.TestCase):
    def setUp(self):
        self.m = load_uploader()
        self.m._delete_existing_screenshots = Mock()
        self.data = {'title': 'SSD', 'retailprice': None}

    def capture(self, strict=True):
        return self.m.capture_and_upload(Mock(), 'fnac', 'example', 'https://example.test/product',
                                         self.data, require_monitoring_link=strict)

    def test_db_failure_is_not_reported_as_success(self):
        self.m._insert_monitoring_file_and_anomaly.return_value = None
        self.assertIsNone(self.capture())
        self.assertEqual(self.m._insert_monitoring_file_and_anomaly.call_count, 2)
        self.m._get_s3_client().put_object.assert_called_once()
        self.m._capture_bytes.assert_called_once()
        self.m._delete_existing_screenshots.assert_not_called()
        self.assertEqual(self.data['_screenshot_reason'], 'monitoring_link_failed')

    def test_db_retry_reuses_uploaded_image(self):
        self.m._insert_monitoring_file_and_anomaly.side_effect = [None, 123]
        key = self.capture()
        self.assertTrue(key)
        self.m._get_s3_client().put_object.assert_called_once()
        self.m._capture_bytes.assert_called_once()
        self.assertEqual(self.m._delete_existing_screenshots.call_args.kwargs['keep_key'], key)

    def test_failed_storage_does_not_delete_previous_evidence(self):
        self.m._get_s3_client().put_object.side_effect = RuntimeError('synthetic failure')
        self.assertIsNone(self.capture())
        self.m._delete_existing_screenshots.assert_not_called()
        self.m._insert_monitoring_file_and_anomaly.assert_not_called()

    def test_legacy_callers_keep_existing_contract(self):
        self.m._insert_monitoring_file_and_anomaly.return_value = None
        self.assertTrue(self.capture(strict=False))
        self.m._insert_monitoring_file_and_anomaly.assert_called_once()
        self.assertEqual(self.m._delete_existing_screenshots.call_args.kwargs, {})

    def test_cleanup_keeps_newly_registered_key(self):
        module = load_uploader()
        client = Mock()
        client.list_objects_v2.return_value = {'Contents': [{'Key': 'old'}, {'Key': 'new'}]}
        self.assertEqual(module._delete_existing_screenshots(client, 'test', 'prefix', keep_key='new'), 1)
        self.assertEqual(client.delete_objects.call_args.kwargs['Delete']['Objects'], [{'Key': 'old'}])

    def test_blank_picture_is_never_registered(self):
        self.m._is_blank_or_white_screenshot.return_value = True
        self.assertIsNone(self.capture())
        self.m._get_s3_client().put_object.assert_not_called()
        self.m._insert_monitoring_file_and_anomaly.assert_not_called()


if __name__ == '__main__':
    unittest.main()
