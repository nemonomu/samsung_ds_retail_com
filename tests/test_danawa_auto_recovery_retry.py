import ast
from pathlib import Path
from unittest.mock import Mock

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _load_retry_functions():
    tree = ast.parse((ROOT / 'auto_recovery.py').read_text(encoding='utf-8-sig'))
    names = {
        '_is_blank_recovery_value',
        '_merge_nonblank_recovery_result',
        '_recrawl_null_record',
    }
    nodes = [node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    module = ast.Module(body=nodes, type_ignores=[])
    namespace = {
        'pd': pd,
        'logger': Mock(),
        'time': Mock(),
    }
    exec(compile(module, 'auto_recovery.py', 'exec'), namespace)
    return namespace


def test_danawa_retries_and_merges_partial_title_and_image_results():
    ns = _load_retry_functions()
    manager = Mock()
    manager.recrawl_url.side_effect = [
        {'title': 'Danawa title', 'imageurl': None, 'retailprice': 1000},
        {'title': None, 'imageurl': 'https://img.example/item.jpg', 'retailprice': None},
    ]

    result, attempts = ns['_recrawl_null_record'](
        manager, Mock(), 'https://example/item',
        pd.Series({'title': None, 'imageurl': None}), 'danawa'
    )

    assert attempts == 2
    assert manager.recrawl_url.call_count == 2
    assert result['title'] == 'Danawa title'
    assert result['imageurl'] == 'https://img.example/item.jpg'
    assert result['retailprice'] == 1000


def test_danawa_stops_after_ten_attempts_when_identity_fields_stay_null():
    ns = _load_retry_functions()
    manager = Mock()
    manager.recrawl_url.return_value = {'title': None, 'imageurl': ''}

    result, attempts = ns['_recrawl_null_record'](
        manager, Mock(), 'https://example/item',
        pd.Series({'title': None, 'imageurl': None}), 'danawa'
    )

    assert attempts == 10
    assert manager.recrawl_url.call_count == 10
    assert ns['time'].sleep.call_count == 9
    assert result is None


def test_other_retailers_keep_single_attempt_for_title_or_image_null():
    ns = _load_retry_functions()
    manager = Mock()
    manager.recrawl_url.return_value = {'title': None, 'imageurl': None}

    _, attempts = ns['_recrawl_null_record'](
        manager, Mock(), 'https://example/item',
        pd.Series({'title': None, 'imageurl': None}), 'it'
    )

    assert attempts == 1
    assert manager.recrawl_url.call_count == 1


def test_danawa_price_only_null_keeps_single_identity_attempt():
    ns = _load_retry_functions()
    manager = Mock()
    manager.recrawl_url.return_value = {
        'title': 'Danawa title',
        'imageurl': 'https://img.example/item.jpg',
        'retailprice': None,
    }

    _, attempts = ns['_recrawl_null_record'](
        manager, Mock(), 'https://example/item',
        pd.Series({
            'title': 'Danawa title',
            'imageurl': 'https://img.example/item.jpg',
            'retailprice': None,
        }),
        'danawa',
    )

    assert attempts == 1
    assert manager.recrawl_url.call_count == 1


def test_danawa_preserves_existing_title_when_only_image_is_recovered():
    ns = _load_retry_functions()
    manager = Mock()
    manager.recrawl_url.return_value = {
        'title': None,
        'imageurl': 'https://img.example/item.jpg',
        'retailprice': None,
    }

    result, attempts = ns['_recrawl_null_record'](
        manager, Mock(), 'https://example/item',
        pd.Series({'title': 'Existing title', 'imageurl': None}),
        'danawa',
    )

    assert attempts == 1
    assert result['title'] == 'Existing title'
    assert result['imageurl'] == 'https://img.example/item.jpg'


def test_price_retry_cannot_erase_recovered_title_or_image():
    ns = _load_retry_functions()
    recovered = {
        'title': 'Recovered title',
        'imageurl': 'https://img.example/recovered.jpg',
        'retailprice': None,
    }
    price_retry = {
        'title': None,
        'imageurl': None,
        'retailprice': 123000,
    }

    merged = ns['_merge_nonblank_recovery_result'](recovered, price_retry)

    assert merged['title'] == 'Recovered title'
    assert merged['imageurl'] == 'https://img.example/recovered.jpg'
    assert merged['retailprice'] == 123000
