import ast
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]


def _load_image_helper():
    tree = ast.parse((ROOT / 'danawa_v2.py').read_text(encoding='utf-8-sig'))
    node = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == '_get_element_image_url'
    )
    namespace = {}
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), 'danawa_v2.py', 'exec'),
        namespace,
    )
    return namespace['_get_element_image_url']


def _element(**attributes):
    element = Mock()
    element.get_attribute.side_effect = lambda name: attributes.get(name)
    return element


def test_image_helper_prefers_normal_src():
    get_url = _load_image_helper()

    assert get_url(_element(src='https://img.example/main.jpg')) == (
        'https://img.example/main.jpg', 'src'
    )


def test_image_helper_falls_back_when_src_is_empty():
    get_url = _load_image_helper()

    assert get_url(_element(src='', **{
        'data-src': 'https://img.example/lazy.jpg'
    })) == ('https://img.example/lazy.jpg', 'data-src')


def test_image_helper_uses_first_srcset_candidate():
    get_url = _load_image_helper()

    assert get_url(_element(
        srcset='https://img.example/340.jpg 340w, '
               'https://img.example/680.jpg 680w'
    )) == ('https://img.example/340.jpg', 'srcset')


def test_image_helper_rejects_elements_without_any_url():
    get_url = _load_image_helper()

    assert get_url(_element()) == (None, None)
