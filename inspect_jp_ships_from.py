"""Inspect JP ships_from selectors to find why combined-label
'shukkamoto / hanbaimoto' (出荷元 / 販売元) pages fail to populate
ships_from after Phase B refactor.

Old jp_v2.py combined-label branch merged sold_by + ships_from selector
lists and assigned the matched value to both fields. Phase B replaced
this with separate ships_from / sold_by extraction, which assumes both
element_type rows have combined-label predicates. If only sold_by has
the predicate, combined-label pages now leak ships_from.
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


def has_combined_predicate(sv):
    if not sv:
        return False
    s = sv
    # JP native labels
    if '出荷元' in s and '販売元' in s:
        return True
    # English fallback
    if 'Shipper' in s and 'Seller' in s:
        return True
    return False


def dump(cur, element_type):
    print('=' * 110)
    print(f'JP {element_type} selectors (priority DESC):')
    print('=' * 110)
    cur.execute(
        "SELECT id, priority, is_active, selector_value "
        "FROM amazon_selectors "
        "WHERE country_code='jp' AND element_type=%s "
        "ORDER BY priority DESC, id",
        (element_type,)
    )
    rows = cur.fetchall()
    combined_count = 0
    for row in rows:
        id_, prio, active, sv = row
        active_mark = 'A' if active else 'X'
        marker = ''
        if has_combined_predicate(sv):
            marker = '  <-- combined-label predicate'
            combined_count += 1
        starts_paren = sv and sv.startswith('(')
        wrap_marker = '  WRAPPED!' if starts_paren else ''
        print(f'  id={id_:>4} prio={prio:>3} [{active_mark}] {sv!r}{marker}{wrap_marker}')
    print(f'  -> {combined_count} of {len(rows)} rows have combined-label predicate')
    print()


def main():
    conn = pymysql.connect(**DB_CONFIG_V2)
    cur = conn.cursor()
    dump(cur, 'ships_from')
    dump(cur, 'sold_by')
    conn.close()


if __name__ == '__main__':
    main()
