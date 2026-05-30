"""Inspect NL ships_from selectors to find why combined-label
'Verzender / Verkoper' rows fail (sold_by matches, ships_from NULL).
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


def main():
    conn = pymysql.connect(**DB_CONFIG_V2)
    cur = conn.cursor()

    print('=' * 100)
    print('NL ships_from selectors (priority DESC):')
    print('=' * 100)
    cur.execute(
        "SELECT id, priority, is_active, selector_value "
        "FROM amazon_selectors "
        "WHERE country_code='nl' AND element_type='ships_from' "
        "ORDER BY priority DESC, id"
    )
    for row in cur.fetchall():
        id_, prio, active, sv = row
        active_mark = 'A' if active else 'X'
        has_combined = 'Verzender' in (sv or '') and 'Verkoper' in (sv or '')
        marker = '  <-- combined' if has_combined else ''
        print(f'  id={id_:>4} prio={prio:>3} [{active_mark}] {sv!r}{marker}')

    print()
    print('=' * 100)
    print('NL sold_by selectors (for comparison):')
    print('=' * 100)
    cur.execute(
        "SELECT id, priority, is_active, selector_value "
        "FROM amazon_selectors "
        "WHERE country_code='nl' AND element_type='sold_by' "
        "ORDER BY priority DESC, id"
    )
    for row in cur.fetchall():
        id_, prio, active, sv = row
        active_mark = 'A' if active else 'X'
        has_combined = 'Verzender' in (sv or '') and 'Verkoper' in (sv or '')
        marker = '  <-- combined' if has_combined else ''
        print(f'  id={id_:>4} prio={prio:>3} [{active_mark}] {sv!r}{marker}')

    conn.close()


if __name__ == '__main__':
    main()
