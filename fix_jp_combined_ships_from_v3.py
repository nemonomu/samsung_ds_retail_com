"""
Widen JP ships_from combined-label predicate to include English fallback
('Shipper / Seller') in addition to Japanese labels.

Why:
  fix_jp_combined_ships_from_v2.py inserted 5 new ships_from selectors
  (id 717-721) at priority 5-9 with predicate:
    [.//*[contains(.,'出荷元') and contains(.,'販売元')]]

  This only fires for Japanese-rendered pages. When Amazon JP serves
  English UI (e.g. /-/en/ URL prefix, or locale-leaked sessions on the
  RDP), the label is 'Shipper / Seller' and the predicate misses.

  User reported B0C6XP42SC, B0DM4JSB4J, B0DM3V95MV, B0DM47M71X,
  B0F2DMYG5R all have ships_from = None when label is in English.

  Existing id 669, 670 already have the right predicate shape
  (with 'or Shipper' / 'or Seller') so we mirror that.

Idempotent: REPLACE-based, only updates rows that still have the
narrow predicate text.
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


NARROW = "[.//*[contains(.,'出荷元') and contains(.,'販売元')]]"
WIDE = (
    "[.//*[(contains(.,'出荷元') or contains(.,'発送元') or contains(.,'Shipper'))"
    " and (contains(.,'販売元') or contains(.,'Seller'))]]"
)


SQL_LIST_BEFORE = """
SELECT id, priority, is_active, selector_value
FROM amazon_selectors
WHERE country_code = 'jp'
  AND element_type = 'ships_from'
  AND selector_value LIKE %s
ORDER BY priority DESC
"""

SQL_UPDATE = """
UPDATE amazon_selectors
SET selector_value = REPLACE(selector_value, %s, %s)
WHERE country_code = 'jp'
  AND element_type = 'ships_from'
  AND selector_value LIKE %s
"""

SQL_VERIFY_AFTER = """
SELECT id, priority, is_active, selector_value
FROM amazon_selectors
WHERE country_code = 'jp'
  AND element_type = 'ships_from'
ORDER BY priority DESC, id
"""


def main():
    conn = pymysql.connect(**DB_CONFIG_V2)
    try:
        cur = conn.cursor()

        like_narrow = f'%{NARROW}%'
        cur.execute(SQL_LIST_BEFORE, (like_narrow,))
        narrow_rows = cur.fetchall()
        print(f'[before] {len(narrow_rows)} rows with narrow JP-only predicate:')
        for r in narrow_rows:
            id_, prio, active, sv = r
            print(f'  id={id_:>4} prio={prio:>3} [{"A" if active else "X"}] {(sv or "")[:120]!r}')

        if not narrow_rows:
            print('[skip] nothing to widen.')
            return

        cur.execute(SQL_UPDATE, (NARROW, WIDE, like_narrow))
        updated = cur.rowcount
        conn.commit()
        print(f'\n[fix] updated {updated} rows.')

        print('\n[verify] all JP ships_from selectors (priority DESC):')
        cur.execute(SQL_VERIFY_AFTER)
        for row in cur.fetchall():
            id_, prio, active, sv = row
            active_mark = 'A' if active else 'X'
            print(f'  id={id_:>4} prio={prio:>3} [{active_mark}] {(sv or "")[:160]!r}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
