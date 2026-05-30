"""
Strip leading '(' and trailing ')[1]' from legacy used-layout selectors
(priority 119-121) in amazon_selectors.

Why:
  fix_topup_strip_parens.py handled only priority 122-126 (Phase A topup
  inserts). The earlier used-layout INSERT batch (priority 119-121) also
  uses (...)[1] wrapping. Production v2 dispatch rejects '(' as XPath
  (`if selector.startswith('//')`), so these never match.

  NL diagnosis (2026-05-30):
    - id 634 (NL ships_from prio 119) wrapped -> rejected
    - id 635 (NL sold_by   prio 120) wrapped -> rejected
    - id 633 (NL ships_from prio 120) wrapped -> rejected
  Result: NL used-combined ASINs (B0CTRVZKG7, B0CK2S298S) lose
  ships_from extraction because no priority 119-120 selector executes;
  lower-priority fallbacks have no sellerProfileTriggerId-based ships_from.

Idempotent: only updates rows whose selector_value starts with '(' and
ends with ')[1]'.

Lists candidates first, then applies update, then verifies.
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


SQL_LIST_CANDIDATES = """
SELECT id, country_code, element_type, priority,
       LEFT(selector_value, 80) AS preview
FROM amazon_selectors
WHERE element_type IN ('ships_from', 'sold_by')
  AND selector_value LIKE '(/%'
  AND selector_value LIKE '%)[1]'
ORDER BY country_code, element_type, priority DESC
"""

SQL_UPDATE = """
UPDATE amazon_selectors
SET selector_value = SUBSTRING(selector_value, 2, CHAR_LENGTH(selector_value) - 5)
WHERE element_type IN ('ships_from', 'sold_by')
  AND selector_value LIKE '(/%'
  AND selector_value LIKE '%)[1]'
"""

SQL_VERIFY = """
SELECT id, country_code, element_type, priority,
       LEFT(selector_value, 80) AS preview
FROM amazon_selectors
WHERE element_type IN ('ships_from', 'sold_by')
  AND (selector_value LIKE '(/%' OR selector_value LIKE '%)[1]')
ORDER BY country_code, element_type, priority DESC
"""


def main():
    conn = pymysql.connect(**DB_CONFIG_V2)
    try:
        cur = conn.cursor()

        cur.execute(SQL_LIST_CANDIDATES)
        candidates = cur.fetchall()
        print(f'[before] {len(candidates)} wrapped selectors:')
        for r in candidates:
            id_, cc, etype, prio, preview = r
            print(f'  id={id_:>4} {cc:<4} {etype:<11} prio={prio:>3} {preview!r}')

        if not candidates:
            print('[skip] nothing to fix.')
            return

        cur.execute(SQL_UPDATE)
        updated = cur.rowcount
        conn.commit()
        print(f'\n[fix] updated {updated} rows.')

        cur.execute(SQL_VERIFY)
        remaining = cur.fetchall()
        print(f'\n[verify] {len(remaining)} still wrapped after fix:')
        for r in remaining:
            id_, cc, etype, prio, preview = r
            print(f'  id={id_:>4} {cc:<4} {etype:<11} prio={prio:>3} {preview!r}')
        if not remaining:
            print('  (clean)')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
