"""
Fix JP combined-label predicate to use '//*' instead of '//div' for the
label element match — mirrors the working NL pattern (id 715).

Why:
  JP ships_from combined-label selectors (id 669, 670) use
  `//div[contains(@class,'offer-display-feature-label')]` as the label
  predicate. NL's working equivalent uses `//*[contains(...)]` so it
  matches a <span>, <div>, or any element carrying the class.

  Symptom: JP combined-label pages ('出荷元 / 販売元: <seller>') extract
  sold_by but leave ships_from = None after Phase B refactor (old code
  used hardcoded combined-label branch that didn't care about the label
  element type).

Idempotent. Only matches rows that still have '//div[contains'. Updates
both ships_from and sold_by (sold_by has no combined-predicate rows
today but if any get added later with the same bug, this fixes them).

Lists candidates first, then applies update, then verifies.
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


SQL_LIST_CANDIDATES = """
SELECT id, country_code, element_type, priority,
       LEFT(selector_value, 100) AS preview
FROM amazon_selectors
WHERE country_code = 'jp'
  AND element_type IN ('ships_from', 'sold_by')
  AND selector_value LIKE '%[.//div[contains(@class,\\'offer-display-feature-label\\')%'
ORDER BY country_code, element_type, priority DESC
"""

SQL_UPDATE = """
UPDATE amazon_selectors
SET selector_value = REPLACE(
    selector_value,
    '[.//div[contains(@class,\\'offer-display-feature-label\\')',
    '[.//*[contains(@class,\\'offer-display-feature-label\\')'
)
WHERE country_code = 'jp'
  AND element_type IN ('ships_from', 'sold_by')
  AND selector_value LIKE '%[.//div[contains(@class,\\'offer-display-feature-label\\')%'
"""

SQL_VERIFY_ALL = """
SELECT id, country_code, element_type, priority,
       LEFT(selector_value, 200) AS preview
FROM amazon_selectors
WHERE country_code = 'jp'
  AND element_type IN ('ships_from', 'sold_by')
  AND selector_value LIKE '%offer-display-feature-label%'
ORDER BY country_code, element_type, priority DESC
"""


def main():
    conn = pymysql.connect(**DB_CONFIG_V2)
    try:
        cur = conn.cursor()

        cur.execute(SQL_LIST_CANDIDATES)
        candidates = cur.fetchall()
        print(f'[before] {len(candidates)} JP rows with //div label predicate:')
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

        cur.execute(SQL_VERIFY_ALL)
        all_rows = cur.fetchall()
        print(f'\n[verify] {len(all_rows)} JP combined-label rows total:')
        for r in all_rows:
            id_, cc, etype, prio, preview = r
            has_div = '[.//div[contains(@class,' in (preview or '')
            mark = '  STILL HAS //div!' if has_div else '  OK'
            print(f'  id={id_:>4} {cc:<4} {etype:<11} prio={prio:>3} {preview!r}{mark}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
