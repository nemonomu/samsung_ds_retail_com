"""
Strip the leading '(' and trailing ')[1]' wrapping from the 36 selectors
inserted by amazon_combined_label_selectors_topup.sql.

Why:
  Production v2 crawlers (in_v2.py, fr_v2.py, it_v2.py, uk_v2.py, usa_v2.py,
  nl.py) dispatch selectors with `if selector.startswith('//'):` only.
  Anything starting with '(' falls through to CSS selector and raises
  "invalid selector" in Selenium, so the new combined-label / used-buy
  selectors never get tried.

  Stripping '(...)[1]' to plain '//xpath' makes the same XPath work
  uniformly across all per-country v2 files. Semantically equivalent
  for our use because extract_element_text() iterates the returned list
  and picks the first displayed match.

Idempotent: only updates rows whose selector_value starts with '(' and
ends with ')[1]'.
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


SQL_UPDATE = """
UPDATE samsung_ds_retail_com.amazon_selectors
SET selector_value = SUBSTRING(selector_value, 2, CHAR_LENGTH(selector_value) - 5)
WHERE country_code IN ('fr', 'it', 'in', 'gb', 'usa', 'es', 'nl')
  AND element_type IN ('ships_from', 'sold_by')
  AND priority BETWEEN 122 AND 126
  AND selector_value LIKE '(/%'
  AND selector_value LIKE '%)[1]'
"""

SQL_VERIFY = """
SELECT country_code, element_type, priority, LEFT(selector_value, 100) AS selector_preview
FROM samsung_ds_retail_com.amazon_selectors
WHERE country_code IN ('fr', 'it', 'in', 'gb', 'usa', 'es', 'nl')
  AND element_type IN ('ships_from', 'sold_by')
  AND priority BETWEEN 122 AND 126
ORDER BY country_code, element_type, priority DESC
"""


def main():
    conn = pymysql.connect(**DB_CONFIG_V2)
    try:
        cur = conn.cursor()
        cur.execute(SQL_UPDATE)
        updated = cur.rowcount
        conn.commit()
        print(f'[fix] updated {updated} rows (stripped leading "(" and trailing ")[1]")')

        cur.execute(SQL_VERIFY)
        rows = cur.fetchall()
        print(f'\n[verify] {len(rows)} rows in priority 122-126 range:')
        for r in rows:
            cc, etype, prio, preview = r
            starts_open_paren = preview.startswith('(')
            marker = ' <-- still wrapped!' if starts_open_paren else ''
            print(f'  {cc} {etype} prio={prio} {preview!r}{marker}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
