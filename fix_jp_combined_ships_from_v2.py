"""
Add JP ships_from selectors that mirror working sold_by absolute paths
but with a combined-label text predicate.

Why:
  Previous fix (//div -> //*) made the label predicate match but the
  VALUE selector `//a[@id='sellerProfileTriggerId']` does NOT exist
  when Amazon itself is the seller (no seller profile page link).

  JP combined-label page B0DM47M71X:
    sold_by extracted 'Amazon.co.jp' via one of prio-50 absolute paths
    (id 622-624: //*[@id='merchantInfoFeature_feature_div']/div[2]/...)
    ships_from = None (sellerProfileTriggerId doesn't exist)

  Solution: clone those working absolute paths to ships_from but only
  fire for combined-label pages (text-based predicate, no class
  assumption since Amazon JP class names may differ from NL).

Selectors added (priority 5-7, above existing combined-predicate prio
3-4; below sold_by prio 50 — same scale as existing JP ships_from):
  prio 7: /div[2]/span         (most lenient, last-resort)
  prio 6: /div[2]/span/a/span  (link wrapping case)
  prio 5: /div[2]/span/span    (nested span case)

Predicate: `[.//*[contains(.,'出荷元') and contains(.,'販売元')]]` —
matches a single descendant element whose text contains BOTH label
words. Separate-label pages won't satisfy this (label words live in
distinct elements), so these selectors only fire for combined labels.

Idempotent: skips insert if exact same selector_value already exists
for jp/ships_from.
"""
import os
import sys

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


PREDICATE_REG = (
    "[.//*[contains(.,'出荷元') and contains(.,'販売元')]]"
)
PREDICATE_USED = PREDICATE_REG  # same — only the anchor div id differs

REGULAR_ANCHOR = "//*[@id='merchantInfoFeature_feature_div']"
USED_ANCHOR = "//*[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']"


NEW_SELECTORS = [
    # priority, xpath
    (7, f"{REGULAR_ANCHOR}{PREDICATE_REG}/div[2]/span"),
    (6, f"{REGULAR_ANCHOR}{PREDICATE_REG}/div[2]/span/a/span"),
    (5, f"{REGULAR_ANCHOR}{PREDICATE_REG}/div[2]/span/span"),
    (8, f"{USED_ANCHOR}{PREDICATE_USED}/div[2]/span"),
    (9, f"{USED_ANCHOR}{PREDICATE_USED}/div[2]/span/a/span"),
]


SQL_INSERT = """
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'jp', 'ships_from', %s, %s, 1
WHERE NOT EXISTS (
    SELECT 1 FROM amazon_selectors
    WHERE country_code = 'jp'
      AND element_type = 'ships_from'
      AND selector_value = %s
)
"""

SQL_VERIFY = """
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

        inserted = 0
        for prio, xpath in NEW_SELECTORS:
            cur.execute(SQL_INSERT, (xpath, prio, xpath))
            if cur.rowcount:
                inserted += 1
                print(f'[insert] prio={prio} {xpath!r}')
            else:
                print(f'[skip]   prio={prio} already exists: {xpath!r}')
        conn.commit()
        print(f'\n[done] inserted {inserted} of {len(NEW_SELECTORS)} new rows.')

        print('\n[verify] all JP ships_from selectors (priority DESC):')
        cur.execute(SQL_VERIFY)
        for row in cur.fetchall():
            id_, prio, active, sv = row
            active_mark = 'A' if active else 'X'
            print(f'  id={id_:>4} prio={prio:>3} [{active_mark}] {(sv or "")[:120]!r}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
