"""Refresh FNAC report rows after the crawl rows have been committed.

No network fetch or screenshot capture. Only affected date/SKU pairs are synced.
Unrelated retailers, dates, user causes, memos and daily collection stats stay intact.
"""
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import logging

logger = logging.getLogger(__name__)
FIELDS = ('country_code', 'title', 'retailprice', 'ships_from', 'sold_by', 'imageurl', 'producturl')


def quality_flags(row):
    """Match Layer 2 report anomaly conditions, not screenshot eligibility."""
    def missing(key):
        value = row.get(key)
        return value is None or not str(value).strip()

    title_null, image_null = missing('title'), missing('imageurl')
    null_union = title_null or image_null
    image_valid = not image_null and str(row['imageurl']).startswith('https://')
    image_invalid = not null_union and not image_valid
    try:
        zero = not title_null and Decimal(str(row.get('retailprice')).lstrip('$')) == 0
    except (InvalidOperation, ValueError):
        zero = False
    absent = [missing(k) for k in ('retailprice', 'ships_from', 'sold_by')]
    partial = not title_null and image_valid and any(absent) and not all(absent)
    return {
        'anomaly_total': int(null_union) + int(image_invalid) + int(zero) + int(partial),
        'anomaly_title_null': int(null_union),
        'anomaly_image_null': int(image_null),
        'anomaly_partial_null': int(partial),
        'anomaly_price_zero': int(zero),
    }


def sync_saved_fnac_results(records):
    # Import production adapters only when invoked after a successful DB save.
    from null_screenshot import _get_db_connection, _get_monitoring_target, _result_value
    from crawler_session import MONITORING_CREATED_ID

    result = {'success': False, 'updated': 0, 'closed_dates': 0}
    groups = {}
    for row in records:
        stamp = str(row.get('crawl_strdatetime') or '')
        day = datetime.strptime(stamp[:8], '%Y%m%d').date()
        sku = str(row.get('retailersku') or '').strip()
        if not sku:
            raise ValueError('Cannot sync a saved FNAC row without SKU')
        groups.setdefault(day, set()).add(sku)
    if not groups:
        result['success'] = True
        return result

    conn = None
    try:
        conn = _get_db_connection()
        with conn.cursor() as cursor:
            target = _get_monitoring_target(cursor, 'fnac')
            if not target:
                raise RuntimeError('FNAC monitoring target missing')
            retailer_id = target[0]
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for day, skus in sorted(groups.items()):
                cursor.execute('''SELECT is_closed FROM ssd_crawl_db.ds_monitoring_report_close
                                  WHERE crawl_date = %s''', (day,))
                closed = cursor.fetchone()
                if closed and closed[0]:
                    result['closed_dates'] += 1
                    continue

                # Read committed values, not an earlier in-memory candidate.
                # Latest per SKU also prevents an older finished batch winning.
                placeholders = ','.join(['%s'] * len(skus))
                cursor.execute(f'''
                    SELECT retailersku, country_code, title, retailprice, ships_from,
                           sold_by, imageurl, producturl, crawl_strdatetime
                    FROM samsung_ds_retail_com.fnac_price_crawl_tbl_fr
                    WHERE crawl_strdatetime >= %s AND crawl_strdatetime < %s
                      AND retailersku IN ({placeholders})
                    ORDER BY crawl_strdatetime DESC
                ''', (day.strftime('%Y%m%d') + '0000',
                      (day + timedelta(days=1)).strftime('%Y%m%d') + '0000', *sorted(skus)))
                latest = {}
                for values in cursor.fetchall():
                    sku = str(values[0]).strip()
                    if sku not in latest:
                        raw = dict(zip(FIELDS, values[1:8]))
                        latest[sku] = {key: _result_value(raw, key) for key in FIELDS}
                if set(latest) != skus:
                    raise RuntimeError('Committed FNAC rows unavailable for report sync')

                for sku, row in latest.items():
                    flags = quality_flags(row)
                    cursor.execute('''
                        SELECT id FROM ssd_crawl_db.ds_monitoring_report_anomaly
                        WHERE crawl_date = %s AND retailer_id = %s AND retailersku = %s AND is_del = 0
                    ''', (day, retailer_id, sku))
                    existing = cursor.fetchall()
                    if existing:
                        # Explicit NULL is a value. Preserve review notes and photo links.
                        cursor.execute('''
                            UPDATE ssd_crawl_db.ds_monitoring_report_anomaly
                            SET country_code=%s, title=%s, retailprice=%s, ships_from=%s,
                                sold_by=%s, imageurl=%s, producturl=%s, is_del=%s,
                                updated_at=%s, updated_id=%s
                            WHERE crawl_date=%s AND retailer_id=%s AND retailersku=%s AND is_del=0
                        ''', (*(row[k] for k in FIELDS), int(not flags['anomaly_total']),
                              now, MONITORING_CREATED_ID, day, retailer_id, sku))
                    elif flags['anomaly_total']:
                        cursor.execute('''
                            INSERT INTO ssd_crawl_db.ds_monitoring_report_anomaly
                            (crawl_date, retailer_id, retailersku, country_code, title,
                             retailprice, ships_from, sold_by, imageurl, producturl,
                             screenshot_id, cause, memo, is_del, created_at, created_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,'','',0,%s,%s)
                        ''', (day, retailer_id, sku, *(row[k] for k in FIELDS), now, MONITORING_CREATED_ID))
                    result['updated'] += 1

                cursor.execute('''SELECT title, retailprice, ships_from, sold_by, imageurl
                                  FROM ssd_crawl_db.ds_monitoring_report_anomaly
                                  WHERE crawl_date=%s AND retailer_id=%s AND is_del=0''', (day, retailer_id))
                counts = dict.fromkeys(quality_flags({}), 0)
                for values in cursor.fetchall():
                    flags = quality_flags(dict(zip(('title', 'retailprice', 'ships_from', 'sold_by', 'imageurl'), values)))
                    for key, value in flags.items():
                        counts[key] += value
                # Existing reports only: retain the monitor's explicit first-save workflow.
                cursor.execute('''
                    UPDATE ssd_crawl_db.ds_monitoring_report_daily
                    SET anomaly_total=%s, anomaly_title_null=%s, anomaly_image_null=%s,
                        anomaly_partial_null=%s, anomaly_price_zero=%s, updated_at=%s, updated_id=%s
                    WHERE crawl_date=%s AND retailer_id=%s AND is_del=0
                ''', (*counts.values(), now, MONITORING_CREATED_ID, day, retailer_id))
            conn.commit()
        result['success'] = True
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        result['updated'] = 0
        logger.warning('FNAC monitoring sync failed error=%s', type(exc).__name__)
    finally:
        if conn is not None:
            conn.close()
    return result
