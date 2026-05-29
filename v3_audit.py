# -*- coding: utf-8 -*-
"""
Post-run artifact audit for Amazon v3 crawlers.

The audit compares the same crawl result across:
- in-memory result DataFrame
- local result CSV
- file-server source CSV copied before upload
- DB rows written by the crawler
- crawler log values
- saved HTML DOM values

Only mismatched SKUs keep HTML/screenshot evidence under output_dir/error/<SKU>.
Matched SKUs have temporary HTML/screenshot evidence removed.
"""

import csv
import json
import logging
import os
import re
import shutil
from datetime import datetime
from html import unescape
from html.parser import HTMLParser

import pandas as pd
from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)

AUDIT_FIELDS = ["title", "retailprice", "ships_from", "sold_by"]
NULL_STRINGS = {"", "none", "nan", "null", "nat"}


def _is_empty(value):
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip().casefold() in NULL_STRINGS


def _normalize_text(value):
    if _is_empty(value):
        return None
    text_value = unescape(str(value))
    text_value = re.sub(r"\s+", " ", text_value).strip()
    return text_value or None


def _normalize_price(value):
    if _is_empty(value):
        return None
    text_value = str(value).strip()
    text_value = re.sub(r"[^\d,.\-]", "", text_value)
    if not text_value:
        return None
    if "," in text_value and "." in text_value:
        text_value = text_value.replace(".", "").replace(",", ".")
    elif "," in text_value:
        text_value = text_value.replace(",", ".")
    try:
        return f"{float(text_value):.2f}"
    except Exception:
        return None


def _normalize_value(field, value):
    if field == "retailprice":
        return _normalize_price(value)
    return _normalize_text(value)


def _display_value(value):
    normalized = _normalize_text(value)
    return normalized if normalized is not None else None


def _safe_sku(value):
    text_value = _normalize_text(value) or "unknown"
    return re.sub(r"[^A-Za-z0-9_-]+", "_", text_value).strip("_") or "unknown"


def _lowercase_columns(df):
    if df is None:
        return None
    copy_df = df.copy()
    copy_df.columns = copy_df.columns.str.lower()
    return copy_df


def _map_by_sku(df):
    df = _lowercase_columns(df)
    if df is None or df.empty or "retailersku" not in df.columns:
        return {}
    result = {}
    for _, row in df.iterrows():
        sku = _normalize_text(row.get("retailersku"))
        if sku:
            result[sku] = row.to_dict()
    return result


def _read_csv_df(path):
    if not path or not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, dtype=str)
    except Exception as exc:
        logger.error("audit CSV read failed: %s (%s)", path, exc)
        return None


def _load_db_rows(db_engine, table_name, base_df):
    if db_engine is None or not table_name:
        return None
    if not re.match(r"^[A-Za-z0-9_]+$", table_name):
        raise ValueError(f"Unsafe table name for audit: {table_name}")

    base_df = _lowercase_columns(base_df)
    if base_df is None or base_df.empty:
        return pd.DataFrame()

    keys = []
    if "kr_crawl_strdatetime" in base_df.columns:
        keys = [
            str(value).strip()
            for value in base_df["kr_crawl_strdatetime"].tolist()
            if not _is_empty(value)
        ]
    if not keys:
        return pd.DataFrame()

    columns = [
        "retailerid", "country_code", "ships_from", "channel_name", "channel",
        "retailersku", "brand", "brand_eng", "form_factor",
        "segment_lv1", "segment_lv2", "segment_lv3", "capacity", "item",
        "retailprice", "sold_by", "imageurl", "producturl",
        "crawl_datetime", "crawl_strdatetime", "kr_crawl_datetime",
        "kr_crawl_strdatetime", "title", "vat",
    ]
    query = text(
        f"SELECT {', '.join(columns)} FROM {table_name} "
        "WHERE kr_crawl_strdatetime IN :keys"
    ).bindparams(bindparam("keys", expanding=True))
    try:
        return pd.read_sql(query, db_engine, params={"keys": sorted(set(keys))})
    except Exception as exc:
        logger.error("audit DB read failed: %s", exc)
        raise


class _Node:
    __slots__ = ("tag", "attrs", "children", "text_parts", "parent")

    def __init__(self, tag, attrs=None, parent=None):
        self.tag = tag
        self.attrs = dict(attrs or [])
        self.children = []
        self.text_parts = []
        self.parent = parent


class _TreeBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = _Node("document")
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = _Node(tag.lower(), attrs, self.stack[-1])
        self.stack[-1].children.append(node)
        self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        node = _Node(tag.lower(), attrs, self.stack[-1])
        self.stack[-1].children.append(node)

    def handle_endtag(self, tag):
        tag = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        if data:
            self.stack[-1].text_parts.append(data)


def _walk(node):
    yield node
    for child in node.children:
        yield from _walk(child)


def _node_text(node):
    parts = []
    for current in _walk(node):
        parts.extend(current.text_parts)
    return _normalize_text(" ".join(parts))


def _has_class(node, class_name):
    classes = (node.attrs.get("class") or "").split()
    return class_name in classes


def _find_by_id(root, node_id):
    for node in _walk(root):
        if node.attrs.get("id") == node_id:
            return node
    return None


def _find_all(root, predicate):
    return [node for node in _walk(root) if predicate(node)]


def _strip_tags(text_value):
    text_value = re.sub(r"<script\b.*?</script>", " ", text_value, flags=re.I | re.S)
    text_value = re.sub(r"<style\b.*?</style>", " ", text_value, flags=re.I | re.S)
    text_value = re.sub(r"<[^>]+>", " ", text_value)
    return _normalize_text(text_value)


def _clean_offer_text(value):
    value = _normalize_text(value)
    if not value:
        return None
    lowered = value.casefold()
    blocked_exact = {
        "details",
        "secure transaction",
        "sichere transaktion",
        "payment",
        "returns",
        "condition",
        "zustand",
        "seller",
        "shipper",
        "versender",
        "verkaufer",
        "verkauft von",
        "versand durch",
        "versand",
    }
    if lowered in blocked_exact:
        return None
    if "versender" in lowered and "verk" in lowered:
        return None
    if "shipper" in lowered and "seller" in lowered:
        return None
    if lowered.startswith("verkauft von "):
        return value.split(" ", 2)[-1].strip() or None
    if lowered.startswith("versand durch "):
        return value.split(" ", 2)[-1].strip() or None
    if lowered.startswith("ships from "):
        return value.split(" ", 2)[-1].strip() or None
    if lowered.startswith("sold by "):
        return value.split(" ", 2)[-1].strip() or None
    return value


def _first_text(nodes):
    for node in nodes:
        text_value = _clean_offer_text(_node_text(node))
        if text_value:
            return text_value
    return None


def _extract_offer_value(section):
    if section is None:
        return None
    link_nodes = _find_all(section, lambda node: node.tag == "a" and node.attrs.get("id") == "sellerProfileTriggerId")
    value = _first_text(link_nodes)
    if value:
        return value

    message_nodes = _find_all(section, lambda node: _has_class(node, "offer-display-feature-text-message"))
    value = _first_text(message_nodes)
    if value:
        return value

    link_nodes = _find_all(section, lambda node: node.tag == "a")
    return _first_text(link_nodes)


def _extract_value_after_label(text_value, label_patterns):
    text_value = _normalize_text(text_value)
    if not text_value:
        return None
    stop_words = (
        "Details|Condition|Zustand|Returns|Retouren|Payment|Zahlung|"
        "Secure transaction|Sichere Transaktion|Nur noch|Only"
    )
    for label_pattern in label_patterns:
        match = re.search(
            rf"{label_pattern}\s+(.+?)(?:\s+(?:{stop_words})|$)",
            text_value,
            flags=re.I,
        )
        if match:
            return _clean_offer_text(match.group(1))
    return None


def _extract_price(root, has_offer_source):
    if not has_offer_source:
        return None

    price_section_ids = [
        "corePriceDisplay_desktop_feature_div",
        "corePrice_feature_div",
        "apex_desktop",
        "usedBuySection",
        "buybox",
        "newAccordionRow",
    ]
    for section_id in price_section_ids:
        section = _find_by_id(root, section_id)
        if section is None:
            continue
        price_nodes = _find_all(section, lambda node: _has_class(node, "a-offscreen"))
        for node in price_nodes:
            price = _normalize_price(_node_text(node))
            if price:
                return price

    price_id_candidates = [
        "priceblock_ourprice",
        "priceblock_dealprice",
        "priceblock_saleprice",
        "price_inside_buybox",
    ]
    for price_id in price_id_candidates:
        node = _find_by_id(root, price_id)
        price = _normalize_price(_node_text(node) if node else None)
        if price:
            return price

    return None


def _extract_de_html_values(html_path):
    if not html_path or not os.path.exists(html_path):
        return None

    try:
        with open(html_path, "r", encoding="utf-8", errors="ignore") as file_obj:
            html_text = file_obj.read()
    except Exception as exc:
        return {"_parse_error": f"read failed: {exc}"}

    builder = _TreeBuilder()
    try:
        builder.feed(html_text)
    except Exception:
        pass
    root = builder.root

    result = {field: None for field in AUDIT_FIELDS}
    title_node = _find_by_id(root, "productTitle")
    result["title"] = _node_text(title_node) if title_node else None

    ships_from = None
    for section_id in [
        "fulfillerInfoFeature_feature_div",
        "usedOnlyLayoutFulfillerInfoFeature_feature_div",
    ]:
        section = _find_by_id(root, section_id)
        ships_from = _extract_offer_value(section)
        if not ships_from:
            ships_from = _extract_value_after_label(_node_text(section) if section else None, [r"Versand durch", r"Ships from"])
        if ships_from:
            break

    sold_by = None
    for section_id in [
        "merchantInfoFeature_feature_div",
        "usedOnlyLayoutMerchantInfoFeature_feature_div",
        "shipsFromSoldBy_feature_div",
        "shipFromSoldByAbbreviated_feature_div",
        "shipsFromSoldByAbbreviatedPSUFeature_feature_div",
        "sfsbFallbackExpanded_feature_div",
    ]:
        section = _find_by_id(root, section_id)
        if section is None:
            continue
        section_text = (_node_text(section) or "").casefold()
        value = _extract_offer_value(section)
        if not value:
            value = _extract_value_after_label(
                _node_text(section),
                [
                    r"Versender\s*/\s*Ver\w+ufer",
                    r"Shipper\s*/\s*Seller",
                    r"Verkauft von",
                    r"Sold by",
                ],
            )
        if not value:
            continue
        if ("versender" in section_text and "verk" in section_text) or ("shipper" in section_text and "seller" in section_text):
            ships_from = ships_from or value
            sold_by = sold_by or value
        elif "verkauft" in section_text or "sold by" in section_text or "seller" in section_text:
            sold_by = sold_by or value
        elif not sold_by:
            sold_by = value

    result["ships_from"] = ships_from
    result["sold_by"] = sold_by
    result["retailprice"] = _extract_price(root, bool(ships_from or sold_by))

    if result["title"] is None:
        match = re.search(r'id=["\']productTitle["\'][^>]*>(.*?)</', html_text, re.I | re.S)
        if match:
            result["title"] = _strip_tags(match.group(1))

    return result


def _latest_matching_file(output_dir, pattern):
    matches = [
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if re.fullmatch(pattern, name)
    ]
    if not matches:
        return None
    return max(matches, key=lambda path: os.path.getmtime(path))


def _load_html_source(output_dir, artifact_prefix, base_map):
    html_map = {}
    for sku in base_map:
        safe_sku = _safe_sku(sku)
        pattern = re.escape(artifact_prefix) + "_" + re.escape(safe_sku) + r"_.*\.html"
        html_path = _latest_matching_file(output_dir, pattern)
        if html_path:
            html_map[sku] = _extract_de_html_values(html_path)
            html_map[sku]["_html_path"] = html_path
        else:
            html_map[sku] = {"_missing": True}
    return html_map


def _parse_log_values(log_path):
    if not log_path or not os.path.exists(log_path):
        return {}

    by_sku = {}
    current_sku = None
    current_lines = []

    def flush():
        if current_sku and current_sku in by_sku:
            by_sku[current_sku].setdefault("_lines", []).extend(current_lines)

    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as file_obj:
            for line in file_obj:
                sku_match = re.search(r"/dp/([A-Z0-9]{10})", line, re.I)
                if sku_match:
                    flush()
                    current_sku = sku_match.group(1).upper()
                    current_lines = [line.rstrip("\n")]
                    by_sku.setdefault(current_sku, {field: None for field in AUDIT_FIELDS})
                    continue

                if current_sku:
                    current_lines.append(line.rstrip("\n"))
                    stripped = line.strip()
                    for label, field in [
                        ("제목:", "title"),
                        ("가격:", "retailprice"),
                        ("판매자:", "sold_by"),
                        ("배송지:", "ships_from"),
                    ]:
                        if label in stripped:
                            by_sku[current_sku][field] = stripped.split(label, 1)[1].strip()
                if len(current_lines) > 120:
                    current_lines = current_lines[-120:]
        flush()
    except Exception as exc:
        logger.error("audit log parse failed: %s", exc)
    return by_sku


def _compare_sources(base_map, sources, required_sources):
    errors = []
    values_by_sku = {}

    for sku, base_row in base_map.items():
        sku_values = {"result": {field: base_row.get(field) for field in AUDIT_FIELDS}}
        values_by_sku[sku] = sku_values
        for source_name, source_map in sources.items():
            row = source_map.get(sku) if source_map else None
            if row is None:
                if source_name in required_sources:
                    errors.append({
                        "sku": sku,
                        "source": source_name,
                        "field": "__row__",
                        "expected": "present",
                        "actual": "missing",
                    })
                continue
            if row.get("_missing"):
                errors.append({
                    "sku": sku,
                    "source": source_name,
                    "field": "__artifact__",
                    "expected": "saved HTML",
                    "actual": "missing",
                })
                sku_values[source_name] = row
                continue
            sku_values[source_name] = {field: row.get(field) for field in AUDIT_FIELDS}
            if row.get("_parse_error"):
                errors.append({
                    "sku": sku,
                    "source": source_name,
                    "field": "__parse__",
                    "expected": "parsed",
                    "actual": row.get("_parse_error"),
                })
                continue

            for field in AUDIT_FIELDS:
                expected = _normalize_value(field, base_row.get(field))
                actual = _normalize_value(field, row.get(field))
                if expected != actual:
                    errors.append({
                        "sku": sku,
                        "source": source_name,
                        "field": field,
                        "expected": expected,
                        "actual": actual,
                    })

    return errors, values_by_sku


def _artifact_paths(output_dir, artifact_prefix, sku):
    safe_sku = _safe_sku(sku)
    escaped_prefix = re.escape(artifact_prefix)
    escaped_sku = re.escape(safe_sku)
    patterns = [
        escaped_prefix + "_" + escaped_sku + r"_.*\.html",
        escaped_prefix + "_" + escaped_sku + r"_.*\.png",
    ]
    paths = []
    for name in os.listdir(output_dir):
        for pattern in patterns:
            if re.fullmatch(pattern, name):
                paths.append(os.path.join(output_dir, name))
                break
    return paths


def _write_dict_json(path, payload):
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2, default=str)


def _write_row_csv(path, row_dict):
    with open(path, "w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=list(row_dict.keys()))
        writer.writeheader()
        writer.writerow(row_dict)


def _preserve_error_evidence(output_dir, artifact_prefix, sku, source_values, sku_errors, source_rows, log_map):
    sku_dir = os.path.join(output_dir, "error", _safe_sku(sku))
    os.makedirs(sku_dir, exist_ok=True)

    for path in _artifact_paths(output_dir, artifact_prefix, sku):
        try:
            shutil.move(path, os.path.join(sku_dir, os.path.basename(path)))
        except Exception as exc:
            logger.warning("audit evidence move failed: %s (%s)", path, exc)

    _write_dict_json(os.path.join(sku_dir, "source_values.json"), source_values)
    _write_dict_json(os.path.join(sku_dir, "audit_errors.json"), sku_errors)

    for source_name, rows in source_rows.items():
        row = rows.get(sku) if rows else None
        if row:
            _write_row_csv(os.path.join(sku_dir, f"{source_name}_row.csv"), row)

    log_row = log_map.get(sku) if log_map else None
    if log_row and log_row.get("_lines"):
        with open(os.path.join(sku_dir, "log_excerpt.txt"), "w", encoding="utf-8") as file_obj:
            file_obj.write("\n".join(log_row["_lines"]))


def _delete_matched_artifacts(output_dir, artifact_prefix, sku):
    for path in _artifact_paths(output_dir, artifact_prefix, sku):
        try:
            os.remove(path)
        except Exception as exc:
            logger.warning("audit cleanup failed: %s (%s)", path, exc)


def run_artifact_audit(
    *,
    target_key,
    country_code,
    results_df,
    output_dir,
    db_engine=None,
    table_name=None,
    local_result_csv_path=None,
    file_server_csv_path=None,
    log_path=None,
    artifact_prefix=None,
    compare_db=True,
    compare_log=True,
    compare_html=True,
    cleanup_matched=True,
):
    """Run artifact audit and return a summary dict."""
    if results_df is None or results_df.empty:
        return {
            "enabled": True,
            "checked_count": 0,
            "error_count": 1,
            "errors": [{"sku": "__audit__", "source": "result", "field": "__row__", "expected": "rows", "actual": "empty"}],
        }

    if not output_dir:
        raise ValueError("output_dir is required for v3 artifact audit")

    os.makedirs(output_dir, exist_ok=True)
    artifact_prefix = artifact_prefix or f"{country_code}_v3"
    base_df = _lowercase_columns(results_df)
    base_map = _map_by_sku(base_df)

    source_maps = {}
    required_sources = set()
    source_rows = {"result": base_map}

    local_csv_df = _read_csv_df(local_result_csv_path)
    if local_csv_df is not None:
        source_maps["csv"] = _map_by_sku(local_csv_df)
        source_rows["csv"] = source_maps["csv"]
        required_sources.add("csv")

    file_server_df = _read_csv_df(file_server_csv_path)
    if file_server_df is not None:
        source_maps["fileserver_csv"] = _map_by_sku(file_server_df)
        source_rows["fileserver_csv"] = source_maps["fileserver_csv"]
        required_sources.add("fileserver_csv")

    if compare_db:
        db_df = _load_db_rows(db_engine, table_name, base_df)
        source_maps["db"] = _map_by_sku(db_df)
        source_rows["db"] = source_maps["db"]
        required_sources.add("db")

    log_map = {}
    if compare_log:
        log_map = _parse_log_values(log_path)
        source_maps["log"] = log_map
        required_sources.add("log")

    if compare_html:
        source_maps["html"] = _load_html_source(output_dir, artifact_prefix, base_map)
        required_sources.add("html")

    errors, source_values = _compare_sources(base_map, source_maps, required_sources)
    error_skus = sorted({error["sku"] for error in errors if error.get("sku")})

    error_dir = os.path.join(output_dir, "error")
    if error_skus:
        os.makedirs(error_dir, exist_ok=True)
        for sku in error_skus:
            sku_errors = [error for error in errors if error.get("sku") == sku]
            _preserve_error_evidence(
                output_dir,
                artifact_prefix,
                sku,
                source_values.get(sku, {}),
                sku_errors,
                source_rows,
                log_map,
            )

        errors_csv = os.path.join(error_dir, "audit_errors.csv")
        pd.DataFrame(errors).to_csv(errors_csv, index=False, encoding="utf-8-sig")

    if cleanup_matched:
        for sku in base_map:
            if sku not in error_skus:
                _delete_matched_artifacts(output_dir, artifact_prefix, sku)

    summary = {
        "enabled": True,
        "target_key": target_key,
        "country_code": country_code,
        "checked_count": len(base_map),
        "error_count": len(errors),
        "error_skus": error_skus,
        "error_dir": error_dir if error_skus else None,
        "errors": errors,
        "local_result_csv_path": local_result_csv_path,
        "file_server_csv_path": file_server_csv_path,
        "log_path": log_path,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    summary_path = os.path.join(output_dir, "audit_summary.json")
    _write_dict_json(summary_path, summary)
    summary["summary_path"] = summary_path

    if errors:
        logger.error("V3 artifact audit failed: %s mismatch(es), %s SKU(s)", len(errors), len(error_skus))
    else:
        logger.info("V3 artifact audit passed: %s SKU(s)", len(base_map))

    return summary
