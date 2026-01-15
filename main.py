from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple

from io_utils import read_csv_loose, df_to_json_row, now_utc_str
from cleanse import canonicalize_row, validate_row_canonical
from db_utils import get_conn, init_schema, insert_staging, insert_final_batch, insert_bad_batch



def process_dataframe(df: pd.DataFrame, src_file: str):
    """
    Returns (clean_rows, bad_rows, dropped_exact_dupes_count, conflicting_dupes_count)
    bad_rows elements: (order_id, item_sku, error_reasons, raw_json, ingested_at)
    """
    clean_rows: List[Dict] = []
    bad_rows: List[Tuple[str, str, str, str, str]] = []
    ingested_at = now_utc_str()

    # Dedup policy:
    # - Exact duplicate rows (all columns equal) -> drop silently (counted)
    # - Conflicting dup on (order_id, item_sku) with different values -> route both to bad with reason
    df_nodup = df.drop_duplicates()
    dropped_exact_dupes_count = len(df) - len(df_nodup)

    key_counts = df_nodup.groupby(["order_id", "item_sku"], dropna=False).size()
    conflict_keys = set(k for k, v in key_counts.items() if v > 1)

    conflicting_dupes_count = 0
    for idx, row in df_nodup.iterrows():
        raw_dict = row.to_dict()
        raw_json = df_to_json_row(row)
        canon = canonicalize_row(raw_dict)
        errs = validate_row_canonical(canon)

        key = (row.get("order_id"), row.get("item_sku"))
        if key in conflict_keys:
            errs.append("conflicting duplicate key (order_id, item_sku)")
            conflicting_dupes_count += 1

        if errs:
            bad_rows.append((row.get("order_id"), row.get("item_sku"), "; ".join(errs), raw_json, ingested_at))
        else:
            clean_rows.append(canon)

    return clean_rows, bad_rows, dropped_exact_dupes_count, conflicting_dupes_count



def run_pipeline(csv_path: str, db_path: str = "orders.db"):
    print(f"▶ Reading CSV: {csv_path}")
    df = read_csv_loose(csv_path)

    conn = get_conn(db_path)
    init_schema(conn)

    print("▶ Writing to staging …")
    insert_staging(conn, df, src_file=csv_path)

    print("▶ Canonicalizing & validating …")
    clean_rows, bad_rows, dropped_dupes, conflicting_dupes = process_dataframe(df, csv_path)

    print(f"   - exact duplicates dropped: {dropped_dupes}")
    print(f"   - conflicting duplicates routed to rejects: {conflicting_dupes}")
    print(f"   - valid rows: {len(clean_rows)} ; rejects: {len(bad_rows)}")

    print("▶ Loading final & rejects …")
    insert_final_batch(conn, clean_rows, src_file=csv_path)
    insert_bad_batch(conn, bad_rows, src_file=csv_path)

    conn.commit()
    conn.close()
    print("✅ Done.")



if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="CSV → SQLite ingestion pipeline")
    ap.add_argument("--csv", required=True, help="Path to input CSV")
    ap.add_argument("--db", default="orders.db", help="SQLite DB file path")
    args = ap.parse_args()
    run_pipeline(args.csv, args.db)
