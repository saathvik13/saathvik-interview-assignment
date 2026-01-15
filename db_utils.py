from __future__ import annotations
import sqlite3
from typing import List, Dict, Tuple
import pandas as pd
from io_utils import write_sql_script_to_db, now_utc_str


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_schema(conn: sqlite3.Connection, schema_sql_path: str = "schema.sql"):
    write_sql_script_to_db(conn, schema_sql_path)


def insert_staging(conn: sqlite3.Connection, df: pd.DataFrame, src_file: str):
    ingested_at = now_utc_str()
    df2 = df.copy()
    df2["ingested_at"] = ingested_at
    df2["source_file"] = src_file
    df2.to_sql("orders_staging", conn, if_exists="append", index=False)


def insert_final_batch(conn: sqlite3.Connection, rows: List[Dict], src_file: str):
    # Use executemany for speed
    ingested_at = now_utc_str()
    payload = []
    for r in rows:
        payload.append((
            r["order_id"], r["customer_id"], r["customer_name"], r["email"], r["phone_e164"],
            r["country"], r["state"], r["city"], r["address"], r["postal_code"],
            r["order_date"], r["ship_date"], r["ship_mode"],
            r["item_sku"], r["item_name"], r["quantity"], r["unit_price"], r["currency"],
            r["discount_code"], r["order_notes"], ingested_at, src_file
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO orders (
          order_id, customer_id, customer_name, email, phone_e164, country, state, city, address, postal_code,
          order_date, ship_date, ship_mode, item_sku, item_name, quantity, unit_price, currency,
          discount_code, order_notes, ingested_at, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, payload)


def insert_bad_batch(conn: sqlite3.Connection, bad_rows: List[Tuple[str, str, str, str, str]], src_file: str):
    # Each item: (order_id, item_sku, error_reasons, raw_json, ingested_at)
    conn.executemany("""
        INSERT INTO orders_bad (order_id, item_sku, error_reasons, raw_row_json, ingested_at, source_file)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [(o, s, e, j, t, src_file) for (o, s, e, j, t) in bad_rows])
