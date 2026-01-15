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


def get_next_version(conn: sqlite3.Connection, table: str) -> int:
    """
    Returns 1 + MAX(version) from the given table.
    If the table doesn't exist or has no version yet → returns 1.
    """
    cur = conn.cursor()
    try:
        row = cur.execute(f"SELECT MAX(version) FROM {table}").fetchone()
        max_ver = row[0] if row and row[0] is not None else 0
        return str(int(max_ver) + 1)
    except sqlite3.OperationalError:
        # table may not exist yet
        return 1


def insert_staging(conn: sqlite3.Connection, df: pd.DataFrame, src_file: str):
    ingested_at = now_utc_str()
    df2 = df.copy()
    df2["ingested_at"] = ingested_at
    df2["source_file"] = src_file
    df2["version"] = get_next_version(conn, "orders_staging")
    df2.to_sql("orders_staging", conn, if_exists="append", index=False)



def insert_final_batch(conn: sqlite3.Connection, rows: List[Dict], src_file: str):
    ingested_at = now_utc_str()
    version = get_next_version(conn, "orders")
    payload = []
    for r in rows:
        payload.append((
            r["order_id"], r["customer_id"], r["customer_name"], r["email"], r["phone"],
            r["country"], r["state"], r["city"], r["address"], r["postal_code"],
            r["order_date"], r["ship_date"], r["ship_mode"],
            r["item_sku"], r["item_name"], r["quantity"], r["unit_price"], r["currency"],
            r["discount_code"], r["order_notes"], ingested_at, src_file, version
        ))
    conn.executemany("""
        INSERT INTO orders (
          order_id, customer_id, customer_name, email, phone, country, state, city, address, postal_code,
          order_date, ship_date, ship_mode, item_sku, item_name, quantity, unit_price, currency,
          discount_code, order_notes, ingested_at, source_file, version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id, item_sku) DO UPDATE SET
          customer_id   = excluded.customer_id,
          customer_name = excluded.customer_name,
          email         = excluded.email,
          phone         = excluded.phone,
          country       = excluded.country,
          state         = excluded.state,
          city          = excluded.city,
          address       = excluded.address,
          postal_code   = excluded.postal_code,
          order_date    = excluded.order_date,
          ship_date     = excluded.ship_date,
          ship_mode     = excluded.ship_mode,
          item_name     = excluded.item_name,
          quantity      = excluded.quantity,
          unit_price    = excluded.unit_price,
          currency      = excluded.currency,
          discount_code = excluded.discount_code,
          order_notes   = excluded.order_notes,
          ingested_at   = excluded.ingested_at,
          source_file   = excluded.source_file,
          version       = excluded.version;
    """, payload)
    


def insert_bad_batch(conn: sqlite3.Connection, bad_rows: List[Tuple[str, str, str, str, str]], src_file: str):
    # Each item: (order_id, item_sku, error_reasons, raw_json, ingested_at)
    version = get_next_version(conn, "orders_bad")
    conn.executemany("""
        INSERT INTO orders_bad (order_id, item_sku, error_reasons, raw_row_json, ingested_at, source_file, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [(o, s, e, j, t, src_file, version) for (o, s, e, j, t) in bad_rows])

