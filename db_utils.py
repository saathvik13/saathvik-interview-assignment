from __future__ import annotations
import sqlite3
import pandas as pd
from typing import List, Dict, Tuple
from io_utils import *


def get_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_schema(conn, schema_sql_path="schema.sql"):
    write_sql_script_to_db(conn, schema_sql_path)


def get_next_version(conn, table: str) -> int:
    try:
        cur = conn.execute(f"SELECT MAX(version) FROM {table}")
        val = cur.fetchone()[0]
        return (val or 0) + 1
    except sqlite3.OperationalError:
        return 1


# -------------------------------------------------------------------
# 1. INSERT INTO transaction_raw
# -------------------------------------------------------------------

def insert_staged(conn, df: pd.DataFrame, src_file: str):
    df2 = df.copy()
    df2["ingested_at"] = now_utc_str()
    df2["source_file"] = src_file
    df2["version"] = get_next_version(conn, "transaction_raw")
    df2.to_sql("transaction_raw", conn, if_exists="append", index=False)


# -------------------------------------------------------------------
# 2. INSERT CLEAN ROWS INTO transaction_cleaned
# -------------------------------------------------------------------

def insert_clean_transactions(conn, clean_rows: List[Dict], src_file: str):
    version = get_next_version(conn, "transaction_cleaned")
    payload = []

    for r in clean_rows:
        payload.append((
            r["order_id"], r["customer_id"], r["customer_name"], r["email"], r["phone"], r["country"], r["state"], r["city"], r["address"], r["postal_code"], r["order_date"], r["ship_date"], r["ship_mode"], r["item_sku"], r["item_name"], r["quantity"], r["unit_price"], r["currency"],
            r["discount_code"], r["order_notes"], now_utc_str(), src_file, version
        ))

    conn.executemany("""
        INSERT INTO transaction_cleaned (
            order_id, customer_id, customer_name, email, phone, country, state, city, address, postal_code, order_date, ship_date, ship_mode, item_sku, item_name, quantity, unit_price, currency, discount_code, order_notes, ingested_at, source_file, version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, payload)


# -------------------------------------------------------------------
# 3. INSERT INTO transaction_bad
# -------------------------------------------------------------------

def insert_bad_batch(conn, bad_rows, src_file: str):
    version = get_next_version(conn, "transaction_bad")
    payload = [
        (o, s, e, j, t, src_file, version)
        for (o, s, e, j, t) in bad_rows
    ]

    conn.executemany("""
        INSERT INTO transaction_bad (
           order_id, item_sku, error_reasons, raw_row_json, ingested_at, source_file, version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, payload)

# -------------------------------------------------------------------
# 4. DERIVED TABLES (UPSERT instead of DELETE + INSERT)
# -------------------------------------------------------------------

def derive_customer(conn):
    conn.executemany("""
        INSERT INTO customer (customer_id, customer_name, email, phone, country, state, city, address, postal_code
        )
        SELECT DISTINCT
            customer_id, customer_name, email, phone, country, state, city, address, postal_code
        FROM transaction_cleaned
        WHERE customer_id IS NOT NULL
    """, [])

    conn.execute("""
        INSERT INTO customer (
            customer_id, customer_name, email, phone, country, state, city, address, postal_code
        )
        SELECT DISTINCT
            customer_id, customer_name, email, phone, country, state, city, address, postal_code
        FROM transaction_cleaned
        WHERE customer_id IS NOT NULL
        ON CONFLICT(customer_id) DO UPDATE SET
            customer_name = excluded.customer_name,
            email         = excluded.email,
            phone         = excluded.phone,
            country       = excluded.country,
            state         = excluded.state,
            city          = excluded.city,
            address       = excluded.address,
            postal_code   = excluded.postal_code;
    """)


def derive_product(conn):
    conn.execute("""
        INSERT INTO product (item_sku, item_name)
        SELECT DISTINCT item_sku, item_name
        FROM transaction_cleaned
        WHERE item_sku IS NOT NULL
        ON CONFLICT(item_sku) DO UPDATE SET
            item_name = excluded.item_name;
    """)


def derive_order_info(conn):
    conn.execute("""
        INSERT INTO order_info (
            order_id, customer_id, order_date, ship_date, ship_mode, discount_code, order_notes
        )
        SELECT DISTINCT
            order_id, customer_id, order_date, ship_date, ship_mode, discount_code, order_notes
        FROM transaction_cleaned
        WHERE order_id IS NOT NULL
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id   = excluded.customer_id,
            order_date    = excluded.order_date,
            ship_date     = excluded.ship_date,
            ship_mode     = excluded.ship_mode,
            discount_code = excluded.discount_code,
            order_notes   = excluded.order_notes;
    """)


def derive_order_detail(conn):
    conn.execute("""
        INSERT INTO order_detail (
            order_id, item_sku,
            quantity, unit_price, currency
        )
        SELECT
            order_id, item_sku,
            quantity, unit_price, currency
        FROM transaction_cleaned
        WHERE order_id IS NOT NULL AND item_sku IS NOT NULL
        ON CONFLICT(order_id, item_sku) DO UPDATE SET
            quantity   = excluded.quantity,
            unit_price = excluded.unit_price,
            currency   = excluded.currency;
    """)
