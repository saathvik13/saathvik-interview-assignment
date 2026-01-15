PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS orders_staging (
  order_id          TEXT,
  customer_id       TEXT,
  customer_name     TEXT,
  email             TEXT,
  phone             TEXT,
  country           TEXT,
  state             TEXT,
  city              TEXT,
  address           TEXT,
  postal_code       TEXT,
  order_date        TEXT,
  ship_date         TEXT,
  ship_mode         TEXT,
  item_sku          TEXT,
  item_name         TEXT,
  quantity          TEXT,
  unit_price        TEXT,
  currency          TEXT,
  discount_code     TEXT,
  order_notes       TEXT,
  ingested_at       TEXT NOT NULL,
  source_file       TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS orders (
  order_id              INTEGER NOT NULL,
  customer_id           TEXT,
  customer_name         TEXT,
  email                 TEXT,
  phone_e164            TEXT,
  country               TEXT,
  state                 TEXT,
  city                  TEXT,
  address               TEXT,
  postal_code           TEXT,
  order_date            TEXT NOT NULL,   
  ship_date             TEXT,            
  ship_mode             TEXT,
  item_sku              TEXT NOT NULL,
  item_name             TEXT,
  quantity              INTEGER NOT NULL CHECK (quantity >= 0),
  unit_price            REAL NOT NULL CHECK (unit_price >= 0),
  currency              TEXT NOT NULL,
  discount_code         TEXT,
  order_notes           TEXT,
  ingested_at           TEXT NOT NULL,
  source_file           TEXT NOT NULL,
  PRIMARY KEY (order_id, item_sku)
);


CREATE TABLE IF NOT EXISTS orders_bad (
  order_id          TEXT,
  item_sku          TEXT,
  error_reasons     TEXT NOT NULL,       
  raw_row_json      TEXT NOT NULL,       
  ingested_at       TEXT NOT NULL,
  source_file       TEXT NOT NULL
);
