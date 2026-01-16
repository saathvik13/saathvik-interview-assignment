PRAGMA foreign_keys = ON;


---------------------------------------------------------------
-- 1. RAW STAGING TABLE 
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_raw (
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
  source_file       TEXT NOT NULL,
  version           TEXT NOT NULL
);


---------------------------------------------------------------
-- 2. CLEAN TRANSACTION TABLE
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_cleaned (
  order_id              TEXT NOT NULL,
  customer_id           TEXT,
  customer_name         TEXT,
  email                 TEXT,
  phone                 TEXT,
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
  quantity              TEXT,
  unit_price            TEXT,
  currency              TEXT NOT NULL,
  discount_code         TEXT,
  order_notes           TEXT,
  ingested_at           TEXT NOT NULL,
  source_file           TEXT NOT NULL,
  version               TEXT NOT NULL,
  PRIMARY KEY (order_id, item_sku)
);


---------------------------------------------------------------
-- 3. BAD RECORDS TABLE
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_bad (
  order_id          TEXT,
  item_sku          TEXT,
  error_reasons     TEXT NOT NULL,
  raw_row_json      TEXT NOT NULL,
  ingested_at       TEXT NOT NULL,
  source_file       TEXT NOT NULL,
  version           TEXT NOT NULL
);


---------------------------------------------------------------
-- 4. CUSTOMER DIMENSION 
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer (
  customer_id   TEXT PRIMARY KEY,
  customer_name TEXT,
  email         TEXT,
  phone         TEXT,
  country       TEXT,
  state         TEXT,
  city          TEXT,
  address       TEXT,
  postal_code   TEXT
);


---------------------------------------------------------------
-- 5. PRODUCT DIMENSION
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product (
  item_sku    TEXT PRIMARY KEY,
  item_name   TEXT
);


---------------------------------------------------------------
-- 6. ORDER HEADER TABLE
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_info (
  order_id       TEXT PRIMARY KEY,
  customer_id    TEXT,
  order_date     TEXT,
  ship_date      TEXT,
  ship_mode      TEXT,
  discount_code  TEXT,
  order_notes    TEXT,
  FOREIGN KEY(customer_id) REFERENCES customer(customer_id)
);


---------------------------------------------------------------
-- 7. ORDER DETAIL TABLE
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_detail (
  order_id     TEXT NOT NULL,
  item_sku     TEXT NOT NULL,
  quantity     TEXT,
  unit_price   TEXT,
  currency     TEXT,
  PRIMARY KEY (order_id, item_sku),
  FOREIGN KEY(order_id) REFERENCES order_info(order_id),
  FOREIGN KEY(item_sku) REFERENCES product(item_sku)
);

