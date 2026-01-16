# Data Ingestion Pipeline — README

This project implements a local data ingestion pipeline that reads messy CSV files, cleans and validates their contents, and loads them into a structured analytical model with full traceability and metadata.

---

## 1) Overview

### End-to-End Pipeline Flow
1. Ingest raw CSV  
2. Write raw rows into `transaction_raw`
3. Canonicalize and validate → produce `clean_rows` + `bad_rows`
4. Insert validated rows into `transaction_cleaned`
5. Insert invalid rows into `transaction_bad`
6. Derive final analytics layer split into Dims and Fact Table
   - `customer`
   - `product`
   - `order_info`
   - `order_detail`
- Provides ingestion traceability (`ingested_at`, `source_file`, `version`)

---



## 2) Project Structure

saathvik-interview-assignment/
├── input.csv                # Input CSV file
├── main.py                  # pipeline orchestration
├── io_utils.py              # I/O Helper functions
├── cleanse.py               # Data Preprocessing file
├── db_utils.py              # DB interface - inserts, upserts, derivations
├── schema.sql               # DB Table Initialization
└── README.md                # ReadMe file


---


## 3) Prerequisites
- Python **3.9+**
- `pip`
- SQLite (bundled with Python as `sqlite3`)


---


## 4) Setup

### A. Clone & enter the project
```bash
git clone https://github.com/saathvik13/saathvik-interview-assignment
```

### B. Create a virtual environment and install deps (Windows)
```bash
python -m venv .venv
.venv\Scripts\activate
pip install pandas
```

---


## 5) How to Run
```bash
python main.py --csv ./input.csv --db ./shopping.db``
```


---



## 6) Schema Design


The CSV input is treated as untrusted, and all canonicalization + validation is performed before ingestion.

### Canonicalization Includes
- Unicode normalization (NFKC)
- Whitespace cleanup
- Normalize Phone number using country mapping
- Currency and price parsing (`$1,234.56`, `1.234,56 €`, etc.)
- Quantity parsing (supports spelled‑out numbers)
- Date parsing into normalized `YYYY-MM-DD` format
- Extracting numeric order IDs
- Converting 'N/A' strings to `None` type

### Validation Rules Include
- **order_id** must exist  
- **order_date** must be valid  
- **ship_date ≥ order_date**  
- **quantity** must be a non‑negative integer  
- **unit_price** must be a non‑negative number  
- **currency** required  
- **item_sku** required  
- Email must be syntactically valid  
- Must have **at least one** contact method (email/phone/address)  
- Must have **customer_id** or **customer_name**  

### Duplicate Handling
- Exact duplicates → **silently dropped**
- Conflicting duplicates on `(order_id, item_sku)` → **rejected**

### Rejected Records
Every rejected row is written to `transaction_bad` with:
- Original raw fields  
- JSON representation  
- Error message(s) with `Reason`
- Metadata - `ingested_at` , `source_file` , `version`  


---


## 7) Key Considerations

- **Use UTC (Zulu) timestamps** — Ensures consistent, timezone‑agnostic tracking across global sources and reruns.
- **Read all fields as TEXT** — Prevents ingestion failures caused by messy, inconsistent, or locale‑specific data.
- **Schema‑on‑read approach (ELT)** — Parse and standardize data only after ingestion, enabling resilience to malformed CSVs.
- **Strict canonicalization before validation** — Ensures dates, numbers, currency, phones, and emails are normalized before applying business rules.
- **3 Layer Architecture (Staging, Storage, and Analytical layer)** — Preserves original data, ensures clean records, and normalized data form (1NF, 2NF, 3NF etc.) for clarity and traceability.
- **UPSERT-based ingestion (UPDATE + INSERT)** — Allows safe reprocessing without creating duplicates or corrupting clean tables.
- **Consistent normalization for global data** — Handles Unicode, multilingual fields, mixed currency formats, and locale-dependent number conventions.
- **Focus on reproducibility and lineage** — Every load captures file name, version, timestamps, and row-level context for debugging and audit trails.
- **Lightweight but scalable design** — SQLite used for simplicity, but code structured for easy migration to PostgreSQL or cloud warehouses.



---


## 8) Known Limitations
- All columns stored as TEXT — Type enforcement happens in Python; the database cannot natively prevent invalid numeric/date/boolean values.
- Hardcoded reference dictionaries — Currency symbols, phone country codes, and quantity/word-to-number maps live in code, making governance and updates manual.
- Basic currency detection — Relies on token/symbol matching; ambiguous or mixed-locale price strings may be misinterpreted.
- Simplified phone normalization — Minimal cleanup and prefixing only; no full E.164 validation or country-specific rules.
- Limited date parsing — Handles common formats but struggles with locale-specific or non-standard patterns.
- No multilingual text handling — Names/addresses/descriptions lack transliteration and cross-alphabet normalization.
- Shallow email validation — Checks only syntactic shape; no domain verification or alias normalization.
- Configuration hardcoded — Paths, DB settings, and feature flags aren’t externalized via .env or environment variables; not CI-friendly.
- No SCD strategy — Customer/product updates overwrite history (no Type 2).
- Minimal versioning — No file checksum tracking; risk of duplicate processing across multiple CSVs/batches.
- Missing ingestion metadata — No central table for batch IDs, lineage, row counts, durations, schema version, or error summaries.
- No enrichment logic — Missing values are flagged but not inferred from historical loads or auxiliary sources.
- Currency not standardized — Prices remain in original currencies without FX normalization to a base currency.
- Pricing at transaction level — Unit price lives only in fact rows; no stable product-level pricing model.
- No job orchestration — Single-script execution without scheduling, retries, or monitoring (no Airflow/Prefect).
- Limited logging/observability — No structured logs, row-level metrics, DQ checks, or alerting on failures.
- No schema evolution — New/unexpected columns aren’t auto-detected or migrated.
- Multi-file ingestion not robust — No cross-batch dedupe or lineage across multiple CSVs.


## 8) Next Steps

- Implement SCD Type 2 — Add `*_history` tables with effective dating and hash‑diff change detection.  
- Add configurable rules engine — Externalize validation rules (nullability, regex, ranges, referential) in YAML/JSON.  
- Support cloud warehouses — Add Snowflake, BigQuery, and Redshift loaders using staging/COPY patterns.
- CI/CD Pipeline - Expand Data Engineering pipelines thorugh Github Actions and Databricks 
- Move reference dictionaries into DB tables — Currency, phone_prefix, word_to_number with lineage and effective dating.  
- Add enrichment layer — Infer missing customer/product attributes using historical snapshots and lookup files.  
- Enhance text cleanup — Normalize emails, transliterate multilingual text, unify casing rules.  
- Introduce `.env` configuration — Externalize paths, credentials, and environment flags; provide `.env.example`.  
- Normalize currency — Convert to a base currency using FX rates and move stable product pricing upstream.  
- Add structured logging & dashboards — Emit JSON logs and build Grafana/Metabase DQ dashboards.  
- Migrate to PostgreSQL + orchestration — Add Airflow/Prefect DAGs for scheduling, retries, and lineage tracking.  
- Implement schema evolution — Detect new/unexpected columns and auto‑handle schema drift.  
