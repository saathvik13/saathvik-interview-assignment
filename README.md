# Data Ingestion Pipeline — README

This project implements a local data ingestion pipeline that reads messy CSV files, cleans and validates their contents, and loads them into a structured analytical model with full traceability and metadata.

---

## 1) Overview

### End-to-End Pipeline Flow
1. Load CSV with loose parsing (all columns treated as TEXT)
2. Write raw rows into `transaction_raw`
3. Canonicalize and validate → produce `clean_rows` + `bad_rows`
4. Insert validated rows into `transaction_cleaned`
5. Insert invalid rows into `transaction_bad`
6. Derive final analytics tables:
   - `customer`
   - `product`
   - `order_info`
   - `order_detail`
7. Commit and close database connection
- Provides ingestion traceability (`ingested_at`, `source_file`, `version`)

---



## 2) Project Structure

data-pipeline/
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
cd data-pipeline  ????????
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


he CSV input is treated as untrusted, and all canonicalization + validation is performed before ingestion.

### Canonicalization Includes
- Unicode normalization (NFKC)
- Whitespace cleanup
- Phone number normalization using country mapping
- Currency and price parsing (`$1,234.56`, `1.234,56 €`, etc.)
- Quantity parsing (supports spelled‑out numbers)
- Date parsing into `YYYY-MM-DD`
- Extracting numeric order IDs
- Converting empty strings to `None`

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
- Error message(s)  
- `ingested_at`  
- `source_file`  
- `version`  

### Key Consideration
- All ingestion timestamps use **Zulu (UTC)** time.



---


## 7) Known Limitations
### Limitations
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
- SQLite only — Fine for local dev but unsuitable for high volume, concurrency, or cloud-scale.
- No job orchestration — Single-script execution without scheduling, retries, or monitoring (no Airflow/Prefect).
- Limited logging/observability — No structured logs, row-level metrics, DQ checks, or alerting on failures.
- No schema evolution — New/unexpected columns aren’t auto-detected or migrated.
- Multi-file ingestion not robust — No cross-batch dedupe or lineage across multiple CSVs.
- No partitioned staging/checkpointing — Large files not split/streamed; failures can force full re-runs.
- No cloud DW support — Not yet deployable to Snowflake, BigQuery, or Redshift.
- No CLI parameterization — Source name and batch identifiers aren’t provided via CLI flags.


## 8) Next Steps

- Implement SCD Type 2 — Add `*_history` tables with effective dating and hash‑diff change detection.  
- Add configurable rules engine — Externalize validation rules (nullability, regex, ranges, referential) in YAML/JSON.  
- Expand observability — Emit row counts, per‑column DQ metrics, and configure alerting.  
- Support cloud warehouses — Add Snowflake, BigQuery, and Redshift loaders using staging/COPY patterns.  
- Parameterize CLI — Add flags such as `--source`, `--batch-id`, `--as-of-date`, `--schema-version`, `--fail-fast`.  
- Add partitioned staging & checkpointing — Chunk large files, persist checkpoints, support resume.  
- Move reference dictionaries into DB tables — Currency, phone_prefix, word_to_number with lineage and effective dating.  
- Add enrichment layer — Infer missing customer/product attributes using historical snapshots and lookup files.  
- Enhance text cleanup — Normalize emails, transliterate multilingual text, unify casing rules.  
- Introduce `.env` configuration — Externalize paths, credentials, and environment flags; provide `.env.example`.  
- Improve versioning & dedupe — Use SHA‑256 file checksums and cross‑batch deduplication logic.  
- Create ingestion metadata table — Track batch ID, lineage, timestamps, row counts, and errors.  
- Normalize currency — Convert to a base currency using FX rates and move stable product pricing upstream.  
- Add structured logging & dashboards — Emit JSON logs and build Grafana/Metabase DQ dashboards.  
- Migrate to PostgreSQL + orchestration — Add Airflow/Prefect DAGs for scheduling, retries, and lineage tracking.  
- Implement schema evolution — Detect new/unexpected columns and auto‑handle schema drift.  
- Add partitioned staging formats — Write Parquet partitions for incremental and cloud‑optimized ingestion.  
