from __future__ import annotations
import json
import unicodedata
import pandas as pd
from datetime import datetime, timezone


# -------------------------------------------------------------------
# Global constants
# -------------------------------------------------------------------

# ISO‑8601 format in UTC (Zulu). Used for metadata timestamps.
ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"



# -------------------------------------------------------------------
# Utility: Current UTC timestamp
# -------------------------------------------------------------------

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FMT)



# -------------------------------------------------------------------
# Utility: Normalize text reliably
# -------------------------------------------------------------------

def normalize_text(s: str | None) -> str | None:
    if s is None:
        return None

    # Convert all valuees to string (numbers, floats, objects)
    s = str(s)

    # Unicode normalization 
    s = unicodedata.normalize("NFKC", s).strip()

    # Collapse multiple spaces into single space
    return " ".join(s.split())



# -------------------------------------------------------------------
# CSV Reader: Minimal coercion, keep strings intact
# -------------------------------------------------------------------

def read_csv_loose(path: str) -> pd.DataFrame:
    # Read all columns strictly as strings
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    # Normalize column names for consistency
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Normalize each cell value
    for c in df.columns:
        df[c] = df[c].apply(lambda x: normalize_text(x) if x != "" else "")

    # Convert common sentinel values to actual None
    df = df.replace(['N/A', 'n/a', 'None'], None)

    return df



# -------------------------------------------------------------------
# Convert a pandas row → JSON string
# -------------------------------------------------------------------

def df_to_json_row(row: pd.Series) -> str:
    return json.dumps(
        {k: (None if v == "" else v) for k, v in row.to_dict().items()},
        ensure_ascii=False
    )



# -------------------------------------------------------------------
# Execute SQL file (schema.sql) into SQLite database
# -------------------------------------------------------------------

def write_sql_script_to_db(conn, sql_path: str):
    with open(sql_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
