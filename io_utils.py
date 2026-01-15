from __future__ import annotations
import json
import unicodedata
import pandas as pd
from datetime import datetime, timezone


ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"  # UTC Zulu


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FMT)



def normalize_text(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s)
    s = unicodedata.normalize("NFKC", s).strip()
    # collapse whitespace
    return " ".join(s.split())


def read_csv_loose(path: str) -> pd.DataFrame:
    """
    Read CSV with minimal type coercion. Everything as string/object; keep spaces.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    # Normalize headers in a predictable way
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # Strip leading/trailing spaces from all cell strings
    for c in df.columns:
        df[c] = df[c].apply(lambda x: normalize_text(x) if x != "" else "")
    # Convert to Nulls
    df = df.replace(['N/A', 'n/a', 'None'], None)
    return df


def df_to_json_row(row: pd.Series) -> str:
    return json.dumps({k: (None if v == "" else v) for k, v in row.to_dict().items()}, ensure_ascii=False)


def write_sql_script_to_db(conn, sql_path: str):
    with open(sql_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
