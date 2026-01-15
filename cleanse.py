
from __future__ import annotations
import re
from typing import Tuple, List, Dict, Optional
import pandas as pd
from datetime import datetime, timezone
from io_utils import normalize_text


# -------------------------------------------------------------------
# Lookup dictionaries for cleaning and enrichment
# -------------------------------------------------------------------

# Mapp of country code 
COUNTRY_CODE = {
    "US": "+1", "CA": "+1", "GB": "+44", "UK": "+44", "FR": "+33", "DE": "+49", "ES": "+34", "CN": "+86", "JP": "+81", "IN": "+91"
}

# Map currency symbols 
CURRENCY_SYMBOL = {
    "$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "元": "CNY", "円": "JPY"
}


# Map of Numeric text to Number 
WORD_TO_INT = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
}

# Supported date formats for parsing
DATE_FORMATS = [
    "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%d-%m-%Y", "%m/%d/%y", "%d/%m/%Y"
]



# -------------------------------------------------------------------
# DATE PARSING
# -------------------------------------------------------------------

def parse_date(s: str | None) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    # First attempt: flexible pandas parser
    try:
        ts = pd.to_datetime(s, utc=True, errors="raise", dayfirst=False)
        return ts.date().isoformat()
    except Exception:
        pass
    # Manual fallback formats
    for fmt in DATE_FORMATS:
        try:
            if "%z" in fmt:
                # Format includes timezone, normalize to UTC
                dt = datetime.strptime(s, fmt).astimezone(timezone.utc)
                return dt.date().isoformat()
            else:
                # Pure date formats
                if fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%m/%d/%y", "%d/%m/%Y"):
                    dt = datetime.strptime(s, fmt)
                    return dt.date().isoformat()
                # Datetime without timezone → set UTC manually
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                return dt.date().isoformat()
        except Exception:
            continue
    return None  # All attempts failed



# -------------------------------------------------------------------
# PHONE NORMALIZATION
# -------------------------------------------------------------------

def normalize_phone(phone_raw: str | None, country: str | None) -> Optional[str]:
    if not phone_raw:
        return None
    # Keep digits and + only
    digits = re.sub(r"[^\d+]", "", phone_raw)
    # Already normalized international format?
    if digits.startswith("+") and len(re.sub(r"[^\d]", "", digits)) >= 8:
        return digits
    # If country is known, prepend correct code
    cc = COUNTRY_CODE.get((country or "").upper())
    national = re.sub(r"\D", "", digits)
    if cc and national:
        # Remove leading zeros inside national numbers
        national = re.sub(r"^0+", "", national)
        return f"{cc}{national}"
    # Fallback: just return digits, but may be incomplete
    return re.sub(r"\D", "", digits) or None



# -------------------------------------------------------------------
# QUANTITY PARSING
# -------------------------------------------------------------------

def parse_quantity(q: str | None) -> Optional[int]:
    if q is None or q == "":
        return None
    qn = q.strip().lower()
    # Natural-language words (one, two, ...)
    if qn in WORD_TO_INT:
        return WORD_TO_INT[qn]
    # Strict integer check (allows leading '-')
    if re.fullmatch(r"-?\d+", qn):
        return int(qn)
    return None



# -------------------------------------------------------------------
# CURRENCY DETECTION
# -------------------------------------------------------------------

def detect_currency_from_text(txt: str) -> Optional[str]:
    if not txt:
        return None
    # Look for ISO currency
    m = re.search(r"\b([A-Z]{3})\b", txt)
    if m:
        return m.group(1)
    # Look for symbols
    for sym, code in CURRENCY_SYMBOL.items():
        if sym in txt:
            return code
    return None



# -------------------------------------------------------------------
# PRICE NORMALIZATION
# -------------------------------------------------------------------

def parse_price(value: str | None, currency_col: str | None) -> Tuple[Optional[float], Optional[str]]:
    if not value and not currency_col:
        return None, None
    s = (value or "").strip()
    cur = (currency_col or "").strip().upper() or detect_currency_from_text(s)
    # Remove explicit currency tokens
    s_no_cur = re.sub(r"([A-Z]{3}|USD|EUR|GBP|JPY|CNY|CHF|AUD|CAD|NZD)", "", s, flags=re.I)
    # Remove currency symbols
    for sym in CURRENCY_SYMBOL.keys():
        s_no_cur = s_no_cur.replace(sym, "")
    s_no_cur = s_no_cur.strip()
    # Decimal/comma normalization
    if "," in s_no_cur and "." in s_no_cur:
        # European format → "." thousands, "," decimal
        s_no_cur = s_no_cur.replace(".", "").replace(",", ".")
    else:
        if re.search(r"\d+,\d{2}\b", s_no_cur):
            s_no_cur = s_no_cur.replace(",", ".")
        else:
            s_no_cur = s_no_cur.replace(",", "")
    # Remove spaces + non-numeric junk
    s_no_cur = s_no_cur.replace(" ", "")
    s_no_cur = re.sub(r"[^0-9.\-]", "", s_no_cur)
    # Try numeric conversion
    try:
        price = float(s_no_cur)
    except Exception:
        price = None
    return price, cur or detect_currency_from_text(value or "")



# -------------------------------------------------------------------
# VALIDATION RULES
# -------------------------------------------------------------------

def validate_row_canonical(row: Dict) -> List[str]:
    errs: List[str] = []
    # Required ID
    if row.get("order_id") is None:
        errs.append("missing order_id")

    # Date checks
    od = row.get("order_date")
    sd = row.get("ship_date")
    if not od:
        errs.append("invalid order_date")
    if sd and od and sd < od:
        errs.append("ship_date earlier than order_date")

    # Quantity checks
    q = row.get("quantity")
    if q is None:
        errs.append("invalid quantity")
    elif q < 0:
        errs.append("negative quantity")

    # Price checks
    p = row.get("unit_price")
    if p is None:
        errs.append("invalid unit_price")
    elif p < 0:
        errs.append("negative unit_price")

    # Currency
    if not row.get("currency"):
        errs.append("missing currency")

    # SKU presence
    if not row.get("item_sku"):
        errs.append("missing item_sku")

    # Email sanity
    email = row.get("email")
    if email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email, re.IGNORECASE):
        errs.append("invalid email")

    # Ensure at least one contact method exists
    phone = row.get("phone")
    addr = row.get("address")
    if (email is None) and (phone is None) and (addr is None):
        errs.append("Contact info Missing")

    # Ensure at least customer ID or name exists
    cust_id = row.get("customer_id")
    cust_nm = row.get("customer_name")
    if (cust_id is None) and (cust_nm is None):
        errs.append("Customer info Missing")

    return errs



# -------------------------------------------------------------------
# CANONICALIZATION PIPELINE
# -------------------------------------------------------------------

def canonicalize_row(raw: Dict) -> Dict:
    # Shortcut alias
    def N(x): return normalize_text(x) if x not in (None, "") else None
    # --- order_id numeric extraction ---
    try:
        order_id = int(re.sub(r"[^\d]", "", raw.get("order_id", ""))) \
                   if raw.get("order_id") else None
    except Exception:
        order_id = None

    # --- field conversions ---
    order_dt = parse_date(raw.get("order_date"))
    ship_dt = parse_date(raw.get("ship_date"))
    qty = parse_quantity(raw.get("quantity"))
    price, currency = parse_price(raw.get("unit_price"), raw.get("currency"))
    phone = normalize_phone(raw.get("phone"), raw.get("country"))

    # --- unified canonical record ---
    canon = {
        "order_id": order_id,
        "customer_id": N(raw.get("customer_id")),
        "customer_name": N(raw.get("customer_name")),
        "email": N(raw.get("email")),
        "phone": phone,
        "country": (raw.get("country") or "").upper() or None,
        "state": N(raw.get("state")),
        "city": N(raw.get("city")),
        "address": N(raw.get("address")),
        "postal_code": N(raw.get("postal_code")),
        "order_date": order_dt,
        "ship_date": ship_dt,
        "ship_mode": N(raw.get("ship_mode")),
        "item_sku": N(raw.get("item_sku")),
        "item_name": N(raw.get("item_name")),
        "quantity": qty,
        "unit_price": price,
        "currency": currency,
        "discount_code": N(raw.get("discount_code")),
        "order_notes": N(raw.get("order_notes")),
    }

    return canon
