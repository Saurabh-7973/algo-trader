"""
sheets_manager.py — NSE Full Market Edition (feature/nse-full-market branch)

Key addition: read_stored_levels() and write_full_stored_levels()
These store the FULL calculated signal rows (including NRD, Insider, Signal)
so that on non-refresh days we don't need to re-fetch or re-calculate.

Monthly tab: full level rows calculated on 1st of each month
Yearly tab:  full level rows calculated on 1st Jan each year
Both tabs include Signal=YES/NO so the daily run just reads and reports.
"""

import json
import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from config import (
    GOOGLE_CREDENTIALS_JSON, SPREADSHEET_ID,
    BASKET_SHEET, LEVELS_SHEET, SIGNALS_SHEET,
    MONTHLY_SHEET, YEARLY_SHEET,
)

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Settings tab keys
SETTINGS_SHEET = "Settings"
SETTING_MIN_PRICE  = "MIN_PRICE"     # float — minimum stock price filter
SETTING_USE_NSE    = "USE_NSE_LIST"  # TRUE/FALSE — use NSE full list vs Basket tab


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_client() -> gspread.Client:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _sheet(tab: str):
    return _get_client().open_by_key(SPREADSHEET_ID).worksheet(tab)


# ── Settings ──────────────────────────────────────────────────────────────────

def get_settings() -> dict:
    """
    Read Settings tab. Returns dict of {Key: Value}.
    Defaults if tab doesn't exist:
      MIN_PRICE    = 100.0
      USE_NSE_LIST = False
    """
    defaults = {
        SETTING_MIN_PRICE: 100.0,
        SETTING_USE_NSE:   False,
    }
    try:
        records = _sheet(SETTINGS_SHEET).get_all_records()
        for r in records:
            k = str(r.get("Key", "")).strip().upper()
            v = str(r.get("Value", "")).strip()
            if k == SETTING_MIN_PRICE:
                try:
                    defaults[SETTING_MIN_PRICE] = float(v)
                except ValueError:
                    pass
            elif k == SETTING_USE_NSE:
                defaults[SETTING_USE_NSE] = v.upper() in ("TRUE", "YES", "1")
    except Exception as e:
        logger.warning(f"Settings tab not found or error: {e} — using defaults")
    return defaults


# ── Basket ────────────────────────────────────────────────────────────────────

def get_stock_basket() -> list[str]:
    """
    Read Basket tab → return active symbols (no .NS suffix).
    Active = YES / TRUE / Y / 1 (case-insensitive).
    Blank rows skipped.
    """
    records = _sheet(BASKET_SHEET).get_all_records()
    symbols = [
        r["Symbol"].strip().upper().replace(".NS", "")
        for r in records
        if str(r.get("Active", "")).strip().upper() in ("YES", "TRUE", "Y", "1")
        and r.get("Symbol", "").strip()
    ]
    logger.info(f"Basket: {len(symbols)} active stocks")
    return symbols


# ── Levels sheet ──────────────────────────────────────────────────────────────

def write_levels(levels_list: list[dict]) -> None:
    """
    Overwrite Levels tab in one batch (avoids 429 quota).
    Splits into chunks of 500 rows to stay under Google Sheets payload limit.
    """
    if not levels_list:
        return

    ws = _sheet(LEVELS_SHEET)
    ws.clear()

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    headers = list(levels_list[0].keys()) + ["Timestamp"]
    all_rows = [headers]
    for row in levels_list:
        all_rows.append(list(row.values()) + [ts])

    # Chunk to avoid payload size limit
    CHUNK = 500
    for i in range(0, len(all_rows), CHUNK):
        ws.append_rows(all_rows[i:i + CHUNK], value_input_option="USER_ENTERED")

    logger.info(f"Wrote {len(levels_list)} rows to Levels tab.")


# ── Signals sheet ─────────────────────────────────────────────────────────────

def append_signals_batch(signal_rows: list[dict]) -> None:
    """Append signal rows to Signals tab (never overwrites — cumulative log)."""
    if not signal_rows:
        return

    ws = _sheet(SIGNALS_SHEET)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    existing = ws.get_all_values()
    rows = []

    if not existing or not existing[0]:
        rows.append(list(signal_rows[0].keys()) + ["Timestamp"])

    for row in signal_rows:
        rows.append(list(row.values()) + [ts])

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    logger.info(f"Appended {len(signal_rows)} signal(s) to Signals tab.")


# ── Stored levels (Monthly / Yearly) ─────────────────────────────────────────

# Full column set stored for monthly/yearly tabs
_STORED_COLS = [
    "Symbol", "Date",
    "H3", "H4", "H5", "H6",
    "L3", "L4", "L5", "L6",
    "NRD", "Insider", "Signal",
    "GTT_Buy_H6", "GTT_Sell_L6",
    "Stored_On",
]


def write_full_stored_levels(tab_name: str, rows: list[dict]) -> None:
    """
    Write full calculated level rows to Monthly or Yearly tab.
    Called only on 1st of month (Monthly) or 1st Jan (Yearly).

    Stores EVERYTHING including NRD, Insider, Signal so that on
    non-refresh days we just read the pre-computed results.
    """
    if not rows:
        return

    ws = _sheet(tab_name)
    ws.clear()

    stored_on = datetime.now().strftime("%Y-%m-%d")
    all_rows = [_STORED_COLS]
    for r in rows:
        all_rows.append([
            r.get("Symbol", ""),
            r.get("Date", ""),
            r.get("H3", ""), r.get("H4", ""), r.get("H5", ""), r.get("H6", ""),
            r.get("L3", ""), r.get("L4", ""), r.get("L5", ""), r.get("L6", ""),
            r.get("NRD", "NO"),
            r.get("Insider", "NO"),
            r.get("Signal", "NO"),
            r.get("GTT_Buy_H6", ""),
            r.get("GTT_Sell_L6", ""),
            stored_on,
        ])

    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Stored {len(rows)} rows to {tab_name} tab.")


def read_stored_levels(tab_name: str) -> list[dict]:
    """
    Read stored level rows from Monthly or Yearly tab.
    Returns list of dicts (one per symbol) — same format as compute output.
    Returns empty list if tab is empty or doesn't exist.
    """
    try:
        records = _sheet(tab_name).get_all_records()
        logger.info(f"Read {len(records)} stored rows from {tab_name} tab.")
        return records
    except Exception as e:
        logger.warning(f"Could not read {tab_name} tab: {e}")
        return []
