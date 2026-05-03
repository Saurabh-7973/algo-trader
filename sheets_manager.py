"""
sheets_manager.py — Google Sheets integration using gspread (v7).
All writes are batched (one API call) to avoid 429 quota errors.
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


def get_client() -> gspread.Client:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _open_sheet(tab_name: str):
    return get_client().open_by_key(SPREADSHEET_ID).worksheet(tab_name)


def get_stock_basket() -> list[str]:
    """Read Basket tab → return list of active stock symbols (no .NS suffix)."""
    sheet = _open_sheet(BASKET_SHEET)
    records = sheet.get_all_records()
    symbols = [
        r["Symbol"].strip().upper().replace(".NS", "")
        for r in records
        if str(r.get("Active", "")).strip().upper() in ("YES", "TRUE", "Y", "1")
        and r.get("Symbol", "").strip() != ""
    ]
    logger.info(f"Loaded {len(symbols)} active stocks from basket")
    return symbols


def write_levels(levels_list: list[dict]) -> None:
    """
    Overwrite Levels tab with all timeframe levels.
    One batch API call — avoids 429 quota.
    """
    if not levels_list:
        logger.warning("write_levels: nothing to write.")
        return

    ws = _open_sheet(LEVELS_SHEET)
    ws.clear()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    headers = list(levels_list[0].keys()) + ["Timestamp"]
    all_rows = [headers] + [list(row.values()) + [ts] for row in levels_list]
    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Wrote {len(levels_list)} rows to {LEVELS_SHEET} tab.")


def append_signals_batch(signal_rows: list[dict]) -> None:
    """Append all signal rows to Signals tab in one batch call."""
    if not signal_rows:
        return

    ws = _open_sheet(SIGNALS_SHEET)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    existing = ws.get_all_values()
    rows_to_append = []

    if not existing or not existing[0]:
        rows_to_append.append(list(signal_rows[0].keys()) + ["Timestamp"])

    for row in signal_rows:
        rows_to_append.append(list(row.values()) + [ts])

    ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    logger.info(f"Appended {len(signal_rows)} signal(s) to {SIGNALS_SHEET} tab.")


def write_stored_levels(tab_name: str, levels_list: list[dict]) -> None:
    """Write H5/H6/L5/L6 to Monthly or Yearly tab in one batch."""
    if not levels_list:
        return

    ws = _open_sheet(tab_name)
    ws.clear()
    stored_on = datetime.now().strftime("%Y-%m-%d")
    all_rows = [["Symbol", "H5", "H6", "L5", "L6", "Stored On"]]
    for row in levels_list:
        all_rows.append([
            row.get("Symbol", ""),
            row.get("H5", ""),
            row.get("H6", ""),
            row.get("L5", ""),
            row.get("L6", ""),
            stored_on,
        ])
    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Stored {len(levels_list)} rows to {tab_name} tab.")


def get_stored_levels(tab_name: str) -> dict[str, dict]:
    """Read stored Monthly or Yearly levels. Returns dict keyed by symbol."""
    ws = _open_sheet(tab_name)
    records = ws.get_all_records()
    return {r["Symbol"]: r for r in records}
