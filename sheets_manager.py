"""
sheets_manager.py — Google Sheets integration using gspread.

Fixes v2:
  - write_levels() uses append_rows() (one API call) instead of per-row append_row()
  - append_signals_batch() similarly batches all signal rows in one call
  - Both avoid the 429 Write requests per minute quota error
"""

import json
import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from config import (GOOGLE_CREDENTIALS_JSON, SPREADSHEET_ID,
                    BASKET_SHEET, LEVELS_SHEET, SIGNALS_SHEET,
                    MONTHLY_SHEET, YEARLY_SHEET)

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_client() -> gspread.Client:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_stock_basket() -> list[str]:
    """Read Basket tab → return list of active stock symbols."""
    client = _get_client()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(BASKET_SHEET)
    records = sheet.get_all_records()
    symbols = [
        r["Symbol"].strip().upper()
        for r in records
        if str(r.get("Active", "")).strip().upper() == "YES"
    ]
    logger.info(f"Loaded {len(symbols)} active stocks from basket: {symbols}")
    return symbols


def write_levels(levels_list: list[dict]) -> None:
    """
    Overwrite Levels tab with today's calculated levels.
    Uses append_rows() with all rows in ONE API call — avoids 429 quota.
    """
    if not levels_list:
        logger.warning("write_levels: nothing to write.")
        return

    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(LEVELS_SHEET)
    ws.clear()

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    headers = list(levels_list[0].keys()) + ["Timestamp"]
    all_rows = [headers]
    for row in levels_list:
        all_rows.append(list(row.values()) + [ts])

    # ONE batch call — no per-row writes
    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Wrote {len(levels_list)} rows to {LEVELS_SHEET} tab in one batch.")


def append_signals_batch(signal_rows: list[dict]) -> None:
    """
    Append all signal rows to Signals tab in one batch call.
    """
    if not signal_rows:
        return

    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SIGNALS_SHEET)

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

    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    ws.clear()

    stored_on = datetime.now().strftime("%Y-%m-%d")
    all_rows = [["Symbol", "H5", "H6", "L5", "L6", "Stored On"]]
    for row in levels_list:
        all_rows.append([row["Symbol"], row["H5"], row["H6"],
                         row["L5"], row["L6"], stored_on])

    ws.append_rows(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Stored {len(levels_list)} rows to {tab_name} tab.")


def get_stored_levels(tab_name: str) -> dict[str, dict]:
    """Read stored Monthly or Yearly levels. Returns dict keyed by symbol."""
    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    records = ws.get_all_records()
    return {r["Symbol"]: r for r in records}
