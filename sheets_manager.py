"""
sheets_manager.py — Google Sheets integration using gspread.

Sheet structure expected:

TAB: Basket
  Columns: Symbol | Exchange | Active
  Example: RELIANCE | NSE | YES

TAB: Levels
  Auto-written by this script — all calculated H/L levels per stock per run.

TAB: Signals
  Appended when both NRD + Insider conditions fire.

TAB: Monthly
  H5, H6, L5, L6 from previous month HLC (stored on 1st of each month).

TAB: Yearly
  H5, H6, L5, L6 from previous year HLC (stored on 1st of January).
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
    """Authenticate and return gspread client using service account JSON."""
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_stock_basket() -> list[str]:
    """
    Read the Basket tab and return list of active stock symbols.
    Skips rows where Active != YES.
    """
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
    Overwrite the Levels tab with today's calculated levels for all stocks.
    """
    if not levels_list:
        return
    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(LEVELS_SHEET)
    ws.clear()

    headers = list(levels_list[0].keys()) + ["Timestamp"]
    ws.append_row(headers)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    for row in levels_list:
        ws.append_row(list(row.values()) + [ts])

    logger.info(f"Wrote {len(levels_list)} rows to {LEVELS_SHEET} tab.")


def append_signal(signal_row: dict) -> None:
    """
    Append a single signal row to the Signals tab (never overwrites, always appends).
    """
    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SIGNALS_SHEET)

    # Write headers if sheet is empty
    if ws.row_count < 1 or not ws.row_values(1):
        ws.append_row(list(signal_row.keys()) + ["Timestamp"])

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    ws.append_row(list(signal_row.values()) + [ts])
    logger.info(f"Signal appended for {signal_row.get('Symbol')}")


def write_stored_levels(tab_name: str, levels_list: list[dict]) -> None:
    """
    Write stored H5, H6, L5, L6 levels to Monthly or Yearly tab.
    Called only on 1st of month (monthly) or 1st Jan (yearly).
    """
    if not levels_list:
        return
    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    ws.clear()

    headers = ["Symbol", "H5", "H6", "L5", "L6", "Stored On"]
    ws.append_row(headers)

    stored_on = datetime.now().strftime("%Y-%m-%d")
    for row in levels_list:
        ws.append_row([
            row["Symbol"],
            row["H5"], row["H6"],
            row["L5"], row["L6"],
            stored_on
        ])
    logger.info(f"Stored {len(levels_list)} rows to {tab_name} tab.")


def get_stored_levels(tab_name: str) -> dict[str, dict]:
    """
    Read stored Monthly or Yearly levels.
    Returns dict keyed by symbol: { "RELIANCE": {"H5":..., "H6":..., "L5":..., "L6":...} }
    """
    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(tab_name)
    records = ws.get_all_records()
    return {r["Symbol"]: r for r in records}
