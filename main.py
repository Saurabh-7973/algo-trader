"""
main.py — Daily algo trading orchestrator (Nifty 500 scale).

Pipeline:
  1. Optionally auto-refresh basket from Nifty 500 feed (if Settings tab allows)
  2. Load stock basket from Google Sheets
  3. Batch-fetch ALL OHLC data in one pass (15 API calls for 500 stocks)
  4. Calculate levels + signals for every stock/timeframe
  5. Batch-write to Google Sheets
  6. Telegram alerts + GTT orders for signals
"""

import logging
import sys
import os
import time
import io
import csv
import requests
from datetime import datetime
import yfinance as yf

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BROKER
from data_fetcher import fetch_all_ohlc_batch, get_all_ohlc, is_first_day_of_month, is_first_day_of_year
from levels import calculate_levels, levels_to_dict, OHLC
from sheets_manager import (get_stock_basket, write_levels, append_signals_batch,
                            write_stored_levels)
from alerts import send_telegram, broadcast, get_active_chat_ids, format_signal_message, format_daily_summary
from broker import place_gtt_orders

# On-demand scan: if triggered by /scan command, results go to requesting user
REQUESTING_CHAT_ID = os.getenv("REQUESTING_CHAT_ID", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
#  NEW: Auto-basket toggle and refresh functions
# ---------------------------------------------------------------------------

def _read_auto_basket_setting() -> bool:
    """
    Reads the AUTO_BASKET flag from the Settings tab.
    Returns True if AUTO_BASKET is exactly 'TRUE' (case-insensitive),
    otherwise False (including missing tab/cell).
    """
    try:
        from sheets_manager import _get_client, SPREADSHEET_ID
        client = _get_client()
        ws = client.open_by_key(SPREADSHEET_ID).worksheet("Settings")
        # Get cell B2 (row 2, col 2)
        value = ws.acell("B2").value
        return value is not None and value.strip().upper() == "TRUE"
    except Exception as e:
        logger.warning(f"Could not read Settings tab: {e}. Defaulting to manual basket.")
        return False


def refresh_nifty500_basket() -> bool:
    """
    Fetches latest Nifty 500 constituents from NSE, validates via yfinance,
    and overwrites the Basket sheet with all valid symbols set to Active=YES.
    Returns True on success, False if it fails or skips.
    """
    from sheets_manager import _get_client, SPREADSHEET_ID, BASKET_SHEET
    import yfinance as yf
    import pandas as pd

    URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

    # --- Step 1: Download CSV ---
    try:
        response = requests.get(URL, timeout=15)
        response.raise_for_status()
    except Exception as e:
        logger.warning(f"Could not fetch Nifty 500 CSV: {e}. Skipping basket refresh.")
        return False

    # --- Step 2: Parse CSV ---
    reader = csv.DictReader(io.StringIO(response.text))
    symbols_raw = [
        row["Symbol"].strip()
        for row in reader
        if row.get("Symbol") and row["Symbol"].strip()
    ]
    if not symbols_raw:
        logger.warning("No symbols found in Nifty 500 CSV. Skipping refresh.")
        return False

    logger.info(f"Fetched {len(symbols_raw)} raw symbols from NSE.")

    # --- Step 3: Validate with yfinance (batch) ---
    tickers = [f"{s}.NS" for s in symbols_raw]
    valid = []

    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        try:
            df = yf.download(
                batch, period="1d", interval="1d",
                group_by="ticker", auto_adjust=True,
                progress=False, threads=True, timeout=15
            )
        except Exception:
            continue

        # Check which symbols returned data
        for t in batch:
            try:
                if isinstance(df.columns, pd.MultiIndex):
                    if t in df.columns.get_level_values(0):
                        series = df[t]
                        if not series.dropna().empty:
                            valid.append(t)
                else:
                    # Single stock edge case
                    if not df.empty:
                        valid.append(t)
            except Exception:
                pass

        time.sleep(0.5)   # gentle pacing

    if not valid:
        logger.warning("All fetched symbols failed validation. Skipping refresh.")
        return False

    logger.info(f"Validated {len(valid)} symbols – updating Basket sheet.")

    # --- Step 4: Overwrite Basket sheet ---
    client = _get_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(BASKET_SHEET)
    ws.clear()

    # Header
    ws.append_row(["Symbol", "Active", "Notes"])
    # All valid stocks, Active=YES
    rows = [[s, "YES", ""] for s in sorted(valid)]
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    logger.info("Basket refreshed with latest Nifty 500 constituents.")
    return True


# ---------------------------------------------------------------------------
#  Existing helper
# ---------------------------------------------------------------------------

def get_prev_day_for_insider(symbol: str) -> OHLC | None:
    """Fetch day-before-yesterday for insider condition comparison (daily TF only)."""
    try:
        from data_fetcher import _ticker
        df = yf.Ticker(_ticker(symbol)).history(period="5d", interval="1d")
        if df is None or len(df) < 3:
            return None
        row = df.iloc[-3]
        return OHLC(
            high=round(float(row["High"]), 2),
            low=round(float(row["Low"]), 2),
            close=round(float(row["Close"]), 2),
        )
    except Exception:
        return None


# ---------------------------------------------------------------------------
#  Main pipeline
# ---------------------------------------------------------------------------

def run():
    logger.info("=" * 60)
    logger.info(f"Algo run started — {datetime.now().strftime('%Y-%m-%d %H:%M IST')}")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    # ── NEW: Auto-refresh basket if Settings tab says so ──────────────────
    if _read_auto_basket_setting():
        logger.info("AUTO_BASKET=TRUE – refreshing basket from Nifty 500 feed...")
        success = refresh_nifty500_basket()
        if not success:
            logger.warning("Auto-refresh basket failed. Falling back to current basket.")
    else:
        logger.info("AUTO_BASKET=FALSE – using manual basket as-is.")
    # ────────────────────────────────────────────────────────────────────

    # 1. Load basket (now possibly updated)
    symbols = get_stock_basket()
    if not symbols:
        msg = "No active stocks in basket. Add stocks to Basket tab with Active=YES."
        logger.warning(msg)
        send_telegram(f"⚠️ Algo Trader: {msg}", TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        return

    logger.info(f"Basket loaded: {len(symbols)} stocks")

    # 2. Batch fetch ALL OHLC (one pass — 15 API calls for 500 stocks)
    logger.info("Starting batch OHLC fetch...")
    all_ohlc = fetch_all_ohlc_batch(symbols)

    # 3. Calculate levels + signals
    all_levels_rows = []
    signal_rows     = []
    all_results     = []
    monthly_store   = []
    yearly_store    = []

    first_of_month = is_first_day_of_month()
    first_of_year  = is_first_day_of_year()

    for symbol in symbols:
        ohlc = all_ohlc.get(symbol, {"daily": None, "monthly": None, "yearly": None})
        previous_day = get_prev_day_for_insider(symbol)

        for timeframe, data in ohlc.items():
            if data is None:
                logger.warning(f"  {symbol}/{timeframe}: data unavailable, skipping.")
                continue

            prev = previous_day if timeframe == "daily" else None
            lv   = calculate_levels(symbol, data, timeframe, previous=prev)
            row_dict = levels_to_dict(lv)
            all_levels_rows.append(row_dict)
            all_results.append(lv)

            stored = {"Symbol": symbol, "H5": lv.H5, "H6": lv.H6,
                      "L5": lv.L5, "L6": lv.L6}
            if timeframe == "monthly" and first_of_month:
                monthly_store.append(stored)
            if timeframe == "yearly" and first_of_year:
                yearly_store.append(stored)

            if lv.signal:
                logger.info(f"  SIGNAL: {symbol} [{timeframe}] BUY@{lv.H6} SELL@{lv.L6}")
                signal_rows.append(row_dict)
                send_telegram(format_signal_message(lv), TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
                place_gtt_orders(symbol, lv.H6, lv.L6, qty=1)

    # 4. Write to Sheets (all batched)
    logger.info(f"Writing {len(all_levels_rows)} level rows to Sheets...")
    write_levels(all_levels_rows)

    if signal_rows:
        append_signals_batch(signal_rows)

    if first_of_month and monthly_store:
        write_stored_levels("Monthly", monthly_store)

    if first_of_year and yearly_store:
        write_stored_levels("Yearly", yearly_store)

    # 5. Daily summary — get active users from Sheets Users tab, broadcast to all
    import os as _os
    from sheets_manager import _get_client as _gc
    try:
        _sc = _gc()
        _sid = _os.getenv("SPREADSHEET_ID", "")
    except Exception:
        _sc, _sid = None, None
    active_ids = get_active_chat_ids(_sc, _sid)
    summary    = format_daily_summary(all_results)
    broadcast(summary, active_ids)

    logger.info("=" * 60)
    signals_count = sum(1 for r in all_results if r.signal)
    logger.info(f"Run complete. {len(symbols)} stocks scanned. {signals_count} signals.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
