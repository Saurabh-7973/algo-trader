"""
main.py — Daily algo trading orchestrator (Nifty 500 scale).

Pipeline:
  1. Load stock basket from Google Sheets
  2. Batch-fetch ALL OHLC data in one pass (15 API calls for 500 stocks)
  3. Calculate levels + signals for every stock/timeframe
  4. Batch-write to Google Sheets
  5. Telegram alerts + GTT orders for signals
"""

import logging
import sys
from datetime import datetime
import yfinance as yf

import os
from config import TELEGRAM_BOT_TOKEN, BROKER
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


def run():
    logger.info("=" * 60)
    logger.info(f"Algo run started — {datetime.now().strftime('%Y-%m-%d %H:%M IST')}")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    # 1. Load basket
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
