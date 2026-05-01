"""
main.py — Daily algo trading orchestrator.

Runs every trading day at 9:15 AM IST via GitHub Actions.

Pipeline:
  1. Load stock basket from Google Sheets
  2. Fetch OHLC data via yfinance
  3. Calculate H3-H6, L3-L6 levels for all timeframes
  4. Check NRD + Insider Trading Level conditions
  5. Write all levels to Google Sheets (Levels tab) — batched to avoid quota
  6. On 1st of month: store monthly H5/H6/L5/L6 (Monthly tab)
  7. On 1st of Jan:   store yearly  H5/H6/L5/L6 (Yearly tab)
  8. For each signal: Signals tab + Telegram alert + GTT orders
  9. Daily summary via Telegram

Fixes v2:
  - import yfinance moved to top (not inside loop)
  - symbol ticker uses _ticker() — prevents double .NS
  - Google Sheets writes are batched (one append_rows call per tab, not per row)
"""

import logging
import sys
import time
from datetime import datetime

import yfinance as yf

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BROKER
from data_fetcher import (get_all_ohlc, _ticker,
                           is_first_day_of_month, is_first_day_of_year)
from levels import calculate_levels, levels_to_dict, OHLC
from sheets_manager import (get_stock_basket, write_levels, append_signals_batch,
                             write_stored_levels)
from alerts import send_telegram, format_signal_message, format_daily_summary
from broker import place_gtt_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


def get_previous_day_raw(symbol: str) -> OHLC | None:
    """Fetch day-before-yesterday for insider condition comparison."""
    try:
        df = yf.Ticker(_ticker(symbol)).history(period="5d", interval="1d")
        if df is None or len(df) < 3:
            return None
        row = df.iloc[-3]
        return OHLC(high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2))
    except Exception:
        return None


def run():
    logger.info("=" * 60)
    logger.info(f"Algo run started — {datetime.now().strftime('%Y-%m-%d %H:%M IST')}")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    # 1. Load stock basket
    symbols = get_stock_basket()
    if not symbols:
        logger.warning("No active stocks in basket. Exiting.")
        return

    logger.info(f"Processing {len(symbols)} stocks...")

    all_levels_rows  = []   # all rows for Levels tab — written in ONE batch
    signal_rows      = []   # all signal rows — written in ONE batch
    all_results      = []
    monthly_store    = []
    yearly_store     = []

    first_of_month = is_first_day_of_month()
    first_of_year  = is_first_day_of_year()

    # 2-4. Fetch + calculate for each stock
    for symbol in symbols:
        logger.info(f"--- Processing {symbol} ---")

        ohlc         = get_all_ohlc(symbol)
        previous_day = get_previous_day_raw(symbol)

        for timeframe, data in ohlc.items():
            if data is None:
                logger.warning(f"  {symbol}/{timeframe}: data unavailable, skipping.")
                continue

            prev = previous_day if timeframe == "daily" else None
            lv   = calculate_levels(symbol, data, timeframe, previous=prev)
            row_dict = levels_to_dict(lv)
            all_levels_rows.append(row_dict)
            all_results.append(lv)

            stored_row = {"Symbol": symbol, "H5": lv.H5, "H6": lv.H6,
                          "L5": lv.L5, "L6": lv.L6}
            if timeframe == "monthly" and first_of_month:
                monthly_store.append(stored_row)
            if timeframe == "yearly" and first_of_year:
                yearly_store.append(stored_row)

            if lv.signal:
                logger.info(f"  SIGNAL: {symbol} [{timeframe}] | BUY @ {lv.H6} | SELL @ {lv.L6}")
                signal_rows.append(row_dict)

                send_telegram(format_signal_message(lv), TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

                order_result = place_gtt_orders(symbol, lv.H6, lv.L6, qty=1)
                logger.info(f"  Order result: {order_result}")
            else:
                logger.info(f"  {symbol}/{timeframe}: No signal (NRD={lv.is_nrd}, Insider={lv.is_insider})")

    # 5. Write ALL levels in one batch call (avoids 429 quota)
    logger.info("Writing levels to Google Sheets (batched)...")
    write_levels(all_levels_rows)

    # Signals batch write
    if signal_rows:
        append_signals_batch(signal_rows)

    # 6-7. Monthly / yearly stored levels
    if first_of_month and monthly_store:
        write_stored_levels("Monthly", monthly_store)
        logger.info(f"Monthly levels stored for {len(monthly_store)} stocks.")

    if first_of_year and yearly_store:
        write_stored_levels("Yearly", yearly_store)
        logger.info(f"Yearly levels stored for {len(yearly_store)} stocks.")

    # 9. Daily summary
    summary = format_daily_summary(all_results)
    send_telegram(summary, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    logger.info("=" * 60)
    logger.info("Run complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
