"""
main.py — Daily algo trading orchestrator.

Runs every trading day at 9:15 AM IST via GitHub Actions.

Pipeline:
  1. Load stock basket from Google Sheets
  2. Fetch OHLC data via yfinance
  3. Calculate H3–H6, L3–L6 levels for all timeframes
  4. Check NRD + Insider Trading Level conditions
  5. Write all levels to Google Sheets (Levels tab)
  6. On 1st of month → store monthly H5/H6/L5/L6 (Monthly tab)
  7. On 1st of Jan  → store yearly  H5/H6/L5/L6 (Yearly tab)
  8. For each signal: append to Signals tab + send Telegram alert + place GTT orders
  9. Send daily summary via Telegram
"""

import logging
import sys
from datetime import datetime

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BROKER
from data_fetcher import (get_all_ohlc, get_previous_day_ohlc,
                           is_first_day_of_month, is_first_day_of_year)
from levels import calculate_levels, levels_to_dict
from sheets_manager import (get_stock_basket, write_levels, append_signal,
                             write_stored_levels)
from alerts import send_telegram, format_signal_message, format_daily_summary
from broker import place_gtt_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


def run():
    logger.info("=" * 60)
    logger.info(f"Algo run started — {datetime.now().strftime('%Y-%m-%d %H:%M IST')}")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    # ── 1. Load stock basket ──────────────────────────────────────────────────
    symbols = get_stock_basket()
    if not symbols:
        logger.warning("No active stocks in basket. Exiting.")
        return

    logger.info(f"Processing {len(symbols)} stocks...")

    all_levels   = []
    all_results  = []
    monthly_store = []
    yearly_store  = []

    first_of_month = is_first_day_of_month()
    first_of_year  = is_first_day_of_year()

    # ── 2–4. Fetch data + calculate levels ───────────────────────────────────
    for symbol in symbols:
        logger.info(f"--- Processing {symbol} ---")

        ohlc = get_all_ohlc(symbol)

        # Previous day for previous comparison (insider condition)
        # We use daily OHLC as "current" and fetch 2 days back for "previous"
        import yfinance as yf
        raw = yf.Ticker(symbol + ".NS").history(period="5d", interval="1d")

        from levels import OHLC
        current_day  = ohlc["daily"]
        previous_day = None
        if raw is not None and len(raw) >= 3:
            row = raw.iloc[-3]  # day before yesterday
            previous_day = OHLC(high=row["High"], low=row["Low"], close=row["Close"])

        # Calculate levels for each applicable timeframe
        for timeframe, data in ohlc.items():
            if data is None:
                logger.warning(f"  {symbol}/{timeframe}: data unavailable, skipping.")
                continue

            # For insider condition, we pass previous day data (for daily TF)
            # For monthly/yearly we pass None (comparison not applicable intraday)
            prev = previous_day if timeframe == "daily" else None

            lv = calculate_levels(symbol, data, timeframe, previous=prev)
            row_dict = levels_to_dict(lv)
            all_levels.append(row_dict)
            all_results.append(lv)

            # Store H5/H6/L5/L6 if needed
            stored_row = {"Symbol": symbol, "H5": lv.H5, "H6": lv.H6,
                          "L5": lv.L5, "L6": lv.L6}
            if timeframe == "monthly" and first_of_month:
                monthly_store.append(stored_row)
            if timeframe == "yearly" and first_of_year:
                yearly_store.append(stored_row)

            # ── 8. Signal actions ─────────────────────────────────────────────
            if lv.signal:
                logger.info(f"  🟢 SIGNAL: {symbol} [{timeframe}] | BUY @ {lv.H6} | SELL @ {lv.L6}")

                # Append to Signals sheet
                append_signal(row_dict)

                # Telegram individual signal alert
                send_telegram(
                    format_signal_message(lv),
                    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
                )

                # Place GTT orders
                order_result = place_gtt_orders(symbol, lv.H6, lv.L6, qty=1)
                logger.info(f"  Order result: {order_result}")
            else:
                logger.info(f"  {symbol}/{timeframe}: No signal (NRD={lv.is_nrd}, Insider={lv.is_insider})")

    # ── 5. Write all levels to Sheets ─────────────────────────────────────────
    write_levels(all_levels)

    # ── 6–7. Store monthly / yearly levels if applicable ──────────────────────
    if first_of_month and monthly_store:
        write_stored_levels("Monthly", monthly_store)
        logger.info(f"Monthly levels stored for {len(monthly_store)} stocks.")

    if first_of_year and yearly_store:
        write_stored_levels("Yearly", yearly_store)
        logger.info(f"Yearly levels stored for {len(yearly_store)} stocks.")

    # ── 9. Daily summary Telegram ─────────────────────────────────────────────
    summary = format_daily_summary(all_results)
    send_telegram(summary, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    logger.info("=" * 60)
    logger.info("Run complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
