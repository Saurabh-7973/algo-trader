"""
main.py — Daily algo trading orchestrator (v8)

Pipeline:
  1. Load stock basket from Google Sheets
  2. Fetch HLC for ALL three timeframes (current + previous for insider check)
  3. Calculate levels + signals (NRD AND Insider both required)
  4. Write levels + signals to Sheets
  5. On 1st of month  → refresh stored Monthly levels tab
  6. On 1st of Jan    → refresh stored Yearly levels tab
  7. Broadcast Telegram alert
"""

import logging
import os
import sys
from datetime import datetime

from config import BROKER, TELEGRAM_BOT_TOKEN
from data_fetcher import fetch_all_timeframes
from levels import TradingLevels
from sheets_manager import (
    get_stock_basket,
    write_levels,
    append_signals_batch,
    write_stored_levels,
)
from alerts import broadcast
from broker import place_gtt_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

REQUESTING_CHAT_ID = os.getenv("REQUESTING_CHAT_ID", "")
calc = TradingLevels()


def compute_levels(
    symbols: list[str],
    hlc_data: dict[str, dict],
    timeframe_label: str,
) -> tuple[list[dict], list[dict]]:
    """
    Compute levels + signals for one timeframe.

    hlc_data structure (from data_fetcher.fetch_all_timeframes):
        { "RELIANCE": {"current": {H,L,C,Date}, "previous": {H,L,C,Date}}, ... }

    Passes both current and previous period HLC to TradingLevels.calculate()
    so that the Insider condition is properly evaluated.

    Returns:
        levels_rows  — all stocks (for Sheets Levels tab)
        signal_rows  — stocks where Signal = NRD AND Insider = True
    """
    levels_rows = []
    signal_rows = []

    for symbol in symbols:
        data = hlc_data.get(symbol)
        if not data:
            logger.warning(f"  {symbol}/{timeframe_label}: no data, skipping.")
            continue

        current  = data.get("current")
        previous = data.get("previous")

        if not current:
            continue

        try:
            result = calc.calculate(
                high=current["High"],
                low=current["Low"],
                close=current["Close"],
                # Pass previous period data → enables Insider condition
                prev_high=previous["High"]  if previous else None,
                prev_low=previous["Low"]    if previous else None,
                prev_close=previous["Close"] if previous else None,
            )
        except Exception as e:
            logger.error(f"  {symbol}/{timeframe_label}: calc error — {e}")
            continue

        row = {
            "Symbol":    symbol,
            "Timeframe": timeframe_label,
            "Date":      current.get("Date", ""),
            "High":      current["High"],
            "Low":       current["Low"],
            "Close":     current["Close"],
            **{k: round(v, 2) if isinstance(v, float) else v
               for k, v in result.items()},
        }
        levels_rows.append(row)

        if result.get("Signal"):
            signal_rows.append(row)
            logger.info(
                f"  SIGNAL {symbol} [{timeframe_label}] "
                f"H6={result.get('H6')} L6={result.get('L6')} "
                f"NRD={result.get('NRD')} Insider={result.get('Insider')}"
            )

    return levels_rows, signal_rows


def run():
    now = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Algo run started — {now.strftime('%Y-%m-%d %H:%M')} IST")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    # 1. Load basket
    symbols = get_stock_basket()
    if not symbols:
        msg = "Basket is empty — no active stocks. Add stocks to Basket tab (Active=YES)."
        logger.warning(msg)
        broadcast(message=msg, requesting_chat_id=REQUESTING_CHAT_ID)
        return
    logger.info(f"Basket: {len(symbols)} active stocks")

    # 2. Fetch HLC — current + previous for all three timeframes
    logger.info("Fetching HLC for all timeframes ...")
    all_hlc     = fetch_all_timeframes(symbols)
    daily_hlc   = all_hlc["daily"]
    monthly_hlc = all_hlc["monthly"]
    yearly_hlc  = all_hlc["yearly"]

    logger.info(
        f"Fetched — daily:{len(daily_hlc)} monthly:{len(monthly_hlc)} yearly:{len(yearly_hlc)}"
    )

    # 3. Calculate levels + signals for each timeframe
    daily_levels,   daily_signals   = compute_levels(symbols, daily_hlc,   "Daily")
    monthly_levels, monthly_signals = compute_levels(symbols, monthly_hlc, "Monthly")
    yearly_levels,  yearly_signals  = compute_levels(symbols, yearly_hlc,  "Yearly")

    logger.info(
        f"Signals — daily:{len(daily_signals)} "
        f"monthly:{len(monthly_signals)} yearly:{len(yearly_signals)}"
    )

    all_levels  = daily_levels  + monthly_levels  + yearly_levels
    all_signals = daily_signals + monthly_signals + yearly_signals

    # 4. Write to Sheets
    if all_levels:
        write_levels(all_levels)
    if all_signals:
        append_signals_batch(all_signals)

    # 5. Refresh stored Monthly tab on 1st of month
    if now.day == 1 and monthly_levels:
        logger.info("1st of month — refreshing Monthly stored levels ...")
        write_stored_levels("Monthly", [
            {"Symbol": r["Symbol"], "H5": r.get("H5"), "H6": r.get("H6"),
             "L5": r.get("L5"),     "L6": r.get("L6")}
            for r in monthly_levels
        ])

    # 6. Refresh stored Yearly tab on 1st Jan
    if now.month == 1 and now.day == 1 and yearly_levels:
        logger.info("1st Jan — refreshing Yearly stored levels ...")
        write_stored_levels("Yearly", [
            {"Symbol": r["Symbol"], "H5": r.get("H5"), "H6": r.get("H6"),
             "L5": r.get("L5"),     "L6": r.get("L6")}
            for r in yearly_levels
        ])

    # 7. GTT orders (only when broker is configured)
    if BROKER not in ("", "paper"):
        for sig in all_signals:
            sym = sig["Symbol"]
            h6  = sig.get("H6")
            l6  = sig.get("L6")
            if h6 and l6:
                try:
                    result = place_gtt_orders(symbol=sym, h6=h6, l6=l6, qty=1)
                    logger.info(f"GTT {sym}: {result}")
                except Exception as e:
                    logger.error(f"GTT failed {sym}: {e}")
    else:
        logger.info("BROKER=PAPER — GTT orders skipped")

    # 8. Broadcast Telegram alert
    broadcast(
        daily_signals=daily_signals,
        monthly_signals=monthly_signals,
        yearly_signals=yearly_signals,
        run_date=now.strftime("%d %b %Y"),
        requesting_chat_id=REQUESTING_CHAT_ID,
    )

    logger.info("=" * 60)
    logger.info(f"Run complete. {len(symbols)} scanned. {len(all_signals)} signals.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
