"""
main.py — Daily algo trading orchestrator (v8.1)

Levels sheet column order (clean, predictable):
  Symbol | Timeframe | Date | H3 | H4 | H5 | H6 | L3 | L4 | L5 | L6
  | NRD | Insider | Signal | GTT_Buy_H6 | GTT_Sell_L6
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


def _build_row(symbol: str, timeframe: str, current: dict, result: dict) -> dict:
    """
    Explicit column order for Levels sheet.
    Symbol | Timeframe | Date | H3 | H4 | H5 | H6 | L3 | L4 | L5 | L6
    | NRD | Insider | Signal | GTT_Buy_H6 | GTT_Sell_L6
    """
    return {
        "Symbol":      symbol,
        "Timeframe":   timeframe,
        "Date":        current.get("Date", ""),
        "H3":          result.get("H3", ""),
        "H4":          result.get("H4", ""),
        "H5":          result.get("H5", ""),
        "H6":          result.get("H6", ""),
        "L3":          result.get("L3", ""),
        "L4":          result.get("L4", ""),
        "L5":          result.get("L5", ""),
        "L6":          result.get("L6", ""),
        "NRD":         "YES" if result.get("NRD")     else "NO",
        "Insider":     "YES" if result.get("Insider") else "NO",
        "Signal":      "YES" if result.get("Signal")  else "NO",
        "GTT_Buy_H6":  result.get("H6", ""),
        "GTT_Sell_L6": result.get("L6", ""),
    }


def compute_levels(
    symbols: list[str],
    hlc_data: dict[str, dict],
    timeframe_label: str,
) -> tuple[list[dict], list[dict]]:
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
                prev_high=previous["High"]   if previous else None,
                prev_low=previous["Low"]     if previous else None,
                prev_close=previous["Close"] if previous else None,
            )
        except Exception as e:
            logger.error(f"  {symbol}/{timeframe_label}: calc error — {e}")
            continue

        row = _build_row(symbol, timeframe_label, current, result)
        levels_rows.append(row)

        if result.get("Signal"):
            signal_rows.append(row)
            logger.info(
                f"  SIGNAL {symbol} [{timeframe_label}] "
                f"H6={result.get('H6')} L6={result.get('L6')}"
            )

    return levels_rows, signal_rows


def run():
    now = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Algo run started — {now.strftime('%Y-%m-%d %H:%M')} IST")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    symbols = get_stock_basket()
    if not symbols:
        msg = "Basket is empty — no active stocks. Add stocks to Basket tab (Active=YES)."
        logger.warning(msg)
        broadcast(message=msg, requesting_chat_id=REQUESTING_CHAT_ID)
        return
    logger.info(f"Basket: {len(symbols)} active stocks")

    logger.info("Fetching HLC for all timeframes ...")
    all_hlc     = fetch_all_timeframes(symbols)
    daily_hlc   = all_hlc["daily"]
    monthly_hlc = all_hlc["monthly"]
    yearly_hlc  = all_hlc["yearly"]

    logger.info(
        f"Fetched — daily:{len(daily_hlc)} "
        f"monthly:{len(monthly_hlc)} yearly:{len(yearly_hlc)}"
    )

    daily_levels,   daily_signals   = compute_levels(symbols, daily_hlc,   "Daily")
    monthly_levels, monthly_signals = compute_levels(symbols, monthly_hlc, "Monthly")
    yearly_levels,  yearly_signals  = compute_levels(symbols, yearly_hlc,  "Yearly")

    logger.info(
        f"Signals — daily:{len(daily_signals)} "
        f"monthly:{len(monthly_signals)} yearly:{len(yearly_signals)}"
    )

    all_levels  = daily_levels  + monthly_levels  + yearly_levels
    all_signals = daily_signals + monthly_signals + yearly_signals

    if all_levels:
        write_levels(all_levels)
    if all_signals:
        append_signals_batch(all_signals)

    if now.day == 1 and monthly_levels:
        write_stored_levels("Monthly", [
            {"Symbol": r["Symbol"], "H5": r.get("H5"), "H6": r.get("H6"),
             "L5": r.get("L5"), "L6": r.get("L6")}
            for r in monthly_levels
        ])

    if now.month == 1 and now.day == 1 and yearly_levels:
        write_stored_levels("Yearly", [
            {"Symbol": r["Symbol"], "H5": r.get("H5"), "H6": r.get("H6"),
             "L5": r.get("L5"), "L6": r.get("L6")}
            for r in yearly_levels
        ])

    if BROKER not in ("", "paper"):
        for sig in all_signals:
            sym = sig["Symbol"]
            h6  = sig.get("GTT_Buy_H6")
            l6  = sig.get("GTT_Sell_L6")
            if h6 and l6:
                try:
                    res = place_gtt_orders(symbol=sym, h6=h6, l6=l6, qty=1)
                    logger.info(f"GTT {sym}: {res}")
                except Exception as e:
                    logger.error(f"GTT failed {sym}: {e}")
    else:
        logger.info("BROKER=PAPER — GTT orders skipped")

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
