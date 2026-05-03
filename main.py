"""
main.py — Daily algo trading orchestrator (v7)

Pipeline:
  1. Load stock basket from Google Sheets
  2. Fetch HLC for ALL three timeframes (daily, monthly, yearly)
  3. Calculate levels + signals for each timeframe independently
  4. Write levels to Sheets (all timeframes)
  5. On 1st of month  → also refresh stored Monthly levels
  6. On 1st of Jan    → also refresh stored Yearly levels
  7. Broadcast consolidated signal alert via Telegram
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
    get_stored_levels,
    get_client,
)
from alerts import broadcast
from broker import place_gtt_orders

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

REQUESTING_CHAT_ID = os.getenv("REQUESTING_CHAT_ID", "")


def compute_levels_for_timeframe(
    symbols: list[str],
    hlc_data: dict[str, dict],
    timeframe_label: str,
) -> tuple[list[dict], list[dict]]:
    """
    Given a dict of {symbol: {High, Low, Close}}, compute levels + signals.

    Returns:
        levels_rows  — list of dicts for Sheets write
        signal_rows  — list of dicts for stocks where NRD + Insider both true
    """
    levels_rows = []
    signal_rows = []
    calc = TradingLevels()

    for symbol in symbols:
        hlc = hlc_data.get(symbol)
        if not hlc:
            logger.warning(f"  {symbol}/{timeframe_label}: HLC unavailable, skipping.")
            continue

        try:
            result = calc.calculate(
                high=hlc["High"],
                low=hlc["Low"],
                close=hlc["Close"],
            )
        except Exception as e:
            logger.error(f"  {symbol}/{timeframe_label}: calculation error — {e}")
            continue

        row = {
            "Symbol":    symbol,
            "Timeframe": timeframe_label,
            "Date":      hlc.get("Date", ""),
            "High":      hlc["High"],
            "Low":       hlc["Low"],
            "Close":     hlc["Close"],
            **{k: round(v, 2) if isinstance(v, float) else v for k, v in result.items()},
        }
        levels_rows.append(row)

        if result.get("Signal"):
            signal_rows.append(row)
            logger.info(
                f"  ✅ SIGNAL {symbol} [{timeframe_label}] "
                f"H6={result.get('H6')} L6={result.get('L6')}"
            )

    return levels_rows, signal_rows


def run():
    now = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Algo run started — {now.strftime('%Y-%m-%d %H:%M')} IST")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    # ── 1. Load basket ────────────────────────────────────────────────────────
    symbols = get_stock_basket()
    if not symbols:
        msg = "⚠️ Basket is empty — no active stocks found. Add stocks to Basket tab."
        logger.warning(msg)
        if TELEGRAM_BOT_TOKEN:
            broadcast(msg, requesting_chat_id=REQUESTING_CHAT_ID)
        return

    logger.info(f"Basket: {len(symbols)} active stocks")

    # ── 2. Fetch all HLC data ─────────────────────────────────────────────────
    logger.info("Fetching HLC for all timeframes …")
    all_hlc = fetch_all_timeframes(symbols)
    daily_hlc   = all_hlc["daily"]
    monthly_hlc = all_hlc["monthly"]
    yearly_hlc  = all_hlc["yearly"]

    logger.info(
        f"Data fetched — daily: {len(daily_hlc)}, "
        f"monthly: {len(monthly_hlc)}, yearly: {len(yearly_hlc)} stocks"
    )

    # ── 3. Calculate levels for each timeframe ────────────────────────────────
    logger.info("Calculating daily levels …")
    daily_levels, daily_signals = compute_levels_for_timeframe(
        symbols, daily_hlc, "Daily"
    )

    logger.info("Calculating monthly levels …")
    monthly_levels, monthly_signals = compute_levels_for_timeframe(
        symbols, monthly_hlc, "Monthly"
    )

    logger.info("Calculating yearly levels …")
    yearly_levels, yearly_signals = compute_levels_for_timeframe(
        symbols, yearly_hlc, "Yearly"
    )

    all_levels  = daily_levels  + monthly_levels  + yearly_levels
    all_signals = daily_signals + monthly_signals + yearly_signals

    logger.info(
        f"Signals — daily: {len(daily_signals)}, "
        f"monthly: {len(monthly_signals)}, yearly: {len(yearly_signals)}"
    )

    # ── 4. Write levels to Sheets ─────────────────────────────────────────────
    if all_levels:
        write_levels(all_levels)

    if all_signals:
        append_signals_batch(all_signals)

    # ── 5. Refresh stored Monthly levels on 1st of every month ───────────────
    if now.day == 1 and monthly_levels:
        logger.info("1st of month — refreshing stored Monthly levels tab …")
        stored_monthly = [
            {"Symbol": r["Symbol"], "H5": r.get("H5"), "H6": r.get("H6"),
             "L5": r.get("L5"), "L6": r.get("L6")}
            for r in monthly_levels
        ]
        write_stored_levels("Monthly", stored_monthly)

    # ── 6. Refresh stored Yearly levels on 1st of January ────────────────────
    if now.month == 1 and now.day == 1 and yearly_levels:
        logger.info("1st Jan — refreshing stored Yearly levels tab …")
        stored_yearly = [
            {"Symbol": r["Symbol"], "H5": r.get("H5"), "H6": r.get("H6"),
             "L5": r.get("L5"), "L6": r.get("L6")}
            for r in yearly_levels
        ]
        write_stored_levels("Yearly", stored_yearly)

    # ── 7. Place GTT orders for all signals ───────────────────────────────────
    if BROKER not in ("", "paper"):
        for sig in all_signals:
            symbol = sig["Symbol"]
            h6 = sig.get("H6")
            l6 = sig.get("L6")
            if h6 and l6:
                try:
                    result = place_gtt_orders(symbol=symbol, h6=h6, l6=l6, qty=1)
                    logger.info(f"GTT order result for {symbol}: {result}")
                except Exception as e:
                    logger.error(f"GTT order failed for {symbol}: {e}")
    else:
        logger.info(f"Broker=PAPER — GTT orders skipped (dry run)")

    # ── 8. Broadcast Telegram alert ───────────────────────────────────────────
    broadcast(
        daily_signals=daily_signals,
        monthly_signals=monthly_signals,
        yearly_signals=yearly_signals,
        run_date=now.strftime("%d %b %Y"),
        requesting_chat_id=REQUESTING_CHAT_ID,
    )

    logger.info("=" * 60)
    logger.info("Run complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
