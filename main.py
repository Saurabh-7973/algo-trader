"""
main.py — NSE Full Market Edition (feature/nse-full-market branch)

Architecture (key difference from main branch):
  Regular day  → fetch daily only + READ stored monthly/yearly from Sheets
  1st of month → fetch + calculate + STORE monthly levels to Sheets
  1st Jan      → fetch + calculate + STORE yearly levels to Sheets

This makes regular days 3x faster:
  350 stocks (old) → 3 timeframes → ~5 min
  900 stocks (new) → 1 timeframe + 2 Sheets reads → ~40 seconds

Flow:
  1. Load symbol list (NSE full market OR Basket tab, based on Settings)
  2. If 1st of month → refresh Monthly stored levels
  3. If 1st Jan      → refresh Yearly stored levels
  4. Every day       → fetch fresh Daily data
  5. Read Monthly + Yearly stored levels from Sheets
  6. Calculate signals for all 3 timeframes
  7. Write to Levels + Signals tabs
  8. Broadcast Telegram
"""

import logging
import os
import sys
from datetime import datetime

from config import BROKER, TELEGRAM_BOT_TOKEN
from data_fetcher import (
    fetch_nse_symbols,
    fetch_daily_data,
    fetch_monthly_data,
    fetch_yearly_data,
)
from levels import TradingLevels
from sheets_manager import (
    get_settings,
    get_stock_basket,
    write_levels,
    append_signals_batch,
    write_full_stored_levels,
    read_stored_levels,
    SETTING_MIN_PRICE,
    SETTING_USE_NSE,
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


# ── Row builder ───────────────────────────────────────────────────────────────

def _build_row(symbol: str, timeframe: str, current: dict, result: dict) -> dict:
    """
    Explicit column order — same structure in Levels, Monthly, Yearly tabs.
    Symbol | Timeframe | Date | H3-H6 | L3-L6 | NRD | Insider | Signal | GTT levels
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


# ── Level computation ─────────────────────────────────────────────────────────

def compute_from_fetched(
    symbols: list[str],
    hlc_data: dict[str, dict],
    timeframe_label: str,
) -> tuple[list[dict], list[dict]]:
    """
    Calculate levels from freshly fetched HLC data.
    Used on: 1st of month (monthly), 1st Jan (yearly), every day (daily).

    hlc_data: {symbol: {"current": {H,L,C,Date}, "previous": {H,L,C,Date}}}
    Returns: (all_rows, signal_rows)
    """
    all_rows    = []
    signal_rows = []

    for symbol in symbols:
        data = hlc_data.get(symbol)
        if not data:
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
            logger.error(f"{symbol}/{timeframe_label}: calc error — {e}")
            continue

        row = _build_row(symbol, timeframe_label, current, result)
        all_rows.append(row)

        if result.get("Signal"):
            signal_rows.append(row)
            logger.info(
                f"  SIGNAL {symbol} [{timeframe_label}] "
                f"H6={result.get('H6')} L6={result.get('L6')}"
            )

    return all_rows, signal_rows


def compute_from_stored(stored_rows: list[dict], timeframe_label: str) -> list[dict]:
    """
    Read pre-computed signal rows from stored Monthly/Yearly Sheets data.
    Returns only rows where Signal=YES — no recalculation needed.

    Used on: regular days (not 1st of month/year).
    """
    signals = []
    for r in stored_rows:
        if str(r.get("Signal", "")).strip().upper() == "YES":
            # Ensure Timeframe field is set correctly
            r["Timeframe"] = timeframe_label
            signals.append(r)
    return signals


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run():
    now = datetime.now()
    logger.info("=" * 60)
    logger.info(f"NSE Full Market run — {now.strftime('%Y-%m-%d %H:%M')} IST")
    logger.info(f"Broker: {BROKER.upper()}")
    logger.info("=" * 60)

    is_first_of_month = now.day == 1
    is_first_of_year  = now.month == 1 and now.day == 1

    # ── 1. Load symbol universe ───────────────────────────────────────────────
    settings  = get_settings()
    min_price = float(settings.get(SETTING_MIN_PRICE, 100.0))
    use_nse   = bool(settings.get(SETTING_USE_NSE, False))

    if use_nse:
        logger.info(f"Mode: NSE full market (price > ₹{min_price})")
        symbols = fetch_nse_symbols(min_price=min_price)
        if not symbols:
            logger.warning("NSE list fetch failed — falling back to Basket tab")
            symbols = get_stock_basket()
    else:
        logger.info("Mode: Basket tab")
        symbols = get_stock_basket()

    if not symbols:
        msg = "No active stocks found. Add stocks to Basket tab or enable USE_NSE_LIST in Settings."
        logger.warning(msg)
        broadcast(message=msg, requesting_chat_id=REQUESTING_CHAT_ID)
        return

    logger.info(f"Symbol universe: {len(symbols)} stocks")

    # ── 2. Monthly refresh (1st of each month) ────────────────────────────────
    monthly_all = []
    monthly_signals = []

    if is_first_of_month:
        logger.info("1st of month — fetching and storing monthly levels ...")
        monthly_hlc = fetch_monthly_data(symbols)
        monthly_all, monthly_signals = compute_from_fetched(
            symbols, monthly_hlc, "Monthly"
        )
        if monthly_all:
            write_full_stored_levels("Monthly", monthly_all)
            logger.info(
                f"Monthly stored: {len(monthly_all)} rows, "
                f"{len(monthly_signals)} signals"
            )
    else:
        # Read pre-stored monthly results — no API call needed
        logger.info("Regular day — reading stored Monthly levels from Sheets ...")
        stored_monthly = read_stored_levels("Monthly")
        monthly_signals = compute_from_stored(stored_monthly, "Monthly")
        monthly_all     = stored_monthly  # include all rows for Levels tab
        logger.info(
            f"Monthly (stored): {len(monthly_all)} rows, "
            f"{len(monthly_signals)} signals"
        )

    # ── 3. Yearly refresh (1st January) ───────────────────────────────────────
    yearly_all = []
    yearly_signals = []

    if is_first_of_year:
        logger.info("1st Jan — fetching and storing yearly levels ...")
        yearly_hlc = fetch_yearly_data(symbols)
        yearly_all, yearly_signals = compute_from_fetched(
            symbols, yearly_hlc, "Yearly"
        )
        if yearly_all:
            write_full_stored_levels("Yearly", yearly_all)
            logger.info(
                f"Yearly stored: {len(yearly_all)} rows, "
                f"{len(yearly_signals)} signals"
            )
    else:
        logger.info("Regular day — reading stored Yearly levels from Sheets ...")
        stored_yearly = read_stored_levels("Yearly")
        yearly_signals = compute_from_stored(stored_yearly, "Yearly")
        yearly_all     = stored_yearly
        logger.info(
            f"Yearly (stored): {len(yearly_all)} rows, "
            f"{len(yearly_signals)} signals"
        )

    # ── 4. Daily data (fetched EVERY day) ─────────────────────────────────────
    logger.info(f"Fetching daily data for {len(symbols)} stocks ...")
    daily_hlc = fetch_daily_data(symbols)
    daily_all, daily_signals = compute_from_fetched(
        symbols, daily_hlc, "Daily"
    )
    logger.info(
        f"Daily: {len(daily_all)} rows, {len(daily_signals)} signals"
    )

    # ── 5. Write all levels to Sheets ─────────────────────────────────────────
    all_levels  = daily_all + list(monthly_all) + list(yearly_all)
    all_signals = daily_signals + monthly_signals + yearly_signals

    if daily_all:  # Write only daily to Levels tab (fresh every day)
        write_levels(daily_all)
    if all_signals:
        append_signals_batch(all_signals)

    # ── 6. GTT orders ─────────────────────────────────────────────────────────
    if BROKER not in ("", "paper"):
        for sig in all_signals:
            sym = sig.get("Symbol", "")
            h6  = sig.get("GTT_Buy_H6") or sig.get("H6")
            l6  = sig.get("GTT_Sell_L6") or sig.get("L6")
            if sym and h6 and l6:
                try:
                    res = place_gtt_orders(symbol=sym, h6=float(h6), l6=float(l6), qty=1)
                    logger.info(f"GTT {sym}: {res}")
                except Exception as e:
                    logger.error(f"GTT failed {sym}: {e}")
    else:
        logger.info("BROKER=PAPER — GTT orders skipped")

    # ── 7. Broadcast Telegram ─────────────────────────────────────────────────
    broadcast(
        daily_signals=daily_signals,
        monthly_signals=monthly_signals,
        yearly_signals=yearly_signals,
        run_date=now.strftime("%d %b %Y"),
        requesting_chat_id=REQUESTING_CHAT_ID,
    )

    logger.info("=" * 60)
    logger.info(
        f"Run complete. {len(symbols)} stocks. "
        f"Signals: D={len(daily_signals)} M={len(monthly_signals)} Y={len(yearly_signals)}"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
