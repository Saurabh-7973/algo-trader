"""
data_fetcher.py — Batch OHLC fetcher using yfinance.download()

Architecture upgrade for Nifty 500 scale:
  - OLD: N individual Ticker().history() calls → N API calls → rate limits
  - NEW: yf.download(batch of 100) → ceil(N/100) API calls → no rate limits

Timeframe mapping:
  - "daily"   → Previous Day HLC   (intraday trading)
  - "monthly" → Previous Month HLC (2-3 day swing)
  - "yearly"  → Previous Year HLC  (positional swing)
"""

import time
import logging
from datetime import date

import yfinance as yf
import pandas as pd

from levels import OHLC

logger = logging.getLogger(__name__)

BATCH_SIZE = 100      # yfinance handles 100 tickers comfortably per call
BATCH_DELAY = 3.0     # seconds between batches — respectful pacing


def _ticker(symbol: str) -> str:
    """Normalize symbol — always strip and re-add .NS to prevent duplicates."""
    return symbol.upper().replace(".NS", "").strip() + ".NS"


def _extract_ohlc(df: pd.DataFrame, symbol: str, idx: int = -2) -> OHLC | None:
    """
    Extract OHLC at position idx from a batch download DataFrame.
    Handles yfinance multi-index format: df[symbol]['High'] or df['High'][symbol]
    """
    if df is None or df.empty:
        return None
    try:
        sym = _ticker(symbol)
        # yfinance batch format: columns are (OHLCV_field, ticker) or (ticker, OHLCV_field)
        # Try both formats
        if isinstance(df.columns, pd.MultiIndex):
            if sym in df.columns.get_level_values(0):
                # Format: (ticker, field)
                sub = df[sym].dropna(how="all")
            elif sym in df.columns.get_level_values(1):
                # Format: (field, ticker)
                sub = df.xs(sym, level=1, axis=1).dropna(how="all")
            else:
                logger.warning(f"{symbol}: not found in batch result")
                return None
        else:
            sub = df.dropna(how="all")

        if len(sub) < abs(idx) + 1:
            logger.warning(f"{symbol}: not enough rows (got {len(sub)}, need {abs(idx)+1})")
            return None

        row = sub.iloc[idx]
        return OHLC(
            high=round(float(row["High"]), 2),
            low=round(float(row["Low"]), 2),
            close=round(float(row["Close"]), 2),
        )
    except Exception as e:
        logger.error(f"{symbol} OHLC extraction error: {e}")
        return None


def _batch_download(symbols: list[str], period: str, interval: str) -> pd.DataFrame | None:
    """Download multiple symbols in one API call with retry on failure."""
    for attempt in range(3):
        try:
            df = yf.download(
                symbols,
                period=period,
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            return df
        except Exception as e:
            if "Too Many Requests" in str(e) or "Rate" in str(e):
                wait = (attempt + 1) * 10
                logger.warning(f"Rate limited on batch, retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"Batch download error: {e}")
                return None
    return None


def fetch_all_ohlc_batch(symbols: list[str]) -> dict[str, dict[str, OHLC | None]]:
    """
    Main entry point for Nifty 500 scale.

    Fetches daily, monthly, and yearly OHLC for ALL symbols using batch downloads.
    Returns: { 'RELIANCE.NS': {'daily': OHLC, 'monthly': OHLC, 'yearly': OHLC}, ... }

    Process:
      - Splits symbols into batches of BATCH_SIZE
      - Fetches each batch with 3s inter-batch delay
      - 500 stocks = 5 daily batches + 5 monthly batches + 5 yearly batches = 15 API calls
      - Expected run time: ~2-3 minutes for 500 stocks
    """
    tickers = [_ticker(s) for s in symbols]
    results = {sym: {"daily": None, "monthly": None, "yearly": None} for sym in symbols}
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    sym_batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]

    # ── Daily ────────────────────────────────────────────────────────────────
    logger.info(f"Fetching daily OHLC for {len(symbols)} stocks in {len(batches)} batches...")
    for i, (batch_tickers, batch_syms) in enumerate(zip(batches, sym_batches)):
        logger.info(f"  Daily batch {i+1}/{len(batches)} ({len(batch_tickers)} stocks)")
        df = _batch_download(batch_tickers, "5d", "1d")
        for sym in batch_syms:
            results[sym]["daily"] = _extract_ohlc(df, sym, idx=-2)
        if i < len(batches) - 1:
            time.sleep(BATCH_DELAY)

    # ── Monthly ──────────────────────────────────────────────────────────────
    logger.info(f"Fetching monthly OHLC for {len(symbols)} stocks...")
    for i, (batch_tickers, batch_syms) in enumerate(zip(batches, sym_batches)):
        logger.info(f"  Monthly batch {i+1}/{len(batches)}")
        df = _batch_download(batch_tickers, "3mo", "1mo")
        for sym in batch_syms:
            results[sym]["monthly"] = _extract_ohlc(df, sym, idx=-2)
        if i < len(batches) - 1:
            time.sleep(BATCH_DELAY)

    # ── Yearly ───────────────────────────────────────────────────────────────
    logger.info(f"Fetching yearly OHLC for {len(symbols)} stocks...")
    for i, (batch_tickers, batch_syms) in enumerate(zip(batches, sym_batches)):
        logger.info(f"  Yearly batch {i+1}/{len(batches)}")
        df_raw = _batch_download(batch_tickers, "2y", "1mo")
        for sym in batch_syms:
            try:
                sym_ticker = _ticker(sym)
                if df_raw is None or df_raw.empty:
                    continue
                # Extract per-symbol monthly data
                if isinstance(df_raw.columns, pd.MultiIndex):
                    if sym_ticker in df_raw.columns.get_level_values(0):
                        sub = df_raw[sym_ticker].dropna(how="all")
                    elif sym_ticker in df_raw.columns.get_level_values(1):
                        sub = df_raw.xs(sym_ticker, level=1, axis=1).dropna(how="all")
                    else:
                        continue
                else:
                    sub = df_raw.dropna(how="all")

                sub.index = pd.to_datetime(sub.index)
                annual = sub.resample("YE").agg({
                    "High": "max", "Low": "min", "Close": "last"
                }).dropna()

                if len(annual) < 2:
                    logger.warning(f"{sym}: not enough yearly data after resample")
                    continue

                row = annual.iloc[-2]
                results[sym]["yearly"] = OHLC(
                    high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2),
                )
            except Exception as e:
                logger.error(f"{sym} yearly resample error: {e}")

        if i < len(batches) - 1:
            time.sleep(BATCH_DELAY)

    # Summary
    fetched = sum(1 for v in results.values() if v["daily"] is not None)
    logger.info(f"Batch fetch complete: {fetched}/{len(symbols)} stocks with daily data.")
    return results


# ── Convenience helpers ───────────────────────────────────────────────────────

def get_all_ohlc(symbol: str) -> dict[str, OHLC | None]:
    """Single-stock fallback (used in testing). For production use fetch_all_ohlc_batch()."""
    res = fetch_all_ohlc_batch([symbol])
    return res.get(symbol, {"daily": None, "monthly": None, "yearly": None})


def is_first_day_of_month() -> bool:
    return date.today().day == 1


def is_first_day_of_year() -> bool:
    today = date.today()
    return today.day == 1 and today.month == 1
