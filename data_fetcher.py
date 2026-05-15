"""
data_fetcher.py — NSE Full Market Edition (feature/nse-full-market branch)

Key changes from main branch:
  1. fetch_nse_symbols() — downloads all NSE equity symbols, filters price > min_price
  2. fetch_daily_only() — fetches ONLY daily data (used on regular trading days)
  3. fetch_monthly_data() — called ONLY on 1st of each month
  4. fetch_yearly_data()  — called ONLY on 1st January

Architecture:
  Regular day  → fetch_daily_only() + read Sheets for monthly/yearly
  1st of month → fetch_monthly_data() + store to Sheets
  1st Jan      → fetch_yearly_data()  + store to Sheets

This makes regular days 3x faster than fetching all timeframes every day.
"""

import csv
import io
import logging
import time

import pandas as pd
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE  = 100    # yfinance handles up to 100 tickers per call reliably
BATCH_DELAY = 2.0    # seconds between batches
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ns(symbol: str) -> str:
    """Normalize to Yahoo Finance NSE format: RELIANCE → RELIANCE.NS"""
    return symbol.upper().replace(".NS", "").strip() + ".NS"


def _batches(lst: list, n: int = BATCH_SIZE):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _download(tickers: list[str], period: str, interval: str) -> pd.DataFrame | None:
    """Batch download with one retry on failure."""
    for attempt in range(2):
        try:
            df = yf.download(
                tickers,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            return None if df.empty else df
        except Exception as e:
            wait = (attempt + 1) * 5
            logger.warning(f"Download error (attempt {attempt+1}): {e} — retrying in {wait}s")
            time.sleep(wait)
    return None


def _extract(df: pd.DataFrame, ticker: str, pos: int) -> dict | None:
    """Extract one OHLC row from a batch download result."""
    if df is None or df.empty:
        return None
    try:
        if isinstance(df.columns, pd.MultiIndex):
            h = df[("High",  ticker)]
            l = df[("Low",   ticker)]
            c = df[("Close", ticker)]
            sub = pd.concat([h, l, c], axis=1)
            sub.columns = ["High", "Low", "Close"]
        else:
            sub = df[["High", "Low", "Close"]].copy()

        sub = sub.dropna()
        if len(sub) < abs(pos) + (0 if pos >= 0 else 1):
            return None

        row = sub.iloc[pos]
        date_idx = sub.index[pos if pos >= 0 else len(sub) + pos]
        return {
            "High":  round(float(row["High"]),  2),
            "Low":   round(float(row["Low"]),   2),
            "Close": round(float(row["Close"]), 2),
            "Date":  date_idx.strftime("%Y-%m-%d"),
        }
    except Exception as e:
        logger.debug(f"_extract({ticker}, {pos}): {e}")
        return None


def _extract_yearly(df: pd.DataFrame, ticker: str, pos: int) -> dict | None:
    """Extract yearly OHLC via monthly→annual resample."""
    try:
        if isinstance(df.columns, pd.MultiIndex):
            h = df[("High",  ticker)]
            l = df[("Low",   ticker)]
            c = df[("Close", ticker)]
            sub = pd.concat([h, l, c], axis=1)
            sub.columns = ["High", "Low", "Close"]
        else:
            sub = df[["High", "Low", "Close"]].copy()

        sub = sub.dropna()
        if sub.empty:
            return None

        annual = sub.resample("YE").agg({
            "High":  "max",
            "Low":   "min",
            "Close": "last",
        }).dropna()

        needed = abs(pos) + (0 if pos >= 0 else 1)
        if len(annual) < needed:
            return None

        row = annual.iloc[pos]
        date_idx = annual.index[pos if pos >= 0 else len(annual) + pos]
        return {
            "High":  round(float(row["High"]),  2),
            "Low":   round(float(row["Low"]),   2),
            "Close": round(float(row["Close"]), 2),
            "Date":  date_idx.strftime("%Y"),
        }
    except Exception as e:
        logger.debug(f"_extract_yearly({ticker}, {pos}): {e}")
        return None


# ── NSE Full Market Symbol List ───────────────────────────────────────────────

def fetch_nse_symbols(min_price: float = 100.0) -> list[str]:
    """
    Download all NSE-listed equity symbols and filter by minimum price.

    Steps:
      1. Download NSE's EQUITY_L.csv (all ~2000 listed stocks)
      2. Filter Series=EQ (regular equity, not ETF/SME/debt)
      3. Fetch current price in batches
      4. Keep only stocks with Close > min_price

    Returns list of symbols WITHOUT .NS suffix (e.g. ["RELIANCE", "TCS", ...])
    """
    # Step 1: Download NSE equity list
    try:
        resp = requests.get(NSE_EQUITY_URL, timeout=15,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        all_symbols = [
            row["SYMBOL"].strip()
            for row in reader
            if row.get("SERIES", "").strip().upper() == "EQ"
            and row.get("SYMBOL", "").strip()
        ]
        logger.info(f"NSE equity list: {len(all_symbols)} EQ series symbols")
    except Exception as e:
        logger.error(f"Failed to fetch NSE equity list: {e}")
        logger.warning("Falling back to Basket tab for symbols")
        return []

    # Step 2: Filter by price using a quick batch price check
    tickers = [_ns(s) for s in all_symbols]
    valid = []

    for batch_t, batch_s in zip(_batches(tickers), _batches(all_symbols)):
        df = _download(batch_t, "2d", "1d")
        if df is None:
            continue
        for ticker, sym in zip(batch_t, batch_s):
            row = _extract(df, ticker, pos=-1)
            if row and row["Close"] >= min_price:
                valid.append(sym)
        time.sleep(BATCH_DELAY)

    logger.info(
        f"After price filter (>{min_price}): {len(valid)} symbols out of {len(all_symbols)}"
    )
    return valid


# ── Per-timeframe fetchers ────────────────────────────────────────────────────

def fetch_daily_data(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch previous trading day HLC for all symbols (called every day).

    current  = idx -1 = last complete trading day
    previous = idx -2 = day before (for Insider condition comparison)

    yfinance only returns actual NSE trading days — weekends and NSE
    holidays (Maharashtra Day, Diwali, etc.) are automatically skipped.

    Returns: {symbol: {"current": {...}, "previous": {...}}}
    """
    results = {}
    tickers = [_ns(s) for s in symbols]

    for batch_t, batch_s in zip(_batches(tickers), _batches(symbols)):
        logger.info(f"Daily fetch: {len(batch_t)} symbols")
        df = _download(batch_t, "15d", "1d")
        if df is None:
            continue
        for ticker, sym in zip(batch_t, batch_s):
            cur  = _extract(df, ticker, pos=-1)
            prev = _extract(df, ticker, pos=-2)
            if cur:
                results[sym] = {"current": cur, "previous": prev}
        time.sleep(BATCH_DELAY)

    logger.info(f"Daily fetch complete: {len(results)}/{len(symbols)} stocks")
    return results


def fetch_monthly_data(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch previous month HLC (called ONLY on 1st of each month).

    current  = idx -2 = last fully complete month
    previous = idx -3 = month before that (for Insider comparison)

    Returns: {symbol: {"current": {...}, "previous": {...}}}
    """
    results = {}
    tickers = [_ns(s) for s in symbols]

    for batch_t, batch_s in zip(_batches(tickers), _batches(symbols)):
        logger.info(f"Monthly fetch: {len(batch_t)} symbols")
        df = _download(batch_t, "6mo", "1mo")
        if df is None:
            continue
        for ticker, sym in zip(batch_t, batch_s):
            cur  = _extract(df, ticker, pos=-2)
            prev = _extract(df, ticker, pos=-3)
            if cur:
                results[sym] = {"current": cur, "previous": prev}
        time.sleep(BATCH_DELAY)

    logger.info(f"Monthly fetch complete: {len(results)}/{len(symbols)} stocks")
    return results


def fetch_yearly_data(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch previous year HLC (called ONLY on 1st January).

    current  = annual.iloc[-2] = last complete calendar year
    previous = annual.iloc[-3] = year before that (for Insider comparison)

    Returns: {symbol: {"current": {...}, "previous": {...}}}
    """
    results = {}
    tickers = [_ns(s) for s in symbols]

    for batch_t, batch_s in zip(_batches(tickers), _batches(symbols)):
        logger.info(f"Yearly fetch: {len(batch_t)} symbols")
        df = _download(batch_t, "40mo", "1mo")
        if df is None:
            continue
        for ticker, sym in zip(batch_t, batch_s):
            cur  = _extract_yearly(df, ticker, pos=-2)
            prev = _extract_yearly(df, ticker, pos=-3)
            if cur:
                results[sym] = {"current": cur, "previous": prev}
        time.sleep(BATCH_DELAY)

    logger.info(f"Yearly fetch complete: {len(results)}/{len(symbols)} stocks")
    return results
