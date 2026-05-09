"""
data_fetcher.py — Batch OHLC fetcher using yfinance.download()

Returns BOTH current AND previous period for each timeframe.
The 'previous' period is used for the Insider condition in levels.py.

Timeframe logic:
  Daily   — current = last complete trading day (idx -1)
             previous = day before that            (idx -2)
             yfinance only returns actual NSE trading days — weekends,
             Maharashtra Day, Diwali, all holidays automatically excluded.
             No manual holiday calendar needed.

  Monthly — current = last complete calendar month (idx -2)
             previous = the month before that       (idx -3)
             idx -1 = current month (in progress, incomplete)

  Yearly  — current = last complete calendar year  (annual.iloc[-2])
             previous = the year before that        (annual.iloc[-3])
"""

import logging
import time

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ticker(symbol: str) -> str:
    base = symbol.upper().replace(".NS", "").strip()
    return f"{base}.NS"


def _batches(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _extract_row(df: pd.DataFrame, ticker: str, pos: int) -> dict | None:
    """Extract one row from a batch download result by position."""
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
        if len(sub) < abs(pos) + (1 if pos < 0 else 0):
            return None

        row  = sub.iloc[pos]
        date = sub.index[pos if pos >= 0 else len(sub) + pos]

        return {
            "High":  round(float(row["High"]),  2),
            "Low":   round(float(row["Low"]),   2),
            "Close": round(float(row["Close"]), 2),
            "Date":  date.strftime("%Y-%m-%d"),
        }
    except Exception as e:
        logger.debug(f"_extract_row({ticker}, {pos}): {e}")
        return None


def _download(tickers: list[str], period: str, interval: str) -> pd.DataFrame | None:
    """Single batch download with one retry on failure."""
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
            return df if not df.empty else None
        except Exception as e:
            logger.warning(f"Download error (attempt {attempt+1}): {e}")
            time.sleep(3)
    return None


# ── Per-timeframe fetchers ────────────────────────────────────────────────────

def fetch_daily(symbols: list[str]) -> dict[str, dict]:
    """
    Returns {symbol: {"current": {...}, "previous": {...}}}

    current  = most recent complete trading day  (idx -1)
    previous = the trading day before that       (idx -2)
    """
    results = {}
    tickers = [_ticker(s) for s in symbols]

    for batch in _batches(tickers, BATCH_SIZE):
        logger.info(f"Daily fetch: {len(batch)} symbols")
        df = _download(batch, "15d", "1d")
        if df is None:
            continue
        for ticker in batch:
            sym = ticker.replace(".NS", "")
            cur  = _extract_row(df, ticker, pos=-1)
            prev = _extract_row(df, ticker, pos=-2)
            if cur:
                results[sym] = {"current": cur, "previous": prev}
            else:
                logger.warning(f"Daily data unavailable: {ticker}")
        time.sleep(0.5)

    return results


def fetch_monthly(symbols: list[str]) -> dict[str, dict]:
    """
    Returns {symbol: {"current": {...}, "previous": {...}}}

    Fetches 6 months of monthly candles:
      idx -1 = current month (incomplete — skip)
      idx -2 = last complete month  ← current
      idx -3 = month before that    ← previous (for insider check)
    """
    results = {}
    tickers = [_ticker(s) for s in symbols]

    for batch in _batches(tickers, BATCH_SIZE):
        logger.info(f"Monthly fetch: {len(batch)} symbols")
        df = _download(batch, "6mo", "1mo")
        if df is None:
            continue
        for ticker in batch:
            sym  = ticker.replace(".NS", "")
            cur  = _extract_row(df, ticker, pos=-2)
            prev = _extract_row(df, ticker, pos=-3)
            if cur:
                results[sym] = {"current": cur, "previous": prev}
            else:
                logger.warning(f"Monthly data unavailable: {ticker}")
        time.sleep(0.5)

    return results


def fetch_yearly(symbols: list[str]) -> dict[str, dict]:
    """
    Returns {symbol: {"current": {...}, "previous": {...}}}

    Fetches 40 months of monthly data, resamples to annual:
      annual.iloc[-1] = current year (incomplete — skip)
      annual.iloc[-2] = last complete year  ← current
      annual.iloc[-3] = year before that    ← previous (for insider check)
    """
    results = {}
    tickers = [_ticker(s) for s in symbols]

    for batch in _batches(tickers, BATCH_SIZE):
        logger.info(f"Yearly fetch: {len(batch)} symbols")
        df = _download(batch, "40mo", "1mo")
        if df is None:
            continue

        for ticker in batch:
            sym = ticker.replace(".NS", "")
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
                    continue

                annual = sub.resample("YE").agg({
                    "High":  "max",
                    "Low":   "min",
                    "Close": "last",
                }).dropna()

                if len(annual) < 3:
                    # need at least 3 rows: [..., prev-year, curr-year, in-progress]
                    logger.warning(f"Not enough yearly data for {ticker} (rows={len(annual)})")
                    continue

                def _row(idx):
                    r = annual.iloc[idx]
                    d = annual.index[idx if idx >= 0 else len(annual) + idx]
                    return {
                        "High":  round(float(r["High"]),  2),
                        "Low":   round(float(r["Low"]),   2),
                        "Close": round(float(r["Close"]), 2),
                        "Date":  d.strftime("%Y"),
                    }

                results[sym] = {
                    "current":  _row(-2),   # last complete year
                    "previous": _row(-3),   # year before that
                }

            except Exception as e:
                logger.warning(f"Yearly extract failed for {ticker}: {e}")

        time.sleep(0.5)

    return results


# ── Unified entry point ───────────────────────────────────────────────────────

def fetch_all_timeframes(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch all three timeframes for a list of symbols.

    Returns:
    {
        "daily": {
            "RELIANCE": {
                "current":  {"High": x, "Low": y, "Close": z, "Date": "2026-04-30"},
                "previous": {"High": x, "Low": y, "Close": z, "Date": "2026-04-29"},
            },
            ...
        },
        "monthly": { ... },
        "yearly":  { ... },
    }
    """
    logger.info(f"Fetching all timeframes for {len(symbols)} symbols")
    return {
        "daily":   fetch_daily(symbols),
        "monthly": fetch_monthly(symbols),
        "yearly":  fetch_yearly(symbols),
    }
