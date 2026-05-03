"""
data_fetcher.py — Batch OHLC fetcher using yfinance.download()

v7 Fixes:
  - Daily: always uses iloc[-1] = most recent COMPLETED trading day.
    yfinance only returns trading days, so weekends + NSE holidays are
    automatically skipped. No manual calendar needed.
  - Monthly: fetches 4 months, takes iloc[-2] = last FULLY completed month
    (iloc[-1] is current incomplete month).
  - Yearly: fetches 30 months of monthly data, resamples to annual,
    takes iloc[-2] = last fully completed year.
  - Batch size 100 to stay within rate limits.
"""

import logging
import time
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 100   # yfinance.download handles up to ~100 at once reliably


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _ticker(symbol: str) -> str:
    """Ensure symbol ends with .NS exactly once."""
    base = symbol.upper().replace(".NS", "").strip()
    return f"{base}.NS"


def _batch(symbols: list[str], size: int = BATCH_SIZE):
    """Yield successive chunks of a list."""
    for i in range(0, len(symbols), size):
        yield symbols[i:i + size]


def _safe_row(df: pd.DataFrame | pd.Series, pos: int = -1) -> dict | None:
    """
    Safely extract a row from a DataFrame or Series by position.
    Returns None if df is empty or pos is out of range.
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return None
    try:
        row = df.iloc[pos]
        if row is None:
            return None
        return row
    except (IndexError, KeyError):
        return None


def _extract_hlc(download_result, symbol_ticker: str, row_pos: int = -1) -> dict | None:
    """
    Extract High, Low, Close from a yf.download() result for a single symbol.
    Handles both single-ticker (flat columns) and multi-ticker (MultiIndex) output.
    """
    if download_result is None or download_result.empty:
        return None

    try:
        # Multi-ticker: columns are MultiIndex (Price, Ticker)
        if isinstance(download_result.columns, pd.MultiIndex):
            high  = download_result[("High",  symbol_ticker)]
            low   = download_result[("Low",   symbol_ticker)]
            close = download_result[("Close", symbol_ticker)]
            df_hlc = pd.concat([high, low, close], axis=1)
            df_hlc.columns = ["High", "Low", "Close"]
        else:
            # Single ticker: flat columns
            df_hlc = download_result[["High", "Low", "Close"]].copy()

        df_hlc = df_hlc.dropna()
        if df_hlc.empty:
            return None

        row = df_hlc.iloc[row_pos]
        return {
            "High":  float(row["High"]),
            "Low":   float(row["Low"]),
            "Close": float(row["Close"]),
            "Date":  df_hlc.index[row_pos if row_pos >= 0 else len(df_hlc) + row_pos].strftime("%Y-%m-%d"),
        }
    except Exception as e:
        logger.warning(f"_extract_hlc failed for {symbol_ticker}: {e}")
        return None


# ─── Core Fetchers ────────────────────────────────────────────────────────────

def fetch_daily_prev_day(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch the most recent COMPLETED trading day's HLC for each symbol.

    Uses period="15d" + interval="1d" and takes iloc[-1].
    yfinance returns only actual NSE trading days — weekends AND exchange
    holidays (e.g. Maharashtra Day, Diwali) are automatically excluded.
    Running at 9:15 AM IST before market close means iloc[-1] = yesterday's
    completed candle (today's candle doesn't exist yet).
    """
    results = {}
    tickers = [_ticker(s) for s in symbols]

    for batch_tickers in _batch(tickers):
        logger.info(f"Daily fetch: {len(batch_tickers)} symbols")
        try:
            df = yf.download(
                batch_tickers,
                period="15d",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            time.sleep(0.5)  # polite delay between batches

            for ticker in batch_tickers:
                hlc = _extract_hlc(df, ticker, row_pos=-1)
                if hlc:
                    symbol = ticker.replace(".NS", "")
                    results[symbol] = hlc
                    logger.debug(f"Daily {symbol}: {hlc}")
                else:
                    logger.warning(f"Daily data unavailable for {ticker}")

        except Exception as e:
            logger.error(f"Daily batch fetch error: {e}")
            time.sleep(5)

    return results


def fetch_monthly_prev_month(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch the most recent FULLY COMPLETED month's HLC.

    Fetches 4 months of monthly candles and takes iloc[-2]:
      iloc[-1] = current month (incomplete, in progress)
      iloc[-2] = last completed month  ✓

    Example on 5 May 2026:
      iloc[-1] = May 2026 (incomplete)
      iloc[-2] = April 2026 (complete) ✓
    """
    results = {}
    tickers = [_ticker(s) for s in symbols]

    for batch_tickers in _batch(tickers):
        logger.info(f"Monthly fetch: {len(batch_tickers)} symbols")
        try:
            df = yf.download(
                batch_tickers,
                period="4mo",
                interval="1mo",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            time.sleep(0.5)

            for ticker in batch_tickers:
                # row_pos=-2 = last completed month
                hlc = _extract_hlc(df, ticker, row_pos=-2)
                if hlc:
                    symbol = ticker.replace(".NS", "")
                    results[symbol] = hlc
                    logger.debug(f"Monthly {symbol}: {hlc}")
                else:
                    logger.warning(f"Monthly data unavailable for {ticker}")

        except Exception as e:
            logger.error(f"Monthly batch fetch error: {e}")
            time.sleep(5)

    return results


def fetch_yearly_prev_year(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch the most recent FULLY COMPLETED year's HLC.

    Fetches ~28 months of monthly data, resamples to annual (calendar year),
    then takes iloc[-2] = last completed year.

    Example on 5 May 2026:
      Annual row 2026 = incomplete (year in progress) → iloc[-1]
      Annual row 2025 = complete ✓                   → iloc[-2]
    """
    results = {}
    tickers = [_ticker(s) for s in symbols]

    for batch_tickers in _batch(tickers):
        logger.info(f"Yearly fetch: {len(batch_tickers)} symbols")
        try:
            df = yf.download(
                batch_tickers,
                period="28mo",
                interval="1mo",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            time.sleep(0.5)

            for ticker in batch_tickers:
                try:
                    if isinstance(df.columns, pd.MultiIndex):
                        high  = df[("High",  ticker)]
                        low   = df[("Low",   ticker)]
                        close = df[("Close", ticker)]
                        df_hlc = pd.concat([high, low, close], axis=1)
                        df_hlc.columns = ["High", "Low", "Close"]
                    else:
                        df_hlc = df[["High", "Low", "Close"]].copy()

                    df_hlc = df_hlc.dropna()
                    if df_hlc.empty:
                        logger.warning(f"Yearly data empty for {ticker}")
                        continue

                    # Resample monthly → annual
                    annual = df_hlc.resample("YE").agg({
                        "High":  "max",
                        "Low":   "min",
                        "Close": "last",
                    }).dropna()

                    if len(annual) < 2:
                        logger.warning(f"Not enough yearly data for {ticker}")
                        continue

                    # iloc[-2] = last fully completed calendar year
                    row = annual.iloc[-2]
                    symbol = ticker.replace(".NS", "")
                    results[symbol] = {
                        "High":  float(row["High"]),
                        "Low":   float(row["Low"]),
                        "Close": float(row["Close"]),
                        "Date":  annual.index[-2].strftime("%Y"),
                    }
                    logger.debug(f"Yearly {symbol}: {results[symbol]}")

                except Exception as inner_e:
                    logger.warning(f"Yearly extract failed for {ticker}: {inner_e}")

        except Exception as e:
            logger.error(f"Yearly batch fetch error: {e}")
            time.sleep(5)

    return results


# ─── Unified Fetcher ──────────────────────────────────────────────────────────

def fetch_all_timeframes(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch HLC for all three timeframes in one call.

    Returns:
        {
            "daily":   { "RELIANCE": {"High":..., "Low":..., "Close":..., "Date":...}, ... },
            "monthly": { ... },
            "yearly":  { ... },
        }
    """
    logger.info(f"Fetching all timeframes for {len(symbols)} symbols")
    return {
        "daily":   fetch_daily_prev_day(symbols),
        "monthly": fetch_monthly_prev_month(symbols),
        "yearly":  fetch_yearly_prev_year(symbols),
    }
