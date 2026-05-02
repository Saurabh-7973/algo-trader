"""
data_fetcher.py — Fetches OHLC data from NSE via yfinance (100% free).

Fixes applied v2:
  - _ticker() strips .NS before re-adding — prevents RELIANCE.NS.NS double suffix
  - interval=1y removed (yfinance dropped support); yearly uses 1mo resample
  - rounding applied to all OHLC values
"""

from datetime import date
import time
import yfinance as yf
import pandas as pd
from levels import OHLC
import logging

logger = logging.getLogger(__name__)


def _ticker(symbol: str) -> str:
    """
    Always strip .NS first, then re-add.
    Handles both 'RELIANCE' and 'RELIANCE.NS' inputs safely.
    """
    return symbol.upper().replace(".NS", "").strip() + ".NS"


def _fetch_with_retry(symbol: str, period: str, interval: str, retries: int = 3) -> any:
    """Fetch yfinance data with retry on rate limit. Adds 1s delay between each call."""
    time.sleep(0.5)   # base delay — 50 stocks × 0.5s = 25s total, well within rate limits
    for attempt in range(retries):
        try:
            df = yf.Ticker(_ticker(symbol)).history(period=period, interval=interval)
            return df
        except Exception as e:
            if "Too Many Requests" in str(e) or "Rate limited" in str(e):
                wait = (attempt + 1) * 5
                logger.warning(f"{symbol}: rate limited, waiting {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                raise e
    return None


def get_previous_day_ohlc(symbol: str) -> OHLC | None:
    """Previous trading day HLC — used for daily intraday levels."""
    try:
        df = _fetch_with_retry(symbol, "5d", "1d")
        if df is None or df.empty or len(df) < 2:
            logger.warning(f"{symbol}: not enough daily data")
            return None
        row = df.iloc[-2]
        return OHLC(high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2))
    except Exception as e:
        logger.error(f"{symbol} daily fetch error: {e}")
        return None


def get_previous_month_ohlc(symbol: str) -> OHLC | None:
    """Previous calendar month HLC — used for daily timeframe levels."""
    try:
        df = _fetch_with_retry(symbol, "3mo", "1mo")
        if df is None or df.empty or len(df) < 2:
            logger.warning(f"{symbol}: not enough monthly data")
            return None
        row = df.iloc[-2]
        return OHLC(high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2))
    except Exception as e:
        logger.error(f"{symbol} monthly fetch error: {e}")
        return None


def get_previous_year_ohlc(symbol: str) -> OHLC | None:
    """
    Previous calendar year HLC — used for weekly/monthly timeframe levels.
    yfinance no longer supports interval=1y, so we fetch 2y of monthly data
    and resample to annual ourselves.
    """
    try:
        df = _fetch_with_retry(symbol, "2y", "1mo")
        if df is None or df.empty or len(df) < 13:
            logger.warning(f"{symbol}: not enough data for yearly OHLC")
            return None
        df.index = pd.to_datetime(df.index)
        annual = df.resample("YE").agg({
            "High":  "max",
            "Low":   "min",
            "Close": "last",
        }).dropna()
        if len(annual) < 2:
            logger.warning(f"{symbol}: not enough yearly rows after resample")
            return None
        row = annual.iloc[-2]
        return OHLC(high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2))
    except Exception as e:
        logger.error(f"{symbol} yearly fetch error: {e}")
        return None


def get_all_ohlc(symbol: str) -> dict[str, OHLC | None]:
    """Fetch daily, monthly, yearly OHLC in one call."""
    return {
        "daily":   get_previous_day_ohlc(symbol),
        "monthly": get_previous_month_ohlc(symbol),
        "yearly":  get_previous_year_ohlc(symbol),
    }


def is_first_day_of_month() -> bool:
    return date.today().day == 1


def is_first_day_of_year() -> bool:
    today = date.today()
    return today.day == 1 and today.month == 1
