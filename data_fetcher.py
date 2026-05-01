"""
data_fetcher.py — Fetches OHLC data from NSE via yfinance (100% free).

Timeframe mapping (from API doc):
  - "daily"   → Previous Day HLC   (for 15-min / intraday trading)
  - "monthly" → Previous Month HLC (for 2–3 day trading)
  - "yearly"  → Previous Year HLC  (for swing trading)
"""

from datetime import datetime, date
import yfinance as yf
import pandas as pd
from levels import OHLC
import logging

logger = logging.getLogger(__name__)


def _ticker(symbol: str) -> str:
    """Convert plain symbol to NSE yfinance format. e.g. RELIANCE → RELIANCE.NS"""
    return symbol.upper() + ".NS" if not symbol.endswith(".NS") else symbol.upper()


def get_previous_day_ohlc(symbol: str) -> OHLC | None:
    """
    Fetch previous trading day's High, Low, Close.
    Used for daily (intraday) level calculation.
    """
    try:
        df = yf.Ticker(_ticker(symbol)).history(period="5d", interval="1d")
        if df.empty or len(df) < 2:
            logger.warning(f"{symbol}: not enough daily data")
            return None
        row = df.iloc[-2]  # -1 is today (may be incomplete), -2 is last complete day
        return OHLC(high=row["High"], low=row["Low"], close=row["Close"])
    except Exception as e:
        logger.error(f"{symbol} daily fetch error: {e}")
        return None


def get_previous_month_ohlc(symbol: str) -> OHLC | None:
    """
    Fetch previous calendar month's High, Low, Close.
    Used for Daily Timeframe level calculation (recalculated on 1st of each month).
    """
    try:
        df = yf.Ticker(_ticker(symbol)).history(period="3mo", interval="1mo")
        if df.empty or len(df) < 2:
            logger.warning(f"{symbol}: not enough monthly data")
            return None
        row = df.iloc[-2]  # last completed month
        return OHLC(high=row["High"], low=row["Low"], close=row["Close"])
    except Exception as e:
        logger.error(f"{symbol} monthly fetch error: {e}")
        return None


def get_previous_year_ohlc(symbol: str) -> OHLC | None:
    """
    Fetch previous calendar year's High, Low, Close.
    Used for Weekly/Monthly Timeframe levels (recalculated on 1st Jan).
    """
    try:
        df = yf.Ticker(_ticker(symbol)).history(period="5y", interval="1y")
        if df.empty or len(df) < 2:
            logger.warning(f"{symbol}: not enough yearly data")
            return None
        row = df.iloc[-2]  # last completed year
        return OHLC(high=row["High"], low=row["Low"], close=row["Close"])
    except Exception as e:
        logger.error(f"{symbol} yearly fetch error: {e}")
        return None


def get_all_ohlc(symbol: str) -> dict[str, OHLC | None]:
    """Fetch all three timeframes in one call. Returns dict with keys: daily, monthly, yearly."""
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
