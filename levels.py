"""
levels.py — Core strategy calculations.

Implements all formulas from the Trading Strategy document:
  - H3, H4, H5, H6 (resistance levels)
  - L3, L4, L5, L6 (support levels)
  - Pivot Range (Middle, Bottom, Top)
  - Narrow Range Definition (NRD)
  - Insider Trading Level condition
  - Final signal: trade only when BOTH conditions are met
"""

from dataclasses import dataclass
from typing import Optional
from config import NRD_THRESHOLD


@dataclass
class OHLC:
    high: float
    low: float
    close: float


@dataclass
class TradingLevels:
    symbol: str
    timeframe: str       # "daily", "monthly", "yearly"

    # Resistance levels
    H3: float
    H4: float
    H5: float
    H6: float

    # Support levels
    L3: float
    L4: float
    L5: float
    L6: float

    # Pivot Range
    pivot_middle: float
    pivot_bottom: float
    pivot_top: float
    range_width: float

    # Conditions
    is_nrd: bool                       # Narrow Range Day
    is_insider: bool                   # Insider Trading Level condition
    signal: bool                       # True = trade signal (both conditions met)
    signal_type: Optional[str] = None  # "BUY" or "SELL" or "BOTH"


def calculate_levels(symbol: str, current: OHLC, timeframe: str,
                     previous: Optional[OHLC] = None) -> TradingLevels:
    """
    Calculate all trading levels for a stock.

    Args:
        symbol:    Stock ticker (e.g. "RELIANCE")
        current:   Today's OHLC (used for level calculation)
        timeframe: "daily" | "monthly" | "yearly"
        previous:  Yesterday's OHLC (needed for Insider condition check)
    """
    H, L, C = current.high, current.low, current.close
    HL = H - L

    # ── Resistance levels ─────────────────────────────────────────────────────
    H3 = C + (HL * 0.275)
    H4 = C + (HL * 0.55)
    H5 = H4 + 1.168 * (H4 - H3)
    H6 = (H / L) * C

    # ── Support levels ────────────────────────────────────────────────────────
    L3 = C - (HL * 0.275)
    L4 = C - (HL * 0.55)
    L5 = L4 - 1.168 * (L3 - L4)
    L6 = C - (H6 - C)

    # ── Pivot Range ───────────────────────────────────────────────────────────
    pivot_middle = (H + L + C) / 3
    pivot_bottom = (H + L) / 2
    pivot_top    = (pivot_middle - pivot_bottom) + pivot_middle
    range_width  = pivot_top - pivot_bottom

    # ── Narrow Range Definition (NRD) ─────────────────────────────────────────
    # Signal: Range Width < 0.4% of Closing Price (use abs for edge cases)
    is_nrd = abs(range_width) < (NRD_THRESHOLD * C)

    # ── Insider Trading Level condition ───────────────────────────────────────
    # Requires previous period's levels for comparison
    is_insider = False
    if previous is not None:
        prev = calculate_levels(symbol, previous, timeframe, previous=None)
        # Today's H levels must all be BELOW yesterday's H levels
        h_condition = (H3 < prev.H3 and H4 < prev.H4 and
                       H5 < prev.H5 and H6 < prev.H6)
        # Today's L levels must all be ABOVE yesterday's L levels
        l_condition = (L3 > prev.L3 and L4 > prev.L4 and
                       L5 > prev.L5 and L6 > prev.L6)
        is_insider = h_condition and l_condition

    # ── Final signal ──────────────────────────────────────────────────────────
    signal = is_nrd and is_insider
    signal_type = "BOTH" if signal else None

    return TradingLevels(
        symbol=symbol,
        timeframe=timeframe,
        H3=round(H3, 2), H4=round(H4, 2),
        H5=round(H5, 2), H6=round(H6, 2),
        L3=round(L3, 2), L4=round(L4, 2),
        L5=round(L5, 2), L6=round(L6, 2),
        pivot_middle=round(pivot_middle, 2),
        pivot_bottom=round(pivot_bottom, 2),
        pivot_top=round(pivot_top, 2),
        range_width=round(range_width, 2),
        is_nrd=is_nrd,
        is_insider=is_insider,
        signal=signal,
        signal_type=signal_type,
    )


def levels_to_dict(lv: TradingLevels) -> dict:
    """Convert TradingLevels to a flat dict for writing to Google Sheets."""
    return {
        "Symbol":       lv.symbol,
        "Timeframe":    lv.timeframe,
        "H3": lv.H3, "H4": lv.H4, "H5": lv.H5, "H6": lv.H6,
        "L3": lv.L3, "L4": lv.L4, "L5": lv.L5, "L6": lv.L6,
        "Pivot Middle": lv.pivot_middle,
        "Pivot Bottom": lv.pivot_bottom,
        "Pivot Top":    lv.pivot_top,
        "Range Width":  lv.range_width,
        "NRD":          "YES" if lv.is_nrd else "NO",
        "Insider":      "YES" if lv.is_insider else "NO",
        "Signal":       "YES" if lv.signal else "NO",
        "Signal Type":  lv.signal_type or "-",
        "GTT Buy at":   lv.H6,   # GTT Buy CNC at H6
        "GTT Sell at":  lv.L6,   # GTT Sell MIS at L6
    }
