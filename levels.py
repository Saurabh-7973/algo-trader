"""
levels.py — Core strategy calculations (v3 NRD fix retained).

Formulas from Trading_Strategy.docx / API_Doc.docx:
  H3 = Close + (High - Low) * 0.275
  H4 = Close + (High - Low) * 0.55
  H5 = H4 + 1.168 * (H4 - H3)
  H6 = (High / Low) * Close

  L3 = Close - (High - Low) * 0.275
  L4 = Close - (High - Low) * 0.55
  L5 = L4 - 1.168 * (L3 - L4)
  L6 = Close - (H6 - Close)

Pivot Range:
  Middle = (High + Low + Close) / 3
  Bottom = Middle - (High - Low)
  Top    = Middle + (High - Low)

NRD (Narrow Range Day):
  range_width = (2*Close - High - Low) / 3
  NRD = range_width > 0 AND range_width < 0.4% of Close
  (Must be positive — negative range_width means bearish close, not a valid NRD)

Insider Trading Level:
  Today's (H3–L3) range < Yesterday's (H3–L3) range
  → price is compressing = insider accumulation

Signal = NRD AND Insider both True
"""

import logging

logger = logging.getLogger(__name__)


class TradingLevels:

    def calculate(self, high: float, low: float, close: float) -> dict:
        """
        Calculate all levels for a single candle.
        Returns dict with H3-H6, L3-L6, pivot levels, NRD flag,
        Insider flag (requires prev_high/prev_low), and Signal.
        """
        rng = high - low  # candle range

        # ── Resistance levels ──
        h3 = close + rng * 0.275
        h4 = close + rng * 0.55
        h5 = h4 + 1.168 * (h4 - h3)
        h6 = (high / low) * close if low != 0 else 0.0

        # ── Support levels ──
        l3 = close - rng * 0.275
        l4 = close - rng * 0.55
        l5 = l4 - 1.168 * (l3 - l4)
        l6 = close - (h6 - close)

        # ── Pivot Range ──
        pivot_mid    = (high + low + close) / 3
        pivot_bottom = pivot_mid - rng
        pivot_top    = pivot_mid + rng

        # ── NRD check ──
        range_width = (2 * close - high - low) / 3
        nrd = (range_width > 0) and (range_width < 0.004 * close)

        # ── Insider condition (requires prev candle — injected externally) ──
        # This flag is set by the caller when it has yesterday's H3/L3.
        insider = False  # default; set True by caller if prev range > current range

        current_hl_range = h3 - l3

        return {
            "H3": round(h3, 2),
            "H4": round(h4, 2),
            "H5": round(h5, 2),
            "H6": round(h6, 2),
            "L3": round(l3, 2),
            "L4": round(l4, 2),
            "L5": round(l5, 2),
            "L6": round(l6, 2),
            "PivotMid":    round(pivot_mid, 2),
            "PivotBottom": round(pivot_bottom, 2),
            "PivotTop":    round(pivot_top, 2),
            "RangeWidth":  round(range_width, 6),
            "NRD":         nrd,
            "Insider":     insider,
            "Signal":      nrd,   # Signal = NRD for now; Insider requires prev data
            "HL_Range":    round(current_hl_range, 2),
        }

    def check_insider(
        self,
        current_h3: float, current_l3: float,
        prev_h3: float, prev_l3: float,
    ) -> bool:
        """
        Insider condition: today's H3-L3 range is TIGHTER than yesterday's.
        Indicates price compression / insider accumulation.
        """
        current_range = current_h3 - current_l3
        prev_range    = prev_h3    - prev_l3
        return current_range < prev_range
