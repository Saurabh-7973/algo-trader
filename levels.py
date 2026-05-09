"""
levels.py — Core strategy calculations.

Formulas from Trading_Strategy.docx / API_Doc.docx (UNCHANGED):
  H3 = Close + (High - Low) * 0.275
  H4 = Close + (High - Low) * 0.55
  H5 = H4 + 1.168 * (H4 - H3)
  H6 = (High / Low) * Close          ← GTT BUY trigger (CNC)
  L3 = Close - (High - Low) * 0.275
  L4 = Close - (High - Low) * 0.55
  L5 = L4 - 1.168 * (L3 - L4)
  L6 = Close - (H6 - Close)          ← GTT SELL trigger (MIS)

Signal conditions (BOTH must be true):
  1. NRD  — range_width > 0 AND range_width < 0.4% of Close
  2. Insider — today's H3-L3 range < yesterday's H3-L3 range (compression)

FIX: Previously "Signal": nrd — insider check was bypassed.
     Now Signal = nrd AND insider (correct original logic).
"""

import logging

logger = logging.getLogger(__name__)


class TradingLevels:

    def calculate(
        self,
        high: float,
        low: float,
        close: float,
        prev_high: float | None = None,
        prev_low: float | None = None,
        prev_close: float | None = None,
    ) -> dict:
        """
        Calculate all levels for a single candle.

        Pass prev_high / prev_low / prev_close to enable the Insider condition.
        Without previous data, Insider = False, Signal = False.
        """
        rng = high - low

        # ── Resistance levels (original formulas, unchanged) ──────────────────
        h3 = close + rng * 0.275
        h4 = close + rng * 0.55
        h5 = h4 + 1.168 * (h4 - h3)
        h6 = (high / low) * close if low != 0 else 0.0

        # ── Support levels (original formulas, unchanged) ─────────────────────
        l3 = close - rng * 0.275
        l4 = close - rng * 0.55
        l5 = l4 - 1.168 * (l3 - l4)
        l6 = close - (h6 - close)

        # ── Pivot Range ───────────────────────────────────────────────────────
        pivot_mid    = (high + low + close) / 3
        pivot_bottom = pivot_mid - rng
        pivot_top    = pivot_mid + rng

        # ── NRD — range_width must be POSITIVE and < 0.4% of Close ───────────
        # range_width = (2*Close - High - Low) / 3
        # Positive means close is in upper half of candle (bullish compression)
        range_width = (2 * close - high - low) / 3
        nrd = (range_width > 0) and (range_width < 0.004 * close)

        # ── Insider condition ─────────────────────────────────────────────────
        # Today's H3-L3 range must be SMALLER than previous period's H3-L3 range
        # = price is compressing = institutional accumulation
        insider = False
        if prev_high is not None and prev_low is not None and prev_close is not None:
            prev_rng   = prev_high - prev_low
            prev_h3    = prev_close + prev_rng * 0.275
            prev_l3    = prev_close - prev_rng * 0.275
            curr_range = h3 - l3          # today's H3-L3 width
            prev_range = prev_h3 - prev_l3  # previous period's H3-L3 width
            insider    = curr_range < prev_range

        # ── Signal = NRD AND Insider ──────────────────────────────────────────
        signal = nrd and insider

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
            "Signal":      signal,
            "HL_Range":    round(h3 - l3, 2),
        }
