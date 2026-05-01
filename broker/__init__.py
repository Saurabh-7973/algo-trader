"""
broker/__init__.py — Auto-selects the right broker based on BROKER env var.

Usage in main.py:
    from broker import place_gtt_orders
    result = place_gtt_orders(symbol="RELIANCE", h6=2500.0, l6=2400.0, qty=1)
"""

from config import BROKER

if BROKER == "zerodha":
    from broker.zerodha import place_gtt_orders
elif BROKER == "angel":
    from broker.angel_one import place_gtt_orders
else:
    def place_gtt_orders(symbol, h6, l6, qty=1):
        print(f"[DRY RUN] {symbol}: GTT BUY @ {h6} | GTT SELL @ {l6}")
        return {"symbol": symbol, "status": "DRY_RUN"}
