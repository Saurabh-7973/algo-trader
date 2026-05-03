"""
broker/__init__.py — Auto-selects broker based on BROKER env var.
"""
from config import BROKER

if BROKER == "zerodha":
    from broker.zerodha import place_gtt_orders
elif BROKER == "angel":
    from broker.angel_one import place_gtt_orders
else:
    def place_gtt_orders(symbol, h6, l6, qty=1):
        print(f"[DRY RUN] {symbol}: GTT BUY @ {h6:.2f} | GTT SELL @ {l6:.2f}")
        return {"symbol": symbol, "status": "DRY_RUN"}
