"""
broker/zerodha.py — Zerodha Kite Connect integration.

Handles:
  - Session via pre-generated access token (refresh daily via Zerodha login flow)
  - GTT Buy order at H6 level (CNC product type)
  - GTT Sell order at L6 level (MIS product type)

Install: pip install kiteconnect

Note on access token: Zerodha access tokens expire daily.
For full automation, you can use selenium/playwright to auto-login
and refresh the token each morning — or use a Kite Sessions API.
For now, paste today's token as ZERODHA_ACCESS_TOKEN in GitHub Secrets.

Docs: https://kite.trade/docs/connect/v3/
"""

import logging
from kiteconnect import KiteConnect

from config import ZERODHA_API_KEY, ZERODHA_ACCESS_TOKEN

logger = logging.getLogger(__name__)


def login() -> KiteConnect | None:
    """Create an authenticated Kite session."""
    try:
        kite = KiteConnect(api_key=ZERODHA_API_KEY)
        kite.set_access_token(ZERODHA_ACCESS_TOKEN)
        profile = kite.profile()
        logger.info(f"Zerodha login OK — {profile.get('user_name')}")
        return kite
    except Exception as e:
        logger.error(f"Zerodha login error: {e}")
        return None


def place_gtt_buy(kite: KiteConnect, symbol: str,
                  trigger_price: float, last_price: float,
                  qty: int = 1) -> dict | None:
    """
    Place a GTT Buy (single-leg) at H6 level.
    Product: CNC (delivery)
    """
    try:
        order = kite.place_gtt(
            trigger_type=kite.GTT_TYPE_SINGLE,
            tradingsymbol=symbol,
            exchange=kite.EXCHANGE_NSE,
            trigger_values=[trigger_price],
            last_price=last_price,
            orders=[{
                "transaction_type": kite.TRANSACTION_TYPE_BUY,
                "quantity": qty,
                "product": kite.PRODUCT_CNC,
                "order_type": kite.ORDER_TYPE_LIMIT,
                "price": trigger_price,
            }]
        )
        logger.info(f"GTT BUY placed for {symbol} @ {trigger_price}: {order}")
        return order
    except Exception as e:
        logger.error(f"GTT BUY failed for {symbol}: {e}")
        return None


def place_gtt_sell(kite: KiteConnect, symbol: str,
                   trigger_price: float, last_price: float,
                   qty: int = 1) -> dict | None:
    """
    Place a GTT Sell (single-leg) at L6 level.
    Product: MIS (intraday)
    """
    try:
        order = kite.place_gtt(
            trigger_type=kite.GTT_TYPE_SINGLE,
            tradingsymbol=symbol,
            exchange=kite.EXCHANGE_NSE,
            trigger_values=[trigger_price],
            last_price=last_price,
            orders=[{
                "transaction_type": kite.TRANSACTION_TYPE_SELL,
                "quantity": qty,
                "product": kite.PRODUCT_MIS,
                "order_type": kite.ORDER_TYPE_LIMIT,
                "price": trigger_price,
            }]
        )
        logger.info(f"GTT SELL placed for {symbol} @ {trigger_price}: {order}")
        return order
    except Exception as e:
        logger.error(f"GTT SELL failed for {symbol}: {e}")
        return None


def get_ltp(kite: KiteConnect, symbol: str) -> float | None:
    """Get last traded price — needed as reference for Zerodha GTT."""
    try:
        data = kite.ltp(f"NSE:{symbol}")
        return data[f"NSE:{symbol}"]["last_price"]
    except Exception as e:
        logger.error(f"LTP fetch failed for {symbol}: {e}")
        return None


def place_gtt_orders(symbol: str, h6: float, l6: float, qty: int = 1) -> dict:
    """
    Main entry point. Login → get LTP → place GTT Buy at H6 + GTT Sell at L6.
    Returns dict with order results.
    """
    kite = login()
    if not kite:
        return {"symbol": symbol, "status": "LOGIN_FAILED"}

    last_price = get_ltp(kite, symbol)
    if not last_price:
        return {"symbol": symbol, "status": "LTP_FETCH_FAILED"}

    buy_result  = place_gtt_buy(kite, symbol, h6, last_price, qty)
    sell_result = place_gtt_sell(kite, symbol, l6, last_price, qty)

    return {
        "symbol": symbol,
        "broker": "zerodha",
        "gtt_buy":  buy_result,
        "gtt_sell": sell_result,
        "status": "SUCCESS" if buy_result and sell_result else "PARTIAL",
    }
