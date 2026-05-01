"""
broker/angel_one.py — Angel One SmartAPI integration.

Handles:
  - Login with TOTP 2FA
  - GTT Buy order at H6 level (CNC product type)
  - GTT Sell order at L6 level (MIS product type)

Install: pip install smartapi-python pyotp

Docs: https://smartapi.angelbroking.com/docs
"""

import pyotp
import logging
from SmartApi import SmartConnect

from config import (ANGEL_API_KEY, ANGEL_CLIENT_ID,
                    ANGEL_PASSWORD, ANGEL_TOTP_KEY)

logger = logging.getLogger(__name__)


def login() -> SmartConnect | None:
    """Login to Angel One SmartAPI. Returns authenticated client or None."""
    try:
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        totp = pyotp.TOTP(ANGEL_TOTP_KEY).now()
        data = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)

        if data["status"] is False:
            logger.error(f"Angel One login failed: {data['message']}")
            return None

        logger.info("Angel One login successful.")
        return obj
    except Exception as e:
        logger.error(f"Angel One login error: {e}")
        return None


def get_token(obj: SmartConnect, symbol: str, exchange: str = "NSE") -> str | None:
    """
    Look up the instrument token for a symbol.
    Needed to place orders via SmartAPI.
    """
    try:
        ltp_data = obj.ltpData(exchange, symbol, "")
        return ltp_data["data"]["instrumenttoken"]
    except Exception as e:
        logger.error(f"Token fetch failed for {symbol}: {e}")
        return None


def place_gtt_buy(obj: SmartConnect, symbol: str, token: str,
                  trigger_price: float, qty: int = 1) -> dict | None:
    """
    Place a GTT Buy order at H6 level.
    Product: CNC (delivery)
    """
    try:
        params = {
            "tradingsymbol": symbol,
            "symboltoken":   token,
            "exchange":      "NSE",
            "producttype":   "DELIVERY",   # CNC equivalent in Angel One
            "transactiontype": "BUY",
            "price":         trigger_price,
            "qty":           qty,
            "triggerprice":  trigger_price,
            "disclosedqty":  0,
        }
        response = obj.gttCreateRule(params)
        logger.info(f"GTT BUY placed for {symbol} @ {trigger_price}: {response}")
        return response
    except Exception as e:
        logger.error(f"GTT BUY failed for {symbol}: {e}")
        return None


def place_gtt_sell(obj: SmartConnect, symbol: str, token: str,
                   trigger_price: float, qty: int = 1) -> dict | None:
    """
    Place a GTT Sell order at L6 level.
    Product: MIS (intraday)
    """
    try:
        params = {
            "tradingsymbol": symbol,
            "symboltoken":   token,
            "exchange":      "NSE",
            "producttype":   "INTRADAY",   # MIS equivalent in Angel One
            "transactiontype": "SELL",
            "price":         trigger_price,
            "qty":           qty,
            "triggerprice":  trigger_price,
            "disclosedqty":  0,
        }
        response = obj.gttCreateRule(params)
        logger.info(f"GTT SELL placed for {symbol} @ {trigger_price}: {response}")
        return response
    except Exception as e:
        logger.error(f"GTT SELL failed for {symbol}: {e}")
        return None


def place_gtt_orders(symbol: str, h6: float, l6: float, qty: int = 1) -> dict:
    """
    Main entry point. Login → place GTT Buy at H6 + GTT Sell at L6.
    Returns dict with order results.
    """
    obj = login()
    if not obj:
        return {"symbol": symbol, "status": "LOGIN_FAILED"}

    token = get_token(obj, symbol)
    if not token:
        return {"symbol": symbol, "status": "TOKEN_FETCH_FAILED"}

    buy_result  = place_gtt_buy(obj, symbol, token, h6, qty)
    sell_result = place_gtt_sell(obj, symbol, token, l6, qty)

    return {
        "symbol": symbol,
        "broker": "angel_one",
        "gtt_buy":  buy_result,
        "gtt_sell": sell_result,
        "status": "SUCCESS" if buy_result and sell_result else "PARTIAL",
    }
