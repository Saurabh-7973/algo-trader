"""
alerts.py — Sends trade signal alerts via Telegram Bot.

Setup (5 minutes):
  1. Open Telegram → search @BotFather → /newbot → copy the token
  2. Send any message to your new bot
  3. Visit: https://api.telegram.org/bot<TOKEN>/getUpdates
  4. Copy the "id" from result.message.chat → that's your CHAT_ID
  5. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to GitHub Secrets
"""

import requests
import logging
from levels import TradingLevels

logger = logging.getLogger(__name__)


def send_telegram(message: str, bot_token: str, chat_id: str) -> bool:
    """Send a plain text message to Telegram."""
    if not bot_token or not chat_id:
        logger.warning("Telegram not configured. Skipping alert.")
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


def format_signal_message(lv: TradingLevels) -> str:
    """Format a signal alert message for Telegram."""
    return (
        f"🟢 <b>TRADE SIGNAL FIRED</b>\n\n"
        f"<b>Stock:</b>     {lv.symbol}\n"
        f"<b>Timeframe:</b> {lv.timeframe.upper()}\n\n"
        f"<b>CONDITIONS MET:</b>\n"
        f"  ✅ Narrow Range Day (NRD)\n"
        f"  ✅ Insider Trading Level\n\n"
        f"<b>GTT ORDERS TO PLACE:</b>\n"
        f"  🔼 GTT BUY  (CNC) at H6 = <b>{lv.H6}</b>\n"
        f"  🔽 GTT SELL (MIS) at L6 = <b>{lv.L6}</b>\n\n"
        f"<b>Key Levels:</b>\n"
        f"  H5: {lv.H5} | H6: {lv.H6}\n"
        f"  L5: {lv.L5} | L6: {lv.L6}\n"
        f"  Range Width: {lv.range_width}"
    )


def format_daily_summary(results: list[TradingLevels]) -> str:
    """Send a daily summary of all signals (or no-signal report)."""
    signals = [r for r in results if r.signal]
    total   = len(results)

    if not signals:
        return (
            f"📊 <b>Daily Algo Run Complete</b>\n"
            f"Stocks scanned: {total}\n"
            f"Signals today: <b>0</b>\n"
            f"No trade conditions met."
        )

    lines = [
        f"📊 <b>Daily Algo Run — {len(signals)} Signal(s) Found</b>\n"
        f"Stocks scanned: {total}\n"
    ]
    for lv in signals:
        lines.append(
            f"\n🟢 <b>{lv.symbol}</b> [{lv.timeframe.upper()}]\n"
            f"   GTT BUY  @ {lv.H6}\n"
            f"   GTT SELL @ {lv.L6}"
        )
    return "\n".join(lines)
