"""
alerts.py — Telegram alerts with dynamic user list from Google Sheets.

Broadcast goes to ALL ACTIVE users in the Users tab.
No hardcoded IDs. Add/remove users in Sheets only.
"""

import os
import json
import logging
import requests
from levels import TradingLevels

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
REQUESTING_CHAT_ID = os.getenv("REQUESTING_CHAT_ID", "").strip()


def get_active_chat_ids(sheets_client=None, spreadsheet_id=None) -> list[str]:
    """
    Read active user chat IDs from the 'Users' tab in Google Sheet.
    Falls back to TELEGRAM_CHAT_IDS env var if Sheets unavailable.
    """
    # Fallback: env var (for backward compat / daily scheduled run)
    env_ids = [c.strip() for c in
               os.getenv("TELEGRAM_CHAT_IDS", os.getenv("TELEGRAM_CHAT_ID", "")).split(",")
               if c.strip()]

    if sheets_client is None or spreadsheet_id is None:
        return env_ids

    try:
        ws      = sheets_client.open_by_key(spreadsheet_id).worksheet("Users")
        records = ws.get_all_records()
        active  = [
            str(r.get("ChatID", "")).strip()
            for r in records
            if str(r.get("Status", "ACTIVE")).strip().upper() == "ACTIVE"
            and str(r.get("ChatID", "")).strip()
        ]
        if active:
            logger.info(f"Broadcasting to {len(active)} active users from Sheets.")
            return active
    except Exception as e:
        logger.warning(f"Could not read Users tab: {e}. Falling back to env var.")

    return env_ids


def send_telegram(message: str, bot_token: str = None, chat_id: str = None) -> bool:
    """Send message to one chat ID."""
    token = bot_token or BOT_TOKEN
    if not token or not chat_id:
        logger.warning(f"Telegram: missing token or chat_id, skipping.")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id":    chat_id,
            "text":       message,
            "parse_mode": "HTML",
        }, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Telegram send failed ({chat_id}): {e}")
        return False


def broadcast(message: str, chat_ids: list[str]) -> None:
    """Send message to all active users."""
    for cid in chat_ids:
        send_telegram(message, BOT_TOKEN, cid)

    # Also send to the user who triggered /scan, if not already in broadcast list
    if REQUESTING_CHAT_ID and REQUESTING_CHAT_ID not in chat_ids:
        send_telegram(message, BOT_TOKEN, REQUESTING_CHAT_ID)


def _signal_score(lv: TradingLevels) -> float:
    """
    Score a signal 0-100 for ranking.
    Higher = stronger / more reliable setup.

    Criteria:
      50 pts base  — both NRD + Insider conditions met
      up to 40 pts — range tightness (tighter = stronger compression)
      up to 10 pts — timeframe reliability (yearly > monthly > daily)
    """
    if not lv.signal:
        return 0.0

    score = 50.0

    # Tightness bonus
    try:
        approx_close = (lv.H6 + lv.L6) / 2
        if approx_close > 0 and lv.range_width > 0:
            width_pct = (lv.range_width / approx_close) * 100
            tightness = max(0.0, 40.0 * (1 - width_pct / 0.4))
            score += tightness
    except Exception:
        pass

    # Timeframe bonus
    score += {"yearly": 10, "monthly": 5, "daily": 0}.get(lv.timeframe, 0)

    return round(min(score, 100.0), 1)


def format_signal_table(signals: list[TradingLevels]) -> str:
    """
    Format signals as a Telegram-friendly ranked table.
    Sorted best → least by score.
    """
    if not signals:
        return (
            "📊 <b>Scan Complete</b>\n\n"
            "No signals today.\n"
            "Market not showing NRD + Insider compression on any stock."
        )

    scored = sorted(
        [(lv, _signal_score(lv)) for lv in signals],
        key=lambda x: x[1],
        reverse=True
    )

    header = [
        f"🟢 <b>{len(signals)} Signal{'s' if len(signals)>1 else ''} Found</b>",
        "<i>Ranked best → least  |  NRD + Insider ✅</i>",
        "",
        "<pre>",
        f"{'#':<3} {'Symbol':<13} {'TF':<10} {'Buy(H6)':>9} {'Sell(L6)':>9} {'Score':>6}",
        "─" * 55,
    ]

    tf_label = {"daily": "Intraday", "monthly": "Swing", "yearly": "Position"}

    rows = []
    for rank, (lv, score) in enumerate(scored, 1):
        sym = lv.symbol.replace(".NS", "")[:11]
        tf  = tf_label.get(lv.timeframe, lv.timeframe)
        rows.append(
            f"{rank:<3} {sym:<13} {tf:<10} {lv.H6:>9.2f} {lv.L6:>9.2f} {score:>5.1f}%"
        )

    footer = [
        "─" * 55,
        "</pre>",
        "",
        "<b>Action:</b> GTT Buy @ H6 (CNC)  ·  GTT Sell @ L6 (MIS)",
    ]

    return "\n".join(header + rows + footer)


def format_daily_summary(results: list[TradingLevels]) -> str:
    signals  = [r for r in results if r.signal]
    scanned  = len(set(r.symbol for r in results))
    on_demand = REQUESTING_CHAT_ID != ""

    trigger_note = "⚡ On-demand scan" if on_demand else "🕘 Daily 9:15 AM scan"

    header = (
        f"📊 <b>Algo Trader — {trigger_note}</b>\n"
        f"Stocks scanned: <b>{scanned}</b>  |  "
        f"Signals: <b>{len(signals)}</b>\n\n"
    )

    return header + format_signal_table(signals)


def format_signal_message(lv: TradingLevels) -> str:
    """Individual signal alert (sent immediately when signal fires)."""
    return (
        f"🟢 <b>{lv.symbol}</b> [{lv.timeframe.upper()}]\n\n"
        f"GTT BUY  (CNC): <b>{lv.H6}</b>\n"
        f"GTT SELL (MIS): <b>{lv.L6}</b>\n\n"
        f"H5: {lv.H5}  ·  L5: {lv.L5}\n"
        f"Score: <b>{_signal_score(lv)}%</b>  |  NRD ✅  Insider ✅"
    )
