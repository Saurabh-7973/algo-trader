"""
alerts.py — Telegram alerts (v7)

Changes:
  - broadcast() now accepts daily_signals, monthly_signals, yearly_signals
    as separate lists so each timeframe section is clearly shown.
  - Each section header shows the HLC reference date so user can verify
    which previous day/month/year was actually used.
  - No-signal message is sent per timeframe so user knows it ran correctly.
  - Dynamic user list from Google Sheets Users tab (unchanged from v6).
"""

import os
import logging
import requests
import csv
import io

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
USERS_CSV_URL       = os.getenv("USERS_CSV_URL", "")       # published Google Sheet CSV
TELEGRAM_CHAT_IDS   = os.getenv("TELEGRAM_CHAT_IDS", "")   # fallback: comma-separated


# ─── User Management ──────────────────────────────────────────────────────────

def _get_active_chat_ids() -> list[str]:
    """
    Fetch active chat IDs from Google Sheets Users tab (published CSV).
    Falls back to TELEGRAM_CHAT_IDS env var if URL is not set.
    """
    if USERS_CSV_URL:
        try:
            resp = requests.get(USERS_CSV_URL, timeout=10)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            ids = [
                str(row.get("ChatID", "")).strip()
                for row in reader
                if str(row.get("Status", "")).strip().upper() == "ACTIVE"
                and str(row.get("ChatID", "")).strip()
            ]
            logger.info(f"Loaded {len(ids)} active users from Sheets")
            return ids
        except Exception as e:
            logger.warning(f"Could not load users CSV: {e} — falling back to env var")

    # Fallback
    return [cid.strip() for cid in TELEGRAM_CHAT_IDS.split(",") if cid.strip()]


def _send(chat_id: str, text: str) -> bool:
    """Send a Telegram message to one chat_id."""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set — skipping send")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "HTML",
        }, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Telegram send failed for {chat_id}: {e}")
        return False


# ─── Message Formatters ───────────────────────────────────────────────────────

def _signal_table(signals: list[dict]) -> str:
    """Format a list of signal dicts into a monospace table string."""
    if not signals:
        return "  — No signals\n"

    header = f"  {'#':<3} {'Symbol':<12} {'Buy@H6':>9} {'Sell@L6':>9}  Ref Date\n"
    sep    = "  " + "─" * 48 + "\n"
    rows   = ""
    for i, sig in enumerate(signals, 1):
        rows += (
            f"  {i:<3} {sig['Symbol']:<12} "
            f"{sig.get('H6', 0):>9.2f} "
            f"{sig.get('L6', 0):>9.2f}  "
            f"{sig.get('Date', '')}\n"
        )
    return header + sep + rows


def _build_message(
    daily_signals:   list[dict],
    monthly_signals: list[dict],
    yearly_signals:  list[dict],
    run_date:        str,
) -> str:
    """Build the full consolidated alert message."""

    total = len(daily_signals) + len(monthly_signals) + len(yearly_signals)

    # ── Header ──
    if total == 0:
        header = (
            f"📊 <b>Algo Scan — {run_date}</b>\n"
            f"✅ Scan complete — No NRD + Insider signals today across all timeframes.\n"
        )
    else:
        header = (
            f"🚨 <b>Algo Scan — {run_date}</b>\n"
            f"<b>{total} signal(s)</b> across all timeframes\n"
            f"GTT BUY @ H6 (CNC) · GTT SELL @ L6 (MIS)\n"
        )

    msg = header + "\n"

    # ── Daily Section ──
    msg += f"<b>📅 Daily TF (Prev Trading Day HLC)</b>\n"
    msg += "<pre>"
    msg += _signal_table(daily_signals)
    msg += "</pre>\n"

    # ── Monthly Section ──
    msg += f"<b>📆 Daily Positional TF (Prev Month HLC)</b>\n"
    msg += "<pre>"
    msg += _signal_table(monthly_signals)
    msg += "</pre>\n"

    # ── Yearly Section ──
    msg += f"<b>📈 Swing TF (Prev Year HLC)</b>\n"
    msg += "<pre>"
    msg += _signal_table(yearly_signals)
    msg += "</pre>\n"

    msg += f"<i>Run: {run_date} · Basket scanned for NRD + Insider conditions</i>"
    return msg


# ─── Public API ───────────────────────────────────────────────────────────────

def broadcast(
    daily_signals:   list[dict] | None = None,
    monthly_signals: list[dict] | None = None,
    yearly_signals:  list[dict] | None = None,
    run_date:        str = "",
    requesting_chat_id: str = "",
    # Legacy: plain string message support
    message:         str | None = None,
) -> None:
    """
    Broadcast the signal alert to all active users.

    If `message` is provided (string), it is sent as-is (used for error alerts).
    Otherwise builds a formatted table from the three signal lists.
    """
    # ── Determine recipients ──
    chat_ids = _get_active_chat_ids()
    if requesting_chat_id and requesting_chat_id not in chat_ids:
        chat_ids.append(requesting_chat_id)

    if not chat_ids:
        logger.warning("No active Telegram recipients found — alert skipped.")
        return

    # ── Build message ──
    if message is not None:
        text = message
    else:
        text = _build_message(
            daily_signals   = daily_signals   or [],
            monthly_signals = monthly_signals or [],
            yearly_signals  = yearly_signals  or [],
            run_date        = run_date,
        )

    # ── Send ──
    sent = 0
    for cid in chat_ids:
        if _send(cid, text):
            sent += 1

    logger.info(f"Alert sent to {sent}/{len(chat_ids)} users.")
