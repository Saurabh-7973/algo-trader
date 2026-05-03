"""
alerts.py — Telegram alerts (v7 fix)

Root cause of 400 Bad Request: Telegram has a 4096 character limit.
144 signals in one message = way over limit.

Fix:
  - Send ONE message per timeframe (3 messages max per run)
  - Each message chunked to stay under 4000 chars
  - Top 30 signals shown per timeframe, rest summarised as "+ N more"
  - Plain text tables (no <pre> HTML) — avoids HTML parse errors
"""

import os
import csv
import io
import logging
import requests

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
USERS_CSV_URL       = os.getenv("USERS_CSV_URL", "")
TELEGRAM_CHAT_IDS   = os.getenv("TELEGRAM_CHAT_IDS", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")

MAX_MSG_LEN = 3800   # safe under Telegram's 4096 limit
MAX_SIGNALS = 30     # max signals shown per timeframe before truncating


# ─── User Management ──────────────────────────────────────────────────────────

def _get_active_chat_ids() -> list[str]:
    """Fetch active chat IDs from Google Sheets CSV, fallback to env vars."""
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
            if ids:
                logger.info(f"Loaded {len(ids)} active users from Sheets")
                return ids
        except Exception as e:
            logger.warning(f"Could not load users CSV: {e} — using env var fallback")

    # Fallback: env vars
    ids = []
    for val in [TELEGRAM_CHAT_IDS, TELEGRAM_CHAT_ID]:
        for cid in val.split(","):
            cid = cid.strip()
            if cid and cid not in ids:
                ids.append(cid)
    return ids


def _send(chat_id: str, text: str) -> bool:
    """Send one Telegram message — plain text, no parse_mode."""
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text":    text,
            # No parse_mode — avoids ALL HTML/Markdown parse errors
        }, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Telegram send failed for {chat_id}: {e}")
        return False


def _send_to_all(text: str, chat_ids: list[str], requesting_chat_id: str = "") -> None:
    """Send a message to all active users + requesting user."""
    recipients = list(chat_ids)
    if requesting_chat_id and requesting_chat_id not in recipients:
        recipients.append(requesting_chat_id)

    sent = 0
    for cid in recipients:
        if _send(cid, text):
            sent += 1
    logger.info(f"Message sent to {sent}/{len(recipients)} users.")


# ─── Message Formatters ───────────────────────────────────────────────────────

def _format_timeframe_message(
    label: str,
    emoji: str,
    signals: list[dict],
    run_date: str,
) -> str:
    """
    Build a plain-text message for one timeframe.
    Caps at MAX_SIGNALS rows to stay within Telegram limit.
    """
    if not signals:
        return (
            f"{emoji} {label} | {run_date}\n"
            f"No signals today.\n"
        )

    total = len(signals)
    shown = signals[:MAX_SIGNALS]
    header = (
        f"{emoji} {label}\n"
        f"Date: {run_date} | {total} signal(s)\n"
        f"GTT BUY @ H6 (CNC)  |  GTT SELL @ L6 (MIS)\n"
        f"{'─'*42}\n"
        f"{'#':<3} {'Symbol':<12} {'Buy@H6':>9}  {'Sell@L6':>9}\n"
        f"{'─'*42}\n"
    )

    rows = ""
    for i, sig in enumerate(shown, 1):
        symbol = sig.get("Symbol", "")[:11]
        h6 = sig.get("H6", 0)
        l6 = sig.get("L6", 0)
        rows += f"{i:<3} {symbol:<12} {h6:>9.2f}  {l6:>9.2f}\n"

    footer = f"{'─'*42}\n"
    if total > MAX_SIGNALS:
        footer += f"... and {total - MAX_SIGNALS} more signals\n"

    return header + rows + footer


def _chunk_message(text: str) -> list[str]:
    """
    Split a message into chunks of MAX_MSG_LEN characters.
    Splits on newlines to avoid cutting mid-row.
    """
    if len(text) <= MAX_MSG_LEN:
        return [text]

    chunks = []
    current = ""
    for line in text.splitlines(keepends=True):
        if len(current) + len(line) > MAX_MSG_LEN:
            if current:
                chunks.append(current)
            current = line
        else:
            current += line
    if current:
        chunks.append(current)
    return chunks


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
    Broadcast signal alerts to all active users.

    Sends 3 separate messages (one per timeframe) to stay under
    Telegram's 4096 character limit.

    If `message` is a plain string (error/warning), sends it directly.
    """
    chat_ids = _get_active_chat_ids()

    if not chat_ids and not requesting_chat_id:
        logger.warning("No Telegram recipients found — alert skipped.")
        return

    # ── Plain string message (errors / warnings) ──
    if message is not None:
        for chunk in _chunk_message(str(message)):
            _send_to_all(chunk, chat_ids, requesting_chat_id)
        return

    # ── Structured signal messages ──
    daily   = daily_signals   or []
    monthly = monthly_signals or []
    yearly  = yearly_signals  or []

    total_signals = len(daily) + len(monthly) + len(yearly)

    # Header message
    if total_signals == 0:
        header = (
            f"Algo Scan | {run_date}\n"
            f"Scan complete. No NRD + Insider signals today.\n"
            f"All 3 timeframes checked: Daily, Monthly, Yearly."
        )
        _send_to_all(header, chat_ids, requesting_chat_id)
        return

    header = (
        f"Algo Scan | {run_date}\n"
        f"Total signals: {total_signals} "
        f"(Daily:{len(daily)} Monthly:{len(monthly)} Yearly:{len(yearly)})\n"
    )
    _send_to_all(header, chat_ids, requesting_chat_id)

    # Daily timeframe message
    daily_msg = _format_timeframe_message(
        label="Daily TF (Prev Trading Day HLC)",
        emoji="[D]",
        signals=daily,
        run_date=run_date,
    )
    for chunk in _chunk_message(daily_msg):
        _send_to_all(chunk, chat_ids, requesting_chat_id)

    # Monthly timeframe message
    monthly_msg = _format_timeframe_message(
        label="Monthly TF (Prev Month HLC)",
        emoji="[M]",
        signals=monthly,
        run_date=run_date,
    )
    for chunk in _chunk_message(monthly_msg):
        _send_to_all(chunk, chat_ids, requesting_chat_id)

    # Yearly timeframe message
    yearly_msg = _format_timeframe_message(
        label="Yearly TF (Prev Year HLC)",
        emoji="[Y]",
        signals=yearly,
        run_date=run_date,
    )
    for chunk in _chunk_message(yearly_msg):
        _send_to_all(chunk, chat_ids, requesting_chat_id)

    logger.info(f"Broadcast complete — {total_signals} signals sent across 3 timeframes.")
