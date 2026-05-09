"""
alerts.py — Telegram alerts (v8)

Signal display strategy:
  - One message per timeframe (3 messages max per run)
  - Each signal on ONE line: Symbol | Buy@ | Sell@
  - If >25 signals in a timeframe: show top 25, rest as count
  - If still too long: split into multiple messages automatically
  - Plain text only (no HTML) — zero parse errors, works on all Telegram clients
  - Full signal list always available in Google Sheets (linked in message)

Why this format:
  - Telegram monospace block makes columns readable
  - One line per stock = scannable, easy to act on
  - No scrolling through 144 entries — quality signals only (NRD + Insider)
  - If still many signals, multiple messages are sent automatically
"""

import csv
import io
import logging
import os
import requests

logger = logging.getLogger(__name__)

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
USERS_CSV_URL = os.getenv("USERS_CSV_URL", "")
CHAT_IDS_ENV  = os.getenv("TELEGRAM_CHAT_IDS", os.getenv("TELEGRAM_CHAT_ID", ""))

MAX_LINES_PER_MSG = 25   # signals per Telegram message chunk
MAX_CHARS         = 3800  # safe margin under Telegram's 4096 limit


# ── Recipients ────────────────────────────────────────────────────────────────

def _get_chat_ids() -> list[str]:
    """Load active chat IDs from Google Sheets CSV, fall back to env var."""
    if USERS_CSV_URL:
        try:
            r = requests.get(USERS_CSV_URL, timeout=10)
            r.raise_for_status()
            reader = csv.DictReader(io.StringIO(r.text))
            ids = [
                str(row.get("ChatID", "")).strip()
                for row in reader
                if str(row.get("Status", "")).strip().upper() == "ACTIVE"
                and str(row.get("ChatID", "")).strip()
            ]
            if ids:
                return ids
        except Exception as e:
            logger.warning(f"Could not load users CSV: {e}")

    return [c.strip() for c in CHAT_IDS_ENV.split(",") if c.strip()]


# ── Sender ────────────────────────────────────────────────────────────────────

def _send(chat_id: str, text: str) -> bool:
    if not BOT_TOKEN or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Send failed ({chat_id}): {e}")
        return False


def _send_to_all(text: str, recipients: list[str]) -> None:
    for cid in recipients:
        _send(cid, text)


def _split_and_send(text: str, recipients: list[str]) -> None:
    """Auto-split long messages on newlines and send each chunk."""
    if len(text) <= MAX_CHARS:
        _send_to_all(text, recipients)
        return

    lines   = text.splitlines(keepends=True)
    chunk   = ""
    for line in lines:
        if len(chunk) + len(line) > MAX_CHARS:
            if chunk:
                _send_to_all(chunk, recipients)
            chunk = line
        else:
            chunk += line
    if chunk:
        _send_to_all(chunk, recipients)


# ── Formatters ────────────────────────────────────────────────────────────────

def _timeframe_block(label: str, tag: str, signals: list[dict], run_date: str) -> str:
    """
    Format one timeframe's signals as a compact plain-text table.

    Example output:
        [D] Daily TF — 30 Apr 2026
        GTT Buy @ H6 (CNC)  |  GTT Sell @ L6 (MIS)
        ──────────────────────────────────────────
         #  Symbol        Buy@H6    Sell@L6   H5        L5
        ──────────────────────────────────────────
         1  RELIANCE     2980.35   2849.65  3012.40  2817.60
         2  TCS          3418.20   3281.80  3450.10  3249.90
        ──────────────────────────────────────────
        2 signal(s) | Full list: Google Sheets > Signals tab
    """
    if not signals:
        return (
            f"{tag} {label} — {run_date}\n"
            f"No signals (NRD + Insider conditions not met)\n"
        )

    total = len(signals)
    shown = signals[:MAX_LINES_PER_MSG]

    header = (
        f"{tag} {label} — {run_date}\n"
        f"GTT Buy @ H6 (CNC)  |  GTT Sell @ L6 (MIS)\n"
        f"{'─'*52}\n"
        f"{'#':>2}  {'Symbol':<13} {'Buy@H6':>9} {'Sell@L6':>9} {'H5':>9} {'L5':>9}\n"
        f"{'─'*52}\n"
    )

    rows = ""
    for i, sig in enumerate(shown, 1):
        sym  = sig.get("Symbol", "")[:12]
        h6   = sig.get("H6", 0)
        l6   = sig.get("L6", 0)
        h5   = sig.get("H5", 0)
        l5   = sig.get("L5", 0)
        rows += f"{i:>2}  {sym:<13} {h6:>9.2f} {l6:>9.2f} {h5:>9.2f} {l5:>9.2f}\n"

    footer = f"{'─'*52}\n"
    if total > MAX_LINES_PER_MSG:
        footer += f"+{total - MAX_LINES_PER_MSG} more signals in Google Sheets > Signals tab\n"
    else:
        footer += f"{total} signal(s) | Check Sheets > Signals tab for full details\n"

    return header + rows + footer


# ── Public API ────────────────────────────────────────────────────────────────

def broadcast(
    daily_signals:   list[dict] | None = None,
    monthly_signals: list[dict] | None = None,
    yearly_signals:  list[dict] | None = None,
    run_date:        str = "",
    requesting_chat_id: str = "",
    message:         str | None = None,
) -> None:
    """
    Send scan results to all active users.

    Three separate messages (one per timeframe) to stay under
    Telegram's 4096 character limit.

    If `message` is a plain string (e.g. error/warning), sends it directly.
    """
    chat_ids   = _get_chat_ids()
    recipients = list(chat_ids)
    if requesting_chat_id and requesting_chat_id not in recipients:
        recipients.append(requesting_chat_id)

    if not recipients:
        logger.warning("No Telegram recipients — alert skipped.")
        return

    # Plain string (errors / warnings)
    if message is not None:
        _split_and_send(str(message), recipients)
        return

    daily   = daily_signals   or []
    monthly = monthly_signals or []
    yearly  = yearly_signals  or []
    total   = len(daily) + len(monthly) + len(yearly)

    # ── Summary header (always sent first) ───────────────────────────────────
    if total == 0:
        header = (
            f"Algo Scan | {run_date}\n"
            f"No signals today.\n"
            f"NRD + Insider conditions not met on any stock.\n"
            f"(Daily | Monthly | Yearly — all checked)"
        )
        _send_to_all(header, recipients)
        return

    header = (
        f"Algo Scan | {run_date}\n"
        f"Total: {total} signal(s) "
        f"[D:{len(daily)} M:{len(monthly)} Y:{len(yearly)}]\n"
        f"Signals below need both NRD + Insider conditions."
    )
    _send_to_all(header, recipients)

    # ── Daily TF ─────────────────────────────────────────────────────────────
    _split_and_send(
        _timeframe_block("[D]", "[D]", daily, run_date),
        recipients,
    )

    # ── Monthly TF ───────────────────────────────────────────────────────────
    _split_and_send(
        _timeframe_block("[M]", "[M]", monthly, run_date),
        recipients,
    )

    # ── Yearly TF ────────────────────────────────────────────────────────────
    _split_and_send(
        _timeframe_block("[Y]", "[Y]", yearly, run_date),
        recipients,
    )

    logger.info(f"Broadcast complete — {total} signals to {len(recipients)} users.")
