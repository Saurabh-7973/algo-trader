"""
alerts.py — NSE Full Market Edition (feature/nse-full-market branch)

Telegram format redesign:
  - 2 columns only: Buy@H6 | Sell@L6 (H5/L5 removed from Telegram)
  - Symbols right-aligned, prices right-aligned
  - No line wrapping on any phone screen
  - Top 25 per timeframe, +N more → Google Sheets
  - H5/L5 still in Google Sheets Levels tab for analysis

Why H5/L5 removed from Telegram:
  The only actionable GTT levels are H6 (buy) and L6 (sell).
  H5/L5 are reference levels useful in Sheets — not for placing orders.
  Removing them keeps every signal on ONE line even on small screens.
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

MAX_SIGNALS   = 25    # signals shown per timeframe before truncation
MAX_CHARS     = 3800  # safe under Telegram's 4096 char limit


# ── Recipients ────────────────────────────────────────────────────────────────

def _get_chat_ids() -> list[str]:
    """Load active chat IDs from Users CSV, fall back to env var."""
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
            logger.warning(f"Users CSV load failed: {e}")
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
        logger.error(f"Telegram send failed ({chat_id}): {e}")
        return False


def _send_all(text: str, recipients: list[str]) -> None:
    for cid in recipients:
        _send(cid, text)


def _split_send(text: str, recipients: list[str]) -> None:
    """Split long messages on newlines and send each chunk."""
    if len(text) <= MAX_CHARS:
        _send_all(text, recipients)
        return
    lines, chunk = text.splitlines(keepends=True), ""
    for line in lines:
        if len(chunk) + len(line) > MAX_CHARS:
            if chunk:
                _send_all(chunk, recipients)
            chunk = line
        else:
            chunk += line
    if chunk:
        _send_all(chunk, recipients)


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_block(tag: str, label: str, signals: list[dict], date: str) -> str:
    """
    Format one timeframe block.

    Example output (fits on any phone screen — 2 columns):
        [D] Daily — 13 May 2026
        GTT Buy @ H6 (CNC)  |  GTT Sell @ L6 (MIS)
        ─────────────────────────────────────
         #  Symbol        Buy@H6    Sell@L6
        ─────────────────────────────────────
         1  AADHAR HFC    497.13    476.47
         2  ABB          6490.22   6119.78
        ...
        +63 more — see Sheets > Signals tab
    """
    if not signals:
        return (
            f"{tag} {label} — {date}\n"
            f"No signals (NRD + Insider not met)\n"
        )

    total = len(signals)
    shown = signals[:MAX_SIGNALS]

    lines = [
        f"{tag} {label} — {date}",
        f"GTT Buy @ H6 (CNC)  |  GTT Sell @ L6 (MIS)",
        "─" * 43,
        f"{'#':>2}  {'Symbol':<13} {'Buy@H6':>9} {'Sell@L6':>9}",
        "─" * 43,
    ]

    for i, sig in enumerate(shown, 1):
        sym  = str(sig.get("Symbol", ""))[:12]
        h6   = sig.get("GTT_Buy_H6")  or sig.get("H6",  0)
        l6   = sig.get("GTT_Sell_L6") or sig.get("L6",  0)
        try:
            h6 = f"{float(h6):>9.2f}"
            l6 = f"{float(l6):>9.2f}"
        except (TypeError, ValueError):
            h6 = f"{'?':>9}"
            l6 = f"{'?':>9}"
        lines.append(f"{i:>2}  {sym:<13} {h6} {l6}")

    lines.append("─" * 43)
    if total > MAX_SIGNALS:
        lines.append(f"+{total - MAX_SIGNALS} more — see Sheets > Signals tab")
    else:
        lines.append(f"{total} signal(s) — full details in Sheets > Signals tab")

    return "\n".join(lines)


# ── Public API ────────────────────────────────────────────────────────────────

def broadcast(
    daily_signals:      list[dict] | None = None,
    monthly_signals:    list[dict] | None = None,
    yearly_signals:     list[dict] | None = None,
    run_date:           str = "",
    requesting_chat_id: str = "",
    message:            str | None = None,
) -> None:
    """
    Send scan results to all active users.

    Sends 4 messages per run:
      1. Summary header (counts)
      2. Daily timeframe block
      3. Monthly timeframe block
      4. Yearly timeframe block

    Each block auto-chunked if > 3800 chars.
    Plain text only — no HTML/Markdown parse errors.
    """
    chat_ids   = _get_chat_ids()
    recipients = list(chat_ids)
    if requesting_chat_id and requesting_chat_id not in recipients:
        recipients.append(requesting_chat_id)

    if not recipients:
        logger.warning("No Telegram recipients — skipping alert.")
        return

    # Plain string message (errors / warnings)
    if message is not None:
        _split_send(str(message), recipients)
        return

    daily   = daily_signals   or []
    monthly = monthly_signals or []
    yearly  = yearly_signals  or []
    total   = len(daily) + len(monthly) + len(yearly)

    # ── 1. Summary header ─────────────────────────────────────────────────────
    if total == 0:
        _send_all(
            f"Algo Scan | {run_date}\n"
            f"No signals today.\n"
            f"NRD + Insider not met on any stock (Daily, Monthly, Yearly).",
            recipients,
        )
        return

    _send_all(
        f"Algo Scan | {run_date}\n"
        f"Total: {total} signal(s) [D:{len(daily)} M:{len(monthly)} Y:{len(yearly)}]\n"
        f"Both NRD + Insider conditions required.",
        recipients,
    )

    # ── 2. Daily block ────────────────────────────────────────────────────────
    _split_send(
        _fmt_block("[D]", "Daily TF (Prev Trading Day)", daily, run_date),
        recipients,
    )

    # ── 3. Monthly block ──────────────────────────────────────────────────────
    _split_send(
        _fmt_block("[M]", "Monthly TF (Prev Month)", monthly, run_date),
        recipients,
    )

    # ── 4. Yearly block ───────────────────────────────────────────────────────
    _split_send(
        _fmt_block("[Y]", "Yearly TF (Prev Year)", yearly, run_date),
        recipients,
    )

    logger.info(f"Broadcast complete — {total} signals to {len(recipients)} users.")
