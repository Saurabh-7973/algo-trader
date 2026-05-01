"""
config.py — All settings loaded from environment variables (GitHub Secrets).
Never hardcode credentials here.
"""

import os

# ─── Broker ───────────────────────────────────────────────────────────────────
# Set BROKER to "angel" or "zerodha" in GitHub Secrets
BROKER = os.getenv("BROKER", "angel").lower()

# ─── Angel One ────────────────────────────────────────────────────────────────
ANGEL_API_KEY    = os.getenv("ANGEL_API_KEY", "")
ANGEL_CLIENT_ID  = os.getenv("ANGEL_CLIENT_ID", "")
ANGEL_PASSWORD   = os.getenv("ANGEL_PASSWORD", "")
ANGEL_TOTP_KEY   = os.getenv("ANGEL_TOTP_KEY", "")   # for TOTP 2FA

# ─── Zerodha ──────────────────────────────────────────────────────────────────
ZERODHA_API_KEY    = os.getenv("ZERODHA_API_KEY", "")
ZERODHA_API_SECRET = os.getenv("ZERODHA_API_SECRET", "")
ZERODHA_ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN", "")  # refresh daily

# ─── Google Sheets ────────────────────────────────────────────────────────────
# Paste your entire service account JSON as a single GitHub Secret
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")

# Sheet tab names (change if you rename tabs in your Sheets doc)
BASKET_SHEET   = "Basket"      # Stock list: Symbol | Exchange | Active
LEVELS_SHEET   = "Levels"      # Calculated H/L levels per stock
SIGNALS_SHEET  = "Signals"     # Trade signals log
MONTHLY_SHEET  = "Monthly"     # Stored monthly levels (updated 1st of month)
YEARLY_SHEET   = "Yearly"      # Stored yearly levels (updated 1st of Jan)

# ─── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ─── Strategy ─────────────────────────────────────────────────────────────────
NRD_THRESHOLD = 0.004   # 0.4% — Narrow Range Definition threshold
EXCHANGE      = "NSE"   # Default exchange
