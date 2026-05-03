"""
config.py — All settings loaded from environment variables (GitHub Secrets).
Never hardcode credentials here.
"""
import os

# ─── Broker ───────────────────────────────────────────────────────────────────
BROKER = os.getenv("BROKER", "paper").lower()

# ─── Angel One ────────────────────────────────────────────────────────────────
ANGEL_API_KEY    = os.getenv("ANGEL_API_KEY", "")
ANGEL_CLIENT_ID  = os.getenv("ANGEL_CLIENT_ID", "")
ANGEL_PASSWORD   = os.getenv("ANGEL_PASSWORD", "")
ANGEL_TOTP_KEY   = os.getenv("ANGEL_TOTP_KEY", "")

# ─── Zerodha ──────────────────────────────────────────────────────────────────
ZERODHA_API_KEY      = os.getenv("ZERODHA_API_KEY", "")
ZERODHA_API_SECRET   = os.getenv("ZERODHA_API_SECRET", "")
ZERODHA_ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN", "")

# ─── Google Sheets ────────────────────────────────────────────────────────────
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
SPREADSHEET_ID          = os.getenv("SPREADSHEET_ID", "")

BASKET_SHEET  = "Basket"
LEVELS_SHEET  = "Levels"
SIGNALS_SHEET = "Signals"
MONTHLY_SHEET = "Monthly"
YEARLY_SHEET  = "Yearly"

# ─── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_CHAT_IDS  = os.getenv("TELEGRAM_CHAT_IDS", "")
USERS_CSV_URL      = os.getenv("USERS_CSV_URL", "")

# ─── Strategy ─────────────────────────────────────────────────────────────────
NRD_THRESHOLD = 0.004   # 0.4%
EXCHANGE      = "NSE"
