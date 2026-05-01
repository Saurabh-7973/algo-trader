# Algo Trader — Free NSE Trading System

Automated trading strategy based on H3–H6 / L3–L6 levels with Narrow Range Day (NRD) and Insider Trading Level conditions. Runs free on GitHub Actions. Zero server cost.

---

## Architecture

```
yfinance (free data)
    ↓
GitHub Actions (free scheduler — runs at 9:15 AM IST)
    ↓
Python script (calculates levels, checks conditions)
    ↓
Google Sheets (stock basket + signal log)
    ↓
Angel One / Zerodha API (places GTT orders)
    ↓
Telegram Bot (instant alerts on phone)
```

---

## Setup — Step by Step

### Step 1: Fork / clone this repo to your GitHub account

### Step 2: Set up Google Sheets

1. Create a new Google Sheet
2. Create these 5 tabs: `Basket`, `Levels`, `Signals`, `Monthly`, `Yearly`
3. In the `Basket` tab, add headers: `Symbol | Exchange | Active`
4. Add your stocks:
   ```
   Symbol    Exchange  Active
   RELIANCE  NSE       YES
   TCS       NSE       YES
   INFY      NSE       YES
   ```
5. Copy the Spreadsheet ID from the URL:
   `https://docs.google.com/spreadsheets/d/THIS_IS_THE_ID/edit`

### Step 3: Create Google Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (or use existing)
3. Enable **Google Sheets API** and **Google Drive API**
4. Go to IAM → Service Accounts → Create Service Account
5. Download the JSON key file
6. Open the JSON and copy the entire content (will be used as a Secret)
7. Share your Google Sheet with the service account email (Editor access)

### Step 4: Set up Telegram Bot

1. Open Telegram → search `@BotFather` → send `/newbot`
2. Follow steps → copy the **Bot Token**
3. Send any message to your new bot
4. Visit: `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
5. Find `result[0].message.chat.id` → that's your **Chat ID**

### Step 5: Set up Broker API

**Angel One:**
1. Login to [Angel One SmartAPI](https://smartapi.angelbroking.com)
2. Create an app → copy API Key
3. Enable TOTP in your Angel One account → copy TOTP secret key

**Zerodha:**
1. Login to [Kite Connect](https://developers.kite.trade)
2. Create an app → copy API Key and Secret
3. Note: Access token must be refreshed daily (paste in Secrets each morning initially)

### Step 6: Add GitHub Secrets

Go to your repo → Settings → Secrets and variables → Actions → New repository secret

Add these secrets:

| Secret Name | Value |
|---|---|
| `BROKER` | `angel` or `zerodha` |
| `ANGEL_API_KEY` | Your Angel One API key |
| `ANGEL_CLIENT_ID` | Your Angel One client ID |
| `ANGEL_PASSWORD` | Your Angel One password |
| `ANGEL_TOTP_KEY` | Your TOTP secret key |
| `ZERODHA_API_KEY` | Your Zerodha API key |
| `ZERODHA_API_SECRET` | Your Zerodha secret |
| `ZERODHA_ACCESS_TOKEN` | Today's access token |
| `GOOGLE_CREDENTIALS_JSON` | Entire service account JSON |
| `SPREADSHEET_ID` | Your Google Sheet ID |
| `TELEGRAM_BOT_TOKEN` | Your Telegram bot token |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID |

### Step 7: Test manually

1. Go to Actions tab in your GitHub repo
2. Select "Daily Algo Trade Run"
3. Click "Run workflow" → "Run workflow"
4. Watch the logs — check Telegram + Google Sheets for output

---

## How the Strategy Works

### Level Formulas
```
H3 = Close + ((High - Low) × 0.275)
H4 = Close + ((High - Low) × 0.55)
H5 = H4 + 1.168 × (H4 - H3)
H6 = (High / Low) × Close        ← GTT BUY trigger (CNC)

L3 = Close - ((High - Low) × 0.275)
L4 = Close - ((High - Low) × 0.55)
L5 = L4 - 1.168 × (L3 - L4)
L6 = Close - (H6 - Close)         ← GTT SELL trigger (MIS)
```

### Timeframes
| Timeframe | Data Used | When Recalculated |
|---|---|---|
| Daily (intraday) | Previous Day HLC | Every morning |
| Monthly (2–3 day) | Previous Month HLC | 1st of each month |
| Yearly (swing) | Previous Year HLC | 1st Jan every year |

### Trade Conditions (both must be true)
1. **Insider Trading Level**: Today's H3/H4/H5/H6 ALL below yesterday's AND Today's L3/L4/L5/L6 ALL above yesterday's
2. **Narrow Range Day (NRD)**: Range Width < 0.4% of Closing Price

### Orders Placed
- **GTT Buy** at H6 — Product: CNC (delivery)
- **GTT Sell** at L6 — Product: MIS (intraday)

---

## Adding/Removing Stocks

Just edit the `Basket` tab in Google Sheets:
- Set `Active = YES` to include a stock
- Set `Active = NO` to pause it (levels won't be calculated)
- Add as many rows as you want — no code changes needed

---

## Cost Summary

| Component | Cost |
|---|---|
| yfinance data | ₹0 |
| GitHub Actions | ₹0 (2000 min/month free) |
| Google Sheets | ₹0 |
| Broker API | ₹0 (with broker account) |
| Telegram Bot | ₹0 |
| **Total** | **₹0/month** |

---

## File Structure

```
algo_trader/
├── main.py              # Main orchestrator
├── config.py            # All env var settings
├── data_fetcher.py      # yfinance OHLC data
├── levels.py            # H3–H6, L3–L6 calculations + signals
├── sheets_manager.py    # Google Sheets read/write
├── alerts.py            # Telegram notifications
├── broker/
│   ├── __init__.py      # Auto-selects broker
│   ├── angel_one.py     # Angel One GTT orders
│   └── zerodha.py       # Zerodha GTT orders
├── requirements.txt
└── .github/
    └── workflows/
        └── trade.yml    # GitHub Actions cron schedule
```
