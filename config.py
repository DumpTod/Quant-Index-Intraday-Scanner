# config.py — All credentials and constants for Nifty Intraday Scanner

import os

# ── Fyers API ──────────────────────────────────────────────────────────────────
FYERS_CLIENT_ID   = os.environ.get("FYERS_CLIENT_ID",  "VS55VDHYCW-100")
FYERS_SECRET_KEY  = os.environ.get("FYERS_SECRET_KEY",  "724FOKKSFS")
FYERS_REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"

# ── Supabase ───────────────────────────────────────────────────────────────────
SUPABASE_URL     = os.environ.get("SUPABASE_URL",
    "https://ntxkqmjnmaowvwduswea.supabase.co")
SUPABASE_API_KEY = os.environ.get("SUPABASE_API_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im50eGtxbWpubWFvd3Z3ZHVzd2VhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE5ODg0OTMsImV4cCI6MjA4NzU2NDQ5M30."
    "7NV0yDkMHRVpiYpoUXbcz3LIm9t__ocKlDGJV0HRIVE")
SUPABASE_TOKENS_TABLE = "fyers_tokens"

# ── Instruments ────────────────────────────────────────────────────────────────
# Update month/year every expiry (current: March 2026)
INSTRUMENTS = {
    "NIFTY":     "NSE:NIFTY26MARFUT",
    "BANKNIFTY": "NSE:BANKNIFTY26MARFUT",
}

# ── Market Timing (IST) ────────────────────────────────────────────────────────
MARKET_OPEN    = "09:15"
SIGNAL_START   = "09:30"
SIGNAL_END     = "14:00"
DEAD_ZONE_START= "11:30"
DEAD_ZONE_END  = "12:00"
MARKET_CLOSE   = "15:15"
AUTO_EXIT_TIME = "15:00"

# ── Scanner Settings ───────────────────────────────────────────────────────────
MIN_MODELS_AGREE    = 3
DEAD_ZONE_MIN_SCORE = 20
GRADE_A_HIGH_SCORE  = 20
GRADE_A_MED_SCORE   = 16
ALL_AGREE_BONUS     = 3

# ── Risk Management ────────────────────────────────────────────────────────────
SL_PCT          = 0.004   # 0.4% stop loss
TARGET_RR_MIN   = 1.5     # minimum 1:1.5 risk-reward
TARGET_RR_IDEAL = 2.0     # ideal 1:2 risk-reward
ORB_VOLUME_MULT = 1.5     # ORB volume threshold multiplier

# ── Fyers API Resolutions ──────────────────────────────────────────────────────
RESOLUTION_15M = "15"
RESOLUTION_DAY = "D"

# ── Flask ──────────────────────────────────────────────────────────────────────
FLASK_PORT  = 10000
FLASK_DEBUG = False

# ── Render URL ─────────────────────────────────────────────────────────────────
RENDER_URL = "https://quant-index-intraday-scanner.onrender.com"
