# token_manager.py — Fyers token management for Quant Index Intraday Scanner
#
# Token lifecycle:
#   - access_token  : expires daily at midnight
#   - refresh_token : valid for 15 days
#
# Daily flow  : GET /api/token?pin=XXXX  → uses refresh_token + PIN → new access_token
# Every 15 days: Re-run Colab notebook to get fresh refresh_token + access_token

import hashlib
import requests
import logging
from datetime import datetime, timezone

from config import (
    FYERS_CLIENT_ID, FYERS_SECRET_KEY,
    SUPABASE_URL, SUPABASE_API_KEY, SUPABASE_TOKENS_TABLE,
)

logger = logging.getLogger(__name__)

SUPABASE_HEADERS = {
    "apikey":        SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type":  "application/json",
}

FYERS_REFRESH_URL = "https://api-t1.fyers.in/api/v3/validate-refresh-token"


# ── Supabase helpers ───────────────────────────────────────────────────────────

def get_tokens_from_supabase() -> dict:
    url  = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}?id=eq.1&select=*"
    resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError("No token row in Supabase. Run Colab activator first.")
    return data[0]


def save_tokens_to_supabase(access_token: str, refresh_token: str = "") -> None:
    """Upsert using POST + merge-duplicates (same as working Colab)."""
    payload = {
        "id":           1,
        "access_token": access_token,
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }
    if refresh_token:
        payload["refresh_token"] = refresh_token

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}",
        json=payload,
        headers={
            **SUPABASE_HEADERS,
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
        timeout=15,
    )
    resp.raise_for_status()
    logger.info("Tokens saved to Supabase.")


# ── Daily refresh using refresh_token + PIN ───────────────────────────────────

def refresh_access_token(pin: str) -> str:
    """
    Exchange refresh_token + PIN for a new access_token.
    Correct Fyers v3 endpoint: POST /api/v3/validate-refresh-token
    appIdHash = SHA256(appId:secret)  where appId is client_id WITHOUT '-100'
    """
    tokens        = get_tokens_from_supabase()
    refresh_token = tokens.get("refresh_token", "")

    if not refresh_token:
        raise ValueError("refresh_token empty in Supabase. Re-run Colab notebook.")

    # SHA256 of "appId:secret" — strip "-100" suffix from client_id
    app_id      = FYERS_CLIENT_ID.split("-")[0]          # e.g. "EMRCD1JW93"
    app_id_hash = hashlib.sha256(
        f"{app_id}:{FYERS_SECRET_KEY}".encode()
    ).hexdigest()

    payload = {
        "grant_type":    "refresh_token",
        "appIdHash":     app_id_hash,
        "refresh_token": refresh_token,
        "pin":           str(pin),
    }

    resp = requests.post(
        FYERS_REFRESH_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    resp.raise_for_status()
    result = resp.json()

    if result.get("s") != "ok":
        raise ValueError(f"Fyers refresh failed: {result}")

    new_access  = result["access_token"]
    new_refresh = result.get("refresh_token", refresh_token)
    save_tokens_to_supabase(new_access, new_refresh)
    logger.info("Access token refreshed via PIN successfully.")
    return new_access


# ── Main accessor ──────────────────────────────────────────────────────────────

def get_access_token() -> str:
    tokens = get_tokens_from_supabase()
    tok    = tokens.get("access_token", "")
    if not tok:
        raise ValueError("access_token empty. Call /api/token?pin=XXXX to refresh.")
    return tok
