# token_manager.py — Fyers token management for Quant Index Intraday Scanner
# Token strategy:
#   - Full Colab login every 15 days (refresh_token lasts 15 days)
#   - Daily: call /api/token to get fresh access_token using refresh_token
#   - access_token expires daily at midnight

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


# ── Supabase helpers ───────────────────────────────────────────────────────────

def get_tokens_from_supabase() -> dict:
    """Return the token row (id=1) from Supabase."""
    url  = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}?id=eq.1&select=*"
    resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError("No token row found in Supabase (id=1). Run Colab activator first.")
    return data[0]


def save_tokens_to_supabase(access_token: str, refresh_token: str = "") -> None:
    """Upsert tokens to Supabase using POST + merge-duplicates (same as Colab)."""
    payload = {
        "id":            1,
        "access_token":  access_token,
        "updated_at":    datetime.now(timezone.utc).isoformat(),
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


# ── Daily token refresh using refresh_token ───────────────────────────────────

def refresh_access_token() -> str:
    """
    Get a fresh access_token using the stored refresh_token.
    Call this once daily before market opens (via /api/token endpoint).
    No PIN needed — uses refresh_token directly with Fyers SDK.
    """
    from fyers_apiv3 import fyersModel

    tokens        = get_tokens_from_supabase()
    refresh_token = tokens.get("refresh_token", "")

    if not refresh_token:
        raise ValueError("refresh_token is empty. Re-run the Colab activator notebook.")

    # Use Fyers SDK SessionModel to exchange refresh_token for new access_token
    session = fyersModel.SessionModel(
        client_id    = FYERS_CLIENT_ID,
        secret_key   = FYERS_SECRET_KEY,
        redirect_uri = "https://trade.fyers.in/api-login/redirect-uri/index.html",
        response_type= "code",
        grant_type   = "refresh_token",
    )
    session.set_token(refresh_token)
    response = session.generate_token()

    if response.get("s") != "ok":
        raise ValueError(f"Fyers token refresh failed: {response}")

    new_access    = response["access_token"]
    new_refresh   = response.get("refresh_token", refresh_token)

    save_tokens_to_supabase(new_access, new_refresh)
    logger.info("Access token refreshed successfully via refresh_token.")
    return new_access


# ── Main accessor ──────────────────────────────────────────────────────────────

def get_access_token() -> str:
    """Return current access_token from Supabase."""
    tokens = get_tokens_from_supabase()
    tok    = tokens.get("access_token", "")
    if not tok:
        raise ValueError("access_token is empty. Run /api/token to refresh.")
    return tok
