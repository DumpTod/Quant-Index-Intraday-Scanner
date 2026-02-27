# token_manager.py — Fyers token refresh via Supabase

import hashlib
import requests
import logging
from datetime import datetime

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
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}?id=eq.1&select=*"
    resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError("No token row found in Supabase (id=1).")
    return data[0]


def save_tokens_to_supabase(access_token: str, refresh_token: str = "") -> None:
    """Upsert tokens back to Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}?id=eq.1"
    payload = {
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "updated_at":    datetime.utcnow().isoformat(),
    }
    resp = requests.patch(url, json=payload, headers=SUPABASE_HEADERS, timeout=15)
    resp.raise_for_status()
    logger.info("Tokens saved to Supabase.")


# ── Token refresh via PIN ──────────────────────────────────────────────────────

def _sha256_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def refresh_token_with_pin(pin: str) -> str:
    """
    Refresh Fyers access token using SHA256(client_id:secret_key) + PIN flow.
    Returns the new access_token and persists it in Supabase.
    """
    tokens   = get_tokens_from_supabase()
    ref_tok  = tokens.get("refresh_token", "")
    if not ref_tok:
        raise ValueError("refresh_token is empty in Supabase. Re-authenticate manually.")

    # Build app_id hash
    app_id_hash = _sha256_hash(f"{FYERS_CLIENT_ID}:{FYERS_SECRET_KEY}")

    payload = {
        "grant_type":    "refresh_token",
        "appIdHash":     app_id_hash,
        "refreshToken":  ref_tok,
        "pin":           pin,
    }
    resp = requests.post(
        "https://api-t2.fyers.in/api/v3/validate-refresh-token",
        json=payload, timeout=20,
    )
    resp.raise_for_status()
    result = resp.json()

    if result.get("s") != "ok":
        raise ValueError(f"Fyers token refresh failed: {result}")

    new_access  = result["access_token"]
    new_refresh = result.get("refresh_token", ref_tok)
    save_tokens_to_supabase(new_access, new_refresh)
    logger.info("Access token refreshed successfully.")
    return new_access


# ── Main accessor ──────────────────────────────────────────────────────────────

def get_access_token() -> str:
    """Return current access_token from Supabase (no refresh)."""
    tokens = get_tokens_from_supabase()
    tok = tokens.get("access_token", "")
    if not tok:
        raise ValueError("access_token is empty. Refresh token first via /api/token.")
    return tok
