# token_manager.py
# Daily flow: GET /api/token?pin=XXXX
# Every 15 days: Re-run Colab to get fresh refresh_token

import requests
import logging
from datetime import datetime, timezone

from config import FYERS_CLIENT_ID, SUPABASE_URL, SUPABASE_API_KEY, SUPABASE_TOKENS_TABLE

logger = logging.getLogger(__name__)

# SHA256 of "EMRCD1JW93:SECRET_KEY" — fixed, never changes
APP_ID_HASH = "b0f268639d92f52edaaba45b701d9a17df1b4790e32db45fa22eb445b6058369"

SUPABASE_HEADERS = {
    "apikey":        SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type":  "application/json",
}


def get_tokens_from_supabase() -> dict:
    url  = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}?id=eq.1&select=*"
    resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError("No token row in Supabase. Run Colab first.")
    return data[0]


def save_tokens_to_supabase(access_token: str, refresh_token: str = "") -> None:
    payload = {
        "id":           1,
        "access_token": access_token,
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }
    if refresh_token:
        payload["refresh_token"] = refresh_token

    requests.post(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TOKENS_TABLE}",
        json=payload,
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
        timeout=15,
    ).raise_for_status()
    logger.info("Tokens saved to Supabase.")


def refresh_access_token(pin: str) -> str:
    """Use refresh_token + PIN + appIdHash to get new access_token."""
    tokens        = get_tokens_from_supabase()
    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        raise ValueError("No refresh_token in Supabase. Re-run Colab.")

    payload = {
        "grant_type":    "refresh_token",
        "appIdHash":     APP_ID_HASH,
        "refresh_token": refresh_token,
        "pin":           str(pin),
    }

    resp = requests.post(
        "https://api-t1.fyers.in/api/v3/validate-refresh-token",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    result = resp.json()
    logger.info(f"Fyers refresh response: {result}")

    if result.get("s") != "ok":
        raise ValueError(f"Fyers refresh failed: {result}")

    new_access  = result["access_token"]
    new_refresh = result.get("refresh_token", refresh_token)
    save_tokens_to_supabase(new_access, new_refresh)
    return new_access


def get_access_token() -> str:
    return get_tokens_from_supabase().get("access_token", "")
