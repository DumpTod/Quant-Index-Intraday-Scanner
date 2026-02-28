# app.py — Flask API for Nifty Intraday Scanner

import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request
from flask_cors import CORS

from config import FLASK_PORT, FLASK_DEBUG
from token_manager import refresh_access_token, get_access_token, get_tokens_from_supabase
from scanner import run_scan_background, scan_state, debug_scan_index, _fetch_cpr
from data_fetcher import fetch_candles_after
from risk_manager import evaluate_outcome

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# ── App ────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app, origins="*")


# ─────────────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status":    "ok",
        "service":   "Nifty Intraday Scanner",
        "timestamp": datetime.now(tz=IST).isoformat(),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Token management
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/token", methods=["GET", "POST"])
def token_refresh():
    """
    Refresh access_token using refresh_token + PIN.
    Daily usage: /api/token?pin=XXXX
    PIN = your 4-digit Fyers login PIN.
    refresh_token lasts 15 days — re-run Colab every 15 days.
    """
    pin = request.args.get("pin") or (request.get_json(silent=True) or {}).get("pin")
    if not pin:
        return jsonify({"error": "PIN required. Use /api/token?pin=XXXX"}), 400
    try:
        new_token = refresh_access_token(pin)
        return jsonify({"status": "ok", "message": "Token refreshed", "token_preview": new_token[:15] + "..."})
    except Exception as e:
        logger.exception(f"Token refresh failed: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Debug — connectivity test
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/debug", methods=["GET"])
def debug():
    import requests as req
    from config import FYERS_CLIENT_ID
    result = {"fyers": {}, "supabase": {}}

    # Supabase check
    try:
        tokens = get_tokens_from_supabase()
        acc = tokens.get("access_token", "")
        ref = tokens.get("refresh_token", "")
        result["supabase"] = {
            "status":             "ok",
            "has_access_token":   bool(acc),
            "has_refresh_token":  bool(ref),
            "access_token_preview":  acc[:20] + "..." if acc else "",
            "refresh_token_preview": ref[:20] + "..." if ref else "",
        }
    except Exception as e:
        result["supabase"] = {"status": "error", "error": str(e)}

    # Fyers check — test with funds endpoint directly
    try:
        tok = get_access_token()
        full_token = f"{FYERS_CLIENT_ID}:{tok}"
        result["fyers"] = {
            "status":       "ok",
            "client_id":    FYERS_CLIENT_ID,
            "full_token_preview": full_token[:40] + "...",
        }
        # Live test against Fyers API
        resp = req.get(
            "https://api-t1.fyers.in/api/v3/funds",
            headers={"Authorization": full_token, "Content-Type": "application/json"},
            timeout=10,
        )
        fyers_resp = resp.json()
        result["fyers"]["live_test"] = fyers_resp.get("s")
        result["fyers"]["live_message"] = fyers_resp.get("message", "")
        if fyers_resp.get("s") != "ok":
            result["fyers"]["live_error"] = fyers_resp
    except Exception as e:
        result["fyers"] = {"status": "error", "error": str(e)}

    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Scan trigger
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/scan", methods=["GET"])
def trigger_scan():
    """Trigger background scan — returns immediately."""
    run_scan_background()
    return jsonify({
        "status":  "started",
        "message": "Scan running in background. Poll /api/results for progress.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/results", methods=["GET"])
def results():
    """Return current scan state and signals."""
    return jsonify(dict(scan_state))


# ─────────────────────────────────────────────────────────────────────────────
# Raw Fyers API test — shows exact response from Fyers history API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/debug_fyers/<index>", methods=["GET"])
def debug_fyers(index: str):
    """Test Fyers history API directly and return raw response."""
    from datetime import date
    from fyers_apiv3 import fyersModel
    from config import FYERS_CLIENT_ID, INSTRUMENTS
    from token_manager import get_access_token

    index = index.upper()
    if index not in INSTRUMENTS:
        return jsonify({"error": f"Unknown index {index}"}), 400

    try:
        tok   = get_access_token()
        token = f"{FYERS_CLIENT_ID}:{tok}"
        fyers = fyersModel.FyersModel(client_id=FYERS_CLIENT_ID, token=token, log_path="", is_async=False)
        today = date.today()
        data  = {
            "symbol":      INSTRUMENTS[index],
            "resolution":  "15",
            "date_format": "1",
            "range_from":  today.strftime("%Y-%m-%d"),
            "range_to":    today.strftime("%Y-%m-%d"),
            "cont_flag":   "1",
        }
        resp = fyers.history(data=data)
        candles = resp.get("candles", [])
        return jsonify({
            "status":       resp.get("s"),
            "message":      resp.get("message", ""),
            "candle_count": len(candles),
            "first_candle": candles[0]  if candles else None,
            "last_candle":  candles[-1] if candles else None,
            "symbol":       INSTRUMENTS[index],
            "date":         str(today),
            "raw_keys":     list(resp.keys()),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Debug scan for one index
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/debug_scan/<index>", methods=["GET"])
def debug_scan(index: str):
    index = index.upper()
    from config import INSTRUMENTS
    if index not in INSTRUMENTS:
        return jsonify({"error": f"Unknown index {index}. Valid: {list(INSTRUMENTS.keys())}"}), 400
    data = debug_scan_index(index)
    return jsonify(data)


# ─────────────────────────────────────────────────────────────────────────────
# Historical prices — used by frontend for outcome check
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/prices", methods=["POST"])
def prices():
    """
    Fetch 15-min candles after a signal and evaluate outcome.
    Body: {
        "index": "NIFTY",
        "direction": "BUY",
        "entry": 22500,
        "sl": 22410,
        "target_1": 22635,
        "signal_time": "2026-03-10T10:15:00+05:30"
    }
    """
    body = request.get_json(silent=True) or {}
    index      = body.get("index", "").upper()
    direction  = body.get("direction", "").upper()
    entry      = float(body.get("entry", 0))
    sl         = float(body.get("sl", 0))
    target_1   = float(body.get("target_1", 0))
    signal_time_str = body.get("signal_time")

    if not all([index, direction, entry, sl, target_1, signal_time_str]):
        return jsonify({"error": "Missing required fields."}), 400

    from config import INSTRUMENTS
    if index not in INSTRUMENTS:
        return jsonify({"error": f"Unknown index {index}"}), 400

    try:
        signal_dt = datetime.fromisoformat(signal_time_str)
    except ValueError as e:
        return jsonify({"error": f"Invalid signal_time format: {e}"}), 400

    try:
        candles = fetch_candles_after(index, signal_dt)
        outcome = evaluate_outcome(direction, entry, sl, target_1, candles)
        return jsonify({
            "index":    index,
            "outcome":  outcome,
            "candles_checked": len(candles),
        })
    except Exception as e:
        logger.exception(f"/api/prices error: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# CPR levels
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/cprs", methods=["GET"])
def cprs():
    """Return today's CPR levels for NIFTY and BANKNIFTY."""
    from config import INSTRUMENTS
    result = {}
    for index in INSTRUMENTS:
        try:
            result[index] = _fetch_cpr(index)
        except Exception as e:
            result[index] = {"error": str(e)}
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", FLASK_PORT))
    app.run(host="0.0.0.0", port=port, debug=FLASK_DEBUG)
