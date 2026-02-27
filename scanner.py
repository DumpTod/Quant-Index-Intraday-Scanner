# scanner.py — Core scan logic, indicator preparation, signal generation

import logging
import threading
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np

from config import (
    INSTRUMENTS, MIN_MODELS_AGREE, DEAD_ZONE_MIN_SCORE,
    GRADE_A_HIGH_SCORE, GRADE_A_MED_SCORE, ALL_AGREE_BONUS,
    SIGNAL_START, SIGNAL_END, DEAD_ZONE_START, DEAD_ZONE_END,
)
from data_fetcher import fetch_15min_candles, fetch_daily_candles
from indicators import ema, rsi, macd, vwap, atr, avg_volume, cpr_levels
from models import model_orb, model_vwap, model_ema_trend, model_momentum, model_cpr
from risk_manager import calculate_risk, suggest_options

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

# ── Global scan state (polled by /api/results) ─────────────────────────────────
scan_state = {
    "running":    False,
    "progress":   0,
    "message":    "Idle",
    "signals":    [],
    "cprs":       {},
    "timestamp":  None,
    "error":      None,
}
_scan_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_time(t_str: str) -> dtime:
    h, m = map(int, t_str.split(":"))
    return dtime(h, m)


def _ist_now() -> datetime:
    return datetime.now(tz=IST)


def _in_signal_window(now: datetime) -> bool:
    t = now.time()
    start = _parse_time(SIGNAL_START)
    end   = _parse_time(SIGNAL_END)
    return start <= t <= end


def _in_dead_zone(now: datetime) -> bool:
    t = now.time()
    dz_start = _parse_time(DEAD_ZONE_START)
    dz_end   = _parse_time(DEAD_ZONE_END)
    return dz_start <= t <= dz_end


# ─────────────────────────────────────────────────────────────────────────────
# Indicator enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add all indicator columns to the OHLCV DataFrame."""
    df = df.copy()

    # EMAs
    df["ema9"]  = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)

    # RSI
    df["rsi14"] = rsi(df["close"], 14)

    # MACD
    df["macd_line"], df["macd_signal"], df["macd_hist"] = macd(df["close"])

    # VWAP (intraday, resets each day)
    df["vwap"] = vwap(df)

    # Average volume (20-period rolling)
    df["avg_vol"] = avg_volume(df["volume"], 20)

    # ATR
    df["atr14"] = atr(df, 14)

    # Opening Range: first candle of each day
    df["or_high"] = np.nan
    df["or_low"]  = np.nan
    for day, group in df.groupby(df["datetime"].dt.date):
        if len(group) < 1:
            continue
        first_idx = group.index[0]
        or_h = group["high"].iloc[0]
        or_l = group["low"].iloc[0]
        # Propagate OR to all candles of that day
        df.loc[group.index, "or_high"] = or_h
        df.loc[group.index, "or_low"]  = or_l

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Grade logic
# ─────────────────────────────────────────────────────────────────────────────

def _grade(total_score: int, agreeing: int) -> str:
    if total_score >= GRADE_A_HIGH_SCORE and agreeing >= 4:
        return "A+ HIGH"
    if total_score >= GRADE_A_MED_SCORE and agreeing >= MIN_MODELS_AGREE:
        return "A+ MEDIUM"
    return "WATCH"


# ─────────────────────────────────────────────────────────────────────────────
# Single-index scan
# ─────────────────────────────────────────────────────────────────────────────

def scan_index(index: str, cpr: dict = None) -> dict | None:
    """
    Run all 5 models for *index* and return a signal dict or None.
    """
    logger.info(f"Scanning {index}...")

    # ── Fetch data
    df = fetch_15min_candles(index)
    if df.empty or len(df) < 5:
        logger.warning(f"{index}: insufficient candles ({len(df)})")
        return None

    # ── Enrich
    df = _enrich(df)

    # ── Current candle context
    now       = _ist_now()
    cur_close = df["close"].iloc[-1]
    cur_time  = df["datetime"].iloc[-1]

    # ── Time gate
    if not _in_signal_window(now):
        logger.info(f"{index}: outside signal window")
        return None

    # ── Run models
    m1 = model_orb(df)
    m2 = model_vwap(df)
    m3 = model_ema_trend(df)
    m4 = model_momentum(df)
    m5 = model_cpr(df, cpr)

    results = [m1, m2, m3, m4, m5]
    model_names = ["ORB", "VWAP", "EMA", "MACD", "CPR"]

    # ── Tally votes
    buy_models  = [m for m in results if m["direction"] == "BUY"]
    sell_models = [m for m in results if m["direction"] == "SELL"]

    if len(buy_models) >= len(sell_models) and len(buy_models) >= MIN_MODELS_AGREE:
        direction = "BUY"
        agreeing  = len(buy_models)
        scores    = [m["score"] for m in buy_models]
    elif len(sell_models) > len(buy_models) and len(sell_models) >= MIN_MODELS_AGREE:
        direction = "SELL"
        agreeing  = len(sell_models)
        scores    = [m["score"] for m in sell_models]
    else:
        logger.info(f"{index}: no consensus (BUY={len(buy_models)}, SELL={len(sell_models)})")
        return None

    # ── Total score
    all_scores  = [m["score"] for m in results]
    total_score = sum(all_scores)
    if agreeing == 5:
        total_score += ALL_AGREE_BONUS

    # ── Dead zone gate
    if _in_dead_zone(now) and total_score < DEAD_ZONE_MIN_SCORE:
        logger.info(f"{index}: dead zone, score {total_score} < {DEAD_ZONE_MIN_SCORE}")
        return None

    # ── Grade
    grade = _grade(total_score, agreeing)
    if grade == "WATCH":
        logger.info(f"{index}: score {total_score} below grade thresholds")
        return None

    # ── VWAP trend filter for BUY — only buy when price > VWAP
    if direction == "BUY":
        cur_vwap = df["vwap"].iloc[-1]
        if not pd.isna(cur_vwap) and cur_close < cur_vwap:
            logger.info(f"{index}: BUY filtered out — price below VWAP")
            return None

    # ── Risk management
    or_low  = df["or_low"].iloc[-1]
    or_high = df["or_high"].iloc[-1]
    orb_level = or_low if direction == "BUY" else or_high
    risk = calculate_risk(index, direction, cur_close, or_level=orb_level)

    # ── Options suggestion
    options = suggest_options(index, direction, cur_close)

    # ── Build model info list
    model_info = []
    for i, m in enumerate(results):
        model_info.append({
            "name":      model_names[i],
            "direction": m["direction"],
            "score":     m["score"],
            "reason":    m["reason"],
            "active":    m["direction"] == direction,
        })

    signal = {
        "index":        index,
        "direction":    direction,
        "grade":        grade,
        "total_score":  total_score,
        "agreeing":     agreeing,
        "models":       model_info,
        "entry":        risk["entry"],
        "sl":           risk["sl"],
        "target_1":     risk["target_1"],
        "target_2":     risk["target_2"],
        "rr_1":         risk["rr_1"],
        "rr_2":         risk["rr_2"],
        "risk_pts":     risk["risk_pts"],
        "lot_size":     risk["lot_size"],
        "options":      options,
        "signal_time":  cur_time.strftime("%H:%M"),
        "timestamp":    now.isoformat(),
        "cpr":          cpr,
    }

    logger.info(f"✅ Signal: {index} {direction} {grade} score={total_score}")
    return signal


# ─────────────────────────────────────────────────────────────────────────────
# Full scan (both indices) — runs in background thread
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_cpr(index: str) -> dict:
    try:
        daily = fetch_daily_candles(index, n_days=5)
        if daily.empty:
            return {}
        prev = daily.iloc[-1]
        return cpr_levels(prev["high"], prev["low"], prev["close"])
    except Exception as e:
        logger.error(f"CPR fetch error for {index}: {e}")
        return {}


def run_scan_background():
    with _scan_lock:
        if scan_state["running"]:
            logger.info("Scan already running — skipping.")
            return

    def _scan():
        with _scan_lock:
            scan_state["running"]   = True
            scan_state["progress"]  = 0
            scan_state["message"]   = "Starting scan..."
            scan_state["error"]     = None
            scan_state["signals"]   = []

        try:
            cprs = {}
            signals = []

            for i, index in enumerate(INSTRUMENTS.keys()):
                with _scan_lock:
                    scan_state["progress"] = int((i / len(INSTRUMENTS)) * 80)
                    scan_state["message"]  = f"Fetching CPR for {index}..."

                cpr = _fetch_cpr(index)
                cprs[index] = cpr

                with _scan_lock:
                    scan_state["message"] = f"Scanning {index}..."

                try:
                    sig = scan_index(index, cpr=cpr)
                    if sig:
                        signals.append(sig)
                except Exception as e:
                    logger.error(f"Error scanning {index}: {e}")

            with _scan_lock:
                scan_state["signals"]   = signals
                scan_state["cprs"]      = cprs
                scan_state["progress"]  = 100
                scan_state["message"]   = f"Scan complete. {len(signals)} signal(s) found."
                scan_state["timestamp"] = datetime.now(tz=IST).isoformat()

        except Exception as e:
            logger.exception(f"Scan error: {e}")
            with _scan_lock:
                scan_state["error"]   = str(e)
                scan_state["message"] = f"Scan failed: {e}"
        finally:
            with _scan_lock:
                scan_state["running"] = False

    t = threading.Thread(target=_scan, daemon=True)
    t.start()
    return t


# ─────────────────────────────────────────────────────────────────────────────
# Debug scan — full pipeline detail for one index
# ─────────────────────────────────────────────────────────────────────────────

def debug_scan_index(index: str) -> dict:
    """Run scan for one index and return full debug payload."""
    try:
        cpr = _fetch_cpr(index)
        df  = fetch_15min_candles(index)

        if df.empty:
            return {"error": f"No candles returned for {index}"}

        df_enriched = _enrich(df)

        # Run models
        m1 = model_orb(df_enriched)
        m2 = model_vwap(df_enriched)
        m3 = model_ema_trend(df_enriched)
        m4 = model_momentum(df_enriched)
        m5 = model_cpr(df_enriched, cpr)

        last = df_enriched.iloc[-1]

        return {
            "index":         index,
            "candle_count":  len(df),
            "last_candle": {
                "time":      str(last["datetime"]),
                "open":      last["open"],
                "high":      last["high"],
                "low":       last["low"],
                "close":     last["close"],
                "volume":    int(last["volume"]),
                "ema9":      round(last["ema9"],  2),
                "ema21":     round(last["ema21"], 2),
                "ema50":     round(last["ema50"], 2),
                "rsi14":     round(last["rsi14"], 2),
                "macd_hist": round(last["macd_hist"], 2),
                "vwap":      round(last["vwap"],  2),
                "or_high":   round(last["or_high"], 2),
                "or_low":    round(last["or_low"],  2),
            },
            "cpr":    cpr,
            "models": {
                "ORB":  m1,
                "VWAP": m2,
                "EMA":  m3,
                "MACD": m4,
                "CPR":  m5,
            },
        }
    except Exception as e:
        logger.exception(f"Debug scan error for {index}: {e}")
        return {"error": str(e)}
