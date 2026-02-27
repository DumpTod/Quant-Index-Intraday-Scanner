# risk_manager.py — SL, Target, R:R calculation + Options strike suggestion

import math
import logging
from config import SL_PCT, TARGET_RR_MIN, TARGET_RR_IDEAL

logger = logging.getLogger(__name__)

# Options lot sizes
LOT_SIZES = {
    "NIFTY":     25,
    "BANKNIFTY": 15,
}

# Strike intervals
STRIKE_INTERVALS = {
    "NIFTY":     50,
    "BANKNIFTY": 100,
}


def _round_to_strike(price: float, interval: int) -> int:
    """Round price to nearest strike interval."""
    return int(round(price / interval) * interval)


def _atm_strike(ltp: float, interval: int) -> int:
    return _round_to_strike(ltp, interval)


def _itm_strike_ce(ltp: float, interval: int, steps: int = 1) -> int:
    """ITM Call = strike below LTP by *steps* intervals."""
    atm = _atm_strike(ltp, interval)
    return atm - interval * steps


def _itm_strike_pe(ltp: float, interval: int, steps: int = 1) -> int:
    """ITM Put = strike above LTP by *steps* intervals."""
    atm = _atm_strike(ltp, interval)
    return atm + interval * steps


def calculate_risk(
    index: str,
    direction: str,
    entry: float,
    or_level: float = None,
) -> dict:
    """
    Calculate SL, targets, and R:R for a given signal.

    SL logic:
      - Primary: 0.4% of entry price
      - If ORB level is tighter and in correct direction, use ORB SL

    Returns dict with: entry, sl, target_1, target_2, rr_1, rr_2, risk_pts, lot_size
    """
    pct_sl_pts = entry * SL_PCT

    if direction == "BUY":
        pct_sl    = entry - pct_sl_pts
        orb_sl    = (or_level - entry * 0.001) if or_level and or_level < entry else None
        # Use ORB SL only if it's tighter (higher) than % SL
        sl = max(pct_sl, orb_sl) if orb_sl else pct_sl
        sl = max(sl, 0)
    else:  # SELL
        pct_sl    = entry + pct_sl_pts
        orb_sl    = (or_level + entry * 0.001) if or_level and or_level > entry else None
        sl = min(pct_sl, orb_sl) if orb_sl else pct_sl

    risk_pts = abs(entry - sl)
    if risk_pts == 0:
        risk_pts = pct_sl_pts  # fallback

    target_1 = entry + risk_pts * TARGET_RR_MIN  if direction == "BUY" else entry - risk_pts * TARGET_RR_MIN
    target_2 = entry + risk_pts * TARGET_RR_IDEAL if direction == "BUY" else entry - risk_pts * TARGET_RR_IDEAL

    rr_1 = round(abs(target_1 - entry) / risk_pts, 2)
    rr_2 = round(abs(target_2 - entry) / risk_pts, 2)

    return {
        "entry":    round(entry,    2),
        "sl":       round(sl,       2),
        "target_1": round(target_1, 2),
        "target_2": round(target_2, 2),
        "rr_1":     rr_1,
        "rr_2":     rr_2,
        "risk_pts": round(risk_pts, 2),
        "lot_size": LOT_SIZES.get(index, 25),
    }


def suggest_options(index: str, direction: str, ltp: float) -> dict:
    """
    Suggest ATM and 1-strike ITM options based on signal direction.
    Returns dict with: atm_strike, itm_strike, option_type, expiry_note
    """
    interval = STRIKE_INTERVALS.get(index, 50)
    atm      = _atm_strike(ltp, interval)

    if direction == "BUY":
        opt_type = "CE"
        itm      = _itm_strike_ce(ltp, interval, steps=1)
    else:  # SELL
        opt_type = "PE"
        itm      = _itm_strike_pe(ltp, interval, steps=1)

    return {
        "option_type":  opt_type,
        "atm_strike":   atm,
        "itm_strike":   itm,
        "atm_symbol":   f"NSE:{index}26MAR{atm}{opt_type}",   # update monthly
        "itm_symbol":   f"NSE:{index}26MAR{itm}{opt_type}",
        "expiry_note":  "26 MAR 2026 — update symbol monthly",
        "lot_size":     LOT_SIZES.get(index, 25),
    }


def evaluate_outcome(
    direction: str,
    entry: float,
    sl: float,
    target_1: float,
    candles_after: "pd.DataFrame",  # noqa: F821 — avoid circular import
) -> dict:
    """
    Given candles AFTER the signal, check if entry was met, then SL or target.
    Returns: {entry_met, entry_met_time, outcome, exit_price, pnl_pct}
    """
    result = {
        "entry_met":       False,
        "entry_met_time":  None,
        "outcome":         "PENDING",
        "exit_price":      None,
        "pnl_pct":         None,
    }

    if candles_after is None or len(candles_after) == 0:
        return result

    for _, row in candles_after.iterrows():
        if not result["entry_met"]:
            # Entry assumed met if high >= entry (BUY) or low <= entry (SELL)
            if direction == "BUY"  and row["high"] >= entry:
                result["entry_met"]      = True
                result["entry_met_time"] = str(row["datetime"])
            elif direction == "SELL" and row["low"] <= entry:
                result["entry_met"]      = True
                result["entry_met_time"] = str(row["datetime"])
            continue   # Skip SL/target check on entry candle

        if result["entry_met"]:
            if direction == "BUY":
                if row["low"] <= sl:
                    result["outcome"]    = "SL_HIT"
                    result["exit_price"] = sl
                    break
                if row["high"] >= target_1:
                    result["outcome"]    = "TARGET_HIT"
                    result["exit_price"] = target_1
                    break
            else:
                if row["high"] >= sl:
                    result["outcome"]    = "SL_HIT"
                    result["exit_price"] = sl
                    break
                if row["low"] <= target_1:
                    result["outcome"]    = "TARGET_HIT"
                    result["exit_price"] = target_1
                    break

    if result["entry_met"] and result["outcome"] == "PENDING":
        result["outcome"] = "WATCHING"

    if result["exit_price"] and result["entry_met"]:
        if direction == "BUY":
            pnl = (result["exit_price"] - entry) / entry * 100
        else:
            pnl = (entry - result["exit_price"]) / entry * 100
        result["pnl_pct"] = round(pnl, 3)

    return result
