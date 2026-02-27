# models.py — 5 Intraday Quantitative Models

"""
Each model receives a fully-populated DataFrame with all indicator columns
pre-computed, plus the CPR dict, and the current candle index (last row).

Returns: {"direction": "BUY"|"SELL"|"NEUTRAL", "score": 0-5, "reason": str}
"""

import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _last(df: pd.DataFrame, col: str, offset: int = 0):
    idx = len(df) - 1 - offset
    if idx < 0:
        return None
    return df[col].iloc[idx]


def _is_bullish(df: pd.DataFrame, offset: int = 0) -> bool:
    return _last(df, "close", offset) > _last(df, "open", offset)


def _candle_body(df: pd.DataFrame, offset: int = 0) -> float:
    return abs(_last(df, "close", offset) - _last(df, "open", offset))


def _candle_range(df: pd.DataFrame, offset: int = 0) -> float:
    return _last(df, "high", offset) - _last(df, "low", offset)


# ─────────────────────────────────────────────────────────────────────────────
# Model 1 — Opening Range Breakout (ORB)
# ─────────────────────────────────────────────────────────────────────────────

def model_orb(df: pd.DataFrame) -> dict:
    """
    First 15-min candle of the day sets OR_high / OR_low.
    BUY  if current close > OR_high AND volume > 1.5x avg_volume
    SELL if current close < OR_low  AND volume > 1.5x avg_volume
    Score 0-5 based on: volume ratio, candle size, gap direction alignment.
    """
    try:
        if len(df) < 2:
            return {"direction": "NEUTRAL", "score": 0, "reason": "ORB: insufficient data"}

        or_high = df["or_high"].iloc[-1]
        or_low  = df["or_low"].iloc[-1]
        if pd.isna(or_high) or pd.isna(or_low):
            return {"direction": "NEUTRAL", "score": 0, "reason": "ORB: OR not yet set"}

        cur_close  = _last(df, "close")
        cur_vol    = _last(df, "volume")
        avg_vol    = _last(df, "avg_vol")
        vol_ratio  = cur_vol / avg_vol if avg_vol and avg_vol > 0 else 0
        gap_up     = df["open"].iloc[0] > df["close"].iloc[0]  # first candle gap approx

        direction = "NEUTRAL"
        score     = 0
        reason    = "ORB: no breakout"

        if cur_close > or_high and vol_ratio >= 1.5:
            direction = "BUY"
            score = 2
            # +1 volume bonus
            if vol_ratio >= 2.0:
                score += 1
            # +1 candle size bonus
            if _candle_body(df) > 0.3 * _candle_range(df) if _candle_range(df) else False:
                score += 1
            # +1 gap alignment
            if gap_up:
                score += 1
            score = min(score, 5)
            reason = f"ORB BUY: close {cur_close:.0f} > OR_high {or_high:.0f}, vol_ratio {vol_ratio:.1f}x"

        elif cur_close < or_low and vol_ratio >= 1.5:
            direction = "SELL"
            score = 2
            if vol_ratio >= 2.0:
                score += 1
            if _candle_body(df) > 0.3 * _candle_range(df) if _candle_range(df) else False:
                score += 1
            if not gap_up:
                score += 1
            score = min(score, 5)
            reason = f"ORB SELL: close {cur_close:.0f} < OR_low {or_low:.0f}, vol_ratio {vol_ratio:.1f}x"

        return {"direction": direction, "score": score, "reason": reason}

    except Exception as e:
        logger.exception(f"Model ORB error: {e}")
        return {"direction": "NEUTRAL", "score": 0, "reason": f"ORB error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# Model 2 — VWAP Reversion & Trend
# ─────────────────────────────────────────────────────────────────────────────

def model_vwap(df: pd.DataFrame) -> dict:
    """
    BUY:  prev close < VWAP AND RSI < 40 AND current close > VWAP
    SELL: prev close > VWAP AND RSI > 60 AND current close < VWAP
    Also validates VWAP trend filter internally.
    Score 0-5.
    """
    try:
        if len(df) < 3:
            return {"direction": "NEUTRAL", "score": 0, "reason": "VWAP: insufficient data"}

        cur_close  = _last(df, "close")
        prev_close = _last(df, "close",  1)
        cur_vwap   = _last(df, "vwap")
        prev_vwap  = _last(df, "vwap",  1)
        cur_rsi    = _last(df, "rsi14")

        if any(v is None or (isinstance(v, float) and np.isnan(v))
               for v in [cur_close, prev_close, cur_vwap, prev_vwap, cur_rsi]):
            return {"direction": "NEUTRAL", "score": 0, "reason": "VWAP: NaN values"}

        direction = "NEUTRAL"
        score     = 0
        reason    = "VWAP: no signal"

        if prev_close < prev_vwap and cur_rsi < 40 and cur_close > cur_vwap:
            direction = "BUY"
            score     = 3
            # Extra confirmation: RSI strongly oversold
            if cur_rsi < 30:
                score += 1
            # Bullish candle
            if _is_bullish(df):
                score += 1
            score  = min(score, 5)
            reason = f"VWAP BUY reversion: RSI {cur_rsi:.1f}, close {cur_close:.0f} > VWAP {cur_vwap:.0f}"

        elif prev_close > prev_vwap and cur_rsi > 60 and cur_close < cur_vwap:
            direction = "SELL"
            score     = 3
            if cur_rsi > 70:
                score += 1
            if not _is_bullish(df):
                score += 1
            score  = min(score, 5)
            reason = f"VWAP SELL reversion: RSI {cur_rsi:.1f}, close {cur_close:.0f} < VWAP {cur_vwap:.0f}"

        return {"direction": direction, "score": score, "reason": reason}

    except Exception as e:
        logger.exception(f"Model VWAP error: {e}")
        return {"direction": "NEUTRAL", "score": 0, "reason": f"VWAP error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# Model 3 — EMA Trend Alignment
# ─────────────────────────────────────────────────────────────────────────────

def model_ema_trend(df: pd.DataFrame) -> dict:
    """
    BUY:  EMA9 > EMA21 > EMA50, price > all, prev candle touched EMA21, bounce
    SELL: EMA9 < EMA21 < EMA50, price < all, prev candle touched EMA21, rejection
    Score 0-5 based on EMA separation and candle strength.
    """
    try:
        if len(df) < 51:
            return {"direction": "NEUTRAL", "score": 0, "reason": "EMA: warming up (<51 candles)"}

        cur_close  = _last(df, "close")
        ema9       = _last(df, "ema9")
        ema21      = _last(df, "ema21")
        ema50      = _last(df, "ema50")
        prev_low   = _last(df, "low",  1)
        prev_high  = _last(df, "high", 1)

        if any(v is None or np.isnan(v) for v in [cur_close, ema9, ema21, ema50]):
            return {"direction": "NEUTRAL", "score": 0, "reason": "EMA: NaN"}

        # EMA separation (normalised) — wider = stronger trend
        sep_pct = abs(ema9 - ema50) / ema50 * 100 if ema50 else 0

        direction = "NEUTRAL"
        score     = 0
        reason    = "EMA: no alignment"

        if ema9 > ema21 > ema50 and cur_close > ema9:
            # Bullish alignment — check pullback to EMA21 and bounce
            if prev_low <= ema21 * 1.001:   # previous candle touched EMA21
                direction = "BUY"
                score = 3
                if sep_pct > 0.3:
                    score += 1
                if _is_bullish(df):
                    score += 1
                score  = min(score, 5)
                reason = f"EMA BUY: EMA9>21>50 pullback bounce, sep {sep_pct:.2f}%"

        elif ema9 < ema21 < ema50 and cur_close < ema9:
            # Bearish alignment — check pullback to EMA21 and rejection
            if prev_high >= ema21 * 0.999:
                direction = "SELL"
                score = 3
                if sep_pct > 0.3:
                    score += 1
                if not _is_bullish(df):
                    score += 1
                score  = min(score, 5)
                reason = f"EMA SELL: EMA9<21<50 pullback rejection, sep {sep_pct:.2f}%"

        return {"direction": direction, "score": score, "reason": reason}

    except Exception as e:
        logger.exception(f"Model EMA error: {e}")
        return {"direction": "NEUTRAL", "score": 0, "reason": f"EMA error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# Model 4 — Momentum & RSI Confirmation
# ─────────────────────────────────────────────────────────────────────────────

def model_momentum(df: pd.DataFrame) -> dict:
    """
    BUY:  RSI 45-65 rising, MACD hist positive & accelerating, higher highs
    SELL: RSI 35-55 falling, MACD hist negative & accelerating down, lower lows
    Score 0-5.
    """
    try:
        if len(df) < 5:
            return {"direction": "NEUTRAL", "score": 0, "reason": "MACD: insufficient data"}

        cur_rsi    = _last(df, "rsi14")
        prev_rsi   = _last(df, "rsi14", 1)
        cur_hist   = _last(df, "macd_hist")
        prev_hist  = _last(df, "macd_hist", 1)
        pp_hist    = _last(df, "macd_hist", 2)
        cur_high   = _last(df, "high")
        prev_high  = _last(df, "high", 1)
        cur_low    = _last(df, "low")
        prev_low   = _last(df, "low",  1)

        if any(v is None or (isinstance(v, float) and np.isnan(v))
               for v in [cur_rsi, prev_rsi, cur_hist, prev_hist]):
            return {"direction": "NEUTRAL", "score": 0, "reason": "MACD: NaN"}

        hist_accel_up   = cur_hist > prev_hist > (pp_hist if pp_hist is not None else prev_hist)
        hist_accel_down = cur_hist < prev_hist < (pp_hist if pp_hist is not None else prev_hist)
        rsi_rising  = cur_rsi > prev_rsi
        rsi_falling = cur_rsi < prev_rsi
        higher_high = cur_high > prev_high
        lower_low   = cur_low  < prev_low

        direction = "NEUTRAL"
        score     = 0
        reason    = "MACD/RSI: no signal"

        if 45 <= cur_rsi <= 65 and rsi_rising and cur_hist > 0 and hist_accel_up and higher_high:
            direction = "BUY"
            score = 3
            if cur_rsi > 55:
                score += 1
            if higher_high and cur_high > df["high"].iloc[-4]:   # 3-candle higher high
                score += 1
            score  = min(score, 5)
            reason = f"MOM BUY: RSI {cur_rsi:.1f}↑, MACD hist {cur_hist:.1f}↑↑, HH"

        elif 35 <= cur_rsi <= 55 and rsi_falling and cur_hist < 0 and hist_accel_down and lower_low:
            direction = "SELL"
            score = 3
            if cur_rsi < 45:
                score += 1
            if lower_low and cur_low < df["low"].iloc[-4]:
                score += 1
            score  = min(score, 5)
            reason = f"MOM SELL: RSI {cur_rsi:.1f}↓, MACD hist {cur_hist:.1f}↓↓, LL"

        return {"direction": direction, "score": score, "reason": reason}

    except Exception as e:
        logger.exception(f"Model Momentum error: {e}")
        return {"direction": "NEUTRAL", "score": 0, "reason": f"MACD error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# Model 5 — CPR & Key Levels
# ─────────────────────────────────────────────────────────────────────────────

def model_cpr(df: pd.DataFrame, cpr: dict) -> dict:
    """
    BUY:  price breaks above TC with strong candle; narrow CPR bonus; near S1/R1
    SELL: price breaks below BC with strong candle
    Score 0-5.
    """
    try:
        if not cpr:
            return {"direction": "NEUTRAL", "score": 0, "reason": "CPR: levels not available"}

        cur_close  = _last(df, "close")
        prev_close = _last(df, "close", 1)
        tc = cpr["tc"]
        bc = cpr["bc"]
        r1 = cpr["r1"]
        s1 = cpr["s1"]
        narrow = cpr["cpr_width_pct"] < 0.2

        if any(v is None for v in [cur_close, prev_close]):
            return {"direction": "NEUTRAL", "score": 0, "reason": "CPR: no close data"}

        direction = "NEUTRAL"
        score     = 0
        reason    = "CPR: no breakout"

        if prev_close <= tc and cur_close > tc:
            direction = "BUY"
            score = 3
            if narrow:
                score += 1
            # Near R1 confluence
            if abs(cur_close - r1) / r1 < 0.002:
                score += 1
            score  = min(score, 5)
            reason = (f"CPR BUY: broke TC {tc:.0f}, narrow={narrow}, "
                      f"CPR_width={cpr['cpr_width_pct']:.3f}%")

        elif prev_close >= bc and cur_close < bc:
            direction = "SELL"
            score = 3
            if narrow:
                score += 1
            if abs(cur_close - s1) / s1 < 0.002:
                score += 1
            score  = min(score, 5)
            reason = (f"CPR SELL: broke BC {bc:.0f}, narrow={narrow}, "
                      f"CPR_width={cpr['cpr_width_pct']:.3f}%")

        return {"direction": direction, "score": score, "reason": reason}

    except Exception as e:
        logger.exception(f"Model CPR error: {e}")
        return {"direction": "NEUTRAL", "score": 0, "reason": f"CPR error: {e}"}
