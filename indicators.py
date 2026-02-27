# indicators.py — All indicators from scratch (no ta-lib, no pandas_ta)
# Uses only pandas and numpy.

import numpy as np
import pandas as pd


# ── EMA ────────────────────────────────────────────────────────────────────────

def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average — uses Wilder's method (adjust=False)."""
    return series.ewm(span=period, adjust=False).mean()


# ── RSI ────────────────────────────────────────────────────────────────────────

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ── MACD ───────────────────────────────────────────────────────────────────────

def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal_period: int = 9):
    """
    Returns (macd_line, signal_line, histogram) as pd.Series each.
    """
    fast_ema   = ema(series, fast)
    slow_ema   = ema(series, slow)
    macd_line  = fast_ema - slow_ema
    signal_ln  = ema(macd_line, signal_period)
    histogram  = macd_line - signal_ln
    return macd_line, signal_ln, histogram


# ── VWAP ───────────────────────────────────────────────────────────────────────

def vwap(df: pd.DataFrame) -> pd.Series:
    """
    Intraday VWAP — resets each day.
    df must have columns: datetime, high, low, close, volume
    Returns a Series aligned to df index.
    """
    df = df.copy()
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"]        = df["typical_price"] * df["volume"]

    result = pd.Series(index=df.index, dtype=float)
    for day, group in df.groupby(df["datetime"].dt.date):
        cum_tp_vol = group["tp_vol"].cumsum()
        cum_vol    = group["volume"].cumsum()
        vwap_vals  = cum_tp_vol / cum_vol.replace(0, np.nan)
        result.loc[group.index] = vwap_vals.values
    return result


# ── ATR ────────────────────────────────────────────────────────────────────────

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


# ── OBV ────────────────────────────────────────────────────────────────────────

def obv(df: pd.DataFrame) -> pd.Series:
    """On-Balance Volume."""
    direction = np.sign(df["close"].diff()).fillna(0)
    return (direction * df["volume"]).cumsum()


# ── CPR ────────────────────────────────────────────────────────────────────────

def cpr_levels(prev_high: float, prev_low: float, prev_close: float) -> dict:
    """
    Central Pivot Range levels from previous day's OHLC.
    Returns dict: pivot, tc, bc, r1, r2, s1, s2, cpr_width_pct
    """
    pivot = (prev_high + prev_low + prev_close) / 3
    tc    = (pivot + prev_high) / 2
    bc    = (pivot + prev_low)  / 2
    r1    = 2 * pivot - prev_low
    r2    = pivot + (prev_high - prev_low)
    s1    = 2 * pivot - prev_high
    s2    = pivot - (prev_high - prev_low)
    cpr_width_pct = abs(tc - bc) / pivot * 100 if pivot else 0

    return {
        "pivot":         round(pivot, 2),
        "tc":            round(tc,    2),
        "bc":            round(bc,    2),
        "r1":            round(r1,    2),
        "r2":            round(r2,    2),
        "s1":            round(s1,    2),
        "s2":            round(s2,    2),
        "cpr_width_pct": round(cpr_width_pct, 4),
    }


# ── Rolling average volume ─────────────────────────────────────────────────────

def avg_volume(series: pd.Series, period: int = 20) -> pd.Series:
    """Simple rolling mean of volume."""
    return series.rolling(window=period, min_periods=1).mean()
