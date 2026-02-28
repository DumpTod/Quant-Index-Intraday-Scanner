# data_fetcher.py — Fyers API candle data using direct HTTP (not SDK)
# Uses same auth pattern confirmed working in /api/debug

import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests
import pandas as pd

from config import (
    FYERS_CLIENT_ID, INSTRUMENTS,
    RESOLUTION_15M, RESOLUTION_DAY,
)
from token_manager import get_access_token

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

FYERS_HISTORY_URL = "https://api-t1.fyers.in/api/v3/history"


def _headers() -> dict:
    """Build auth headers — same pattern that works in /api/debug."""
    tok = get_access_token()
    return {
        "Authorization": f"{FYERS_CLIENT_ID}:{tok}",
        "Content-Type":  "application/json",
    }


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def fetch_15min_candles(index: str, from_date: date = None, to_date: date = None) -> pd.DataFrame:
    """
    Fetch 15-minute OHLCV candles for *index* (e.g. 'NIFTY').
    Defaults to today's candles.
    Returns DataFrame: datetime, open, high, low, close, volume
    """
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = from_date or today
    to_d   = to_date   or today

    range_from = _epoch(datetime(from_d.year, from_d.month, from_d.day, 9, 0,  tzinfo=IST))
    range_to   = _epoch(datetime(to_d.year,   to_d.month,   to_d.day,  15, 30, tzinfo=IST))

    params = {
        "symbol":      symbol,
        "resolution":  RESOLUTION_15M,
        "date_format": "1",
        "range_from":  str(range_from),
        "range_to":    str(range_to),
        "cont_flag":   "1",
    }

    resp = requests.get(FYERS_HISTORY_URL, headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("s") != "ok":
        raise RuntimeError(f"Fyers history error for {index}: {data}")

    candles = data.get("candles", [])
    if not candles:
        logger.warning(f"{index}: no candles returned")
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
    df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
    df = df[df["datetime"].dt.date == today].reset_index(drop=True)

    logger.info(f"Fetched {len(df)} 15-min candles for {index}")
    return df


def fetch_daily_candles(index: str, n_days: int = 5) -> pd.DataFrame:
    """
    Fetch last *n_days* daily candles — used for CPR calculation.
    Returns DataFrame: date, open, high, low, close, volume
    """
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = today - timedelta(days=n_days * 2)  # buffer for weekends

    range_from = _epoch(datetime(from_d.year, from_d.month, from_d.day, 0,  0,  tzinfo=IST))
    range_to   = _epoch(datetime(today.year,  today.month,  today.day,  23, 59, tzinfo=IST))

    params = {
        "symbol":      symbol,
        "resolution":  RESOLUTION_DAY,
        "date_format": "1",
        "range_from":  str(range_from),
        "range_to":    str(range_to),
        "cont_flag":   "1",
    }

    resp = requests.get(FYERS_HISTORY_URL, headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("s") != "ok":
        raise RuntimeError(f"Fyers daily history error for {index}: {data}")

    candles = data.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date
    df = df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    df = df[df["date"] < today]  # exclude today — need previous day for CPR
    return df.tail(n_days).reset_index(drop=True)


def fetch_candles_after(index: str, signal_dt: datetime, to_date: date = None) -> pd.DataFrame:
    """Fetch 15-min candles after a signal datetime — used by history rescan."""
    from_d = signal_dt.date()
    to_d   = to_date or from_d
    df = fetch_15min_candles(index, from_date=from_d, to_date=to_d)
    if df.empty:
        return df
    return df[df["datetime"] > signal_dt].reset_index(drop=True)
