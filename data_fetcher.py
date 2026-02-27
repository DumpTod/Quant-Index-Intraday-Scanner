# data_fetcher.py — Fyers API candle data for NIFTY & BANKNIFTY

import logging
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
from fyers_apiv3 import fyersModel

from config import (
    FYERS_CLIENT_ID, INSTRUMENTS,
    RESOLUTION_15M, RESOLUTION_DAY,
)
from token_manager import get_access_token

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _get_fyers() -> fyersModel.FyersModel:
    """Build an authenticated FyersModel instance."""
    access_token = get_access_token()
    # Fyers SDK expects "client_id:access_token"
    full_token = f"{FYERS_CLIENT_ID}:{access_token}"
    fy = fyersModel.FyersModel(
        client_id=FYERS_CLIENT_ID,
        token=full_token,
        log_path="",
        is_async=False,
    )
    return fy


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def fetch_15min_candles(index: str, from_date: date = None, to_date: date = None) -> pd.DataFrame:
    """
    Fetch 15-minute OHLCV candles for *index* (e.g. 'NIFTY').
    Defaults to today's candles. Returns DataFrame with columns:
        datetime, open, high, low, close, volume
    """
    fy     = _get_fyers()
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = from_date or today
    to_d   = to_date   or today

    # Fyers uses epoch seconds
    range_from = _epoch(datetime(from_d.year, from_d.month, from_d.day, 9, 0, tzinfo=IST))
    range_to   = _epoch(datetime(to_d.year,   to_d.month,   to_d.day,  15, 30, tzinfo=IST))

    data = {
        "symbol":     symbol,
        "resolution": RESOLUTION_15M,
        "date_format": "1",          # epoch
        "range_from": str(range_from),
        "range_to":   str(range_to),
        "cont_flag":  "1",
    }

    resp = fy.history(data=data)
    if resp.get("s") != "ok":
        raise RuntimeError(f"Fyers history error for {index}: {resp}")

    candles = resp.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
    df = df.drop(columns=["ts"])
    df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
    df = df[df["datetime"].dt.date == today].reset_index(drop=True)

    logger.info(f"Fetched {len(df)} 15-min candles for {index}")
    return df


def fetch_daily_candles(index: str, n_days: int = 5) -> pd.DataFrame:
    """
    Fetch last *n_days* daily candles for *index* — used for CPR calculation.
    Returns DataFrame with columns: date, open, high, low, close, volume
    """
    fy     = _get_fyers()
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = today - timedelta(days=n_days * 2)   # buffer for weekends/holidays

    range_from = _epoch(datetime(from_d.year, from_d.month, from_d.day, 0, 0, tzinfo=IST))
    range_to   = _epoch(datetime(today.year,  today.month,  today.day,  23, 59, tzinfo=IST))

    data = {
        "symbol":     symbol,
        "resolution": RESOLUTION_DAY,
        "date_format": "1",
        "range_from": str(range_from),
        "range_to":   str(range_to),
        "cont_flag":  "1",
    }

    resp = fy.history(data=data)
    if resp.get("s") != "ok":
        raise RuntimeError(f"Fyers daily history error for {index}: {resp}")

    candles = resp.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date
    df = df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    # Return only trading days (exclude today — we need *previous* day for CPR)
    df = df[df["date"] < today]
    return df.tail(n_days).reset_index(drop=True)


def fetch_candles_after(index: str, signal_dt: datetime, to_date: date = None) -> pd.DataFrame:
    """
    Fetch 15-min candles after a signal datetime — used by history rescan.
    """
    from_d = signal_dt.date()
    to_d   = to_date or from_d
    df = fetch_15min_candles(index, from_date=from_d, to_date=to_d)
    if df.empty:
        return df
    return df[df["datetime"] > signal_dt].reset_index(drop=True)
