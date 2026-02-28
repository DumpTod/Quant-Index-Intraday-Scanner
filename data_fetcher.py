# data_fetcher.py — Fyers API candle data (corrected per official docs)
# date_format=1 → pass dates as "YYYY-MM-DD" strings (easier, correct)
# date_format=0 → pass dates as epoch integers

import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
from fyers_apiv3 import fyersModel

from config import FYERS_CLIENT_ID, INSTRUMENTS, RESOLUTION_15M, RESOLUTION_DAY
from token_manager import get_access_token

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _get_fyers() -> fyersModel.FyersModel:
    """Build authenticated FyersModel — token: client_id:access_token"""
    access_token = get_access_token()
    token = f"{FYERS_CLIENT_ID}:{access_token}"
    return fyersModel.FyersModel(
        client_id=FYERS_CLIENT_ID,
        token=token,
        log_path="",
        is_async=False,
    )


def fetch_15min_candles(index: str, from_date: date = None, to_date: date = None) -> pd.DataFrame:
    """
    Fetch 15-min OHLCV candles for index. Defaults to today.
    date_format=1 means we pass YYYY-MM-DD strings (per Fyers docs).
    """
    fyers  = _get_fyers()
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = from_date or today
    to_d   = to_date   or today

    data = {
        "symbol":      symbol,
        "resolution":  RESOLUTION_15M,
        "date_format": "1",                        # 1 = YYYY-MM-DD string format
        "range_from":  from_d.strftime("%Y-%m-%d"),
        "range_to":    to_d.strftime("%Y-%m-%d"),
        "cont_flag":   "1",                        # required for FNO
    }

    resp = fyers.history(data=data)
    logger.info(f"Fyers 15m response for {index}: s={resp.get('s')}, candles={len(resp.get('candles', []))}")

    if resp.get("s") != "ok":
        raise RuntimeError(f"Fyers history error for {index}: {resp}")

    candles = resp.get("candles", [])
    if not candles:
        logger.warning(f"{index}: no 15m candles returned")
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    # ts is epoch — convert to IST datetime
    df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
    df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
    # Keep only today's candles
    df = df[df["datetime"].dt.date == today].reset_index(drop=True)

    logger.info(f"Fetched {len(df)} 15-min candles for {index}")
    return df


def fetch_daily_candles(index: str, n_days: int = 5) -> pd.DataFrame:
    """
    Fetch last n_days completed daily candles for CPR calculation.
    Excludes today — CPR needs previous day's OHLC.
    """
    fyers  = _get_fyers()
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = today - timedelta(days=n_days * 2)   # buffer for weekends/holidays

    data = {
        "symbol":      symbol,
        "resolution":  RESOLUTION_DAY,
        "date_format": "1",
        "range_from":  from_d.strftime("%Y-%m-%d"),
        "range_to":    today.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    }

    resp = fyers.history(data=data)
    logger.info(f"Fyers daily response for {index}: s={resp.get('s')}, candles={len(resp.get('candles', []))}")

    if resp.get("s") != "ok":
        raise RuntimeError(f"Fyers daily history error for {index}: {resp}")

    candles = resp.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date
    df = df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    # Exclude today — need only completed days for CPR
    df = df[df["date"] < today]
    return df.tail(n_days).reset_index(drop=True)


def fetch_candles_after(index: str, signal_dt: datetime, to_date: date = None) -> pd.DataFrame:
    """Fetch 15-min candles after signal_dt — for history rescan outcome check."""
    from_d = signal_dt.date()
    to_d   = to_date or from_d
    df = fetch_15min_candles(index, from_date=from_d, to_date=to_d)
    if df.empty:
        return df
    return df[df["datetime"] > signal_dt].reset_index(drop=True)
