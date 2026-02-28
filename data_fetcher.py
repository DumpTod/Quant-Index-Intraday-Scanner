# data_fetcher.py — Fixed per official Fyers API docs
# date_format: int (0=epoch, 1=yyyy-mm-dd)
# cont_flag:   int (1 for FNO continuous data)

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
    access_token = get_access_token()
    # Fyers SDK token format: "client_id:access_token"
    return fyersModel.FyersModel(
        client_id = FYERS_CLIENT_ID,
        token     = f"{FYERS_CLIENT_ID}:{access_token}",
        log_path  = "",
        is_async  = False,
    )


def fetch_15min_candles(index: str, from_date: date = None, to_date: date = None) -> pd.DataFrame:
    fyers  = _get_fyers()
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = from_date or today
    to_d   = to_date   or today

    data = {
        "symbol":      symbol,
        "resolution":  "15",
        "date_format": 1,                          # int, not string
        "range_from":  from_d.strftime("%Y-%m-%d"),
        "range_to":    to_d.strftime("%Y-%m-%d"),
        "cont_flag":   1,                          # int, not string
    }

    logger.info(f"Fetching 15m candles for {index}: {data}")
    resp = fyers.history(data=data)
    logger.info(f"Fyers response: {resp}")

    if resp.get("s") != "ok":
        raise RuntimeError(f"Fyers history error for {index}: {resp}")

    candles = resp.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
    df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
    df = df[df["datetime"].dt.date == today].reset_index(drop=True)
    logger.info(f"Got {len(df)} candles for {index}")
    return df


def fetch_daily_candles(index: str, n_days: int = 5) -> pd.DataFrame:
    fyers  = _get_fyers()
    symbol = INSTRUMENTS[index]
    today  = date.today()
    from_d = today - timedelta(days=n_days * 2)

    data = {
        "symbol":      symbol,
        "resolution":  "D",
        "date_format": 1,
        "range_from":  from_d.strftime("%Y-%m-%d"),
        "range_to":    today.strftime("%Y-%m-%d"),
        "cont_flag":   1,
    }

    resp = fyers.history(data=data)
    logger.info(f"Fyers daily response for {index}: {resp}")

    if resp.get("s") != "ok":
        raise RuntimeError(f"Fyers daily history error for {index}: {resp}")

    candles = resp.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date
    df = df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    df = df[df["date"] < today]
    return df.tail(n_days).reset_index(drop=True)


def fetch_candles_after(index: str, signal_dt: datetime, to_date: date = None) -> pd.DataFrame:
    from_d = signal_dt.date()
    to_d   = to_date or from_d
    df = fetch_15min_candles(index, from_date=from_d, to_date=to_d)
    if df.empty:
        return df
    return df[df["datetime"] > signal_dt].reset_index(drop=True)
