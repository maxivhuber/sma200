from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

EASTERN = ZoneInfo("America/New_York")
NYSE = mcal.get_calendar("NYSE")


def is_trading_day(day: date) -> bool:
    schedule = NYSE.schedule(start_date=day, end_date=day)
    return not schedule.empty


def get_market_schedule(day: date):
    schedule = NYSE.schedule(start_date=day, end_date=day)
    if schedule.empty:
        return None, None
    open_dt = schedule.iloc[0]["market_open"].tz_convert(EASTERN)
    close_dt = schedule.iloc[0]["market_close"].tz_convert(EASTERN)
    return open_dt, close_dt


def market_is_open() -> bool:
    now = datetime.now(EASTERN)
    if not is_trading_day(now.date()):
        return False
    open_dt, close_dt = get_market_schedule(now.date())
    return open_dt <= now <= close_dt


def is_consecutive_trading_day(prev_day: date, current_day: date) -> bool:
    if prev_day >= current_day:
        return False
    valid_days = NYSE.valid_days(start_date=prev_day, end_date=current_day).normalize()
    valid_days = pd.DatetimeIndex(valid_days)

    prev_ts, curr_ts = pd.Timestamp(prev_day), pd.Timestamp(current_day)
    if prev_ts not in valid_days or curr_ts not in valid_days:
        return False

    try:
        next_day = valid_days[valid_days.get_loc(prev_ts) + 1].date()
        return next_day == current_day
    except (IndexError, KeyError):
        return False


def sanitize_symbol(symbol: str) -> str:
    """Remove unsafe characters (e.g., '^') for filesystem use."""
    return symbol.replace("^", "")
