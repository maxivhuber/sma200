from datetime import date, datetime

import pandas_market_calendars as mcal
import pytz

EASTERN = pytz.timezone("US/Eastern")
NYSE = mcal.get_calendar("NYSE")


def is_trading_day(day: date) -> bool:
    """Return True if the date is a valid NYSE trading day."""
    schedule = NYSE.schedule(start_date=day, end_date=day)
    return not schedule.empty


def get_market_schedule(day: date):
    """Return the open and close datetimes in US/Eastern for a trading day."""
    schedule = NYSE.schedule(start_date=day, end_date=day)
    if schedule.empty:
        return None, None
    open_dt = schedule.iloc[0]["market_open"].tz_convert(EASTERN)
    close_dt = schedule.iloc[0]["market_close"].tz_convert(EASTERN)
    return open_dt, close_dt


def market_is_open() -> bool:
    """Return True if the market is open *right now* (in US/Eastern)."""
    now = datetime.now(EASTERN)
    if not is_trading_day(now.date()):
        return False
    open_dt, close_dt = get_market_schedule(now.date())
    return open_dt <= now <= close_dt
