import asyncio
import shutil
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal

from config import config, logger
from sma200.data import get_interday_data, get_intraday_datapoint
from sma200.utils import EASTERN, is_trading_day, market_is_open

# Setup symbol, dirs
SYMBOL = "^GSPC"
DATA_DIR = Path(config["datadir"])
STALE_DIR = DATA_DIR / "stale"
STALE_DIR.mkdir(exist_ok=True, parents=True)


class MarketServer:
    def __init__(self, symbol: str, data_dir: Path):
        self.symbol = symbol
        self.data_dir = data_dir
        self.data: pd.DataFrame | None = None
        self.current_day: date | None = None

    def _get_csv_path(self) -> Path:
        return self.data_dir / f"{self.symbol.replace('^', '')}.csv"

    async def startup(self):
        """Load the historical dataset on startup."""
        logger.info("Server startup: loading interday data...")
        self.data = get_interday_data(self.symbol, self.data_dir)
        self.current_day = self._last_trading_day_from_data()
        logger.info(f"Startup complete. Last trading day in data: {self.current_day}")

    def _last_trading_day_from_data(self) -> date:
        return self.data.index[-1].date() if not self.data.empty else None

    def _archive_current_csv(self):
        """Move current CSV into a 'stale' folder with timestamp."""
        csv_file = self._get_csv_path()
        if csv_file.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = STALE_DIR / f"{csv_file.stem}_{timestamp}.csv"
            shutil.move(csv_file, dest)
            logger.info(f"Archived stale CSV: {dest}")

    async def _check_new_trading_day(self):
        """Detect and handle change of trading day."""
        now = datetime.now(EASTERN).date()
        if now != self.current_day and is_trading_day(now):
            logger.info(f"Detected new trading day: {now}")
            # verify continuity
            if self.current_day and not self._is_consecutive_trading_day(
                self.current_day, now
            ):
                logger.warning(
                    "Gap detected between trading days — refreshing dataset..."
                )
                self._archive_current_csv()
                self.data = get_interday_data(self.symbol, self.data_dir)
            else:
                logger.info("Fetching new daily data ...")
                self.data = get_interday_data(self.symbol, self.data_dir)
            self.current_day = now

    def _is_consecutive_trading_day(self, prev_day: date, current_day: date) -> bool:
        nyse = mcal.get_calendar("NYSE")
        # Get all valid trading days AFTER prev_day up to and including current_day
        valid_days = nyse.valid_days(start_date=prev_day, end_date=current_day)
        # Ensure both are trading days and they are adjacent in the trading calendar
        if (pd.Timestamp(prev_day) not in valid_days) or (
            pd.Timestamp(current_day) not in valid_days
        ):
            return False
        prev_index = valid_days.get_loc(pd.Timestamp(prev_day))
        try:
            next_trading_day = valid_days[prev_index + 1].date()
        except IndexError:
            return False
        return next_trading_day == current_day

    async def _fetch_intraday_update(self):
        """Fetch and update the latest intraday datapoint."""
        if not market_is_open():
            return
        close_value, timestamp = get_intraday_datapoint(self.symbol)
        if close_value is None:
            return
        timestamp = pd.Timestamp(timestamp).tz_convert(EASTERN)
        trading_day = timestamp.date()
        logger.info(f"Intraday update: {timestamp} -> {close_value}")
        day_ts = pd.Timestamp(trading_day)
        # if a daily row exists, replace it; if not, create it
        if day_ts in self.data.index:
            self.data.loc[day_ts] = close_value
        else:
            logger.info(f"Adding new daily row for {trading_day}")
            self.data.loc[day_ts] = close_value
        self.data = self.data.sort_index()
        self._get_csv_path().write_text(self.data.to_csv())

    async def run(self):
        """Main event loop."""
        await self.startup()
        logger.info("Entering main loop. Checking every minute ...")
        while True:
            await self._check_new_trading_day()
            await self._fetch_intraday_update()
            await asyncio.sleep(60)


async def main():
    server = MarketServer(SYMBOL, DATA_DIR)
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
