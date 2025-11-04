import asyncio
import json
import shutil
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal

from config import logger
from sma200.data import get_interday_data, get_intraday_datapoint
from sma200.utils import EASTERN, is_trading_day, market_is_open


class MarketServer:
    def __init__(self, symbol: str, data_dir: Path):
        self.symbol = symbol
        self.data_dir = Path(data_dir)
        self.stale_dir = self.data_dir / "stale"
        self.stale_dir.mkdir(exist_ok=True, parents=True)

        self.data: pd.DataFrame | None = None
        self.current_day: date | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._ws_subscribers: set = set()  # WebSocket clients

    def _get_csv_path(self) -> Path:
        safe_symbol = self.symbol.replace("^", "")
        return self.data_dir / f"{safe_symbol}.csv"

    async def startup(self):
        logger.info(f"[{self.symbol}] Loading interday data...")
        self.data = get_interday_data(self.symbol, self.data_dir)
        self.current_day = self._last_trading_day_from_data()
        logger.info(f"[{self.symbol}] Startup complete. Last day: {self.current_day}")

    def _last_trading_day_from_data(self) -> date | None:
        return (
            self.data.index[-1].date()
            if self.data is not None and not self.data.empty
            else None
        )

    def _archive_current_csv(self):
        csv_file = self._get_csv_path()
        if csv_file.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = self.stale_dir / f"{csv_file.stem}_{timestamp}.csv"
            shutil.move(csv_file, dest)
            logger.info(f"[{self.symbol}] Archived: {dest}")

    def _is_consecutive_trading_day(self, prev_day: date, current_day: date) -> bool:
        nyse = mcal.get_calendar("NYSE")
        valid_days = nyse.valid_days(start_date=prev_day, end_date=current_day)
        valid_days = pd.DatetimeIndex(valid_days.normalize())
        prev_ts, curr_ts = pd.Timestamp(prev_day), pd.Timestamp(current_day)
        if prev_ts not in valid_days or curr_ts not in valid_days:
            return False
        try:
            next_day = valid_days[valid_days.get_loc(prev_ts) + 1].date()
            return next_day == current_day
        except (IndexError, KeyError):
            return False

    async def _check_new_trading_day(self):
        now = datetime.now(EASTERN).date()
        if self.current_day != now and is_trading_day(now):
            logger.info(f"[{self.symbol}] New trading day: {now}")
            if self.current_day and not self._is_consecutive_trading_day(
                self.current_day, now
            ):
                logger.warning(f"[{self.symbol}] Gap detected — refreshing data")
                self._archive_current_csv()
                self.data = get_interday_data(self.symbol, self.data_dir)
            else:
                self.data = get_interday_data(self.symbol, self.data_dir)
            self.current_day = now

    async def _fetch_intraday_update(self):
        if not market_is_open():
            return
        close_value, timestamp = get_intraday_datapoint(self.symbol)
        if close_value is None or timestamp is None:
            return

        timestamp = pd.Timestamp(timestamp).tz_convert(EASTERN)
        trading_day = timestamp.date()
        day_ts = pd.Timestamp(trading_day)

        logger.info(f"[{self.symbol}] Intraday: {timestamp} → {close_value}")

        if self.data is None:
            logger.warning(f"[{self.symbol}] Data not loaded; skipping update")
            return

        if day_ts in self.data.index:
            self.data.loc[day_ts] = close_value
        else:
            logger.info(f"[{self.symbol}] Adding new row for {trading_day}")
            self.data.loc[day_ts] = close_value

        self.data = self.data.sort_index()
        self._get_csv_path().write_text(self.data.to_csv())

        # Push to WebSocket subscribers
        payload = json.dumps(
            {
                "symbol": self.symbol,
                "price": close_value,
                "timestamp": timestamp.isoformat(),
                "trading_day": trading_day.isoformat(),
            }
        )
        disconnected = set()
        for ws in self._ws_subscribers:
            try:
                await ws.send_text(payload)
            except Exception:
                disconnected.add(ws)
        self._ws_subscribers -= disconnected

    async def _run_loop(self):
        await self.startup()
        logger.info(f"[{self.symbol}] Entering main loop...")
        while self._running:
            try:
                await self._check_new_trading_day()
                await self._fetch_intraday_update()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[{self.symbol}] Error in loop: {e}")
                await asyncio.sleep(60)

    def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"[{self.symbol}] Server started.")

    async def stop(self):
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"[{self.symbol}] Server stopped.")
