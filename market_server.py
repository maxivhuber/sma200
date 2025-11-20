import asyncio
import json
import shutil
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from fastapi import WebSocket

from config import logger
from sma200.data import get_interday_data, get_intraday_datapoint
from sma200.utils import EASTERN, is_trading_day, market_is_open


class MarketServer:
    """Handles market data updates and WebSocket broadcasting."""

    def __init__(self, symbol: str, data_dir: Path) -> None:
        self.symbol = symbol
        self.data_dir = Path(data_dir)
        self.stale_dir = self.data_dir / "stale"
        self.stale_dir.mkdir(parents=True, exist_ok=True)

        self.data: pd.DataFrame | None = None
        self.current_day: date | None = None

        self._running = False
        self._task: asyncio.Task | None = None
        self._ws_pools: dict[str, set[WebSocket]] = {}

    def register_websocket(self, pool_name: str, ws: WebSocket) -> None:
        """Register a WebSocket connection under a named pool."""
        self._ws_pools.setdefault(pool_name, set()).add(ws)

    def unregister_websocket(self, pool_name: str, ws: WebSocket) -> None:
        """Remove a WebSocket from its pool, if present."""
        pool = self._ws_pools.get(pool_name)
        if pool:
            pool.discard(ws)

    async def push_update(self, pool_name: str, payload: str) -> None:
        """Send a message to all WebSockets in the specified pool."""
        pool = self._ws_pools.get(pool_name)
        if not pool:
            return

        disconnected = {ws for ws in pool if not await self._safe_send(ws, payload)}
        pool -= disconnected

    async def _safe_send(self, ws: WebSocket, payload: str) -> bool:
        """Safely attempt to send a payload to a WebSocket client."""
        try:
            await ws.send_text(payload)
            return True
        except Exception:
            return False

    async def startup(self) -> None:
        """Load initial interday market data."""
        logger.info(f"[{self.symbol}] Loading interday data...")
        self.data = get_interday_data(self.symbol, self.data_dir)
        self.current_day = self._last_trading_day_from_data()
        logger.info(f"[{self.symbol}] Startup complete. Last day: {self.current_day}")

    def start(self) -> None:
        """Start the background market data update loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"[{self.symbol}] Server started.")

    async def stop(self) -> None:
        """Stop the background update loop and cleanup."""
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

    def _get_csv_path(self) -> Path:
        """Return the canonical CSV file path for this symbol."""
        safe_symbol = self.symbol.replace("^", "")
        return self.data_dir / f"{safe_symbol}.csv"

    def _last_trading_day_from_data(self) -> date | None:
        """Extract the last trading day present in loaded data."""
        if self.data is None or self.data.empty:
            return None
        return self.data.index[-1].date()

    def _archive_current_csv(self) -> None:
        """Move the current CSV file to the stale archive folder."""
        csv_file = self._get_csv_path()
        if not csv_file.exists():
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = self.stale_dir / f"{csv_file.stem}_{timestamp}.csv"
        shutil.move(csv_file, destination)
        logger.info(f"[{self.symbol}] Archived: {destination}")

    def _is_consecutive_trading_day(self, prev_day: date, current_day: date) -> bool:
        """Check if the current day immediately follows the previous trading day."""
        nyse = mcal.get_calendar("NYSE")
        valid_days = nyse.valid_days(start_date=prev_day, end_date=current_day)
        valid_days = pd.DatetimeIndex(valid_days.normalize())

        prev_ts, curr_ts = pd.Timestamp(prev_day), pd.Timestamp(current_day)
        if prev_ts not in valid_days or curr_ts not in valid_days:
            return False

        try:
            next_day = valid_days[valid_days.get_loc(prev_ts) + 1].date()
        except (IndexError, KeyError):
            return False
        return next_day == current_day

    async def _check_new_trading_day(self) -> None:
        """Detect the transition to a new trading day and reload interday data."""
        today = datetime.now(EASTERN).date()
        if self.current_day == today or not is_trading_day(today):
            return

        logger.info(f"[{self.symbol}] New trading day: {today}")

        is_consecutive = not self.current_day or self._is_consecutive_trading_day(
            self.current_day, today
        )
        if not is_consecutive:
            logger.warning(f"[{self.symbol}] Gap detected — refreshing data")
            self._archive_current_csv()

        self.data = get_interday_data(self.symbol, self.data_dir)
        self.current_day = today

    async def _fetch_intraday_update(self, pool_name: str = "live") -> None:
        """Fetch one new minute-level datapoint, update memory + disk, and broadcast."""
        if not market_is_open():
            return

        today = pd.Timestamp.now(tz=EASTERN).date()
        prev_day_data = self.data[self.data.index.date < today]

        ohlcv_row, timestamp = get_intraday_datapoint(self.symbol, prev_day_data)
        if ohlcv_row is None or timestamp is None:
            return

        timestamp = pd.Timestamp(timestamp).tz_convert(EASTERN)
        trading_day = timestamp.date()
        day_ts = pd.Timestamp(trading_day)
        close_value = ohlcv_row["Close"]

        logger.info(f"[{self.symbol}] Intraday: {timestamp} → {close_value}")

        if self.data is None:
            logger.warning(f"[{self.symbol}] Data not loaded; skipping update")
            return

        # Create or update the day's data row
        new_row = pd.DataFrame(
            [ohlcv_row.values], index=[day_ts], columns=ohlcv_row.index
        )
        if day_ts in self.data.index:
            self.data.loc[day_ts] = ohlcv_row.values
        else:
            logger.info(f"[{self.symbol}] Adding new row for {trading_day}")
            self.data = pd.concat([self.data, new_row])

        self.data.sort_index(inplace=True)
        self.data.to_csv(self._get_csv_path())

        # Broadcast update via WebSocket
        payload = json.dumps(
            {
                "symbol": self.symbol,
                "timestamp": timestamp.isoformat(),
                "ohlcv": ohlcv_row.to_dict(),
            }
        )
        await self.push_update(pool_name, payload)

    async def _run_loop(self) -> None:
        """Main event loop: check day transitions and push intraday updates."""
        await self.startup()
        logger.info(f"[{self.symbol}] Entering main loop...")

        while self._running:
            try:
                await self._check_new_trading_day()
                await self._fetch_intraday_update()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception(f"[{self.symbol}] Error in loop: {exc}")
                await asyncio.sleep(60)
