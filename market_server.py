import asyncio
import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from fastapi import WebSocket

from config import logger
from sma200.analytics import Analytics
from sma200.data import get_interday_data, get_intraday_datapoint
from sma200.io import (
    archive_csv,
    get_symbol_csv_path,
    load_interday_data,
    save_interday_data,
)
from sma200.notifications import Notifier
from sma200.utils import (
    EASTERN,
    format_analytics_payload,
    is_consecutive_trading_day,
    is_trading_day,
    market_is_open,
)


class MarketServer:
    """Handles market data updates and WebSocket broadcasting."""

    def __init__(self, symbol: str, config: dict) -> None:
        self.symbol = symbol
        self.data_dir = Path(config["datadir"])
        self.stale_dir = self.data_dir / "stale"
        self.stale_dir.mkdir(parents=True, exist_ok=True)

        self.data: pd.DataFrame | None = None
        self.current_day: date | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._ws_pools: dict[str, set[WebSocket]] = {}

        self.analytics = Analytics(config)
        self.notifier = Notifier(config, symbol)

    # WebSocket Management
    def register_websocket(self, pool_name: str, ws: WebSocket) -> None:
        """Register a WebSocket connection under a named pool."""
        self._ws_pools.setdefault(pool_name, set()).add(ws)

    def unregister_websocket(self, pool_name: str, ws: WebSocket) -> None:
        """Remove a WebSocket from its pool, if present."""
        if pool := self._ws_pools.get(pool_name):
            pool.discard(ws)

    async def push_update(self, pool_name: str, payload: str) -> None:
        """Send a message to all WebSockets in the specified pool."""
        if not (pool := self._ws_pools.get(pool_name)):
            return

        disconnected = {ws for ws in pool if not await self._safe_send(ws, payload)}
        pool -= disconnected

    @staticmethod
    async def _safe_send(ws: WebSocket, payload: str) -> bool:
        """Safely attempt to send a payload to a WebSocket client."""
        try:
            await ws.send_text(payload)
            return True
        except Exception:
            return False

    # Lifecycle Control
    async def startup(self) -> None:
        """Load initial interday market data."""
        logger.info(f"[{self.symbol}] Loading interday data...")
        self.data = load_interday_data(self.symbol, self.data_dir)
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
        self.notifier.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info(f"[{self.symbol}] Server stopped.")

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

    def _get_csv_path(self) -> Path:
        """Return the canonical CSV file path for this symbol."""
        return get_symbol_csv_path(self.data_dir, self.symbol)

    def _last_trading_day_from_data(self) -> date | None:
        """Extract the last trading day present in loaded data."""
        if self.data is None or self.data.empty:
            return None
        return self.data.index[-1].date()

    def _archive_current_csv(self) -> None:
        """Move the current CSV file to the stale archive folder."""
        archive_csv(self.data_dir, self.symbol, self.stale_dir)
        logger.info(f"[{self.symbol}] Archived CSV for symbol")

    async def _check_new_trading_day(self) -> None:
        """Detect the transition to a new trading day and reload interday data."""
        today = datetime.now(EASTERN).date()
        if self.current_day == today or not is_trading_day(today):
            return

        logger.info(f"[{self.symbol}] New trading day: {today}")
        is_consecutive = not self.current_day or is_consecutive_trading_day(
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
        if self.data is None:
            logger.warning(f"[{self.symbol}] Data not loaded; skipping update")
            return

        today = pd.Timestamp.now(tz=EASTERN).date()
        prev_day_data = self.data[self.data.index.date < today]

        ohlcv_row, timestamp = get_intraday_datapoint(self.symbol, prev_day_data)
        if ohlcv_row is None or ohlcv_row.empty or not timestamp:
            return

        timestamp = pd.Timestamp(timestamp).tz_convert(EASTERN)
        trading_day_ts = pd.Timestamp(timestamp.date())
        logger.info(f"[{self.symbol}] Intraday: {timestamp} → {ohlcv_row['Adj Close']}")

        self._update_intraday_data(trading_day_ts, ohlcv_row)
        self._persist_data_to_csv()

        await self._broadcast_intraday_update(pool_name, timestamp, ohlcv_row)
        await self._broadcast_analytics_updates(timestamp)

    def _update_intraday_data(self, day_ts: pd.Timestamp, ohlcv_row: pd.Series) -> None:
        """Insert or update the intraday OHLCV row in memory."""
        new_row = pd.DataFrame(
            [ohlcv_row.values], index=[day_ts], columns=ohlcv_row.index
        )
        if day_ts in self.data.index:
            self.data.loc[day_ts] = ohlcv_row.values
        else:
            self.data = pd.concat([self.data, new_row])
        self.data.sort_index(inplace=True)

    def _persist_data_to_csv(self) -> None:
        """Save current data state to CSV."""
        save_interday_data(self.data, self.symbol, self.data_dir)

    # Broadcasting
    async def _broadcast_intraday_update(
        self, pool_name: str, timestamp: pd.Timestamp, ohlcv_row: pd.Series
    ) -> None:
        """Broadcast the live intraday update to subscribers."""
        payload = json.dumps(
            {
                "symbol": self.symbol,
                "timestamp": timestamp.isoformat(),
                "ohlcv": ohlcv_row.to_dict(),
            }
        )
        await self.push_update(pool_name, payload)

    async def _broadcast_analytics_updates(self, timestamp: pd.Timestamp) -> None:
        for pool_name in self._ws_pools:
            if not pool_name.startswith("analytics-"):
                continue

            strategy = pool_name.removeprefix("analytics-")
            if not self.analytics.exists(strategy):
                continue

            try:
                result, notification = self.analytics.execute(
                    strategy, self.data, self.symbol, True
                )

                if notification:
                    registered = self.notifier.register(notification)
                    if registered:
                        logger.info(
                            f"[{self.symbol}] Notification sent: {notification}"
                        )
                    else:
                        logger.debug(
                            f"[{self.symbol}] Notification suppressed (cooldown): {notification}"
                        )
            except Exception as exc:
                logger.exception(
                    f"[{self.symbol}] Error in '{strategy}' analytics: {exc}"
                )
                continue

            payload = json.dumps(
                format_analytics_payload(
                    self.symbol, strategy, result
                ),
                default=str,
            )
            await self.push_update(pool_name, payload)
