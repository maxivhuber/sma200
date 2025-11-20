from __future__ import annotations

from config import config
from market_server import MarketServer


class MarketManager:
    """Manages initialization, retrieval, and shutdown of MarketServer instances."""

    def __init__(self) -> None:
        self._symbols_to_preload: list[str] = config.get("symbols", [])
        self._servers: dict[str, MarketServer] = {}

    async def _create_server(self, symbol: str) -> MarketServer:
        """Create and start a MarketServer for the given symbol."""
        server = MarketServer(symbol, config)
        server.start()
        self._servers[symbol] = server
        return server

    async def initialize_all_servers(self) -> None:
        """Initialize all configured servers at startup."""
        for symbol in self._symbols_to_preload:
            if symbol not in self._servers:
                await self._create_server(symbol)

    async def get_server(self, symbol: str) -> MarketServer:
        """Return the MarketServer associated with the given symbol."""
        return self._servers[symbol]

    async def stop_all(self) -> None:
        """Stop all active servers cleanly."""
        for server in self._servers.values():
            await server.stop()


manager = MarketManager()
