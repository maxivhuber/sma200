from pathlib import Path

from config import config
from market_server import MarketServer


class MarketManager:
    def __init__(self) -> None:
        self.data_dir: Path = Path(config["datadir"])
        self.symbols_to_preload: list[str] = config.get("symbols", [])
        self.servers: dict[str, MarketServer] = {}

    async def initialize_all_servers(self) -> None:
        """Initialize all servers at startup"""
        for symbol in self.symbols_to_preload:
            if symbol not in self.servers:
                server = MarketServer(symbol, self.data_dir)
                server.start()
                self.servers[symbol] = server

    async def get_server(self, symbol: str) -> MarketServer:
        return self.servers[symbol]

    async def stop_all(self) -> None:
        for server in self.servers.values():
            await server.stop()


manager = MarketManager()
