from pathlib import Path

from config import config
from market_server import MarketServer


class MarketManager:
    def __init__(self):
        self.data_dir = Path(config["datadir"])
        self.servers: dict[str, MarketServer] = {}

    async def get_server(self, symbol: str) -> MarketServer:
        if symbol not in self.servers:
            server = MarketServer(symbol, self.data_dir)
            server.start()
            self.servers[symbol] = server
        return self.servers[symbol]

    async def stop_all(self):
        for server in self.servers.values():
            await server.stop()


manager = MarketManager()
