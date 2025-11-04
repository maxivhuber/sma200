import json
import logging
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("market_server.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)
