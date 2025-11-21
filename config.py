import json
import logging
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config" / "config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# Configure logging to match Gunicorn/Uvicorn format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(process)d] [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %z",
    handlers=[
        logging.FileHandler("market_server.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)
