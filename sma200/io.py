import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from .utils import EASTERN, sanitize_symbol


def get_symbol_csv_path(data_dir: Path, symbol: str) -> Path:
    """Return canonical CSV path for a symbol."""
    return data_dir / f"{sanitize_symbol(symbol)}.csv"


def load_interday_data(symbol: str, data_dir: Path) -> pd.DataFrame | None:
    """Load interday OHLCV data from CSV."""
    csv_path = get_symbol_csv_path(data_dir, symbol)
    if not csv_path.exists():
        return None
    df = pd.read_csv(
        csv_path,
        index_col=0,
        parse_dates=True,
        dtype={
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "float64",
        },
    )
    df.index = df.index.tz_localize("UTC").tz_convert(EASTERN)
    return df


def save_interday_data(data: pd.DataFrame, symbol: str, data_dir: Path) -> None:
    """Persist interday data to CSV."""
    csv_path = get_symbol_csv_path(data_dir, symbol)
    data.to_csv(csv_path)


def archive_csv(data_dir: Path, symbol: str, stale_dir: Path) -> None:
    """Move current CSV to stale directory with timestamp."""
    csv_path = get_symbol_csv_path(data_dir, symbol)
    if not csv_path.exists():
        return

    stale_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = stale_dir / f"{csv_path.stem}_{timestamp}.csv"
    shutil.move(csv_path, dest)
