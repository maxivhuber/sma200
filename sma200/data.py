from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# Import the logger from the config file
from config import logger


def get_interday_data(symbol: str, data_dir: Path):
    """Download or load interday stock data, ensuring cache is clean."""
    data_dir.mkdir(exist_ok=True, parents=True)
    csv_file = data_dir / f"{symbol.replace('^', '')}.csv"

    if csv_file.exists():
        try:
            cached_df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            if not cached_df.empty:
                last_trading_day = pd.bdate_range(end=pd.Timestamp.today(), periods=2)[
                    0
                ].date()
                if cached_df.index[-1].date() >= last_trading_day:
                    logger.info(f"Loading fresh data from {csv_file}")
                    return cached_df
        except Exception:
            logger.warning(
                f"Cache file {csv_file} is corrupt or empty. Will re-download."
            )

    logger.info(f"Downloading data for {symbol}")
    try:
        df = yf.download(
            symbol, period="max", interval="1d", progress=False, auto_adjust=False
        )
        if df.empty:
            logger.warning(f"No data for {symbol}")
            return pd.DataFrame()

        # If multi-index columns, flatten to row-based multiindex
        if isinstance(df.columns, pd.MultiIndex):
            df = df.stack(level="Ticker", future_stack=True)
            df.index.names = ["Date", "Symbol"]
            df = df[["Open", "High", "Low", "Close", "Volume"]]
            df = df.swaplevel(0, 1)
            df = df.sort_index()
            df = df.xs(symbol)
        else:
            # Normal single-symbol structure
            df.index.name = "Date"
            df = df[["Open", "High", "Low", "Close", "Volume"]]

        # Now safe to reindex by Date
        full_range = pd.date_range(df.index.min(), df.index.max(), freq="D")
        df = df.reindex(full_range).ffill().bfill()
        df.index.name = "Date"

        # Save cleaned version
        df.to_csv(csv_file)
        return df

    except Exception as e:
        logger.error(f"Failed to download data for {symbol}: {e}")
        if csv_file.exists():
            logger.info("Falling back to old cached data")
            return pd.read_csv(csv_file, index_col=0, parse_dates=True)
        return pd.DataFrame()


def get_intraday_datapoint(symbol: str):
    """Fetch the most recent completed intraday minute datapoint.

    Returns:
        (ohlcv_row: pd.Series, timestamp: pd.Timestamp)
    """
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    # Go two minutes back to ensure the latest datapoint is complete
    start = now_utc - timedelta(minutes=2)
    end = now_utc - timedelta(minutes=1)

    try:
        df = yf.download(
            symbol,
            start=start,
            end=end,
            interval="1m",
            progress=False,
            auto_adjust=False,
        )

        if df.empty:
            logger.debug(f"No intraday data for {symbol} in the last minute.")
            return None, None

        # Handle column MultiIndex (if multiple tickers are returned)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.stack(level="Ticker", future_stack=True)
            df.index.names = ["Datetime", "Symbol"]
            df = df[["Open", "High", "Low", "Close", "Volume"]]
            df = df.swaplevel(0, 1).sort_index()
            df = df.xs(symbol)
        else:
            df.index.name = "Datetime"
            df = df[["Open", "High", "Low", "Close", "Volume"]]

        # Take latest (chronologically last) complete row
        latest_row = df.iloc[-1]  # pd.Series with OHLCV
        timestamp = df.index[-1]  # datetime of this datapoint

        return latest_row, timestamp

    except Exception as e:
        logger.error(f"Failed to get intraday data for {symbol}: {e}")
        return None, None
