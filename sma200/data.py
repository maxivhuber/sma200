from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from pandas.errors import EmptyDataError

# Import the logger from the config file
from config import logger


def get_interday_data(symbol: str, data_dir: Path):
    """Download or load interday stock data, ensuring cache is clean.
    Args:
        symbol: Stock ticker symbol (e.g., '^GSPC').
        data_dir: Directory to cache the data.
    Returns:
        A DataFrame with continuous calendar dates and filled missing values.
    """
    data_dir.mkdir(exist_ok=True, parents=True)
    csv_file = data_dir / f"{symbol.replace('^', '')}.csv"

    # Try to load from cache if it exists
    if csv_file.exists():
        try:
            cached_df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            if not cached_df.empty:
                # Get the last full trading day (previous business day)
                last_trading_day = pd.bdate_range(end=pd.Timestamp.today(), periods=2)[
                    0
                ].date()
                # If cache is up-to-date, return it directly. It's already clean.
                if cached_df.index[-1].date() >= last_trading_day:
                    logger.info(f"Loading fresh data from {csv_file}")
                    return cached_df
        except (EmptyDataError, IndexError, pd.errors.ParserError):
            logger.warning(
                f"Cache file {csv_file} is corrupt or empty. Will re-download."
            )

    # If cache is missing, stale, or corrupted, download new data
    logger.info(f"Downloading data for {symbol}")
    try:
        df = yf.download(
            symbol, period="max", interval="1d", progress=False, auto_adjust=False
        )
        if df.empty:
            logger.warning(f"No data found for symbol {symbol}.")
            return pd.DataFrame()

        # Standardize multi-level column index if it exists
        if isinstance(df.columns, pd.MultiIndex):
            df = df.droplevel(0, axis=1)

        # Reindex to include all calendar days and fill missing values
        df = (
            df.reindex(pd.date_range(df.index.min(), df.index.max(), freq="D"))
            .ffill()
            .bfill()
        )

        # Save the cleaned data to cache
        df.to_csv(csv_file)
        return df
    except Exception as e:
        logger.error(f"Failed to download data for {symbol}. Error: {e}")
        # As a fallback, try to load the old (possibly stale/dirty) cache file
        if csv_file.exists():
            logger.info("Falling back to old cached data.")
            return pd.read_csv(csv_file, index_col=0, parse_dates=True)
        return pd.DataFrame()


def get_intraday_datapoint(symbol: str):
    """Fetches the most recent completed intraday minute datapoint."""
    # Round current time down to nearest minute
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    # To get the most recent complete minute, we go back 2 minutes to 1 minute
    # This ensures we get data for a minute that has definitely closed
    start = now_utc - timedelta(minutes=2)
    end = now_utc - timedelta(minutes=1)
    df = yf.download(
        symbol,
        start=start,
        end=end,
        interval="1m",
        progress=False,
        auto_adjust=False,
    )
    if df.empty:
        # This is a common, non-critical event, so info is appropriate.
        logger.debug(f"No intraday data for {symbol} in the last minute.")
        return None, None

    # Get the close value and the timestamp of this data point
    close_value = df["Close"].iloc[0].squeeze().item()
    timestamp = df.index[0]  # The datetime of this data point
    return close_value, timestamp
