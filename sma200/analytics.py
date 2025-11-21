import numpy as np
import pandas as pd


class Analytics:
    """Registry for analytics strategies."""

    def __init__(self, config: dict):
        self.config = config
        # map strategy names -> callables
        self._registry = {"sma": self.sma_with_threshold}

    def register(self, name: str, fn) -> None:
        """Register a strategy function."""
        self._registry[name] = fn

    def get(self, name: str):
        """Get a strategy by name, raises KeyError if not found."""
        try:
            return self._registry[name]
        except KeyError:
            raise KeyError(f"Strategy '{name}' not configured or loaded")

    def exists(self, name: str) -> bool:
        """Check if a strategy exists."""
        return name in self._registry

    def names(self) -> list[str]:
        """List registered strategy names."""
        return list(self._registry)

    def execute(self, name: str, df):
        """Execute a registered strategy."""
        return self.get(name)(df)

    def sma_with_threshold(self, df: pd.DataFrame) -> dict:
        """Compute an SMA-based trading signal with dynamic upper/lower thresholds."""

        params = self.config.get("sma", {})
        window = params["window"]
        upper_threshold = params["upper_threshold"]
        lower_threshold = params["lower_threshold"]

        sma = df["Adj Close"].rolling(window=window).mean()
        valid_mask = sma.notna()

        prices = df.loc[valid_mask, "Adj Close"].to_numpy()
        sma_values = sma[valid_mask].to_numpy()
        dates = sma.index[valid_mask].to_numpy()

        upper_band = sma_values * upper_threshold
        lower_band = sma_values * lower_threshold

        signals = np.full(sma_values.shape, "HOLD", dtype=object)
        invested = False
        for i, price in enumerate(prices):
            if not invested and price >= upper_band[i]:
                signals[i] = "BUY"
                invested = True
            elif invested and price < lower_band[i]:
                signals[i] = "SELL"
                invested = False
            # Otherwise: "HOLD"

        return {
            "type": "sma_with_threshold",
            "params": params,
            "time_series": {
                "dates": dates,
                "sma": sma_values,
                "upper_band": upper_band,
                "lower_band": lower_band,
                "signal": signals,
            },
        }
