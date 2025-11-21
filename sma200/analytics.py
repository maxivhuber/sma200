from datetime import datetime
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from sma200.notifications import Notification
from sma200.utils import UTC


class Analytics:
    def __init__(self, config: dict):
        self.config = config
        self._registry = {"sma": self.sma_with_threshold}

    def register(self, name: str, fn) -> None:
        self._registry[name] = fn

    def get(self, name: str):
        try:
            return self._registry[name]
        except KeyError:
            raise KeyError(f"Strategy '{name}' not configured or loaded")

    def exists(self, name: str) -> bool:
        return name in self._registry

    def names(self) -> list[str]:
        return list(self._registry)

    def execute(self, name: str, df, symbol: str):
        return self.get(name)(df, symbol)

    def sma_with_threshold(
        self, df: pd.DataFrame, symbol: str
    ) -> Tuple[dict, Optional[Notification]]:
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
            # otherwise HOLD
        # Notification
        last_signal = signals[-1]
        last_price = float(prices[-1])
        last_sma = float(sma_values[-1])
        last_upper = float(upper_band[-1])
        last_lower = float(lower_band[-1])
        notification: Optional[Notification] = None
        if last_signal == "BUY":
            message = (
                f"The price of {symbol} has reached {last_price:.2f}, "
                f"which is above its buy target of {last_upper:.2f}. "
                f"If the price remains above {last_sma:.2f} + threshold "
                f"until market close, please place a buy order."
            )
            notification = Notification(
                strategy="sma_with_threshold",
                label="BUY",
                timestamp=datetime.now(UTC),
                message=message,
            )
        elif last_signal == "SELL":
            message = (
                f"The price of {symbol} has reached {last_price:.2f}, "
                f"which is below its sell target of {last_lower:.2f}. "
                f"If the price remains below {last_sma:.2f} - threshold "
                f"until market close, please sell all your holdings."
            )
            notification = Notification(
                strategy="sma_with_threshold",
                label="SELL",
                timestamp=datetime.now(UTC),
                message=message,
            )
        result = {
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
        return result, notification
