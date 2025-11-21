from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from sma200.notifications import Notification
from sma200.utils import UTC


class Analytics:
    def __init__(self, config: dict):
        self.config = config
        self._registry = {"sma": self.sma_with_threshold}
        # self._registry = {"dummy": self.dummy_strategy}

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

    # def dummy_strategy(
    #     self, df: pd.DataFrame, symbol: str
    # ) -> Tuple[dict, Optional[Notification]]:
    #     random_label = random.choice(["A", "B"])
    #     # Create notification with random label
    #     message = f"Dummy strategy notification for {symbol} with label {random_label}"
    #     notification = Notification(
    #         strategy="dummy_strategy",
    #         label=random_label,
    #         timestamp=datetime.now(UTC),
    #         message=message,
    #     )

    #     result = {
    #         "type": "dummy_strategy",
    #         "params": {},
    #         "time_series": {
    #             "dates": [],
    #             "prices": [],
    #             "signal": [],
    #         },
    #     }

    #     return result, notification

    def sma_with_threshold(
        self, df: pd.DataFrame, symbol: str
    ) -> Tuple[dict, Optional[Notification]]:
        params = self.config.get("sma", {})
        window = params["window"]
        upper_threshold = params["upper_threshold"]
        lower_threshold = params["lower_threshold"]

        # Compute SMA with pandas
        sma = df["Adj Close"].rolling(window=window).mean()
        valid_mask = sma.notna()
        df_valid = df.loc[valid_mask].copy()
        df_valid["sma"] = sma[valid_mask]
        df_valid["upper_band"] = df_valid["sma"] * upper_threshold
        df_valid["lower_band"] = df_valid["sma"] * lower_threshold

        prices = df_valid["Adj Close"].tolist()
        sma_values = df_valid["sma"].tolist()
        upper_band = df_valid["upper_band"].tolist()
        lower_band = df_valid["lower_band"].tolist()
        dates = [d.isoformat() for d in df_valid.index]

        # Generate signals
        signals = []
        invested = False
        for price, up, low in zip(prices, upper_band, lower_band):
            if not invested and price >= up:
                signals.append("BUY")
                invested = True
            elif invested and price < low:
                signals.append("SELL")
                invested = False
            else:
                signals.append("HOLD")

        # Notifications (same logic)
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
                "prices": prices,
                "sma": sma_values,
                "upper_band": upper_band,
                "lower_band": lower_band,
                "signal": signals,
            },
        }

        return result, notification
