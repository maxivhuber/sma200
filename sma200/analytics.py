import random
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from sma200.notifications import Notification
from sma200.utils import UTC


class BaseStrategy:
    """Abstract base class for all trading strategies."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        self.cooldowns = self._parse_cooldowns(self.config.get("cooldowns", {}))

    @staticmethod
    def _parse_cooldowns(cooldown_cfg: Dict[str, str]) -> Dict[str, timedelta]:
        """Parse cooldown strings like {'BUY': '2 hours'} into timedeltas."""
        cooldowns: Dict[str, timedelta] = {}
        for label, text in cooldown_cfg.items():
            parts = text.split()
            if len(parts) != 2:
                raise ValueError(f"Invalid cooldown format: {text!r}")

            num_str, unit = parts
            try:
                num = int(num_str)
            except ValueError as exc:
                raise ValueError(f"Invalid number in cooldown: {text!r}") from exc

            if "hour" in unit:
                cooldowns[label] = timedelta(hours=num)
            elif "minute" in unit:
                cooldowns[label] = timedelta(minutes=num)
            elif "day" in unit:
                cooldowns[label] = timedelta(days=num)
            else:
                raise ValueError(f"Unsupported cooldown unit: {unit!r}")

        return cooldowns

    def cooldown_for_label(self, label: str) -> timedelta:
        """Return configured timedelta for label, or a 1-hour default."""
        return self.cooldowns.get(label, timedelta(hours=1))

    def compute(
        self, df: pd.DataFrame, symbol: str, streaming_update: bool = False
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def generate_notifications(
        self, result: Dict[str, Any], symbol: str
    ) -> Optional[Notification]:
        raise NotImplementedError


class DummyStrategy(BaseStrategy):
    """Example placeholder strategy that randomly chooses a label."""

    def compute(
        self, df: pd.DataFrame, symbol: str, streaming_update: bool = False
    ) -> Dict[str, Any]:
        random_label = random.choice(["A", "B"])
        return {
            "type": "dummy_strategy",
            "params": self.config,
            "label": random_label,
            "time_series": {"dates": [], "prices": [], "signal": []},
        }

    def generate_notifications(
        self, result: Dict[str, Any], symbol: str
    ) -> Optional[Notification]:
        label = result["label"]
        message = f"Dummy strategy notification for {symbol} with label {label}"
        cooldown = self.cooldown_for_label(label)

        return Notification(
            strategy="dummy_strategy",
            label=label,
            timestamp=datetime.now(UTC),
            message=message,
            cooldown=cooldown,
        )


class SMAWithThresholdStrategy(BaseStrategy):
    """Simple Moving Average strategy using upper/lower threshold bands."""

    def compute(
        self, df: pd.DataFrame, symbol: str, streaming_update: bool = False
    ) -> Dict[str, Any]:
        window = self.config["window"]
        upper_pct = self.config["upper_threshold"]
        lower_pct = self.config["lower_threshold"]

        sma = df["Adj Close"].rolling(window=window).mean()
        valid_mask = sma.notna()
        df_valid = df.loc[valid_mask].copy()

        df_valid["sma"] = sma[valid_mask]
        df_valid["upper_band"] = df_valid["sma"] * (1 + upper_pct)
        df_valid["lower_band"] = df_valid["sma"] * (1 - lower_pct)

        prices = df_valid["Adj Close"].tolist()
        sma_values = df_valid["sma"].tolist()
        upper_band = df_valid["upper_band"].tolist()
        lower_band = df_valid["lower_band"].tolist()
        dates = [d.isoformat() for d in df_valid.index]

        signals: list[str] = []
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

        result = {
            "type": "sma_with_threshold",
            "params": self.config,
            "time_series": {
                "dates": dates,
                "prices": prices,
                "sma": sma_values,
                "upper_band": upper_band,
                "lower_band": lower_band,
                "signal": signals,
            },
        }

        if streaming_update and len(dates) > 0:
            idx = -1
            return {
                "type": "sma_with_threshold",
                "params": self.config,
                "time_series": {
                    "date": dates[idx],
                    "price": prices[idx],
                    "sma": sma_values[idx],
                    "upper_band": upper_band[idx],
                    "lower_band": lower_band[idx],
                    "signal": signals[idx],
                },
            }

        return result

    def generate_notifications(
        self, result: Dict[str, Any], symbol: str
    ) -> Optional[Notification]:
        ts = result["time_series"]

        last_signal = ts["signal"][-1]
        last_price = float(ts["prices"][-1])
        last_sma = float(ts["sma"][-1])
        last_upper = float(ts["upper_band"][-1])
        last_lower = float(ts["lower_band"][-1])

        if last_signal == "BUY":
            threshold = last_upper - last_sma
            message = (
                f"The price of {symbol} has reached {last_price:.2f}, "
                f"which is above its buy target of {last_upper:.2f}. "
                f"If the price remains above {last_sma:.2f} + {threshold:.2f} "
                f"(i.e., above {last_upper:.2f}) until market close, "
                f"please place a buy order."
            )
            label = "BUY"
            cooldown = self.cooldown_for_label(label)

        elif last_signal == "SELL":
            threshold = last_sma - last_lower
            message = (
                f"The price of {symbol} has reached {last_price:.2f}, "
                f"which is below its sell target of {last_lower:.2f}. "
                f"If the price remains below {last_sma:.2f} - {threshold:.2f} "
                f"(i.e., below {last_lower:.2f}) until market close, "
                f"please sell all your holdings."
            )
            label = "SELL"
            cooldown = self.cooldown_for_label(label)

        else:
            return None

        return Notification(
            strategy="sma_with_threshold",
            label=label,
            timestamp=datetime.now(UTC),
            message=message,
            cooldown=cooldown,
        )


class Analytics:
    """Component responsible for strategy registration and execution."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._registry: Dict[str, BaseStrategy] = {}

        # Register known strategy types if present in config.
        if "sma" in config:
            self.register("sma", SMAWithThresholdStrategy(config["sma"]))
        self.register("dummy", DummyStrategy(config.get("dummy", {})))

    def register(self, name: str, strategy: BaseStrategy) -> None:
        self._registry[name] = strategy

    def get(self, name: str) -> BaseStrategy:
        if name not in self._registry:
            raise KeyError(f"Strategy '{name}' not configured or loaded.")
        return self._registry[name]

    def exists(self, name: str) -> bool:
        return name in self._registry

    def names(self) -> list[str]:
        return list(self._registry.keys())

    def execute(
        self, name: str, df: pd.DataFrame, symbol: str, streaming_update: bool = False
    ) -> Tuple[Dict[str, Any], Optional[Notification]]:
        """Run the given strategy by name, returning result and notification."""
        strategy = self.get(name)
        result = strategy.compute(df, symbol, streaming_update)
        notification = strategy.generate_notifications(result, symbol)
        return result, notification
