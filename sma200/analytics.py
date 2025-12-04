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
    def _parse_cooldowns(cooldown_cfg: Dict[str, Any]) -> Dict[str, timedelta]:
        """Parse cooldown strings like {'BUY': '2 hours'} into timedeltas."""
        cooldowns: Dict[str, timedelta] = {}
        for label, value in cooldown_cfg.items():
            if isinstance(value, dict):
                nested = BaseStrategy._parse_cooldowns(value)
                for sub_label, td in nested.items():
                    cooldowns[f"{label}_{sub_label}"] = td
            else:
                cooldowns[label] = BaseStrategy.parse_cooldown(value)
        return cooldowns

    @staticmethod
    def parse_cooldown(text: str) -> timedelta:
        """Parse a single cooldown string like '2 hours' into a timedelta."""
        parts = text.split()
        if len(parts) != 2:
            raise ValueError(f"Invalid cooldown format: {text!r}")
        num_str, unit = parts
        try:
            num = float(num_str)
        except ValueError as exc:
            raise ValueError(f"Invalid number in cooldown: {text!r}") from exc
        if "hour" in unit:
            return timedelta(hours=num)
        elif "minute" in unit:
            return timedelta(minutes=num)
        elif "day" in unit:
            return timedelta(days=num)
        else:
            raise ValueError(f"Unsupported cooldown unit: {unit!r}")

    def cooldown_for_label(self, label: str) -> timedelta:
        """Return configured timedelta for label, or a 1-hour default."""
        return self.cooldowns.get(label, timedelta(hours=1))

    def compute(
        self, df: pd.DataFrame, symbol: str, streaming_update: bool = False
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def generate_notifications(
        self, result: Dict[str, Any], symbol: str, streaming_update: bool = False
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
            "label": random_label,
            "time_series": {"dates": [], "prices": [], "signal": []},
        }

    def generate_notifications(
        self, result: Dict[str, Any], symbol: str, streaming_update: bool = False
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

        if streaming_update and len(dates) > 0:
            idx = -1
            return {
                "date": dates[idx],
                "price": prices[idx],
                "sma": sma_values[idx],
                "upper_band": upper_band[idx],
                "lower_band": lower_band[idx],
                "signal": signals[idx],
            }

        return {
            "dates": dates,
            "prices": prices,
            "sma": sma_values,
            "upper_band": upper_band,
            "lower_band": lower_band,
            "signal": signals,
        }

    def generate_notifications(
        self, result: Dict[str, Any], symbol: str, streaming_update: bool = False
    ) -> Optional[Notification]:
    
        def get_val(*keys):
            val = next((result[k] for k in keys if k in result), None)
            return val[-1] if isinstance(val, list) else val

        last_signal = get_val("signal")
        last_price = float(get_val("prices", "price"))
        last_sma = float(get_val("sma"))
        last_upper = float(get_val("upper_band"))
        last_lower = float(get_val("lower_band"))

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

        elif last_signal == "HOLD":
            reminder_levels = tuple(
                float(k.strip("%"))
                for k in self.config["cooldowns"].get("REMINDERS", {}).keys()
            ) or (5.0, 2.5, 1.0)
            reminder_levels = tuple(sorted(reminder_levels))
            buy_diffs = [(p, last_upper * (1 - p / 100)) for p in reminder_levels]
            sell_diffs = [(p, last_lower * (1 + p / 100)) for p in reminder_levels]
            triggered = None

            # buy side
            for pct, limit in buy_diffs:
                if last_price >= limit and last_price < last_upper:
                    triggered = ("BUY", pct)
                    break

            # sell side
            if triggered is None:
                for pct, limit in sell_diffs:
                    if last_price <= limit and last_price > last_lower:
                        triggered = ("SELL", pct)
                        break

            if triggered is None:
                return None

            direction, pct = triggered
            if direction == "BUY":
                label = f"BUY_REMINDER_{pct:.1f}%"
                message = (
                    f"{symbol} is within {pct:.1f}% of the BUY target ({last_upper:.2f}). "
                    f"Current price: {last_price:.2f}. Monitor for potential breakout."
                )
            else:
                label = f"SELL_REMINDER_{pct:.1f}%"
                message = (
                    f"{symbol} is within {pct:.1f}% of the SELL target ({last_lower:.2f}). "
                    f"Current price: {last_price:.2f}. Consider preparing to sell."
                )

            cooldown = self.cooldown_for_label(label)

        return Notification(
            strategy="sma_with_threshold",
            label=label,
            timestamp=datetime.now(UTC),
            message=message,
            cooldown=cooldown,
        )


class Analytics:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._registry: Dict[str, Tuple[BaseStrategy, str]] = {}

        if "sma" in config:
            self.register(
                "sma", SMAWithThresholdStrategy(config["sma"]), "Simple Moving Average"
            )

        self.register("dummy", DummyStrategy(config.get("dummy", {})), "Dummy Strategy")

    def register(self, name: str, strategy: BaseStrategy, human_name: str) -> None:
        self._registry[name] = (strategy, human_name)

    def get(self, name: str) -> BaseStrategy:
        if name not in self._registry:
            raise KeyError(f"Strategy '{name}' not configured or loaded.")
        return self._registry[name][0]

    def get_all_strategies(self) -> list[tuple[str, str]]:
        return [(internal, human) for internal, (_, human) in self._registry.items()]

    def exists(self, name: str) -> bool:
        return name in self._registry

    def names(self) -> list[str]:
        # internal names (unchanged)
        return list(self._registry.keys())

    def execute(self, name: str, df, symbol, streaming_update=False):
        strategy = self.get(name)
        result = strategy.compute(df, symbol, streaming_update)
        notification = strategy.generate_notifications(result, symbol, streaming_update)
        return result, notification
