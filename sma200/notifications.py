import shelve
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import DefaultDict, List, Optional

from sma200.utils import nested_defaultdict


class Notification:
    """Represents a single notification."""

    def __init__(
        self,
        strategy: str,
        label: str,
        timestamp: datetime,
        message: str,
        cooldown: timedelta,
    ) -> None:
        self.strategy = strategy
        self.label = label
        self.timestamp = timestamp
        self.message = message
        self.cooldown = cooldown

    def __repr__(self) -> str:
        return (
            f"Notification({self.strategy!r}, {self.label!r}, "
            f"{self.timestamp.isoformat()}, {self.message!r})"
        )


class Notifier:
    """Registers and dispatches notifications with per-label cooldown enforcement."""

    def __init__(self, config: dict, symbol: str) -> None:
        self.mailing_list: List[str] = config["mailing_list"]
        self.symbol = symbol

        datadir = Path(config.get("notifications", "."))
        datadir.mkdir(parents=True, exist_ok=True)

        symbol_dir = datadir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        self.SHELVE_PATH = symbol_dir / "notifier_state"
        self._db = shelve.open(str(self.SHELVE_PATH), writeback=True)

        if "notifications" not in self._db:
            self._db["notifications"] = defaultdict(nested_defaultdict)

        self.notifications: DefaultDict[str, DefaultDict[str, List[Notification]]] = (
            self._db["notifications"]
        )

    def _persist(self) -> None:
        """Ensure changes are written to disk."""
        self._db["notifications"] = self.notifications
        self._db.sync()

    def register(self, notification: Notification) -> bool:
        """
        Register a notification for a specific (strategy, label).
        A new notification is only sent if the last one for the same
        (strategy, label) exceeds its cooldown window.
        """
        strategy, label, now, cooldown = (
            notification.strategy,
            notification.label,
            notification.timestamp,
            notification.cooldown,
        )

        label_bucket = self.notifications[strategy][label]
        last_same: Optional[Notification] = label_bucket[-1] if label_bucket else None

        if last_same and (now - last_same.timestamp) < cooldown:
            return False

        label_bucket.append(notification)
        self.notifications[strategy][label] = label_bucket[-2:]

        self._persist()
        self._send(notification)
        return True

    def _send(self, notification: Notification) -> None:
        """Send the notification via the system's `mail` command."""
        subject = f"[{notification.strategy}] {notification.label}"
        body = notification.message
        msg = body.encode("utf-8")
        for recipient in self.mailing_list:
            subprocess.run(
                ["mail", "-s", subject, recipient],
                input=msg,
                check=True,
            )

    def close(self) -> None:
        """Cleanly close the shelve database."""
        try:
            self._db.close()
        except Exception:
            pass

    def __del__(self):
        if hasattr(self, "_db"):
            try:
                self._db.close()
            except Exception:
                pass
