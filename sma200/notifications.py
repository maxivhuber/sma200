from collections import defaultdict
from datetime import datetime, timedelta
from typing import DefaultDict, List, Optional


class Notification:
    def __init__(self, strategy: str, label: str, timestamp: datetime, message: str):
        self.strategy = strategy
        self.label = label
        self.timestamp = timestamp
        self.message = message

    def __repr__(self) -> str:
        return (
            f"Notification({self.strategy!r}, {self.label!r}, "
            f"{self.timestamp.isoformat()}, {self.message!r})"
        )


class Notifier:
    def __init__(
        self, mailing_list: list[str], cooldown: timedelta = timedelta(hours=2)
    ):
        self.cooldown = cooldown
        self.notifications: DefaultDict[str, DefaultDict[str, List[Notification]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        self.mailing_list = mailing_list

    def register(self, notification: Notification) -> bool:
        strategy, label, now = (
            notification.strategy,
            notification.label,
            notification.timestamp,
        )
        # Get the bucket for this (strategy, label) pair
        label_bucket = self.notifications[strategy][label]

        # Get the most recent notification with the same (strategy, label)
        last_same: Optional[Notification] = label_bucket[-1] if label_bucket else None

        # Suppress if within cooldown window
        if last_same and (now - last_same.timestamp) < self.cooldown:
            return False

        # Append new notification and keep only the last 2 (for potential debugging/history)
        label_bucket.append(notification)
        self.notifications[strategy][label] = label_bucket[-2:]

        # Trigger delivery
        self._send(notification)
        return True

    def _send(self, notification: Notification) -> None:
        msg = f"[{notification.strategy}] {notification.label}: {notification.message}"
        print(f"[EMAIL OUT];TO:{self.mailing_list}\n{msg}")
