import subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from typing import DefaultDict, List, Optional


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

    def __init__(self, mailing_list: List[str]) -> None:
        self.notifications: DefaultDict[str, DefaultDict[str, List[Notification]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        self.mailing_list = mailing_list

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

        # Get the bucket for this (strategy, label) pair
        label_bucket = self.notifications[strategy][label]

        # Get the most recent notification with the same (strategy, label)
        last_same: Optional[Notification] = label_bucket[-1] if label_bucket else None

        # Suppress delivery if within label‑specific cooldown window
        if last_same and (now - last_same.timestamp) < cooldown:
            return False

        # Append new notification (keep at most the last two)
        label_bucket.append(notification)
        self.notifications[strategy][label] = label_bucket[-2:]

        # Trigger delivery
        self._send(notification)
        return True

    # Print-based stub for debugging:
    # def _send(self, notification: Notification) -> None:
    #     msg = f"[{notification.strategy}] {notification.label}: {notification.message}"
    #     print(f"[EMAIL OUT]; TO: {self.mailing_list}\n{msg}")

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
