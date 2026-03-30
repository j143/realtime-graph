"""
Event simulator (client / app simulator).

Generates random LOGIN and STARTED_WATCHING events and places them on
the shared queue, mimicking a stream of user actions.

Configurable parameters
-----------------------
users   – pool of user IDs to draw from
devices – pool of device IDs
shows   – pool of show IDs
rate    – approximate events per second (default 2)
"""

from __future__ import annotations

import random
import time
import threading
from queue import Queue
from typing import List, Optional

from events import Event, EVENT_LOGIN, EVENT_STARTED_WATCHING

# Default pools
DEFAULT_USERS = ["U1", "U2", "U3"]
DEFAULT_DEVICES = ["D1", "D2", "D3"]
DEFAULT_SHOWS = ["S1", "S2", "S3", "S4"]


class EventSimulator:
    """Produces random events at a configurable rate."""

    def __init__(
        self,
        queue: Queue,
        users: List[str] = None,
        devices: List[str] = None,
        shows: List[str] = None,
        rate: float = 2.0,
    ) -> None:
        self._queue = queue
        self._users = users or DEFAULT_USERS
        self._devices = devices or DEFAULT_DEVICES
        self._shows = shows or DEFAULT_SHOWS
        self._rate = rate
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._produced = 0

    @property
    def produced(self) -> int:
        return self._produced

    def start(self) -> None:
        """Start producing events in a background thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="EventSimulator", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the simulator to stop."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def emit(self, evt: Event) -> None:
        """Directly enqueue a specific event (useful for testing / demos)."""
        self._queue.put(evt)
        self._produced += 1

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        interval = 1.0 / max(self._rate, 0.01)
        while self._running:
            evt = self._random_event()
            self._queue.put(evt)
            self._produced += 1
            time.sleep(interval)

    def _random_event(self) -> Event:
        user = random.choice(self._users)
        if random.random() < 0.5:
            return Event.login(
                user_id=user,
                device_id=random.choice(self._devices),
            )
        return Event.started_watching(
            user_id=user,
            show_id=random.choice(self._shows),
        )
