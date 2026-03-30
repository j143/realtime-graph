"""
Event definitions for the real-time graph system.

Each event represents a user action (login from a device, or start watching
a show) and flows through the event bus to the stream processor.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

# Supported event types
EVENT_LOGIN = "LOGIN"
EVENT_STARTED_WATCHING = "STARTED_WATCHING"


@dataclass
class Event:
    """A single user-action event placed on the event bus."""

    event_type: str          # EVENT_LOGIN or EVENT_STARTED_WATCHING
    user_id: str             # e.g. "U1"
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    device_id: Optional[str] = None   # present for LOGIN events
    show_id: Optional[str] = None     # present for STARTED_WATCHING events
    ts: int = field(default_factory=lambda: int(time.time()))

    # ------------------------------------------------------------------
    # Convenience constructors
    # ------------------------------------------------------------------

    @classmethod
    def login(cls, user_id: str, device_id: str, **kwargs) -> "Event":
        """Create a LOGIN event."""
        return cls(event_type=EVENT_LOGIN, user_id=user_id,
                   device_id=device_id, **kwargs)

    @classmethod
    def started_watching(cls, user_id: str, show_id: str, **kwargs) -> "Event":
        """Create a STARTED_WATCHING event."""
        return cls(event_type=EVENT_STARTED_WATCHING, user_id=user_id,
                   show_id=show_id, **kwargs)

    def __repr__(self) -> str:
        if self.event_type == EVENT_LOGIN:
            return (f"Event(LOGIN user={self.user_id} "
                    f"device={self.device_id} id={self.event_id})")
        return (f"Event(STARTED_WATCHING user={self.user_id} "
                f"show={self.show_id} id={self.event_id})")
