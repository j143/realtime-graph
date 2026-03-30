"""
Stream processor.

Reads Event objects from a queue and updates the GraphStore.

Processing logic per event
--------------------------
LOGIN event
  1. Ensure node user:<user_id> exists.
  2. Ensure node device:<device_id> exists.
  3. Add edge  user:<user_id> --[LOGGED_IN_FROM]--> device:<device_id>

STARTED_WATCHING event
  1. Ensure node user:<user_id> exists.
  2. Ensure node show:<show_id> exists.
  3. Add edge  user:<user_id> --[STARTED_WATCHING]--> show:<show_id>

Unknown event types are logged to stderr and skipped.

The processor runs in its own daemon thread so the main thread can
continue producing events and serving queries.
"""

from __future__ import annotations

import sys
import threading
from queue import Queue, Empty
from typing import Optional

from events import Event, EVENT_LOGIN, EVENT_STARTED_WATCHING
from graph_store import GraphStore

# Sentinel object used to stop the processor thread cleanly.
_STOP = object()


class StreamProcessor:
    """Consumes events from a queue and updates a GraphStore."""

    def __init__(self, queue: Queue, graph: GraphStore) -> None:
        self._queue = queue
        self._graph = graph
        self._thread: Optional[threading.Thread] = None
        self._processed = 0   # count of successfully processed events

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def processed(self) -> int:
        """Number of events successfully processed (thread-safe read)."""
        return self._processed

    def start(self) -> None:
        """Start the background processing thread."""
        self._thread = threading.Thread(
            target=self._run, name="StreamProcessor", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the processor to stop and wait for it to finish."""
        self._queue.put(_STOP)
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def process_one(self, evt: Event) -> None:
        """Process a single event synchronously (useful for testing)."""
        self._handle(evt)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.1)
            except Empty:
                continue

            if item is _STOP:
                self._queue.task_done()
                break

            try:
                self._handle(item)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[processor] error handling event {item}: {exc}",
                      file=sys.stderr)
            finally:
                self._queue.task_done()

    def _handle(self, evt: Event) -> None:
        """Core graph-update logic for one event."""
        user_key = f"user:{evt.user_id}"
        self._graph.ensure_node(user_key)

        if evt.event_type == EVENT_LOGIN:
            if evt.device_id is None:
                print(f"[processor] LOGIN event missing device_id: {evt}",
                      file=sys.stderr)
                return
            dev_key = f"device:{evt.device_id}"
            self._graph.add_edge(user_key, "LOGGED_IN_FROM", dev_key)

        elif evt.event_type == EVENT_STARTED_WATCHING:
            if evt.show_id is None:
                print(f"[processor] STARTED_WATCHING event missing show_id: "
                      f"{evt}", file=sys.stderr)
                return
            show_key = f"show:{evt.show_id}"
            self._graph.add_edge(user_key, "STARTED_WATCHING", show_key)

        else:
            print(f"[processor] unknown event_type '{evt.event_type}': {evt}",
                  file=sys.stderr)
            return

        self._processed += 1
