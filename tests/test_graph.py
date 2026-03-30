"""
Unit tests for the tiny real-time graph system.

Tests cover:
- Event creation (events.py)
- GraphStore operations (graph_store.py)
- StreamProcessor processing logic (processor.py)
- EventSimulator emission (simulator.py)
- End-to-end: simulator → queue → processor → graph
"""

from __future__ import annotations

import time
import unittest
from queue import Queue

from events import Event, EVENT_LOGIN, EVENT_STARTED_WATCHING
from graph_store import GraphStore
from processor import StreamProcessor
from simulator import EventSimulator


# ---------------------------------------------------------------------------
# Event tests
# ---------------------------------------------------------------------------

class TestEvent(unittest.TestCase):

    def test_login_factory(self):
        evt = Event.login(user_id="U1", device_id="D1")
        self.assertEqual(evt.event_type, EVENT_LOGIN)
        self.assertEqual(evt.user_id, "U1")
        self.assertEqual(evt.device_id, "D1")
        self.assertIsNone(evt.show_id)
        self.assertIsNotNone(evt.event_id)
        self.assertGreater(evt.ts, 0)

    def test_started_watching_factory(self):
        evt = Event.started_watching(user_id="U1", show_id="S1")
        self.assertEqual(evt.event_type, EVENT_STARTED_WATCHING)
        self.assertEqual(evt.user_id, "U1")
        self.assertEqual(evt.show_id, "S1")
        self.assertIsNone(evt.device_id)

    def test_event_id_uniqueness(self):
        e1 = Event.login(user_id="U1", device_id="D1")
        e2 = Event.login(user_id="U1", device_id="D1")
        self.assertNotEqual(e1.event_id, e2.event_id)

    def test_explicit_fields(self):
        evt = Event(
            event_type=EVENT_LOGIN,
            user_id="U99",
            device_id="D99",
            event_id="fixed-id",
            ts=1710000000,
        )
        self.assertEqual(evt.event_id, "fixed-id")
        self.assertEqual(evt.ts, 1710000000)

    def test_repr_login(self):
        evt = Event.login(user_id="U1", device_id="D1")
        r = repr(evt)
        self.assertIn("LOGIN", r)
        self.assertIn("U1", r)
        self.assertIn("D1", r)

    def test_repr_started_watching(self):
        evt = Event.started_watching(user_id="U1", show_id="S1")
        r = repr(evt)
        self.assertIn("STARTED_WATCHING", r)
        self.assertIn("U1", r)
        self.assertIn("S1", r)


# ---------------------------------------------------------------------------
# GraphStore tests
# ---------------------------------------------------------------------------

class TestGraphStore(unittest.TestCase):

    def setUp(self):
        self.g = GraphStore()

    def test_ensure_node_creates_entry(self):
        self.g.ensure_node("user:U1")
        self.assertTrue(self.g.node_exists("user:U1"))

    def test_ensure_node_idempotent(self):
        self.g.ensure_node("user:U1")
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        # calling ensure_node again must NOT wipe existing edges
        self.g.ensure_node("user:U1")
        self.assertEqual(self.g.get_edges("user:U1", "STARTED_WATCHING"),
                         ["show:S1"])

    def test_add_edge_creates_nodes(self):
        self.g.add_edge("user:U1", "LOGGED_IN_FROM", "device:D1")
        self.assertTrue(self.g.node_exists("user:U1"))
        self.assertTrue(self.g.node_exists("device:D1"))

    def test_add_edge_returns_true_on_new(self):
        result = self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        self.assertTrue(result)

    def test_add_edge_returns_false_on_duplicate(self):
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        result = self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        self.assertFalse(result)

    def test_dedup_no_duplicate_edges(self):
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        edges = self.g.get_edges("user:U1", "STARTED_WATCHING")
        self.assertEqual(edges, ["show:S1"])

    def test_multiple_edges_same_relation(self):
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S2")
        edges = self.g.get_edges("user:U1", "STARTED_WATCHING")
        self.assertIn("show:S1", edges)
        self.assertIn("show:S2", edges)
        self.assertEqual(len(edges), 2)

    def test_multiple_relations(self):
        self.g.add_edge("user:U1", "LOGGED_IN_FROM", "device:D1")
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        self.assertEqual(self.g.get_edges("user:U1", "LOGGED_IN_FROM"),
                         ["device:D1"])
        self.assertEqual(self.g.get_edges("user:U1", "STARTED_WATCHING"),
                         ["show:S1"])

    def test_get_edges_missing_node(self):
        self.assertEqual(self.g.get_edges("user:GHOST", "ANY"), [])

    def test_get_edges_missing_relation(self):
        self.g.ensure_node("user:U1")
        self.assertEqual(self.g.get_edges("user:U1", "NONEXISTENT"), [])

    def test_get_node_copy_isolation(self):
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        node_copy = self.g.get_node("user:U1")
        node_copy["STARTED_WATCHING"].append("show:INJECTED")
        # original must be unaffected
        self.assertEqual(self.g.get_edges("user:U1", "STARTED_WATCHING"),
                         ["show:S1"])

    def test_all_nodes_sorted(self):
        self.g.ensure_node("user:U3")
        self.g.ensure_node("user:U1")
        self.g.ensure_node("device:D1")
        nodes = self.g.all_nodes()
        self.assertEqual(nodes, sorted(nodes))

    def test_snapshot(self):
        self.g.add_edge("user:U1", "STARTED_WATCHING", "show:S1")
        snap = self.g.snapshot()
        self.assertIn("user:U1", snap)
        self.assertIn("show:S1", snap)
        self.assertEqual(snap["user:U1"]["STARTED_WATCHING"], ["show:S1"])


# ---------------------------------------------------------------------------
# StreamProcessor tests
# ---------------------------------------------------------------------------

class TestStreamProcessor(unittest.TestCase):

    def setUp(self):
        self.graph = GraphStore()
        self.queue: Queue = Queue()
        self.proc = StreamProcessor(self.queue, self.graph)

    # Process events synchronously (no threading needed for unit tests)

    def test_login_creates_user_and_device_nodes(self):
        evt = Event.login(user_id="U1", device_id="D1")
        self.proc.process_one(evt)
        self.assertTrue(self.graph.node_exists("user:U1"))
        self.assertTrue(self.graph.node_exists("device:D1"))

    def test_login_creates_logged_in_from_edge(self):
        evt = Event.login(user_id="U1", device_id="D1")
        self.proc.process_one(evt)
        self.assertEqual(
            self.graph.get_edges("user:U1", "LOGGED_IN_FROM"),
            ["device:D1"]
        )

    def test_started_watching_creates_user_and_show_nodes(self):
        evt = Event.started_watching(user_id="U1", show_id="S1")
        self.proc.process_one(evt)
        self.assertTrue(self.graph.node_exists("user:U1"))
        self.assertTrue(self.graph.node_exists("show:S1"))

    def test_started_watching_creates_edge(self):
        evt = Event.started_watching(user_id="U1", show_id="S1")
        self.proc.process_one(evt)
        self.assertEqual(
            self.graph.get_edges("user:U1", "STARTED_WATCHING"),
            ["show:S1"]
        )

    def test_multiple_watches_same_user(self):
        self.proc.process_one(Event.started_watching(user_id="U1", show_id="S1"))
        self.proc.process_one(Event.started_watching(user_id="U1", show_id="S2"))
        edges = self.graph.get_edges("user:U1", "STARTED_WATCHING")
        self.assertIn("show:S1", edges)
        self.assertIn("show:S2", edges)

    def test_duplicate_event_deduped(self):
        evt = Event.started_watching(user_id="U1", show_id="S1")
        self.proc.process_one(evt)
        self.proc.process_one(evt)
        edges = self.graph.get_edges("user:U1", "STARTED_WATCHING")
        self.assertEqual(edges.count("show:S1"), 1)

    def test_processed_counter_increments(self):
        self.proc.process_one(Event.login(user_id="U1", device_id="D1"))
        self.proc.process_one(Event.started_watching(user_id="U1", show_id="S1"))
        self.assertEqual(self.proc.processed, 2)

    def test_unknown_event_type_skipped(self):
        evt = Event(event_type="UNKNOWN", user_id="U1")
        self.proc.process_one(evt)   # must not raise
        self.assertEqual(self.proc.processed, 0)

    def test_login_missing_device_skipped(self):
        evt = Event(event_type="LOGIN", user_id="U1", device_id=None)
        self.proc.process_one(evt)
        self.assertEqual(self.proc.processed, 0)

    def test_started_watching_missing_show_skipped(self):
        evt = Event(event_type="STARTED_WATCHING", user_id="U1", show_id=None)
        self.proc.process_one(evt)
        self.assertEqual(self.proc.processed, 0)


# ---------------------------------------------------------------------------
# EventSimulator tests
# ---------------------------------------------------------------------------

class TestEventSimulator(unittest.TestCase):

    def test_emit_puts_event_on_queue(self):
        q: Queue = Queue()
        sim = EventSimulator(q)
        evt = Event.login(user_id="U1", device_id="D1")
        sim.emit(evt)
        self.assertEqual(q.qsize(), 1)
        self.assertEqual(q.get_nowait(), evt)

    def test_produced_counter_increments_on_emit(self):
        q: Queue = Queue()
        sim = EventSimulator(q)
        sim.emit(Event.login(user_id="U1", device_id="D1"))
        sim.emit(Event.started_watching(user_id="U1", show_id="S1"))
        self.assertEqual(sim.produced, 2)


# ---------------------------------------------------------------------------
# End-to-end integration test
# ---------------------------------------------------------------------------

class TestEndToEnd(unittest.TestCase):

    def test_full_pipeline(self):
        """
        Drive a fixed sequence of events through the full pipeline
        (queue + threaded processor) and verify the resulting graph.
        """
        queue: Queue = Queue()
        graph = GraphStore()
        processor = StreamProcessor(queue, graph)
        simulator = EventSimulator(queue)

        processor.start()

        simulator.emit(Event.login(user_id="U1", device_id="D1"))
        simulator.emit(Event.started_watching(user_id="U1", show_id="S1"))
        simulator.emit(Event.started_watching(user_id="U1", show_id="S2"))
        simulator.emit(Event.login(user_id="U2", device_id="D2"))

        # Wait until all events are processed.
        queue.join()
        processor.stop()

        # User U1 logged in from D1
        self.assertEqual(graph.get_edges("user:U1", "LOGGED_IN_FROM"),
                         ["device:D1"])
        # User U1 watched S1 and S2
        watched = graph.get_edges("user:U1", "STARTED_WATCHING")
        self.assertIn("show:S1", watched)
        self.assertIn("show:S2", watched)
        # User U2 logged in from D2
        self.assertEqual(graph.get_edges("user:U2", "LOGGED_IN_FROM"),
                         ["device:D2"])
        # 4 events processed
        self.assertEqual(processor.processed, 4)

    def test_dedup_across_pipeline(self):
        """Duplicate events through the pipeline produce no duplicate edges."""
        queue: Queue = Queue()
        graph = GraphStore()
        processor = StreamProcessor(queue, graph)
        simulator = EventSimulator(queue)

        processor.start()

        for _ in range(5):
            simulator.emit(Event.started_watching(user_id="U1", show_id="S1"))

        queue.join()
        processor.stop()

        edges = graph.get_edges("user:U1", "STARTED_WATCHING")
        self.assertEqual(edges.count("show:S1"), 1)


if __name__ == "__main__":
    unittest.main()
