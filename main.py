"""
Real-time Graph System — main entry point.

Wires the four components together:

  EventSimulator  →  Queue  →  StreamProcessor  →  GraphStore
                                                        ↑
                                              interactive query CLI

Usage
-----
    python main.py              # run with defaults (Ctrl-C to stop)
    python main.py --rate 5     # produce 5 events/s
    python main.py --events 20  # stop after 20 events, then show graph
"""

from __future__ import annotations

import argparse
import sys
import time
from queue import Queue

from events import Event
from graph_store import GraphStore
from processor import StreamProcessor
from simulator import EventSimulator


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

def print_graph(graph: GraphStore) -> None:
    """Print every node and its outgoing edges."""
    nodes = graph.all_nodes()
    if not nodes:
        print("  (graph is empty)")
        return
    for node in nodes:
        adj = graph.get_node(node)
        if adj:
            for relation, targets in sorted(adj.items()):
                print(f"  {node}  --[{relation}]-->  {targets}")
        else:
            print(f"  {node}  (no outgoing edges)")


def query_loop(graph: GraphStore) -> None:
    """Simple interactive query loop (type 'quit' to exit)."""
    print("\nQuery the graph.  Examples:")
    print("  user:U1")
    print("  show:S1")
    print("  device:D1")
    print("  quit\n")
    while True:
        try:
            raw = input("node> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if raw.lower() in {"quit", "exit", "q"}:
            break
        if not raw:
            continue
        adj = graph.get_node(raw)
        if not adj:
            if graph.node_exists(raw):
                print(f"  {raw}  (no outgoing edges)")
            else:
                print(f"  Node '{raw}' not found in graph.")
        else:
            for relation, targets in sorted(adj.items()):
                print(f"  {raw}  --[{relation}]-->  {targets}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Tiny real-time graph system (events → processor → graph)"
    )
    p.add_argument(
        "--rate", type=float, default=2.0,
        help="events produced per second (default: 2)"
    )
    p.add_argument(
        "--events", type=int, default=0,
        help="stop after this many events (0 = run until Ctrl-C)"
    )
    p.add_argument(
        "--no-query", action="store_true",
        help="skip the interactive query loop and just print the final graph"
    )
    return p


def main(argv=None) -> None:
    args = build_arg_parser().parse_args(argv)

    queue: Queue = Queue()
    graph = GraphStore()
    processor = StreamProcessor(queue, graph)
    simulator = EventSimulator(queue, rate=args.rate)

    processor.start()
    simulator.start()

    print(f"[main] producing {args.rate} event(s)/s — Ctrl-C to stop")

    try:
        if args.events > 0:
            # Run until the requested number of events have been processed.
            while processor.processed < args.events:
                time.sleep(0.05)
            simulator.stop()
            # Drain the queue.
            queue.join()
        else:
            # Run indefinitely.
            while True:
                time.sleep(1.0)
                print(f"[main] produced={simulator.produced}  "
                      f"processed={processor.processed}  "
                      f"nodes={len(graph.all_nodes())}")
    except KeyboardInterrupt:
        print("\n[main] stopping …")
        simulator.stop()
        queue.join()

    processor.stop()

    print("\n=== Final graph ===")
    print_graph(graph)

    if not args.no_query:
        query_loop(graph)


if __name__ == "__main__":
    main(sys.argv[1:])
