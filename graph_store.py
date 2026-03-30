"""
In-memory graph store.

Implements a simple adjacency-list graph that mirrors the structure of
Netflix's property-graph KV store, but entirely in memory:

    graph["user:U1"]["STARTED_WATCHING"] = ["show:S1", "show:S2"]

Key design decisions:
- Nodes are keyed by "<type>:<id>" strings.
- Edges are stored per source-node in a dict of relation → [target, ...].
- Duplicate edges are silently ignored (dedup on insert).
- A threading lock makes reads/writes safe when the processor and any
  query thread run concurrently.
"""

from __future__ import annotations

import threading
from typing import Dict, List

# Type alias for the adjacency map
AdjacencyMap = Dict[str, Dict[str, List[str]]]


class GraphStore:
    """Thread-safe in-memory graph store."""

    def __init__(self) -> None:
        # graph[node_key][relation] = [target_node_key, ...]
        self._graph: AdjacencyMap = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def ensure_node(self, node_key: str) -> None:
        """Create an empty node entry if it does not already exist."""
        with self._lock:
            self._graph.setdefault(node_key, {})

    def add_edge(self, src: str, relation: str, dst: str) -> bool:
        """
        Add a directed edge src --[relation]--> dst.

        Both nodes are created automatically if they do not exist.
        Returns True if a new edge was added, False if it already existed.
        """
        with self._lock:
            self._graph.setdefault(src, {})
            self._graph.setdefault(dst, {})
            edges: List[str] = self._graph[src].setdefault(relation, [])
            if dst in edges:
                return False
            edges.append(dst)
            return True

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_edges(self, src: str, relation: str) -> List[str]:
        """Return all targets of *src* for the given *relation*."""
        with self._lock:
            return list(self._graph.get(src, {}).get(relation, []))

    def get_node(self, node_key: str) -> Dict[str, List[str]]:
        """Return the full adjacency dict for *node_key* (copy)."""
        with self._lock:
            return {k: list(v)
                    for k, v in self._graph.get(node_key, {}).items()}

    def node_exists(self, node_key: str) -> bool:
        """Return True if the node is present in the graph."""
        with self._lock:
            return node_key in self._graph

    def all_nodes(self) -> List[str]:
        """Return a sorted list of all node keys."""
        with self._lock:
            return sorted(self._graph.keys())

    def snapshot(self) -> AdjacencyMap:
        """Return a deep copy of the entire graph (useful for debugging)."""
        with self._lock:
            return {
                node: {rel: list(targets) for rel, targets in edges.items()}
                for node, edges in self._graph.items()
            }
