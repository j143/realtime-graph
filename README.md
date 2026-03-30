# realtime-graph

A tiny real-time graph system inspired by Netflix's event-driven graph
architecture: **events → stream processor → graph**.

## Architecture

```
EventSimulator  →  Queue (event bus)  →  StreamProcessor  →  GraphStore
                                                                  ↑
                                                       query CLI / API
```

Four components:

| Component | File | Role |
|-----------|------|------|
| **Event** | `events.py` | Data class for `LOGIN` / `STARTED_WATCHING` actions |
| **GraphStore** | `graph_store.py` | Thread-safe in-memory adjacency-map |
| **StreamProcessor** | `processor.py` | Reads queue, updates graph |
| **EventSimulator** | `simulator.py` | Produces random events |
| **Main** | `main.py` | Wires everything together, interactive query CLI |

## Data model

**Node key**: `"<type>:<id>"` — e.g. `"user:U1"`, `"device:D1"`, `"show:S1"`

**Adjacency map**:
```python
graph["user:U1"]["STARTED_WATCHING"] = ["show:S1", "show:S2"]
graph["user:U1"]["LOGGED_IN_FROM"]   = ["device:D1"]
```

**Input event shape**:
```json
{
  "event_id":   "e123",
  "event_type": "STARTED_WATCHING",
  "user_id":    "U1",
  "device_id":  "D1",
  "show_id":    "S1",
  "ts":         1710000000
}
```

## Processing logic

For each event the processor:

1. Ensures `user:<user_id>` node exists.
2. For **LOGIN**: ensures `device:<device_id>` node, adds edge
   `user:U1 --[LOGGED_IN_FROM]--> device:D1`.
3. For **STARTED_WATCHING**: ensures `show:<show_id>` node, adds edge
   `user:U1 --[STARTED_WATCHING]--> show:S1`.
4. Duplicate edges are silently ignored (dedup on insert).

## Quick start

```bash
# Run for 20 events then show graph (no interactive prompt)
python main.py --events 20 --no-query

# Run indefinitely at 5 events/s (Ctrl-C to stop, then interactive query)
python main.py --rate 5
```

### Interactive query

After the simulator stops you can inspect any node:

```
node> user:U1
  user:U1  --[LOGGED_IN_FROM]-->  ['device:D1']
  user:U1  --[STARTED_WATCHING]-->  ['show:S1', 'show:S2']

node> quit
```

## Tests

```bash
python -m unittest discover -s tests -v
```

All 33 tests cover events, graph store, stream processor, simulator, and
end-to-end pipeline behaviour.