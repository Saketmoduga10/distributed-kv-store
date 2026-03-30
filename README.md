# Distributed KV Store

A fault-tolerant distributed key-value store with multi-node replication and crash recovery, built in Python.

## What is this?
This project is a simple **distributed key-value store** written in Python. You can run multiple nodes locally (or across machines), write keys to any node, and have those writes **replicate** to peer nodes over HTTP. Each node can also optionally persist its state to disk so it can recover after a restart or crash.

## Key Features
- **Multi-node replication**: fan-out replication of PUT/DELETE operations to peer nodes.
- **Fault tolerance**: best-effort replication continues even if some nodes are unreachable.
- **Atomic disk persistence with crash recovery**: JSON persistence using temp-file + `os.replace` + `fsync`.
- **Thread-safe concurrent operations**: in-memory store operations are protected by a lock.
- **Benchmarked performance**: includes an async benchmark runner and example results.

## Architecture

<img width="749" height="379" alt="Screenshot 2026-03-30 at 6 09 13 PM" src="https://github.com/user-attachments/assets/9208e981-eda2-4717-a7f5-a626e386438c" />

Each node runs a **FastAPI** server exposing a small HTTP interface:
- `PUT /keys/{key}` to write
- `GET /keys/{key}` to read
- `DELETE /keys/{key}` to delete
- `GET /keys` to list all keys
- `GET /health` for node status

Nodes discover each other from a shared **cluster config JSON** (`cluster.json`). When a client writes a key to a node, that node:
1. Writes the change to its local in-memory store (and to disk if `DATA_FILE` is configured).
2. Uses an async **Replicator** to forward the operation to all peer nodes over HTTP.

To prevent infinite replication loops, the write endpoints support a `replicate` query parameter. When peers receive forwarded writes, they can be sent with `replicate=false` so they apply the write locally without re-forwarding.

## Benchmark Results
Measured against a single local node at **100 concurrent connections** with **zero errors**:

| Operation | Throughput (req/s) | p99 latency |
|----------|---------------------:|------------:|
| GET      | 746                  | 162ms       |
| PUT      | 507                  | 207ms       |

## Quick Start (Run 3 Nodes Locally)

### 1) Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Create / verify your cluster config
This repo includes a `cluster.json` like:

```json
{
  "nodes": [
    { "id": "node-1", "host": "127.0.0.1", "port": 8001 },
    { "id": "node-2", "host": "127.0.0.1", "port": 8002 },
    { "id": "node-3", "host": "127.0.0.1", "port": 8003 }
  ]
}
```

### 3) Start three nodes (three terminals)

Terminal 1:

```bash
NODE_ID=node-1 CLUSTER_CONFIG_PATH=cluster.json DATA_FILE=data_node1.json uvicorn node:app --port 8001
```

Terminal 2:

```bash
NODE_ID=node-2 CLUSTER_CONFIG_PATH=cluster.json DATA_FILE=data_node2.json uvicorn node:app --port 8002
```

Terminal 3:

```bash
NODE_ID=node-3 CLUSTER_CONFIG_PATH=cluster.json DATA_FILE=data_node3.json uvicorn node:app --port 8003
```

If you don’t want persistence, omit `DATA_FILE` and nodes will run purely in-memory.

### 4) Try it

```bash
curl -X PUT "http://127.0.0.1:8001/keys/hello" -H "Content-Type: application/json" -d '{"value":"world"}'
curl "http://127.0.0.1:8002/keys/hello"
```

### 5) Run the benchmark

```bash
python benchmark.py
```

By default the benchmark disables replication on PUTs (single-node measurement). You can enable it by calling `run_benchmark(..., replicate=True)` in the script.

## Tech Stack
- **Python**
- **FastAPI** + **Uvicorn** (HTTP server)
- **Pydantic** (configuration and API models)
- **httpx** (async HTTP client)
- **asyncio** (concurrency)
