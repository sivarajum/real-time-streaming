# Real-Time Streaming Platform -- Comprehensive Guide

A baby-step walkthrough of a production-style streaming pipeline built entirely
in Python. Simulates Apache Kafka topics/partitions and Spark Streaming
micro-batch processing, then surfaces live analytics through FastAPI and
Streamlit.

---

## Table of Contents

1.  [What You Will Learn](#1-what-you-will-learn)
2.  [Prerequisites](#2-prerequisites)
3.  [Project Structure](#3-project-structure)
4.  [High-Level Architecture](#4-high-level-architecture)
5.  [Core Concepts](#5-core-concepts)
    - 5.1 Streaming vs. Batch Processing
    - 5.2 Event-Driven Architecture
    - 5.3 Message Brokers, Topics, and Partitions
    - 5.4 Hash-Based Partition Routing
    - 5.5 Micro-Batch Processing
    - 5.6 Windowed Aggregations
    - 5.7 Consumer Offsets
    - 5.8 Background Threads for Continuous Processing
6.  [Code Walkthrough -- producer.py](#6-code-walkthrough----producerpy)
7.  [Code Walkthrough -- broker.py](#7-code-walkthrough----brokerpy)
8.  [Code Walkthrough -- processor.py](#8-code-walkthrough----processorpy)
9.  [Code Walkthrough -- api.py](#9-code-walkthrough----apipy)
10. [Code Walkthrough -- ui.py](#10-code-walkthrough----uipy)
11. [Code Walkthrough -- main.py](#11-code-walkthrough----mainpy)
12. [Running the Project](#12-running-the-project)
13. [Testing the API](#13-testing-the-api)
14. [Running with Docker](#14-running-with-docker)
15. [The Full Pipeline in Action](#15-the-full-pipeline-in-action)
16. [Troubleshooting](#16-troubleshooting)
17. [Extending the Project](#17-extending-the-project)
18. [Glossary](#18-glossary)

---

## 1. What You Will Learn

- Why real-time streaming matters and how it differs from batch processing.
- How Kafka's core abstractions (brokers, topics, partitions, offsets) work,
  demonstrated through a pure-Python simulation.
- How Spark Streaming's micro-batch model consumes and aggregates data.
- How to wire a producer, broker, and processor into a continuous pipeline.
- How to expose streaming analytics via a REST API and a live dashboard.

---

## 2. Prerequisites

| Tool       | Minimum Version | Purpose                    |
|------------|-----------------|----------------------------|
| Python     | 3.10+           | Runtime                    |
| pip        | 22+             | Package management         |
| Docker     | 20+ (optional)  | Container deployment       |
| curl       | any             | API testing                |

Python packages (`requirements.txt`):

```
fastapi>=0.110.0    uvicorn>=0.29.0    streamlit>=1.32.0
plotly>=5.18.0      pandas>=2.1.0      numpy>=1.26.0
requests>=2.31.0
```

No prior streaming or Kafka experience is needed.

---

## 3. Project Structure

```
POC-03-Real-Time-Streaming/
|-- main.py                 Entry point (api | ui | all | demo)
|-- requirements.txt        Python dependencies
|-- Dockerfile              Container image
|-- docker-compose.yml      Multi-container orchestration
|-- src/
    |-- __init__.py          Package marker
    |-- producer.py          Synthetic event generator
    |-- broker.py            In-memory Kafka-like message broker
    |-- processor.py         Stream processor with windowed aggregations
    |-- api.py               FastAPI server (REST endpoints)
    |-- ui.py                Streamlit live dashboard
```

The dependency chain flows in one direction:

```
producer.py --> broker.py --> processor.py --> api.py --> ui.py
```

---

## 4. High-Level Architecture

```
+------------+       +-------------------+       +------------------+
|            |       |    BROKER          |       |   STREAM         |
|  PRODUCER  | ----> |  Topic: "events"  | ----> |   PROCESSOR      |
|            |       |  +-- Partition 0   |       |                  |
| generate   |       |  +-- Partition 1   |       |  _process_batch  |
| _event()   |       |  +-- Partition 2   |       |  (windowed agg)  |
+------------+       +-------------------+       +--------+---------+
      |                      |                            |
      |  publish(key, value) |  consume(pid, offset)      |
      +----------------------+                            v
                                                 +------------------+
                                                 |  FastAPI SERVER   |
                                                 |  /broker/stats    |
                                                 |  /processor/*     |
                                                 |  /dashboard-data  |
                                                 +--------+---------+
                                                          |
                                                          | HTTP JSON
                                                          v
                                                 +------------------+
                                                 |  STREAMLIT UI    |
                                                 |  Metrics + Charts|
                                                 +------------------+
```

The API server runs a background producer thread (20 events/sec) and starts
the stream processor. The Streamlit UI is a separate process that polls the
API every 3 seconds.

---

## 5. Core Concepts

### 5.1 Streaming vs. Batch Processing

**Batch:** Collect data over hours or days, then process it all at once
(e.g., a nightly ETL job). High latency, simple to build.

**Streaming:** Handle data as soon as it arrives, within seconds. Low latency,
more complex infrastructure. Essential for fraud detection, live dashboards,
and IoT monitoring.

```
Batch:     [collect all day] ----> [process overnight] ----> Report
Streaming: event->event->event ... [process continuously] -> Live dashboard
```

This project uses **micro-batch streaming** -- events accumulate for a short
window (5 seconds), then are processed together. This is how Spark Structured
Streaming works.

### 5.2 Event-Driven Architecture

An event-driven architecture structures a system around the production,
detection, and reaction to events -- immutable facts about things that
happened.

In this project, every user action is an event dictionary:

```python
{
    "event_id":   "a3f7b2c1e4d9",       # Truncated UUID
    "event_type": "purchase",            # What happened
    "timestamp":  "2026-05-18T10:30:00", # When
    "user_id":    "user_0042",           # Who
    "amount":     149.99,                # How much
    "category":   "electronics",         # Product category
    "region":     "us-east",             # Geographic origin
    "device":     "mobile",              # Device type
}
```

Events flow downstream: broker stores them, processor aggregates them, UI
displays the results.

### 5.3 Message Brokers, Topics, and Partitions

A **broker** is middleware that receives, stores, and delivers messages.
Apache Kafka is the most widely used streaming broker.

A **topic** is a named category of messages (like a database table). This
project has one topic: `"events"`.

Each topic is split into **partitions** -- independent, ordered sequences of
messages. Partitions enable parallelism and ordering guarantees.

```
Topic: "events"
+-------------------+-------------------+-------------------+
|   Partition 0     |   Partition 1     |   Partition 2     |
| offset 0: {...}   | offset 0: {...}   | offset 0: {...}   |
| offset 1: {...}   | offset 1: {...}   | offset 1: {...}   |
| offset 2: {...}   | offset 2: {...}   | offset 2: {...}   |
+-------------------+-------------------+-------------------+
```

Each partition is a `deque(maxlen=10_000)` -- a circular buffer that holds
the most recent 10,000 messages and drops the oldest when full.

### 5.4 Hash-Based Partition Routing

When publishing, the target partition is determined by:

```
partition_id = hash(key) % num_partitions
```

The key is `user_id`, so all events from the same user always land in the
same partition. This enables per-user ordering and even distribution:

```
hash("user_0001") % 3 = 2  -->  Partition 2
hash("user_0002") % 3 = 0  -->  Partition 0
hash("user_0003") % 3 = 1  -->  Partition 1
hash("user_0004") % 3 = 1  -->  Partition 1  (same partition as user_0003)
```

### 5.5 Micro-Batch Processing

Events accumulate, then the processor wakes up every N seconds and processes
the entire batch:

```
|--- Window 1 (5s) ---|--- Window 2 (5s) ---|--- Window 3 (5s) ---|
| event event event    | event event event    | event event event    |
| event event          | event event event    | event                |
+----------------------+----------------------+----------------------+
         |                      |                      |
         v                      v                      v
   [Aggregate]           [Aggregate]           [Aggregate]
```

Trade-off: adds up to `window_seconds` of latency, but achieves higher
throughput by amortizing overhead across many events.

### 5.6 Windowed Aggregations

Each window computes:

| Metric            | Description                                      |
|-------------------|--------------------------------------------------|
| events_in_window  | Total event count                                |
| event_types       | Count per type (page_view, purchase, etc.)       |
| total_revenue     | Sum of amount for purchase events                |
| purchase_count    | Number of purchase events                        |
| avg_purchase      | total_revenue / purchase_count                   |
| category_revenue  | Revenue per product category                     |
| region_counts     | Event count per geographic region                |
| device_counts     | Event count per device type                      |

Up to 100 window snapshots are kept in memory.

### 5.7 Consumer Offsets

A consumer offset is a bookmark recording the last message read from a
partition. Next read starts from that position:

```
Partition 0:  [msg0] [msg1] [msg2] [msg3] [msg4] [msg5] ...
                                     ^
                              offset = 3 --> next read starts here
```

The processor maintains `_offsets: dict[int, int]` mapping partition IDs to
the next offset to read. After consuming, offsets advance:

```python
self._offsets[pid] = msg.offset + 1
```

This ensures no message is processed twice and none are skipped.

### 5.8 Background Threads for Continuous Processing

The API server handles HTTP requests on the main thread. Two daemon threads
run concurrently:

```
Main Thread (uvicorn / FastAPI)
  |-- Producer Thread  --> generates 20 events/sec, sleeps 1s between batches
  |-- Processor Thread --> aggregates every 5s, sleeps between windows
```

Both are daemon threads (`daemon=True`), so they die automatically when the
main process exits. Thread safety is ensured by `threading.Lock` objects in
the broker and processor.

---

## 6. Code Walkthrough -- producer.py

**Location:** `src/producer.py`

**Domain constants:**

```python
EVENT_TYPES = ["page_view", "purchase", "add_to_cart", "signup", "logout"]
CATEGORIES  = ["electronics", "clothing", "books", "food", "sports"]
REGIONS     = ["us-east", "us-west", "eu-west", "eu-east", "ap-south", "ap-east"]
DEVICES     = ["mobile", "desktop", "tablet"]
```

**generate_event()** builds one event:

- `event_type` chosen by weighted random: page_view 40%, purchase 15%,
  add_to_cart 20%, signup 5%, logout 20%. Mimics real traffic where most
  visits do not result in a sale.
- `amount` is non-zero only for purchase (5--500) and add_to_cart (5--300).
- `user_id` formatted as `user_NNNN` (1--500). The limited pool means the
  same user generates multiple events, making partition routing meaningful.
- `event_id` is a truncated UUID (12 hex chars).

**produce_events(rate)** is an infinite generator that yields events at the
target rate by sleeping `1.0 / rate` seconds between yields.

**produce_batch(size)** generates N events instantly in a list comprehension.
Used by the background producer and the `/produce` endpoint.

---

## 7. Code Walkthrough -- broker.py

**Location:** `src/broker.py`

**Message dataclass:** offset (position in partition), value (event dict),
timestamp (Unix epoch, auto-filled).

**Topic class:**

- `num_partitions` defaults to 3.
- `partitions` is a list of `deque(maxlen=10_000)` -- circular buffers.
- `offsets` tracks the next offset to assign per partition.
- `_lock` ensures thread-safe reads and writes.

**publish(key, value):**
1. Compute `partition_id = hash(key) % num_partitions`.
2. Acquire lock, assign offset, create Message, append to deque.
3. Return `(partition_id, offset)`.

**consume(partition_id, from_offset, max_messages):**
1. Acquire lock, iterate partition deque.
2. Collect messages with `offset >= from_offset`, up to `max_messages`.

Because the deque has a max length, a consumer that falls too far behind will
miss messages -- just like real Kafka's log retention.

**Broker class:** manages multiple topics. `create_topic()` is idempotent.
`publish()` auto-creates topics. `get_stats()` returns nested topic/partition
counts.

---

## 8. Code Walkthrough -- processor.py

**Location:** `src/processor.py`

**Initialization:** receives a Broker reference, topic name, and
`window_seconds` (default 5). Initializes `_offsets` as `defaultdict(int)`,
an empty `windows` list, and `total_processed = 0`.

**_consume_all():** iterates all partitions, reads messages from each starting
at the stored offset, advances offsets. Pull-based model.

**_process_batch(events):** computes all aggregations from Section 5.6 in a
single pass over the event list. Returns a dict tagged with `window_end`.

**process_once():** calls `_consume_all()` then `_process_batch()`. Appends
result to `self.windows`, trims to last 100, updates `total_processed`.

**_run_loop():** background thread loop -- calls `process_once()`, sleeps
`window_seconds`, repeats.

**start() / stop():** launch and join the daemon thread.

**Query methods:** `get_latest_window()` returns the last window,
`get_all_windows()` returns a copy of all windows, `get_stats()` returns
processing statistics.

---

## 9. Code Walkthrough -- api.py

**Location:** `src/api.py`

**Shared state:** a single `Broker` and `StreamProcessor` at module level.

**_background_producer():** every 1 second, generates 20 events with
`produce_batch(size=20)` and publishes each to the broker keyed by `user_id`.

**Lifespan (startup/shutdown):**
1. Creates `"events"` topic with 3 partitions.
2. Starts background producer thread.
3. Starts stream processor.
4. On shutdown: stops both.

**CORS middleware:** `allow_origins=["*"]` lets the Streamlit UI (port 8501)
call the API (port 8000) without browser security errors.

**Endpoints:**

| Method | Path                | Description                                |
|--------|---------------------|--------------------------------------------|
| GET    | `/health`           | `{"status": "healthy"}`                    |
| GET    | `/broker/stats`     | Topic count, per-partition message counts   |
| GET    | `/processor/stats`  | Total processed, windows computed, running  |
| GET    | `/processor/latest` | Most recent window aggregation              |
| GET    | `/processor/windows`| All stored window aggregations              |
| POST   | `/produce`          | Inject events + trigger immediate processing|
| GET    | `/dashboard-data`   | Combined payload for the UI                 |

`/produce` calls `processor.process_once()` synchronously so the caller gets
results immediately. `/dashboard-data` bundles everything into one response to
minimize HTTP round-trips.

---

## 10. Code Walkthrough -- ui.py

**Location:** `src/ui.py`

**API_URL** defaults to `http://localhost:8000`, overridable via environment
variable (Docker sets it to `http://api:8000`).

**api_get(path):** fetches JSON from the API. If unreachable, shows an error
and stops rendering.

**Sidebar:**
- Auto-refresh checkbox (3-second polling, on by default).
- "Produce 500 Events" button (POSTs to `/produce`).
- Broker stats: topic count, per-partition message counts.
- Processor stats: total processed, windows computed, window size.

**Main dashboard:**
- 4-column metrics: events, revenue, purchases, avg purchase.
- Row 1: Events by Type (pie chart, Set2 palette), Revenue by Category (bar
  chart, Teal scale).
- Row 2: Events by Region (bar chart), Events by Device (pie chart, Pastel).
- Historical Timeline: dual-axis line chart (events on left Y-axis in
  steelblue, revenue on right Y-axis in coral). Only shown when 2+ windows
  exist.

**Auto-refresh:** `time.sleep(3)` then `st.rerun()` triggers a full page
re-execution.

---

## 11. Code Walkthrough -- main.py

**Location:** `main.py`

| Command               | What It Does                                       |
|-----------------------|----------------------------------------------------|
| `python main.py api`  | Starts FastAPI on port 8000 with hot reload        |
| `python main.py ui`   | Starts Streamlit on port 8501                      |
| `python main.py all`  | Starts both (API as subprocess, UI in foreground)  |
| `python main.py demo` | Terminal demo: produce 200 events, process, print  |

**Demo mode** is self-contained: creates Broker, generates 200 events,
publishes them, creates StreamProcessor, calls `process_once()`, prints
results. Fastest way to verify the pipeline.

**All mode** starts the API via `Popen`, runs Streamlit in foreground, and
terminates the API subprocess in a `finally` block on exit.

---

## 12. Running the Project

### Quick Demo (No Servers)

```bash
cd POC-03-Real-Time-Streaming
pip install -r requirements.txt
python main.py demo
```

Expected output (numbers will vary):

```
Producing 200 events...
Broker stats: {'num_topics': 1, 'topics': {'events': ...}}

Processed 200 events:
  Event types: {'page_view': 82, 'logout': 38, 'add_to_cart': 42, ...}
  Revenue: $3847.23
  Purchases: 28
  Categories: {'electronics': 1203.45, 'clothing': 892.11, ...}
  Regions: {'us-east': 35, 'us-west': 30, ...}
```

### API Server Only

```bash
python main.py api
# Open http://localhost:8000/docs for Swagger UI
```

### API + Dashboard (Two Terminals)

```bash
# Terminal 1
python main.py api

# Terminal 2
python main.py ui
# Dashboard at http://localhost:8501
```

### API + Dashboard (Single Command)

```bash
python main.py all
```

---

## 13. Testing the API

### Health Check

```bash
curl http://localhost:8000/health
```
```json
{"status": "healthy", "service": "streaming-platform"}
```

### Broker Statistics

```bash
curl http://localhost:8000/broker/stats
```
```json
{
  "num_topics": 1,
  "topics": {
    "events": {
      "topic": "events",
      "num_partitions": 3,
      "total_messages": 240,
      "per_partition": [82, 76, 82]
    }
  }
}
```

### Processor Statistics

```bash
curl http://localhost:8000/processor/stats
```
```json
{
  "total_processed": 200,
  "windows_computed": 4,
  "window_seconds": 5,
  "running": true
}
```

### Latest Window

```bash
curl http://localhost:8000/processor/latest
```
```json
{
  "window_end": "2026-05-18T10:30:05+00:00",
  "events_in_window": 100,
  "event_types": {"page_view": 41, "purchase": 14, "add_to_cart": 21, ...},
  "total_revenue": 2847.33,
  "purchase_count": 14,
  "avg_purchase": 203.38,
  "category_revenue": {"electronics": 1203.45, "clothing": 492.11, ...},
  "region_counts": {"us-east": 18, "us-west": 15, ...},
  "device_counts": {"mobile": 38, "desktop": 34, "tablet": 28}
}
```

### Manually Produce Events

```bash
curl -X POST http://localhost:8000/produce \
  -H "Content-Type: application/json" \
  -d '{"count": 500}'
```

### All Windows / Dashboard Data

```bash
curl http://localhost:8000/processor/windows
curl http://localhost:8000/dashboard-data
```

### Interactive Docs

Visit `http://localhost:8000/docs` (Swagger) or `http://localhost:8000/redoc`.

---

## 14. Running with Docker

### docker-compose.yml Explained

```yaml
services:
  api:
    build: .
    command: uvicorn src.api:app --host 0.0.0.0 --port 8000
    ports: ["8000:8000"]
    healthcheck:
      test: ["CMD", "python", "-c",
             "import requests; requests.get('http://localhost:8000/health')"]
      interval: 10s

  ui:
    build: .
    command: streamlit run src/ui.py --server.port 8501 --server.address 0.0.0.0
    ports: ["8501:8501"]
    environment:
      - API_URL=http://api:8000
    depends_on:
      api:
        condition: service_healthy
```

The UI sets `API_URL=http://api:8000` to reach the API by Docker DNS name.
It waits for the API health check to pass before starting.

### Commands

```bash
docker-compose up --build          # Build and start
docker-compose up --build -d       # Run in background
docker-compose logs -f             # View logs
docker-compose down                # Stop
```

---

## 15. The Full Pipeline in Action

Here is what happens second by second after `python main.py api`:

```
t=0s   Server starts. Lifespan hook fires.
       "events" topic created. Producer + processor threads start.

t=0s   Producer: generates 20 events, publishes to broker, sleeps 1s.
t=1s   Producer: another 20 events.
t=2s   Producer: another 20 events.
  ...

t=5s   Processor wakes up (first window).
       _consume_all() reads ~100 events from all partitions.
       _process_batch() computes aggregations.
       Result stored. Offsets advanced. Sleeps 5s.

t=10s  Processor wakes up (second window).
       Reads only NEW messages (offset-based).
       ~100 more events aggregated.
  ...
```

### ASCII Timeline

```
Producer:  [20] . [20] . [20] . [20] . [20] . [20] . [20] . [20] .
            0s   1s   2s   3s   4s   5s   6s   7s   8s

Broker:    Messages accumulate in partitions 0, 1, 2

Processor:                          [Window 1]          [Window 2]
                                     5s                  10s

UI polls:       [poll]       [poll]       [poll]       [poll]
                 ~3s          ~6s          ~9s          ~12s
```

---

## 16. Troubleshooting

### "Cannot reach the API. Is the server running?"

- Make sure the API is running: `python main.py api`.
- Check port 8000 is free: `lsof -i :8000`.
- In Docker, verify `API_URL` is set correctly.

### "No windows processed yet"

- Wait at least 5 seconds. The first window needs time to fill.

### ModuleNotFoundError: "No module named 'src'"

- Run from the project root, not from inside `src/`:
  ```bash
  cd POC-03-Real-Time-Streaming
  python main.py api
  ```

### Port already in use

```bash
lsof -ti :8000 | xargs kill -9   # Free port 8000
lsof -ti :8501 | xargs kill -9   # Free port 8501
```

### Streamlit shows stale data

- Check the "Auto-refresh (3s)" checkbox in the sidebar.
- Hard-refresh the browser (Cmd+Shift+R / Ctrl+Shift+R).
- Click "Produce 500 Events" to force new data.

### pip install fails

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Docker UI exits immediately

The health check should prevent this. If it still happens:
```bash
docker-compose down && docker-compose up --build
```

---

## 17. Extending the Project

**Easy:**
- Add a new event type: add `"search"` to `EVENT_TYPES` with a weight.
- Change window size: set `window_seconds=10` in `api.py`.
- Increase user pool: change `random.randint(1, 500)` to a larger range.

**Medium:**
- Add a late-event detector: compare event timestamps to window boundaries.
- Add a WebSocket endpoint for real-time push instead of polling.
- Persist windows to disk so analytics survive restarts.
- Add filtering endpoints (by time range, region, or category).

**Advanced:**
- Replace the in-memory broker with real Kafka (`confluent-kafka` client).
- Replace the processor with Spark Structured Streaming.
- Implement exactly-once semantics with idempotent writes and transactional
  offset commits.
- Deploy to Kubernetes with Helm charts.

---

## 18. Glossary

| Term                     | Definition                                                     |
|--------------------------|----------------------------------------------------------------|
| **Broker**               | Middleware that receives, stores, and delivers messages.        |
| **Consumer**             | A process that reads messages from a broker.                   |
| **Consumer Offset**      | Per-partition bookmark tracking the last message read.          |
| **Daemon Thread**        | Background thread killed automatically when the main process exits. |
| **Event**                | Immutable record of something that happened.                   |
| **Hash Partitioning**    | Assigning messages via `hash(key) % num_partitions`.           |
| **Lifespan**             | FastAPI mechanism for startup/shutdown code.                   |
| **Message**              | Unit of data in a broker: offset + value + timestamp.          |
| **Micro-Batch**          | Processing a small batch of events at regular intervals.       |
| **Partition**            | Ordered, append-only sequence of messages within a topic.      |
| **Producer**             | Process that generates and publishes messages to a broker.     |
| **Stream Processing**    | Analyzing data continuously as it arrives.                     |
| **Topic**                | Named channel for publishing and consuming messages.           |
| **Window**               | Fixed time interval over which events are grouped.             |
| **Windowed Aggregation** | Summary statistics computed over events within a time window.  |

---

*End of guide.*

---

## 19. Interview Questions

*Situation-based and technical questions from Data Engineer and Streaming Engineer interviews. Sourced from LinkedIn posts, Glassdoor interview reports, and engineering blog discussions.*

---

### Situational / Behavioral Questions

**Q: "Your real-time fraud detection pipeline falls 45 minutes behind during a flash sale. It's 2am and you're on-call. Walk through your incident response."**

A: Runbook: (1) Check consumer lag first — per-partition lag metrics tell you whether lag is concentrated in one partition (single consumer bottleneck or crash) or spread evenly (system-wide throughput issue). (2) Check producer throughput — did event volume spike 10x due to the flash sale? If producers outpaced consumers, the fix is horizontal scale: spin up more consumer instances (in Kubernetes, increase replicas; in this POC, increase thread count). (3) Check consumer health — are processing threads alive? Is GC pressure causing stop-the-world pauses? Memory-bound consumers slow down when buffers fill. (4) Identify the slow transformation — if throughput is fine but individual event processing is slow, profile the hot path. Common culprits: synchronous database lookups inside the processing loop, regex compiled per-event, or blocking I/O. (5) Quick mitigation: increase parallelism. Long-term: move synchronous lookups to in-memory caches (Redis), precompute reference data, or switch the processor window from 5s to 10s to amortize overhead. Document the incident: timeline, root cause, fix, prevention.

**Q: "Marketing wants to know which users saw an ad and bought something within 1 hour. How do you model this as a streaming problem?"**

A: This is a **temporal join** between two event streams — ad impressions and purchase events. Streaming implementation: (1) **Ad impression stream** — keyed by `user_id`, stored in a state store (Kafka Streams KeyValueStore or Flink keyed state) with a TTL of 60 minutes. When an impression event arrives: `state.put(user_id, impression_event)`. (2) **Purchase event stream** — when a purchase arrives: `impression = state.get(user_id)`. If an impression exists within the last 60 minutes, emit a `conversion_event` with both records merged. If not, the purchase is organic. (3) **Late event handling** — set an allowed lateness of 5 minutes: events arriving up to 5 minutes after window close are still processed and can update already-emitted results. Events older than that go to a dead-letter topic for offline reconciliation. (4) **Scale challenge**: if power users have thousands of impressions, storing all of them inflates state. Optimization: store only the last 3 impressions per user in state (most recent ad is most likely to be causal).

**Q: "A consumer group was accidentally reset to offset 0 and reprocessed 3 days of events. Dashboard revenue now shows 4x the real number. How do you remediate without taking the system down?"**

A: The root cause is idempotency failure — the sink accepted duplicate aggregations instead of replacing them. Three-step remediation: (1) **Identify the duplicate window** — from audit logs, determine the exact offset range that was reprocessed. Cross-reference with window timestamps in the `windows` store. (2) **Idempotent re-write** — the sink table (or in this POC, the in-memory `windows` list) must be recomputed from scratch for the affected time range. Use `window_id = f"{window_start_iso}_{window_end_iso}"` as the primary key. A UPSERT/REPLACE into the dashboard table with this key fixes duplicates without downtime. (3) **Prevent recurrence** — all consumer offset resets require a change management approval. Store the target offset in a `consumer_offset_changes` audit table with requester, justification, and rollback plan. Enable Kafka's `allow.auto.create.topics=false` and `auto.offset.reset=latest` (not `earliest`) on all production consumers.

---

### Technical Deep-Dive Questions

**Q: "What's the difference between at-least-once, at-most-once, and exactly-once delivery? Which does this POC implement?"**

A: **At-most-once**: messages may be dropped but never duplicated. The consumer acknowledges before processing. If the consumer crashes after acknowledging but before processing, the message is lost. Use case: high-frequency metrics where occasional loss is acceptable (page view counts). **At-least-once**: messages are never lost but may be processed multiple times. The consumer processes and then acknowledges. If the consumer crashes after processing but before acknowledging, it reprocesses on restart. This is what this POC implements — offsets advance after consumption, but in-memory state means a restart reprocesses from the stored offset. **Exactly-once**: no loss, no duplicates. Requires coordination across producer, broker, and consumer — Kafka transactions, idempotent producers (`enable.idempotence=true`), and atomic writes to both the sink and the Kafka offset commit. Implementation cost is high. For fraud detection specifically: exactly-once is required because a duplicate fraud flag could freeze a legitimate customer's account. For revenue dashboards: at-least-once with idempotent sinks (UPSERT by `window_id`) achieves exactly-once semantics at the application layer with lower infrastructure cost.

**Q: "Why hash partition on user_id instead of round-robin? When does hash partitioning create problems?"**

A: **Hash partitioning** guarantees all events from a given user land on the same partition: `partition_id = hash("user_0001") % 3 = 2`. This is essential for stateful per-user computations. If user events spread across partitions, computing a per-user session or purchase count requires cross-partition joins — complex, expensive, and slow. With hash partitioning, all state for user_0001 lives in the consumer for partition 2. **Round-robin** distributes events evenly (maximizes throughput) but loses colocation — you can't do per-user aggregations without a distributed join. **When hash partitioning fails**: hot partitions. If one user_id generates 80% of all events (e.g., a bot attack or a large enterprise account with automated activity), partition 2 becomes overwhelmed while partitions 0 and 1 are idle. Detection: monitor per-partition message count; alert when any partition has > 3x the average. Mitigation: salted keys (`f"{user_id}_{random.randint(0,3)}"`) spread a hot key across multiple partitions at the cost of losing strict per-user colocation.

**Q: "Explain the difference between micro-batch processing (like this POC) and true streaming (like Apache Flink). When do you choose each?"**

A: **Micro-batch** (this POC, also Spark Structured Streaming): events accumulate for a fixed window (5 seconds), then the entire batch is processed atomically. Latency: seconds to minutes. Implementation is simpler — batch semantics, familiar SQL or pandas operations, no per-event state machine complexity. Throughput is high because overhead is amortized across many events. **True streaming** (Flink, Kafka Streams): each event is processed as it arrives, with per-event latency typically < 100ms. State is maintained per key (not per batch) in RocksDB. More complex to implement: you reason about individual events, keyed state, watermarks, and late data handling. Choose micro-batch when: latency of seconds is acceptable (reporting dashboards, business intelligence), the team is familiar with batch patterns, and throughput is the priority. Choose true streaming when: sub-second latency is required (fraud detection, real-time bidding, live leaderboards), per-event state is natural (user sessions, running totals), or you need accurate late-event handling with watermarks.

---

### System Design Questions

**Q: "Design a real-time fraud detection system that must flag suspicious transactions within 500ms of the event being published."**

A: Five-component architecture: (1) **Event producer** — payment service publishes `transaction_event` to Kafka topic `payments` partitioned by `user_id`. Each event includes `user_id`, `amount`, `merchant_category`, `device_fingerprint`, `geolocation`, `timestamp`. (2) **Feature engine (Flink)** — stateful stream processor computes per-user features in real-time: `transactions_last_1min`, `amount_last_10min`, `unique_merchants_last_hour`, `is_new_device`, `geo_velocity` (distance traveled since last transaction). These features are maintained in RocksDB keyed on `user_id`. At each transaction, the feature vector is computed in < 10ms. (3) **ML scoring service** — a lightweight scoring microservice (FastAPI + XGBoost) receives the feature vector and returns a fraud score in < 20ms. Model is pre-loaded in memory. (4) **Decision engine** — if score > 0.85, publish to `fraud_alerts` topic and trigger a soft decline with SMS OTP challenge. Score 0.6–0.85: allow but flag for async review. (5) **Outcome feedback** — confirmed fraud or false positive labels flow back to retrain the model weekly. Total latency budget: Kafka publish (10ms) + Flink (50ms) + ML scoring (20ms) + Kafka decision publish (10ms) = 90ms. Well within 500ms.

**Q: "How would you replace this POC's in-memory broker with Apache Kafka in production? What code changes are required?"**

A: The `Broker` class is already a clean abstraction — swap its implementation without changing the `StreamProcessor` or API layer. Changes: (1) **Producer**: replace `broker.publish(key, value)` with `KafkaProducer(bootstrap_servers=[...]).send(topic, key=key.encode(), value=json.dumps(value).encode())`. Enable idempotence: `enable.idempotence=True`. (2) **Consumer**: replace `broker.consume(partition_id, from_offset, max_messages)` with a `KafkaConsumer` group. Offset management moves from the in-memory `_offsets` dict to Kafka's consumer group protocol (offsets committed to `__consumer_offsets` topic). (3) **Serialization**: add JSON or Avro serialization/deserialization. With Avro + Schema Registry, schema evolution is enforced — a producer can't add a required field without a migration plan. (4) **Topic management**: Kafka topics must be created explicitly via Admin API (not lazily like this POC's broker). Include topic creation in infrastructure-as-code (Terraform or Kafka Gitops). (5) **Monitoring**: replace the `/broker/stats` endpoint with Kafka's JMX metrics exposed via Prometheus + Grafana. Consumer lag per partition is the most critical metric for production streaming systems.
