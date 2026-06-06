# POC-03: Kafka Semantics Simulator

## What This Actually Is

This POC implements Kafka's message semantics — topics, partitions, key-based routing, consumer offsets, and micro-batch processing — entirely in-process using Python threading. No Kafka cluster, no ZooKeeper, no broker process, no network socket.

**This is a deliberate design choice, not a limitation.** Running Kafka locally requires Docker, cluster configuration, and broker management. This simulator lets you study and demonstrate every core Kafka concept in a self-contained Python process that starts in milliseconds.

### Scope & Fidelity → Production path

This is a **concept simulator**, not operational evidence of running a broker. The
production mapping for each simulated piece:

| Simulated here | Production technology | Concern it introduces |
|---|---|---|
| In-memory `Broker` / topics / partitions | Kafka / Redpanda / MSK / Pub/Sub | Replication, ISR, durability, broker ops |
| Key-hash partition routing | Same logic, but partition count is **migration-hard** | Repartitioning forces consumer-group rebalance |
| In-process consumer offsets | Kafka consumer-group offsets (`__consumer_offsets`) | At-least-once vs exactly-once, commit timing |
| Synchronous micro-batch processor | Flink / Kafka Streams / Spark Structured Streaming | Watermarks, windowing, state backends, backpressure |
| (none) delivery guarantees | Idempotent producer + transactions | Exactly-once is a *system* property, not a flag |

What this POC does **not** prove: operating a cluster, surviving broker failure,
backpressure under load, or exactly-once across a real network. Those are the next
POC — not claims to make from this one.

---

## Section 1: What This POC Actually Implements

### The Broker (`src/broker.py`)

The `Broker` class manages named `Topic` objects. Each `Topic` has:

- **Multiple partitions** — implemented as `deque[Message]` objects, one per partition
- **Key-based routing** — `hash(key) % num_partitions` assigns each message to a partition, exactly as Kafka does
- **Per-partition offsets** — integer counters that increment on every publish, matching Kafka's offset semantics
- **Thread-safe access** — `threading.Lock()` on every publish and consume operation

```python
# From src/broker.py
partition_id = hash(key) % self.num_partitions
offset = self.offsets[partition_id]
self.offsets[partition_id] += 1
```

### The Producer (`src/producer.py`)

Generates synthetic streaming events (page_views, purchases, add_to_cart, signups, logouts) with configurable rates. `produce_events(rate=10.0)` is an iterator that yields events at the specified events/second. `produce_batch(size=100)` generates events instantly for load testing.

### The Stream Processor (`src/processor.py`)

`StreamProcessor` tracks per-partition consumer offsets and consumes only new messages since the last poll — the same pattern used by Kafka consumer groups. It computes windowed aggregations per micro-batch:

- Event type counts
- Revenue by category (purchases only)
- Region distribution
- Device distribution

A background thread runs `process_once()` every `window_seconds`. This mirrors Spark Structured Streaming's trigger interval.

### The API and UI

FastAPI exposes broker stats, window results, and a produce endpoint. The Streamlit dashboard shows live event rates and aggregation results.

---

## Section 2: Why This Is Valuable for Learning

Kafka's concepts are easier to understand when you can read the implementation directly.

**What you can learn by reading this code:**

- **Why key-based partitioning matters** — the `hash(key) % num_partitions` line in `broker.py` is exactly how Kafka ensures all messages with the same key land on the same partition. This is the mechanism that guarantees ordering per key.

- **How consumer offsets work** — `StreamProcessor._offsets` tracks how far each partition has been consumed. When `consume(pid, from_offset=self._offsets[pid])` is called, it returns only messages with `offset >= from_offset`. This is why Kafka consumers can replay or skip messages by manipulating their committed offset.

- **Why micro-batching has windows** — `_process_batch()` aggregates all events in a time window. The window size is the trade-off between latency (small window = faster output) and throughput (large window = more efficient aggregation). Spark Streaming uses the same concept.

- **Thread safety in concurrent producers/consumers** — every `Topic.publish()` and `Topic.consume()` acquires a lock. In a real Kafka cluster, this isolation is provided by partition leadership in the broker. Here you see the raw synchronization requirement.

---

## Section 3: Running It Locally

**Prerequisites:**

```bash
pip install -r requirements.txt
```

**Run the full demo (produce events + process + print results):**

```bash
python main.py
```

**Start the REST API:**

```bash
python main.py api
# Server starts on http://localhost:8000
# Interactive docs: http://localhost:8000/docs
```

**Start the Streamlit dashboard:**

```bash
python main.py ui
# Opens on http://localhost:8501
```

**Interact with the broker via API:**

```bash
# Publish a single event
curl -X POST http://localhost:8000/produce \
  -H "Content-Type: application/json" \
  -d '{"key": "user_001", "event_type": "purchase", "amount": 49.99}'

# Get broker stats (topics, partitions, message counts)
curl http://localhost:8000/broker/stats

# Get latest window aggregation
curl http://localhost:8000/processor/latest
```

---

## Section 4: Real Kafka Swap

The core logic of this POC — producing events and consuming/aggregating them — maps directly to `confluent-kafka` or `kafka-python`. The swap is purely in the I/O layer.

### Using confluent-kafka

```bash
pip install confluent-kafka
```

```python
# BEFORE (this POC — in-memory broker)
from src.broker import Broker
broker = Broker()
broker.publish("events", key=event["user_id"], value=event)

# AFTER (confluent-kafka producer)
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092",
})

import json
producer.produce(
    topic="events",
    key=event["user_id"],
    value=json.dumps(event).encode("utf-8"),
)
producer.flush()
```

```python
# BEFORE (this POC — StreamProcessor consuming from in-memory broker)
from src.processor import StreamProcessor
from src.broker import Broker
processor = StreamProcessor(broker, topic_name="events", window_seconds=5)
processor.start()

# AFTER (confluent-kafka consumer)
from confluent_kafka import Consumer
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "stream-processor-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["events"])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    event = json.loads(msg.value().decode("utf-8"))
    # pass to _process_batch() — same aggregation logic, unchanged
```

### Using kafka-python

```bash
pip install kafka-python
```

```python
from kafka import KafkaProducer, KafkaConsumer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
producer.send("events", key=event["user_id"], value=event)

consumer = KafkaConsumer(
    "events",
    bootstrap_servers=["localhost:9092"],
    group_id="stream-processor-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
for message in consumer:
    event = message.value
    # same aggregation logic
```

The `_process_batch()` aggregation logic in `src/processor.py` is unchanged in both cases — only the message source changes from in-memory `deque` to a network-connected Kafka consumer.
