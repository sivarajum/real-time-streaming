# Real-Time Streaming

Kafka-style event streaming simulator — in-memory broker with topics, partitions, key-based routing, consumer offsets, and windowed micro-batch processing. FastAPI API + Streamlit dashboard.

## What It Does

- **In-Memory Broker**: Kafka-equivalent topics and partitions using Python threading
- **Key-Based Routing**: `hash(key) % num_partitions` for partition assignment (same as Kafka)
- **Consumer Offsets**: Per-partition offset tracking for exactly-once-style consumption
- **Event Producer**: Synthetic streaming events (page views, purchases, cart adds, signups) at configurable rates
- **Windowed Processing**: Micro-batch aggregation (event counts, revenue, region/device distributions)
- **REST API**: FastAPI endpoints for broker stats, event production, window results
- **Dashboard**: Streamlit UI for live event rates and aggregation visualization

## Architecture

```
src/broker.py       # In-memory Broker + Topic + Message (thread-safe partitions)
src/producer.py     # Event generator (batch + streaming modes)
src/processor.py    # StreamProcessor (consumer offsets + windowed aggregation)
src/api.py          # FastAPI REST API
src/ui.py           # Streamlit dashboard
```

## Quick Start

```bash
pip install -r requirements.txt
python main.py api         # API on :8004
python main.py ui          # Dashboard on :8501
python main.py all         # Both
```

## Testing

```bash
pytest                     # 90 tests
```

## Docker

```bash
docker compose up --build
```

See [RUNNING.md](RUNNING.md) for full build, test, and deployment instructions.
