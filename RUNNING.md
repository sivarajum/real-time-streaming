# Real-Time Streaming Platform

An in-memory real-time streaming platform that simulates Kafka-style message brokering with event production, micro-batch processing, and live dashboards. Built with Python, FastAPI, and Streamlit.

## Prerequisites

- Python 3.11+
- pip
- Docker and Docker Compose (optional, for containerized deployment)

## Installation

```bash
cd Sj-Prod/Real-Time-Streaming
pip install -r requirements.txt
```

## Running Tests

```bash
# Run all tests with coverage
pytest

# Run a specific test file
pytest tests/test_broker.py
pytest tests/test_producer.py
pytest tests/test_processor.py
pytest tests/test_api.py

# Run a single test
pytest tests/test_broker.py::TestBroker::test_create_topic

# Run without coverage output
pytest --no-cov

# Run with verbose output
pytest -v
```

Tests are configured via `pytest.ini` with coverage enabled by default (reports on the `src/` package).

## Running the System

The system is launched via `main.py` with one of four modes:

```bash
# Start FastAPI server on port 8000 (default)
python main.py api

# Start Streamlit dashboard on port 8501
python main.py ui

# Start both API and UI together
python main.py all

# Quick demo: produce 200 events, process, and print results
python main.py demo
```

### Mode Details

| Mode   | Description                                      | Ports     |
|--------|--------------------------------------------------|-----------|
| `api`  | FastAPI server with hot reload                   | 8000      |
| `ui`   | Streamlit dashboard (requires API running)       | 8501      |
| `all`  | Launches API and UI in parallel                  | 8000+8501 |
| `demo` | CLI demo: generates events, processes, prints    | N/A       |

When running `ui` mode separately, ensure the API is already running on port 8000 (or set the `API_URL` environment variable).

## Running with Docker

```bash
# Build and start both API and UI containers
docker compose up --build

# Run in detached mode
docker compose up --build -d

# Stop
docker compose down
```

Docker Compose starts two services:
- `api` on port 8000 with a health check
- `ui` on port 8501 (waits for API to be healthy)

## API Endpoint Reference

All endpoints are served at `http://localhost:8000`.

| Method | Path                | Description                                  |
|--------|---------------------|----------------------------------------------|
| GET    | `/health`           | Health check; returns `{"status": "healthy"}` |
| GET    | `/broker/stats`     | Broker statistics: topics, partitions, message counts |
| GET    | `/processor/stats`  | Processor statistics: total processed, window count, running state |
| GET    | `/processor/latest` | Most recent window aggregation result        |
| GET    | `/processor/windows`| All historical window aggregation results    |
| POST   | `/produce`          | Produce a batch of events; body: `{"count": N}` (default 100) |
| GET    | `/dashboard-data`   | Combined data for the Streamlit dashboard    |

### Example requests

```bash
# Health check
curl http://localhost:8000/health

# Produce 200 events
curl -X POST http://localhost:8000/produce -H "Content-Type: application/json" -d '{"count": 200}'

# Get broker statistics
curl http://localhost:8000/broker/stats

# Get latest processing window
curl http://localhost:8000/processor/latest
```

## Architecture Overview

The system has three core components that form a streaming pipeline:

```
Producer --> Broker --> Processor --> API/Dashboard
```

### Broker (`src/broker.py`)

An in-memory message broker simulating Apache Kafka:

- **Topic**: A named channel with configurable partitions (default 3). Each partition is a bounded deque (max 10,000 messages). Messages are routed to partitions by hashing the message key.
- **Broker**: Manages multiple topics. Supports creating topics, publishing messages (auto-creates topics on demand), and consuming messages from specific partitions with offset tracking.
- Thread-safe via `threading.Lock` on all mutable operations.

### Producer (`src/producer.py`)

Generates synthetic e-commerce streaming events with fields:

- `event_id`, `event_type`, `timestamp`, `user_id`, `amount`, `category`, `region`, `device`
- Event types: `page_view`, `purchase`, `add_to_cart`, `signup`, `logout` (weighted distribution)
- Three modes: single event (`generate_event`), batch (`produce_batch`), continuous iterator (`produce_events`)

### Processor (`src/processor.py`)

A stream processor that simulates Spark Streaming micro-batch processing:

- Consumes all new messages across all partitions, tracking consumer offsets
- Computes window aggregations: event type counts, revenue totals, category revenue, region/device breakdowns
- Stores the last 100 windows
- Can run as a background thread with configurable window intervals

### API (`src/api.py`)

FastAPI application with a lifespan manager that:

- Starts a background producer (20 events/second)
- Starts the stream processor (5-second windows)
- Exposes REST endpoints for stats, windows, manual event production, and dashboard data

### UI (`src/ui.py`)

Streamlit dashboard providing:

- Real-time metrics (events, revenue, purchases)
- Pie charts (event types, devices), bar charts (categories, regions)
- Historical window timeline with dual-axis chart
- Auto-refresh every 3 seconds
- Manual event production button

## Production Deployment Notes

- **Scaling**: The in-memory broker is single-process. For production, replace with Apache Kafka, AWS Kinesis, or Google Pub/Sub.
- **Persistence**: All data is in-memory and lost on restart. Add a persistence layer (database, object storage) for durable state.
- **Configuration**: Use environment variables for `API_URL`, port numbers, and window sizes. The `.env` pattern is supported.
- **Monitoring**: The `/health` endpoint supports container orchestrator health checks. Add Prometheus metrics for production observability.
- **Security**: CORS is set to allow all origins (`*`). Restrict this in production. Add authentication/authorization as needed.
- **Resource limits**: Partition deques are capped at 10,000 messages and window history at 100 entries to bound memory usage.
