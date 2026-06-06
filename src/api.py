"""FastAPI server exposing the streaming platform over HTTP."""

import logging
import threading
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.broker import Broker
from src.producer import produce_batch
from src.processor import StreamProcessor
from src.settings import CORS_ORIGINS, DEFAULT_TOPIC, NUM_PARTITIONS, PRODUCE_INTERVAL

logger = logging.getLogger(__name__)

# Shared state
broker = Broker()
processor = StreamProcessor(broker, topic_name=DEFAULT_TOPIC, window_seconds=5)
_producer_running = False
_producer_thread = None


def _background_producer() -> None:
    """Continuously produce events in the background."""
    global _producer_running
    logger.info("Background producer started (interval=%.2fs)", PRODUCE_INTERVAL)
    while _producer_running:
        events = produce_batch(size=20)
        for event in events:
            broker.publish(DEFAULT_TOPIC, event["user_id"], event)
        time.sleep(1)
    logger.info("Background producer stopped")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start the producer and processor on app startup."""
    global _producer_running, _producer_thread
    broker.create_topic(DEFAULT_TOPIC, num_partitions=NUM_PARTITIONS)

    # Start background producer
    _producer_running = True
    _producer_thread = threading.Thread(target=_background_producer, daemon=True)
    _producer_thread.start()

    # Start stream processor
    processor.start()
    logger.info("Application startup complete")
    yield

    # Shutdown
    _producer_running = False
    processor.stop()
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Real-Time Streaming Platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ProduceRequest(BaseModel):
    count: int = Field(default=100, ge=1, le=10000, description="Number of events to produce")


# --- Endpoints ---


@app.get("/health")
def health() -> dict:
    return {"status": "healthy", "service": "streaming-platform"}


@app.get("/broker/stats")
def broker_stats() -> dict:
    """Return broker statistics (topics, partitions, message counts)."""
    return broker.get_stats()


@app.get("/processor/stats")
def processor_stats() -> dict:
    """Return processor statistics (total processed, windows)."""
    return processor.get_stats()


@app.get("/processor/latest")
def latest_window() -> dict:
    """Return the most recent window aggregation."""
    window = processor.get_latest_window()
    if not window:
        return {"message": "No windows processed yet. Events are being produced..."}
    return window


@app.get("/processor/windows")
def all_windows() -> dict:
    """Return all historical window aggregations."""
    return {"windows": processor.get_all_windows(), "total": len(processor.get_all_windows())}


@app.post("/produce")
def produce_events(req: ProduceRequest) -> dict:
    """Manually produce a batch of events."""
    logger.info("Producing %d events via API", req.count)
    events = produce_batch(size=req.count)
    for event in events:
        broker.publish(DEFAULT_TOPIC, event["user_id"], event)
    # Trigger immediate processing
    result = processor.process_once()
    return {
        "produced": len(events),
        "window": result if result else "Processing will happen in next window cycle",
    }


@app.get("/dashboard-data")
def dashboard_data() -> dict:
    """Return all data needed by the Streamlit dashboard in one call."""
    return {
        "broker": broker.get_stats(),
        "processor": processor.get_stats(),
        "latest_window": processor.get_latest_window(),
        "windows": processor.get_all_windows()[-20:],  # Last 20 windows
    }
