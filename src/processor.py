"""Stream processor: consumes events, computes windowed aggregations."""

import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from src.broker import Broker

logger = logging.getLogger(__name__)


class StreamProcessor:
    """Simulates Spark Streaming micro-batch processing.

    Consumes events from the broker in configurable windows,
    computes aggregations, and stores results.
    """

    def __init__(self, broker: Broker, topic_name: str = "events", window_seconds: int = 5) -> None:
        self.broker = broker
        self.topic_name = topic_name
        self.window_seconds = window_seconds

        # Consumer offsets per partition
        self._offsets: dict[int, int] = defaultdict(int)

        # Aggregation results
        self.windows: list[dict] = []
        self.total_processed: int = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        logger.info(
            "StreamProcessor initialised (topic=%s, window=%ds)",
            topic_name,
            window_seconds,
        )

    def _process_batch(self, events: list[dict]) -> dict:
        """Process a micro-batch of events and return aggregations."""
        if not events:
            return {}

        now = datetime.now(timezone.utc).isoformat()

        # Counts by event type
        type_counts: dict[str, int] = defaultdict(int)
        category_revenue: dict[str, float] = defaultdict(float)
        region_counts: dict[str, int] = defaultdict(int)
        device_counts: dict[str, int] = defaultdict(int)
        total_revenue = 0.0
        purchase_count = 0

        for event in events:
            type_counts[event["event_type"]] += 1
            region_counts[event["region"]] += 1
            device_counts[event["device"]] += 1

            if event["event_type"] == "purchase":
                total_revenue += event["amount"]
                purchase_count += 1
                category_revenue[event["category"]] += event["amount"]

        return {
            "window_end": now,
            "events_in_window": len(events),
            "event_types": dict(type_counts),
            "total_revenue": round(total_revenue, 2),
            "purchase_count": purchase_count,
            "avg_purchase": round(total_revenue / purchase_count, 2) if purchase_count else 0.0,
            "category_revenue": {k: round(v, 2) for k, v in category_revenue.items()},
            "region_counts": dict(region_counts),
            "device_counts": dict(device_counts),
        }

    def _consume_all(self) -> list[dict]:
        """Consume all new messages from all partitions."""
        topic = self.broker.get_topic(self.topic_name)
        if topic is None:
            return []

        all_events = []
        for pid in range(topic.num_partitions):
            messages = topic.consume(pid, from_offset=self._offsets[pid])
            for msg in messages:
                all_events.append(msg.value)
                self._offsets[pid] = msg.offset + 1
        return all_events

    def process_once(self) -> dict:
        """Run one processing cycle: consume and aggregate."""
        events = self._consume_all()
        if not events:
            return {}

        result = self._process_batch(events)
        with self._lock:
            self.total_processed += len(events)
            self.windows.append(result)
            # Keep only last 100 windows
            if len(self.windows) > 100:
                self.windows = self.windows[-100:]
        logger.info(
            "Window processed: %d events, revenue=$%.2f, windows_stored=%d",
            len(events),
            result.get("total_revenue", 0.0),
            len(self.windows),
        )
        return result

    def _run_loop(self) -> None:
        """Background processing loop."""
        while self._running:
            self.process_once()
            time.sleep(self.window_seconds)

    def start(self) -> None:
        """Start background processing."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("StreamProcessor background loop started")

    def stop(self) -> None:
        """Stop background processing."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("StreamProcessor background loop stopped")

    def get_latest_window(self) -> dict:
        """Return the most recent window aggregation."""
        with self._lock:
            if self.windows:
                return self.windows[-1]
        return {}

    def get_all_windows(self) -> list[dict]:
        """Return all window aggregations."""
        with self._lock:
            return list(self.windows)

    def get_stats(self) -> dict:
        """Return processor statistics."""
        with self._lock:
            return {
                "total_processed": self.total_processed,
                "windows_computed": len(self.windows),
                "window_seconds": self.window_seconds,
                "running": self._running,
            }
