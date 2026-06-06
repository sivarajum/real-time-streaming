"""Tests for the stream processor (processor.py)."""

import time

import pytest

from src.broker import Broker
from src.processor import StreamProcessor
from src.producer import produce_batch


class TestProcessBatch:
    """Tests for the internal _process_batch method."""

    def test_empty_batch_returns_empty_dict(self, processor):
        """Processing an empty batch returns {}."""
        result = processor._process_batch([])
        assert result == {}

    def test_batch_returns_expected_keys(self, processor):
        """A processed batch contains all required aggregation keys."""
        events = produce_batch(size=20)
        result = processor._process_batch(events)
        expected_keys = {
            "window_end",
            "events_in_window",
            "event_types",
            "total_revenue",
            "purchase_count",
            "avg_purchase",
            "category_revenue",
            "region_counts",
            "device_counts",
        }
        assert set(result.keys()) == expected_keys

    def test_events_in_window_count(self, processor):
        """events_in_window matches the number of input events."""
        events = produce_batch(size=15)
        result = processor._process_batch(events)
        assert result["events_in_window"] == 15

    def test_event_type_counts_sum(self, processor):
        """Sum of event type counts matches total events."""
        events = produce_batch(size=30)
        result = processor._process_batch(events)
        total = sum(result["event_types"].values())
        assert total == 30

    def test_region_counts_sum(self, processor):
        """Sum of region counts matches total events."""
        events = produce_batch(size=30)
        result = processor._process_batch(events)
        total = sum(result["region_counts"].values())
        assert total == 30

    def test_device_counts_sum(self, processor):
        """Sum of device counts matches total events."""
        events = produce_batch(size=30)
        result = processor._process_batch(events)
        total = sum(result["device_counts"].values())
        assert total == 30

    def test_revenue_is_non_negative(self, processor):
        """Total revenue is never negative."""
        events = produce_batch(size=50)
        result = processor._process_batch(events)
        assert result["total_revenue"] >= 0

    def test_purchase_count_matches(self, processor):
        """purchase_count matches the number of purchase events."""
        events = produce_batch(size=100)
        expected_purchases = sum(1 for e in events if e["event_type"] == "purchase")
        result = processor._process_batch(events)
        assert result["purchase_count"] == expected_purchases

    def test_avg_purchase_correct(self, processor):
        """avg_purchase equals total_revenue / purchase_count when there are purchases."""
        events = produce_batch(size=200)
        result = processor._process_batch(events)
        if result["purchase_count"] > 0:
            expected_avg = round(result["total_revenue"] / result["purchase_count"], 2)
            assert result["avg_purchase"] == expected_avg
        else:
            assert result["avg_purchase"] == 0.0

    def test_window_end_is_iso_timestamp(self, processor):
        """window_end is a valid ISO timestamp string."""
        events = produce_batch(size=5)
        result = processor._process_batch(events)
        from datetime import datetime
        parsed = datetime.fromisoformat(result["window_end"])
        assert parsed is not None

    def test_category_revenue_only_from_purchases(self, processor):
        """category_revenue only contains categories from purchase events."""
        events = [
            {"event_id": "1", "event_type": "purchase", "amount": 100.0,
             "category": "books", "region": "us-east", "device": "mobile"},
            {"event_id": "2", "event_type": "page_view", "amount": 0.0,
             "category": "electronics", "region": "us-west", "device": "desktop"},
        ]
        result = processor._process_batch(events)
        assert "books" in result["category_revenue"]
        assert "electronics" not in result["category_revenue"]
        assert result["category_revenue"]["books"] == 100.0


class TestProcessOnce:
    """Tests for the process_once method (consume + aggregate)."""

    def test_process_once_no_events(self, processor):
        """process_once returns {} when there are no events."""
        result = processor.process_once()
        assert result == {}

    def test_process_once_with_events(self, populated_broker):
        """process_once returns aggregations when events exist."""
        proc = StreamProcessor(populated_broker, topic_name="events")
        result = proc.process_once()
        assert result != {}
        assert result["events_in_window"] == 100

    def test_process_once_tracks_offsets(self, populated_broker):
        """After process_once, a second call returns {} (no new events)."""
        proc = StreamProcessor(populated_broker, topic_name="events")
        result1 = proc.process_once()
        assert result1["events_in_window"] == 100

        result2 = proc.process_once()
        assert result2 == {}  # All consumed

    def test_process_once_incremental(self, broker_with_topic):
        """process_once picks up only new events on each call."""
        proc = StreamProcessor(broker_with_topic, topic_name="events")
        events = produce_batch(size=10)
        for e in events:
            broker_with_topic.publish("events", e["user_id"], e)

        r1 = proc.process_once()
        assert r1["events_in_window"] == 10

        # Add more events
        more = produce_batch(size=5)
        for e in more:
            broker_with_topic.publish("events", e["user_id"], e)

        r2 = proc.process_once()
        assert r2["events_in_window"] == 5

    def test_total_processed_accumulates(self, broker_with_topic):
        """total_processed accumulates across multiple process_once calls."""
        proc = StreamProcessor(broker_with_topic, topic_name="events")
        batch1 = produce_batch(size=10)
        for e in batch1:
            broker_with_topic.publish("events", e["user_id"], e)
        proc.process_once()

        batch2 = produce_batch(size=15)
        for e in batch2:
            broker_with_topic.publish("events", e["user_id"], e)
        proc.process_once()

        assert proc.total_processed == 25


class TestWindows:
    """Tests for window storage and retrieval."""

    def test_get_latest_window_empty(self, processor):
        """get_latest_window returns {} when no windows exist."""
        assert processor.get_latest_window() == {}

    def test_get_latest_window_after_processing(self, populated_broker):
        """get_latest_window returns the last window result."""
        proc = StreamProcessor(populated_broker, topic_name="events")
        result = proc.process_once()
        latest = proc.get_latest_window()
        assert latest == result

    def test_get_all_windows(self, broker_with_topic):
        """get_all_windows returns all computed windows."""
        proc = StreamProcessor(broker_with_topic, topic_name="events")
        for i in range(3):
            batch = produce_batch(size=5)
            for e in batch:
                broker_with_topic.publish("events", e["user_id"], e)
            proc.process_once()

        windows = proc.get_all_windows()
        assert len(windows) == 3

    def test_windows_capped_at_100(self, broker_with_topic):
        """Only the last 100 windows are retained."""
        proc = StreamProcessor(broker_with_topic, topic_name="events")
        for i in range(105):
            batch = produce_batch(size=1)
            for e in batch:
                broker_with_topic.publish("events", e["user_id"], e)
            proc.process_once()

        windows = proc.get_all_windows()
        assert len(windows) == 100


class TestProcessorStats:
    """Tests for processor statistics."""

    def test_stats_initial(self, processor):
        """Initial stats show zero processing."""
        stats = processor.get_stats()
        assert stats["total_processed"] == 0
        assert stats["windows_computed"] == 0
        assert stats["window_seconds"] == 1
        assert stats["running"] is False

    def test_stats_after_processing(self, populated_broker):
        """Stats reflect processing activity."""
        proc = StreamProcessor(populated_broker, topic_name="events", window_seconds=2)
        proc.process_once()
        stats = proc.get_stats()
        assert stats["total_processed"] == 100
        assert stats["windows_computed"] == 1
        assert stats["window_seconds"] == 2


class TestProcessorLifecycle:
    """Tests for start/stop background processing."""

    def test_start_and_stop(self, broker_with_topic):
        """Processor can start and stop without errors."""
        proc = StreamProcessor(broker_with_topic, topic_name="events", window_seconds=1)
        proc.start()
        assert proc._running is True
        assert proc._thread is not None
        assert proc._thread.is_alive()

        proc.stop()
        assert proc._running is False

    def test_start_idempotent(self, broker_with_topic):
        """Calling start twice does not create a second thread."""
        proc = StreamProcessor(broker_with_topic, topic_name="events", window_seconds=1)
        proc.start()
        thread1 = proc._thread
        proc.start()
        thread2 = proc._thread
        assert thread1 is thread2
        proc.stop()

    def test_background_processing(self, broker_with_topic):
        """Background processor picks up events published while running."""
        proc = StreamProcessor(broker_with_topic, topic_name="events", window_seconds=1)
        # Publish some events before starting
        batch = produce_batch(size=20)
        for e in batch:
            broker_with_topic.publish("events", e["user_id"], e)

        proc.start()
        # Wait for at least one processing cycle
        time.sleep(2)
        proc.stop()

        assert proc.total_processed >= 20
        assert len(proc.windows) >= 1
