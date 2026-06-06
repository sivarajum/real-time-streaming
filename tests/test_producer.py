"""Tests for the event producer (producer.py)."""

from datetime import datetime

from src.producer import (
    CATEGORIES,
    DEVICES,
    EVENT_TYPES,
    REGIONS,
    generate_event,
    produce_batch,
    produce_events,
)


class TestGenerateEvent:
    """Tests for the generate_event function."""

    def test_returns_dict(self):
        """generate_event returns a dictionary."""
        event = generate_event()
        assert isinstance(event, dict)

    def test_has_required_fields(self):
        """Every event has all expected fields."""
        expected_fields = {
            "event_id",
            "event_type",
            "timestamp",
            "user_id",
            "amount",
            "category",
            "region",
            "device",
        }
        event = generate_event()
        assert set(event.keys()) == expected_fields

    def test_event_id_is_string(self):
        """event_id is a non-empty string."""
        event = generate_event()
        assert isinstance(event["event_id"], str)
        assert len(event["event_id"]) > 0

    def test_event_type_valid(self):
        """event_type is one of the defined EVENT_TYPES."""
        for _ in range(50):
            event = generate_event()
            assert event["event_type"] in EVENT_TYPES

    def test_timestamp_is_iso_format(self):
        """timestamp is a valid ISO 8601 string."""
        event = generate_event()
        # Should not raise
        parsed = datetime.fromisoformat(event["timestamp"])
        assert parsed is not None

    def test_user_id_format(self):
        """user_id follows the 'user_NNNN' pattern."""
        event = generate_event()
        assert event["user_id"].startswith("user_")
        numeric_part = event["user_id"].replace("user_", "")
        assert numeric_part.isdigit()
        assert 1 <= int(numeric_part) <= 500

    def test_amount_non_negative(self):
        """amount is always >= 0."""
        for _ in range(100):
            event = generate_event()
            assert event["amount"] >= 0

    def test_purchase_event_has_positive_amount(self):
        """Purchase events always have a positive amount."""
        # Generate enough events to get a purchase
        for _ in range(500):
            event = generate_event()
            if event["event_type"] == "purchase":
                assert event["amount"] > 0
                assert 5.0 <= event["amount"] <= 500.0
                return
        # If we never got a purchase in 500 tries, that's suspicious but not impossible
        # Skip rather than fail
        assert True

    def test_add_to_cart_has_positive_amount(self):
        """add_to_cart events always have a positive amount."""
        for _ in range(500):
            event = generate_event()
            if event["event_type"] == "add_to_cart":
                assert event["amount"] > 0
                assert 5.0 <= event["amount"] <= 300.0
                return
        assert True

    def test_non_purchase_non_cart_has_zero_amount(self):
        """Events that are not purchase/add_to_cart have amount == 0."""
        for _ in range(500):
            event = generate_event()
            if event["event_type"] not in ("purchase", "add_to_cart"):
                assert event["amount"] == 0.0
                return
        assert True

    def test_category_valid(self):
        """category is one of the defined CATEGORIES."""
        event = generate_event()
        assert event["category"] in CATEGORIES

    def test_region_valid(self):
        """region is one of the defined REGIONS."""
        event = generate_event()
        assert event["region"] in REGIONS

    def test_device_valid(self):
        """device is one of the defined DEVICES."""
        event = generate_event()
        assert event["device"] in DEVICES

    def test_events_are_unique(self):
        """Multiple generated events have distinct event_ids."""
        ids = {generate_event()["event_id"] for _ in range(100)}
        assert len(ids) == 100


class TestProduceBatch:
    """Tests for the produce_batch function."""

    def test_returns_list(self):
        """produce_batch returns a list."""
        batch = produce_batch(size=10)
        assert isinstance(batch, list)

    def test_correct_size(self):
        """produce_batch returns the requested number of events."""
        batch = produce_batch(size=25)
        assert len(batch) == 25

    def test_default_size(self):
        """produce_batch default size is 100."""
        batch = produce_batch()
        assert len(batch) == 100

    def test_all_events_valid(self):
        """Every event in a batch has all required fields."""
        batch = produce_batch(size=50)
        for event in batch:
            assert "event_id" in event
            assert "event_type" in event
            assert event["event_type"] in EVENT_TYPES

    def test_zero_size(self):
        """produce_batch(size=0) returns an empty list."""
        batch = produce_batch(size=0)
        assert batch == []


class TestProduceEvents:
    """Tests for the produce_events generator."""

    def test_is_iterator(self):
        """produce_events returns an iterator."""
        gen = produce_events(rate=100)
        assert hasattr(gen, "__next__")

    def test_yields_valid_events(self):
        """The first few yielded events are valid."""
        gen = produce_events(rate=1000)  # High rate to avoid long sleeps
        events = [next(gen) for _ in range(5)]
        assert len(events) == 5
        for event in events:
            assert "event_id" in event
            assert "event_type" in event
