"""Shared fixtures for Real-Time-Streaming tests."""

import pytest

from src.broker import Broker, Topic
from src.processor import StreamProcessor
from src.producer import generate_event, produce_batch


@pytest.fixture
def broker():
    """Return a fresh Broker instance."""
    return Broker()


@pytest.fixture
def topic():
    """Return a fresh Topic with 3 partitions."""
    return Topic("test-topic", num_partitions=3)


@pytest.fixture
def broker_with_topic(broker):
    """Return a Broker with a pre-created 'events' topic."""
    broker.create_topic("events", num_partitions=3)
    return broker


@pytest.fixture
def populated_broker(broker_with_topic):
    """Return a Broker with 'events' topic containing 100 published events."""
    events = produce_batch(size=100)
    for event in events:
        broker_with_topic.publish("events", event["user_id"], event)
    return broker_with_topic


@pytest.fixture
def processor(broker_with_topic):
    """Return a StreamProcessor wired to a broker with an 'events' topic."""
    return StreamProcessor(broker_with_topic, topic_name="events", window_seconds=1)


@pytest.fixture
def sample_events():
    """Return a list of 50 generated events."""
    return produce_batch(size=50)
