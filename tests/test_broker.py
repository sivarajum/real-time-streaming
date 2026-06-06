"""Tests for the in-memory message broker (broker.py)."""

import threading

import pytest

from src.broker import Broker, Message, Topic


class TestTopic:
    """Tests for the Topic class."""

    def test_topic_creation(self, topic):
        """Topic is created with correct name and partition count."""
        assert topic.name == "test-topic"
        assert topic.num_partitions == 3
        assert len(topic.partitions) == 3

    def test_topic_custom_partitions(self):
        """Topic respects custom partition count."""
        t = Topic("custom", num_partitions=5)
        assert t.num_partitions == 5
        assert len(t.partitions) == 5
        assert len(t.offsets) == 5

    def test_publish_returns_partition_and_offset(self, topic):
        """Publishing a message returns (partition_id, offset)."""
        partition_id, offset = topic.publish("key-1", {"data": "value"})
        assert 0 <= partition_id < topic.num_partitions
        assert offset == 0  # first message in that partition

    def test_publish_increments_offset(self, topic):
        """Successive publishes to the same partition increment the offset."""
        # Use the same key to guarantee same partition
        _, offset1 = topic.publish("same-key", {"seq": 1})
        _, offset2 = topic.publish("same-key", {"seq": 2})
        assert offset2 == offset1 + 1

    def test_partition_routing_by_key_hash(self, topic):
        """Messages with the same key always go to the same partition."""
        p1, _ = topic.publish("user_42", {"a": 1})
        p2, _ = topic.publish("user_42", {"a": 2})
        p3, _ = topic.publish("user_42", {"a": 3})
        assert p1 == p2 == p3

    def test_different_keys_can_route_to_different_partitions(self):
        """Different keys can map to different partitions (with enough keys)."""
        t = Topic("multi", num_partitions=4)
        partitions_seen = set()
        for i in range(100):
            pid, _ = t.publish(f"key-{i}", {"i": i})
            partitions_seen.add(pid)
        # With 100 different keys and 4 partitions, we expect multiple partitions used
        assert len(partitions_seen) > 1

    def test_consume_from_empty_partition(self, topic):
        """Consuming from an empty partition returns an empty list."""
        msgs = topic.consume(partition_id=0, from_offset=0)
        assert msgs == []

    def test_consume_returns_published_messages(self, topic):
        """Consumed messages match what was published."""
        topic.publish("k1", {"val": "hello"})
        topic.publish("k1", {"val": "world"})
        pid = hash("k1") % topic.num_partitions
        msgs = topic.consume(pid, from_offset=0)
        values = [m.value for m in msgs]
        assert {"val": "hello"} in values
        assert {"val": "world"} in values

    def test_consume_respects_from_offset(self, topic):
        """Consuming with from_offset skips earlier messages."""
        topic.publish("same", {"seq": 0})
        topic.publish("same", {"seq": 1})
        topic.publish("same", {"seq": 2})
        pid = hash("same") % topic.num_partitions
        msgs = topic.consume(pid, from_offset=2)
        assert len(msgs) == 1
        assert msgs[0].value == {"seq": 2}

    def test_consume_respects_max_messages(self, topic):
        """Consuming with max_messages limits the number of results."""
        for i in range(10):
            topic.publish("same", {"seq": i})
        pid = hash("same") % topic.num_partitions
        msgs = topic.consume(pid, from_offset=0, max_messages=3)
        assert len(msgs) == 3

    def test_message_has_timestamp(self, topic):
        """Published messages have a timestamp."""
        topic.publish("k", {"data": 1})
        pid = hash("k") % topic.num_partitions
        msgs = topic.consume(pid)
        assert len(msgs) == 1
        assert isinstance(msgs[0].timestamp, float)
        assert msgs[0].timestamp > 0

    def test_get_stats(self, topic):
        """Topic stats reflect published messages."""
        topic.publish("a", {"x": 1})
        topic.publish("b", {"x": 2})
        topic.publish("a", {"x": 3})
        stats = topic.get_stats()
        assert stats["topic"] == "test-topic"
        assert stats["num_partitions"] == 3
        assert stats["total_messages"] == 3
        assert isinstance(stats["per_partition"], list)
        assert sum(stats["per_partition"]) == 3


class TestBroker:
    """Tests for the Broker class."""

    def test_broker_starts_empty(self, broker):
        """A new broker has no topics."""
        assert len(broker.topics) == 0

    def test_create_topic(self, broker):
        """Creating a topic adds it to the broker."""
        t = broker.create_topic("orders", num_partitions=4)
        assert t.name == "orders"
        assert t.num_partitions == 4
        assert "orders" in broker.topics

    def test_create_topic_idempotent(self, broker):
        """Creating the same topic twice returns the existing one."""
        t1 = broker.create_topic("clicks")
        t2 = broker.create_topic("clicks")
        assert t1 is t2

    def test_list_topics(self, broker):
        """Created topics are listed in broker.topics."""
        broker.create_topic("a")
        broker.create_topic("b")
        broker.create_topic("c")
        assert set(broker.topics.keys()) == {"a", "b", "c"}

    def test_get_topic_existing(self, broker):
        """get_topic returns the topic if it exists."""
        broker.create_topic("test")
        assert broker.get_topic("test") is not None
        assert broker.get_topic("test").name == "test"

    def test_get_topic_missing(self, broker):
        """get_topic returns None for a missing topic."""
        assert broker.get_topic("nonexistent") is None

    def test_publish_creates_topic_if_missing(self, broker):
        """Publishing to a nonexistent topic auto-creates it."""
        pid, offset = broker.publish("auto-topic", "k", {"v": 1})
        assert "auto-topic" in broker.topics
        assert offset == 0

    def test_publish_and_consume_round_trip(self, broker):
        """Messages survive a publish -> consume round trip."""
        broker.create_topic("test", num_partitions=2)
        broker.publish("test", "user1", {"action": "click"})
        topic = broker.get_topic("test")
        pid = hash("user1") % 2
        msgs = topic.consume(pid)
        assert len(msgs) == 1
        assert msgs[0].value == {"action": "click"}

    def test_broker_stats(self, broker):
        """Broker stats show correct topic and message counts."""
        broker.create_topic("t1")
        broker.publish("t1", "k", {"x": 1})
        broker.publish("t1", "k", {"x": 2})
        broker.create_topic("t2")
        stats = broker.get_stats()
        assert stats["num_topics"] == 2
        assert "t1" in stats["topics"]
        assert "t2" in stats["topics"]
        assert stats["topics"]["t1"]["total_messages"] == 2
        assert stats["topics"]["t2"]["total_messages"] == 0

    def test_consumer_offset_tracking(self, broker):
        """Consuming with tracked offsets avoids re-reading messages."""
        broker.create_topic("events", num_partitions=1)
        # Publish 5 messages
        for i in range(5):
            broker.publish("events", "same-key", {"seq": i})

        topic = broker.get_topic("events")
        pid = hash("same-key") % 1  # always 0

        # First consume: get all 5
        batch1 = topic.consume(pid, from_offset=0)
        assert len(batch1) == 5
        next_offset = batch1[-1].offset + 1

        # Publish 3 more
        for i in range(5, 8):
            broker.publish("events", "same-key", {"seq": i})

        # Second consume: only the new 3
        batch2 = topic.consume(pid, from_offset=next_offset)
        assert len(batch2) == 3
        assert batch2[0].value["seq"] == 5

    def test_thread_safety_publish(self, broker):
        """Concurrent publishes do not lose messages."""
        broker.create_topic("concurrent", num_partitions=1)
        errors = []

        def publisher(n):
            try:
                for i in range(50):
                    broker.publish("concurrent", "key", {"thread": n, "seq": i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=publisher, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        topic = broker.get_topic("concurrent")
        stats = topic.get_stats()
        assert stats["total_messages"] == 200  # 4 threads x 50 messages
