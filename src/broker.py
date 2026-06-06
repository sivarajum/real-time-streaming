"""In-memory message broker simulating Kafka topics and partitions."""

import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A single message in a topic partition."""
    offset: int
    value: dict
    timestamp: float = field(default_factory=time.time)


class Topic:
    """A topic with multiple partitions, simulating Kafka."""

    def __init__(self, name: str, num_partitions: int = 3) -> None:
        self.name = name
        self.num_partitions = num_partitions
        self.partitions: list[deque[Message]] = [
            deque(maxlen=10_000) for _ in range(num_partitions)
        ]
        self.offsets: list[int] = [0] * num_partitions
        self._lock = threading.Lock()
        logger.info("Topic '%s' created with %d partitions", name, num_partitions)

    def publish(self, key: str, value: dict) -> tuple[int, int]:
        """Publish a message to a partition based on the key hash.

        Returns:
            (partition_id, offset)
        """
        partition_id = hash(key) % self.num_partitions
        with self._lock:
            offset = self.offsets[partition_id]
            self.offsets[partition_id] += 1
            msg = Message(offset=offset, value=value)
            self.partitions[partition_id].append(msg)
        return partition_id, offset

    def consume(
        self, partition_id: int, from_offset: int = 0, max_messages: int = 100
    ) -> list[Message]:
        """Consume messages from a partition starting at the given offset."""
        with self._lock:
            partition = self.partitions[partition_id]
            messages = []
            for msg in partition:
                if msg.offset >= from_offset and len(messages) < max_messages:
                    messages.append(msg)
            return messages

    def get_stats(self) -> dict:
        """Return topic statistics."""
        with self._lock:
            total = sum(len(p) for p in self.partitions)
            per_partition = [len(p) for p in self.partitions]
        return {
            "topic": self.name,
            "num_partitions": self.num_partitions,
            "total_messages": total,
            "per_partition": per_partition,
        }


class Broker:
    """In-memory message broker managing multiple topics."""

    def __init__(self) -> None:
        self.topics: dict[str, Topic] = {}
        self._lock = threading.Lock()
        logger.info("Broker initialised")

    def create_topic(self, name: str, num_partitions: int = 3) -> Topic:
        """Create a topic if it doesn't exist."""
        with self._lock:
            if name not in self.topics:
                self.topics[name] = Topic(name, num_partitions)
                logger.info("Broker: created topic '%s' (%d partitions)", name, num_partitions)
            return self.topics[name]

    def get_topic(self, name: str) -> Optional[Topic]:
        """Get a topic by name."""
        return self.topics.get(name)

    def publish(self, topic_name: str, key: str, value: dict) -> tuple[int, int]:
        """Publish a message to a topic."""
        topic = self.get_topic(topic_name)
        if topic is None:
            topic = self.create_topic(topic_name)
        return topic.publish(key, value)

    def get_stats(self) -> dict:
        """Return broker-wide statistics."""
        return {
            "num_topics": len(self.topics),
            "topics": {
                name: topic.get_stats() for name, topic in self.topics.items()
            },
        }
