"""Event producer: generates synthetic streaming events."""

import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator

logger = logging.getLogger(__name__)


EVENT_TYPES = ["page_view", "purchase", "add_to_cart", "signup", "logout"]
CATEGORIES = ["electronics", "clothing", "books", "food", "sports"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-east", "ap-south", "ap-east"]
DEVICES = ["mobile", "desktop", "tablet"]


def generate_event() -> dict:
    """Generate a single synthetic streaming event."""
    event_type = random.choices(
        EVENT_TYPES, weights=[40, 15, 20, 5, 20], k=1
    )[0]

    amount = 0.0
    if event_type == "purchase":
        amount = round(random.uniform(5.0, 500.0), 2)
    elif event_type == "add_to_cart":
        amount = round(random.uniform(5.0, 300.0), 2)

    return {
        "event_id": str(uuid.uuid4())[:12],
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1, 500):04d}",
        "amount": amount,
        "category": random.choice(CATEGORIES),
        "region": random.choice(REGIONS),
        "device": random.choice(DEVICES),
    }


def produce_events(rate: float = 10.0) -> Iterator[dict]:
    """Continuously yield events at the given rate (events/second).

    Args:
        rate: Target events per second.

    Yields:
        Event dicts.
    """
    interval = 1.0 / rate if rate > 0 else 0.1
    while True:
        yield generate_event()
        time.sleep(interval)


def produce_batch(size: int = 100) -> list[dict]:
    """Generate a batch of events instantly."""
    batch = [generate_event() for _ in range(size)]
    logger.debug("Produced batch of %d events", size)
    return batch
