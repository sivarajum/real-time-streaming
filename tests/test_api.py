"""Tests for the FastAPI endpoints (api.py)."""

import pytest
from fastapi.testclient import TestClient

from src.api import app, broker, processor
from src.producer import produce_batch


@pytest.fixture(autouse=True)
def reset_broker_state():
    """Reset broker and processor state before each test."""
    # Clear topics
    broker.topics.clear()
    broker.create_topic("events", num_partitions=3)

    # Reset processor state
    processor.windows.clear()
    processor.total_processed = 0
    processor._offsets.clear()
    yield


@pytest.fixture
def client():
    """Return a FastAPI TestClient (no lifespan to avoid background threads)."""
    return TestClient(app, raise_server_exceptions=True)


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_returns_200(self, client):
        """Health endpoint returns 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_body(self, client):
        """Health endpoint returns expected JSON."""
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "streaming-platform"


class TestBrokerStatsEndpoint:
    """Tests for GET /broker/stats."""

    def test_returns_200(self, client):
        """Broker stats endpoint returns 200."""
        response = client.get("/broker/stats")
        assert response.status_code == 200

    def test_shows_topics(self, client):
        """Broker stats show created topics."""
        data = client.get("/broker/stats").json()
        assert "num_topics" in data
        assert "topics" in data
        assert data["num_topics"] >= 1
        assert "events" in data["topics"]

    def test_message_count_increases(self, client):
        """Broker stats reflect message count after producing."""
        client.post("/produce", json={"count": 10})
        data = client.get("/broker/stats").json()
        assert data["topics"]["events"]["total_messages"] >= 10


class TestProcessorStatsEndpoint:
    """Tests for GET /processor/stats."""

    def test_returns_200(self, client):
        """Processor stats endpoint returns 200."""
        response = client.get("/processor/stats")
        assert response.status_code == 200

    def test_initial_stats(self, client):
        """Processor stats start at zero."""
        data = client.get("/processor/stats").json()
        assert data["total_processed"] == 0
        assert data["windows_computed"] == 0


class TestLatestWindowEndpoint:
    """Tests for GET /processor/latest."""

    def test_returns_200(self, client):
        """Latest window endpoint returns 200."""
        response = client.get("/processor/latest")
        assert response.status_code == 200

    def test_no_windows_message(self, client):
        """Returns a message when no windows have been processed."""
        data = client.get("/processor/latest").json()
        assert "message" in data or "events_in_window" in data

    def test_returns_window_after_produce(self, client):
        """After producing, latest window shows aggregation data."""
        client.post("/produce", json={"count": 50})
        data = client.get("/processor/latest").json()
        assert "events_in_window" in data
        assert data["events_in_window"] > 0


class TestAllWindowsEndpoint:
    """Tests for GET /processor/windows."""

    def test_returns_200(self, client):
        """Windows endpoint returns 200."""
        response = client.get("/processor/windows")
        assert response.status_code == 200

    def test_structure(self, client):
        """Response has 'windows' list and 'total' count."""
        data = client.get("/processor/windows").json()
        assert "windows" in data
        assert "total" in data
        assert isinstance(data["windows"], list)

    def test_windows_after_produce(self, client):
        """Producing events generates at least one window."""
        client.post("/produce", json={"count": 20})
        data = client.get("/processor/windows").json()
        assert data["total"] >= 1


class TestProduceEndpoint:
    """Tests for POST /produce."""

    def test_returns_200(self, client):
        """Produce endpoint returns 200."""
        response = client.post("/produce", json={"count": 10})
        assert response.status_code == 200

    def test_produces_requested_count(self, client):
        """Response confirms the number of events produced."""
        data = client.post("/produce", json={"count": 25}).json()
        assert data["produced"] == 25

    def test_default_count(self, client):
        """Default count is 100 when not specified."""
        data = client.post("/produce", json={}).json()
        assert data["produced"] == 100

    def test_window_returned(self, client):
        """Producing events triggers processing and returns a window."""
        data = client.post("/produce", json={"count": 50}).json()
        assert "window" in data
        # window should be a dict with aggregation data (not a string)
        if isinstance(data["window"], dict):
            assert "events_in_window" in data["window"]

    def test_validation_error(self, client):
        """Sending invalid data returns 422."""
        response = client.post("/produce", json={"count": "not-a-number"})
        assert response.status_code == 422


class TestDashboardDataEndpoint:
    """Tests for GET /dashboard-data."""

    def test_returns_200(self, client):
        """Dashboard data endpoint returns 200."""
        response = client.get("/dashboard-data")
        assert response.status_code == 200

    def test_structure(self, client):
        """Dashboard data has all required sections."""
        data = client.get("/dashboard-data").json()
        assert "broker" in data
        assert "processor" in data
        assert "latest_window" in data
        assert "windows" in data

    def test_after_produce(self, client):
        """Dashboard data reflects produced events."""
        client.post("/produce", json={"count": 30})
        data = client.get("/dashboard-data").json()
        assert data["processor"]["total_processed"] >= 30
        assert data["broker"]["topics"]["events"]["total_messages"] >= 30
