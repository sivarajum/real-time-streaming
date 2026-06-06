"""Centralized configuration loaded from environment variables with sensible defaults."""

import os

API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
UI_PORT: int = int(os.getenv("UI_PORT", "8501"))
CORS_ORIGINS: list[str] = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8501").split(",")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
PRODUCE_INTERVAL: float = float(os.getenv("PRODUCE_INTERVAL", "0.1"))
PROCESS_INTERVAL: float = float(os.getenv("PROCESS_INTERVAL", "5.0"))
DEFAULT_TOPIC: str = os.getenv("DEFAULT_TOPIC", "events")
NUM_PARTITIONS: int = int(os.getenv("NUM_PARTITIONS", "4"))
