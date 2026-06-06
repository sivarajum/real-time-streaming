"""Entry point: launch the Streaming Platform API and/or UI."""

import subprocess
import sys
from pathlib import Path

from src.logging_config import setup_logging
from src.settings import API_HOST, API_PORT, UI_PORT

PROJECT_ROOT = Path(__file__).parent


def main() -> None:
    setup_logging()

    mode = sys.argv[1] if len(sys.argv) > 1 else "api"

    if mode == "api":
        import uvicorn

        uvicorn.run("src.api:app", host=API_HOST, port=API_PORT, reload=True)

    elif mode == "ui":
        subprocess.run(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                "src/ui.py",
                "--server.port",
                str(UI_PORT),
                "--server.address",
                "0.0.0.0",
            ],
            cwd=PROJECT_ROOT,
        )

    elif mode == "all":
        api_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "src.api:app",
                "--host",
                API_HOST,
                "--port",
                str(API_PORT),
            ],
            cwd=PROJECT_ROOT,
        )
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    "src/ui.py",
                    "--server.port",
                    str(UI_PORT),
                    "--server.address",
                    "0.0.0.0",
                ],
                cwd=PROJECT_ROOT,
            )
        finally:
            api_proc.terminate()

    elif mode == "demo":
        # Quick demo: produce events and process them
        from src.broker import Broker
        from src.producer import produce_batch
        from src.processor import StreamProcessor
        from src.settings import DEFAULT_TOPIC, NUM_PARTITIONS

        broker = Broker()
        broker.create_topic(DEFAULT_TOPIC, num_partitions=NUM_PARTITIONS)

        print("Producing 200 events...")
        events = produce_batch(size=200)
        for e in events:
            broker.publish(DEFAULT_TOPIC, e["user_id"], e)

        print(f"Broker stats: {broker.get_stats()}")

        proc = StreamProcessor(broker, topic_name=DEFAULT_TOPIC)
        result = proc.process_once()
        print(f"\nProcessed {result['events_in_window']} events:")
        print(f"  Event types: {result['event_types']}")
        print(f"  Revenue: ${result['total_revenue']}")
        print(f"  Purchases: {result['purchase_count']}")
        print(f"  Categories: {result['category_revenue']}")
        print(f"  Regions: {result['region_counts']}")

    else:
        print("Usage: python main.py [api|ui|all|demo]")
        print("  api   - Start the FastAPI server (default)")
        print("  ui    - Start the Streamlit dashboard")
        print("  all   - Start both API and UI")
        print("  demo  - Quick demo: produce + process events")
        sys.exit(1)


if __name__ == "__main__":
    main()
