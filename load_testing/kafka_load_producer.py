
import argparse
import csv
import json
import time
import threading
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_FILE = PROJECT_ROOT / "dataset.csv"
RESULTS_FILE = Path(__file__).resolve().parent / "load_test_results.csv"

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
DEFAULT_TOPIC = "sensor-load-test"

SENSOR_COLUMNS = [f"vibration_{i}" for i in range(1, 9)]

# Test profiles: messages per second
TEST_RATES = {
    "normal": 100,
    "high": 500,
    "burst": 0,  # No artificial delay
}


def read_sensor_rows():
    """Read sensor rows repeatedly if more messages are requested."""

    while True:
        with open(DATASET_FILE, "r", newline="") as file:
            reader = csv.DictReader(file)

            for row in reader:
                yield row


def save_result(result):
    """Append test results to a CSV file."""

    file_exists = RESULTS_FILE.exists()

    with open(RESULTS_FILE, "a", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=result.keys()
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(result)


def run_load_test(mode, message_count, topic, rate_override=None):

    target_rate = (
        rate_override
        if rate_override is not None
        else TEST_RATES[mode]
    )

    print("\n========== KAFKA LOAD TEST ==========")
    print(f"Test mode       : {mode}")
    print(f"Kafka server    : {KAFKA_SERVER}")
    print(f"Kafka topic     : {topic}")
    print(f"Messages        : {message_count}")
    print(
        f"Target rate     : "
        f"{target_rate if target_rate > 0 else 'Maximum speed'}"
    )
    print("=====================================\n")

    lock = threading.Lock()
    successful = 0
    failed = 0

    def on_success(metadata):
        nonlocal successful
        with lock:
            successful += 1

    def on_error(error):
        nonlocal failed
        with lock:
            failed += 1

    producer = None
    start_time = None

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
            batch_size=16384,
            linger_ms=5,
            max_block_ms=10000,
            request_timeout_ms=30000
        )

        print("Connected to Kafka. Starting test...\n")

        rows = read_sensor_rows()
        interval = 1 / target_rate if target_rate > 0 else 0

        start_time = time.perf_counter()
        next_send_time = start_time

        for number in range(1, message_count + 1):

            row = next(rows)

            payload = {
                "timestamp": int(time.time() * 1000),
                "device_id": "nasa_bearing_load_test"
            }

            for column in SENSOR_COLUMNS:
                payload[column] = float(row[column])

            try:
                future = producer.send(topic, value=payload)
                future.add_callback(on_success)
                future.add_errback(on_error)

            except Exception as error:
                with lock:
                    failed += 1
                print(f"Send error at message {number}: {error}")

            if interval > 0:
                next_send_time += interval
                delay = next_send_time - time.perf_counter()

                if delay > 0:
                    time.sleep(delay)

            if number % 1000 == 0:
                print(f"Submitted {number}/{message_count} messages")

        print("\nWaiting for Kafka acknowledgements...")
        producer.flush(timeout=120)

        elapsed = time.perf_counter() - start_time

        with lock:
            sent = successful
            errors = failed

        throughput = sent / elapsed if elapsed > 0 else 0

        result = {
            "test_time": datetime.now().isoformat(timespec="seconds"),
            "mode": mode,
            "topic": topic,
            "target_rate_msg_sec": target_rate,
            "messages_requested": message_count,
            "successful_messages": sent,
            "failed_messages": errors,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_msg_sec": round(throughput, 2)
        }

        save_result(result)

        print("\n========== TEST RESULTS ==========")
        print(f"Requested messages : {message_count}")
        print(f"Successful         : {sent}")
        print(f"Failed             : {errors}")
        print(f"Time taken         : {elapsed:.2f} seconds")
        print(f"Throughput         : {throughput:.2f} messages/sec")
        print(f"Results saved      : {RESULTS_FILE}")
        print("==================================")

    except Exception as error:
        print(f"\nLoad test failed: {error}")

    finally:
        if producer is not None:
            producer.close(timeout=60)


def main():

    parser = argparse.ArgumentParser(
        description="Kafka sensor telemetry load testing"
    )

    parser.add_argument(
        "--mode",
        choices=["normal", "high", "burst"],
        required=True
    )

    parser.add_argument(
        "--messages",
        type=int,
        default=5000
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=None,
        help="Optional custom messages-per-second target"
    )

    args = parser.parse_args()

    if args.messages <= 0:
        parser.error("--messages must be greater than zero")

    if args.rate is not None and args.rate <= 0:
        parser.error("--rate must be greater than zero")

    if not DATASET_FILE.exists():
        parser.error(f"Dataset not found: {DATASET_FILE}")

    run_load_test(
        mode=args.mode,
        message_count=args.messages,
        topic=args.topic,
        rate_override=args.rate
    )


if __name__ == "__main__":
    main()
