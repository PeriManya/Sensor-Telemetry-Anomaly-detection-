
import argparse
import csv
import json
import math
import threading
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_FILE = PROJECT_ROOT / "dataset.csv"
RESULTS_FILE = Path(__file__).resolve().parent / "load_test_results.csv"

KAFKA_SERVER = "localhost:9092"
DEFAULT_TOPIC = "sensor-load-test"

SENSOR_COLUMNS = [f"vibration_{i}" for i in range(1, 9)]

TEST_RATES = {
    "normal": 100,
    "high": 500,
    "burst": 0
}

RESULT_FIELDS = [
    "test_time",
    "mode",
    "topic",
    "target_rate_msg_sec",
    "messages_requested",
    "messages_submitted",
    "successful_messages",
    "failed_messages",
    "acknowledgement_rate_percent",
    "elapsed_seconds",
    "throughput_msg_sec",
    "avg_ack_latency_ms",
    "p95_ack_latency_ms",
    "max_ack_latency_ms"
]


# --------------------------------------------------
# DATASET VALIDATION
# --------------------------------------------------

def load_sensor_rows():
    """Load and validate the eight vibration columns."""

    if not DATASET_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET_FILE}"
        )

    with open(DATASET_FILE, "r", newline="") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError("Dataset is empty or has no header.")

        missing = [
            column for column in SENSOR_COLUMNS
            if column not in reader.fieldnames
        ]

        if missing:
            raise ValueError(
                f"Missing required dataset columns: {missing}"
            )

        rows = []

        for row in reader:
            try:
                values = {
                    column: float(row[column])
                    for column in SENSOR_COLUMNS
                }
                rows.append(values)

            except (ValueError, TypeError, KeyError):
                continue

    if not rows:
        raise ValueError(
            "No valid sensor rows found in dataset.csv."
        )

    print(f"Dataset loaded successfully: {len(rows)} valid rows")
    return rows


# --------------------------------------------------
# RESULT FILE HANDLING
# --------------------------------------------------

def get_results_file():
    """Avoid mixing results with an older CSV schema."""

    if not RESULTS_FILE.exists():
        return RESULTS_FILE

    with open(RESULTS_FILE, "r", newline="") as file:
        existing_fields = next(csv.reader(file), [])

    if existing_fields == RESULT_FIELDS:
        return RESULTS_FILE

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return RESULTS_FILE.with_name(
        f"load_test_results_{timestamp}.csv"
    )


def save_result(result):
    """Save one test run without deleting earlier results."""

    output_file = get_results_file()
    file_exists = output_file.exists()

    with open(output_file, "a", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=RESULT_FIELDS
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(result)

    return output_file


# --------------------------------------------------
# LATENCY METRICS
# --------------------------------------------------

def percentile(values, percent):
    if not values:
        return 0.0

    ordered = sorted(values)
    index = max(
        0,
        math.ceil((percent / 100) * len(ordered)) - 1
    )

    return ordered[index]


# --------------------------------------------------
# KAFKA LOAD TEST
# --------------------------------------------------

def run_load_test(mode, message_count, topic, rate_override=None):

    target_rate = (
        rate_override
        if rate_override is not None
        else TEST_RATES[mode]
    )

    sensor_rows = load_sensor_rows()

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
    submitted = 0
    ack_latencies = []

    producer = None
    start_time = None

    def on_success(metadata, sent_at):
        nonlocal successful

        latency = (time.perf_counter() - sent_at) * 1000

        with lock:
            successful += 1
            ack_latencies.append(latency)

    def on_error(error):
        nonlocal failed

        with lock:
            failed += 1

        print(f"Kafka delivery error: {error}")

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

        interval = 1 / target_rate if target_rate > 0 else 0

        start_time = time.perf_counter()
        next_send_time = start_time

        for number in range(1, message_count + 1):

            row = sensor_rows[(number - 1) % len(sensor_rows)]

            payload = {
                "timestamp": int(time.time() * 1000),
                "device_id": "nasa_bearing_load_test",
                "sequence_number": number,
                "test_mode": mode
            }

            payload.update(row)

            sent_at = time.perf_counter()

            try:
                future = producer.send(topic, value=payload)

                future.add_callback(
                    lambda metadata, t=sent_at:
                    on_success(metadata, t)
                )

                future.add_errback(on_error)

                with lock:
                    submitted += 1

            except KafkaError as error:
                on_error(error)

            except Exception as error:
                on_error(error)

            if interval > 0:
                next_send_time += interval
                delay = next_send_time - time.perf_counter()

                if delay > 0:
                    time.sleep(delay)

            if number % 1000 == 0:
                print(
                    f"Submitted {number}/{message_count} messages"
                )

        print("\nWaiting for Kafka acknowledgements...")

        producer.flush(timeout=120)

        elapsed = time.perf_counter() - start_time

        with lock:
            sent = successful
            errors = failed
            total_submitted = submitted
            latency_values = list(ack_latencies)

        throughput = sent / elapsed if elapsed > 0 else 0

        acknowledgement_rate = (
            (sent / total_submitted) * 100
            if total_submitted > 0 else 0
        )

        average_latency = (
            sum(latency_values) / len(latency_values)
            if latency_values else 0
        )

        p95_latency = percentile(latency_values, 95)
        max_latency = max(latency_values) if latency_values else 0

        result = {
            "test_time": datetime.now().isoformat(timespec="seconds"),
            "mode": mode,
            "topic": topic,
            "target_rate_msg_sec": target_rate,
            "messages_requested": message_count,
            "messages_submitted": total_submitted,
            "successful_messages": sent,
            "failed_messages": errors,
            "acknowledgement_rate_percent": round(
                acknowledgement_rate, 2
            ),
            "elapsed_seconds": round(elapsed, 3),
            "throughput_msg_sec": round(throughput, 2),
            "avg_ack_latency_ms": round(average_latency, 3),
            "p95_ack_latency_ms": round(p95_latency, 3),
            "max_ack_latency_ms": round(max_latency, 3)
        }

        output_file = save_result(result)

        print("\n========== TEST RESULTS ==========")
        print(f"Requested messages : {message_count}")
        print(f"Submitted messages : {total_submitted}")
        print(f"Successful         : {sent}")
        print(f"Failed             : {errors}")
        print(
            f"Acknowledgement rate: {acknowledgement_rate:.2f}%"
        )
        print(f"Time taken         : {elapsed:.2f} seconds")
        print(f"Throughput         : {throughput:.2f} messages/sec")
        print(f"Average latency    : {average_latency:.3f} ms")
        print(f"95th percentile    : {p95_latency:.3f} ms")
        print(f"Maximum latency    : {max_latency:.3f} ms")
        print(f"Results saved      : {output_file}")
        print("==================================")

    except Exception as error:
        print(f"\nLoad test failed: {error}")

    finally:
        if producer is not None:
            producer.close(timeout=60)


# --------------------------------------------------
# COMMAND-LINE INTERFACE
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Kafka sensor telemetry load and stress testing"
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
        help="Optional custom target messages per second"
    )

    args = parser.parse_args()

    if args.messages <= 0:
        parser.error("--messages must be greater than zero")

    if args.rate is not None and args.rate <= 0:
        parser.error("--rate must be greater than zero")

    try:
        run_load_test(
            mode=args.mode,
            message_count=args.messages,
            topic=args.topic,
            rate_override=args.rate
        )

    except Exception as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
