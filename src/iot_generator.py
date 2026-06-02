import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "iot_events"

DEVICE_TYPES = [1, 2, 3, 4, 5]
DEVICE_IDS = [f"device_{i:03d}" for i in range(1, 21)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def generate_event():
    device_type_id = random.choice(DEVICE_TYPES)

    return {
        "device_id": random.choice(DEVICE_IDS),
        "device_type_id": device_type_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
    }


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "iot-generator",
        }
    )

    print(f"Sending IoT events to Kafka topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            event = generate_event()
            value = json.dumps(event)

            producer.produce(
                topic=TOPIC_NAME,
                key=str(event["device_id"]),
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)

            print(value)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping generator...")

    finally:
        producer.flush()
        print("Generator stopped.")


if __name__ == "__main__":
    main()
