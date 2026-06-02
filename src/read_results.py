from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "iot_aggregates"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "iot-results-reader",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC_NAME])

    print(f"Reading messages from Kafka topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            print(msg.value().decode("utf-8"))

    except KeyboardInterrupt:
        print("\nStopping reader...")

    finally:
        consumer.close()
        print("Reader stopped.")


if __name__ == "__main__":
    main()
