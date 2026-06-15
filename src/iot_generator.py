"""
Скрипт для генерации тестовых IoT-событий.

Что делает скрипт:
1. Раз в секунду генерирует случайное событие от IoT-устройства.
2. Событие содержит device_id, device_type_id, event_time, temperature и humidity.
3. Отправляет событие в Kafka топик `iot_events` в JSON-формате.
"""

import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

#Kafka топик, куда отправляются сырые IoT-события
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "iot_events"

#Возможные типы устройств
#Эти id должны совпадать с id в PostgreSQL-справочнике device_types
DEVICE_TYPES = [1, 2, 3, 4, 5]

#Список тестовых устройств
#У одного типа устройства может быть несколько конкретных device_id
DEVICE_IDS = [f"device_{i:03d}" for i in range(1, 21)]


def delivery_report(err, msg):
    """
    Функция, которая вызывается после попытки отправить сообщение в Kafka
    и показывает, успешно ли сообщение было доставлено
    """
    
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def generate_event():
    """
    Функция для генерации одного случайного IoT-события
    """
    
    device_type_id = random.choice(DEVICE_TYPES)

    return {
        "device_id": random.choice(DEVICE_IDS),
        "device_type_id": device_type_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
    }


def main():
    """
    Основной цикл генератора.
    Producer отправляет по одному событию в Kafka каждую секунду
    """
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "iot-generator",
        }
    )

    print(f"Sending IoT events to Kafka topic: {TOPIC_NAME}")

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
        pass

    finally:
        producer.flush()
        print("Generator stopped.")


if __name__ == "__main__":
    main()
