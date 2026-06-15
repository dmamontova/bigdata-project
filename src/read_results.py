"""
Скрипт для чтения результатов из Kafka.

Что делает скрипт:
1. Подключается к Kafka.
2. Читает сообщения из ттопика `iot_aggregates`.
3. Выводит итоговые агрегаты .
"""
from confluent_kafka import Consumer

#Kafka топик, в который PyFlink job записывает итоговые агрегаты
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "iot_aggregates"


def main():
    """
    Создаёт Kafka consumer и постоянно читает сообщения из итогового topic
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "iot-results-reader",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC_NAME])

    print(f"Reading messages from Kafka topic: {TOPIC_NAME}")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            #Если новых сообщений пока нет, просто продолжаем ждать
            if msg is None:
                continue

            #Если Kafka вернула ошибку, выводим её и продолжаем работу
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            #Сообщения лежат в Kafka в битах, поэтому декодируем их в строку
            print(msg.value().decode("utf-8"))

    except KeyboardInterrupt:
        pass

    #Закрываем consumer, чтобы корректно завершить подключение к Kafka
    finally:
        consumer.close()
        print("Reader stopped.")


if __name__ == "__main__":
    main()
