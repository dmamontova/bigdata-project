"""
Скрипт для обработки IoT-событий.

Что делает скрипт:
1. Читает сырые события от IoT-устройств из Kafka топик `iot_events`.
2. Загружает справочник типов устройств из PostgreSQL.
3. Обогащает события названием типа устройства.
4. Использует event time и watermarks для корректной работы с временем события.
5. Считает агрегаты:
   - среднюю температуру;
   - медиану влажности.
6. Переводит результат из DataStream в Table.
7. Записывает итоговые агрегаты в Kafka топик `iot_aggregates`
   через SQL/Table API.
"""

import glob
import json
import os
from datetime import datetime, timezone

import psycopg2

from pyflink.common import Configuration, Duration, Row, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment, Schema, DataTypes


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "iot_events" #Входной потом с событиями от IoT-устройств
OUTPUT_TOPIC = "iot_aggregates" #Итоговые агрегаты по минутным окнам

#Здесь просто параметры для подключения к PostgresSQL
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "iot_db"
POSTGRES_USER = "iot_user"
POSTGRES_PASSWORD = "iot_password"


def add_connector_jars(env, t_env):
    """
    У меня были пооблемы с внешними jar-коннекторами, так что это фукнкция, 
    которая добавляет их в окружение Flink
    """
    jar_paths = glob.glob(os.path.abspath("jars/*.jar"))

    if not jar_paths:
        raise RuntimeError("No connector jars found. Put Kafka connector jar into ./jars")

    jar_uris = [f"file://{path}" for path in jar_paths]

    env.add_jars(*jar_uris)
    t_env.get_config().set("pipeline.jars", ";".join(jar_uris))


def load_device_types_from_postgres():
    """
    Функция для загрузки справочника типов устройств из PostgresSQL.
    На выходе получаем словарь device_type_id - type_name.
    Например, 1 - temperature_sensor.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, type_name FROM device_types ORDER BY id")
            rows = cur.fetchall()
            return {int(row[0]): row[1] for row in rows}
    finally:
        conn.close()


def parse_iot_event(raw_message):
    """
    Функция для парсинга одного Kafka-сообщения из JSON в Row.
    В Kafka событие приходит строкой, поэтому сначала делаем json.loads(),
    затем приводим поля к нужным типам.
    event_time переводится в timestamp в миллисекундах, потому что дальше
    мы используем его как event time для watermarks и окон.
    """
    try:
        data = json.loads(raw_message)

        event_time = datetime.fromisoformat(
            data["event_time"].replace("Z", "+00:00")
        )
        event_time_ms = int(event_time.timestamp() * 1000)

        return Row(
            device_id=str(data["device_id"]),
            device_type_id=int(data["device_type_id"]),
            event_time_ms=event_time_ms,
            temperature=float(data["temperature"]),
            humidity=float(data["humidity"]),
        )

    except Exception as exc:
        print(f"Bad message skipped: {raw_message}. Error: {exc}")
        return None


def enrich_event(row, device_types):
    """
    Обогащает событие названием типа устройства.
    Во входном событии есть только device_type_id,
    по этому id берём type_name из справочника PostgreSQL.
    """
    device_type_id = row[1]
    type_name = device_types.get(device_type_id, "unknown_device_type")

    return Row(
        device_type=type_name,
        event_time_ms=row[2],
        temperature=row[3],
        humidity=row[4],
    )


class IotTimestampAssigner(TimestampAssigner):
    """
    Класс для извлечения event time из события.
    Flink будет использовать поле event_time_ms как timestamp события,
    а не время обработки записи.
    """
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


class AvgTempMedianHumidityWindow(ProcessWindowFunction):
    """
    Оконная функция для расчёта агрегатов.
    Для каждого минутного окна и каждого типа устройства считаются:
    - средняя температура;
    - медиана влажности.
    """
    def process(self, key, context, elements):
        rows = list(elements)

        if not rows:
            return

        temperatures = [float(row[2]) for row in rows]
        humidities = sorted(float(row[3]) for row in rows)

        avg_temperature = sum(temperatures) / len(temperatures)

        #Медиану считаем вручную, потому что в используемой версии Flink SQL
        #функции PERCENTILE / PERCENTILE_CONT не поддерживались
        n = len(humidities)
        if n % 2 == 1:
            median_humidity = humidities[n // 2]
        else:
            median_humidity = (humidities[n // 2 - 1] + humidities[n // 2]) / 2

        window = context.window()

        window_start = datetime.fromtimestamp(
            window.start / 1000,
            tz=timezone.utc,
        ).strftime("%H:%M")

        window_end = datetime.fromtimestamp(
            window.end / 1000,
            tz=timezone.utc,
        ).strftime("%H:%M")

        yield Row(
            window_start=window_start,
            window_end=window_end,
            device_type=key,
            avg_temperature=round(avg_temperature, 2),
            median_humidity=round(median_humidity, 2),
        )


def main():
     #Создаём Flink-окружение
    conf = Configuration()
    conf.set_integer("rest.port", 8081)

    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)

    add_connector_jars(env, t_env)
    
    #Загружаем справочник из PostgreSQL один раз
    device_types = load_device_types_from_postgres()
    print(f"Loaded device types from Postgres: {device_types}")
    
    #Описываем Kafka source для чтения входных IoT-событий
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("iot-flink-job")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka iot_events source",
    )

    #Парсим JSON-сообщения из Kafka и отбрасываем некорректные записи
    events_ds = (
        raw_ds
        .map(
            parse_iot_event,
            output_type=Types.ROW_NAMED(
                ["device_id", "device_type_id", "event_time_ms", "temperature", "humidity"],
                [Types.STRING(), Types.INT(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE()],
            ),
        )
        .filter(lambda x: x is not None)
    )

    #Добавляем к событию название типа устройства из PostgreSQL-справочника
    enriched_ds = events_ds.map(
        lambda row: enrich_event(row, device_types),
        output_type=Types.ROW_NAMED(
            ["device_type", "event_time_ms", "temperature", "humidity"],
            [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE()],
        ),
    )

    #Настраиваем event time и watermarks
    #Допускаем, что события могут прийти с опозданием до 5 секунд
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(IotTimestampAssigner())
    )

    enriched_with_watermarks = enriched_ds.assign_timestamps_and_watermarks(
        watermark_strategy
    )
    
    #Группируем события по типу устройства и считаем агрегаты
    aggregates_ds = (
        enriched_with_watermarks
        .key_by(lambda row: row[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            AvgTempMedianHumidityWindow(),
            output_type=Types.ROW_NAMED(
                ["window_start", "window_end", "device_type", "avg_temperature", "median_humidity"],
                [Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
            ),
        )
    )

    #Переводим результат из DataStream в Table
    #Это нужно, чтобы дальше записать результат в Kafka через SQL/Table API
    aggregates_table = t_env.from_data_stream(
        aggregates_ds,
        Schema.new_builder()
        .column("window_start", DataTypes.STRING())
        .column("window_end", DataTypes.STRING())
        .column("device_type", DataTypes.STRING())
        .column("avg_temperature", DataTypes.DOUBLE())
        .column("median_humidity", DataTypes.DOUBLE())
        .build(),
    )

    t_env.create_temporary_view("iot_aggregates_view", aggregates_table)
    
    #Создаём Kafka sink через Table/SQL API.
    #Итоговые агрегаты будут записываться в iot_aggregates в JSON-формате
    t_env.execute_sql(
        f"""
        CREATE TABLE iot_aggregates_sink (
            window_start STRING,
            window_end STRING,
            device_type STRING,
            avg_temperature DOUBLE,
            median_humidity DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{OUTPUT_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json',
            'sink.partitioner' = 'round-robin'
        )
        """
    )

    #Запускаем запись результата из временного вью в Kafka sink
    result = t_env.execute_sql(
        """
        INSERT INTO iot_aggregates_sink
        SELECT
            window_start,
            window_end,
            device_type,
            avg_temperature,
            median_humidity
        FROM iot_aggregates_view
        """
    )

    print("Flink job started.")
    print(f"Input Kafka topic: {INPUT_TOPIC}")
    print(f"Output Kafka topic: {OUTPUT_TOPIC}")
    result.wait()


if __name__ == "__main__":
    main()
