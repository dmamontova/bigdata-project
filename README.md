# BigData project
Выполнила студентка МФТАД251 Мамонтова Дарья


## Структура проекта

```text
bigdata-project/
├── docker-compose.yml
├── requirements.txt
├── constraints.txt
├── README.md
├── sql/
│   ├── ddl.sql
│   └── dml.sql
└── src/
    ├── iot_generator.py
    ├── flink_job.py
    └── read_results.py
```

## Идея проекта

Проект реализует пайплайн:

```text
IoT generator -> Kafka -> PyFlink -> Kafka
                       +
                  PostgreSQL dictionary
```

Генератор раз в секунду отправляет события от IoT-устройств в Kafka. PyFlink job читает поток событий, обогащает его справочником типов устройств из PostgreSQL, считает агрегаты в минутных event-time окнах и записывает результат обратно в Kafka.


## Что было реализовано

В проекте собран небольшой потоковый пайплайн для обработки событий от IoT-устройств.

Сначала запускается генератор сообщений `iot_generator.py`. Он раз в секунду создает случайное событие от устройства и отправляет его в Kafka topic `iot_events`.

Пример входного события:

```json
{
  "device_id": "device_001",
  "device_type_id": 3,
  "event_time": "2026-06-02T16:54:10.123456+00:00",
  "temperature": 25.31,
  "humidity": 60.57
}
```

Отдельно в PostgreSQL хранится справочник типов устройств. Он создается с помощью файлов `sql/ddl.sql` и `sql/dml.sql`.

Пример справочника:

```text
id | type_name
---+--------------------
1  | temperature_sensor
2  | humidity_sensor
3  | weather_station
4  | smart_thermostat
5  | air_quality_sensor
```

Основная обработка происходит в файле `flink_job.py`. В нем PyFlink читает события из Kafka, добавляет к ним название типа устройства из справочника PostgreSQL, после чего считает агрегаты в минутных окнах.

Для каждого типа устройства и каждой минуты считаются:

```text
avg_temperature  — средняя температура
median_humidity  — медиана влажности
```

Результат записывается обратно в Kafka topic `iot_aggregates`.

Пример результата:

```json
{
  "window_start": "16:54",
  "window_end": "16:55",
  "device_type": "weather_station",
  "avg_temperature": 25.31,
  "median_humidity": 60.57
}
```

Для проверки результата добавлен файл `read_results.py`. Он читает сообщения из итогового Kafka topic и выводит их в консоль.

## Как запустить проект

### 1. Создать окружение

```bash
conda create -n bigdata-flink python=3.10 -y
conda activate bigdata-flink
```

### 2. Установить зависимости

```bash
pip install "setuptools<81"
pip install -r requirements.txt
```

Если установка `apache-flink` падает на зависимостях, можно выполнить:

```bash
PIP_CONSTRAINT=constraints.txt pip install apache-flink==1.20.0
```

### 3. Установить Java

Для работы PyFlink нужна Java. В проекте использовался OpenJDK 17.

```bash
conda install -c conda-forge openjdk=17 -y
```

Проверка:

```bash
java -version
```

### 4. Запустить инфраструктуру

```bash
docker compose up -d
```

После этого поднимаются:

```text
PostgreSQL
Kafka
Zookeeper
Kafka UI
```

Kafka UI доступен по адресу:

```text
http://localhost:8080
```

### 5. Скачать Kafka connector для Flink

Kafka connector не хранится в репозитории, поэтому его нужно скачать отдельно:

```bash
mkdir -p jars

curl -fL -o jars/flink-sql-connector-kafka-3.3.0-1.20.jar \
https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
```

Также для локального запуска PyFlink я копировала connector в директорию `pyflink/lib`:

```bash
python - <<'PY'
import os
import shutil
import pyflink

src = "jars/flink-sql-connector-kafka-3.3.0-1.20.jar"
pyflink_dir = os.path.dirname(pyflink.__file__)
lib_dir = os.path.join(pyflink_dir, "lib")

os.makedirs(lib_dir, exist_ok=True)

dst = os.path.join(lib_dir, os.path.basename(src))
shutil.copyfile(src, dst)

print("copied to:", dst)
PY
```

### 6. Запустить генератор сообщений

В отдельном терминале:

```bash
conda activate bigdata-flink
cd ~/bigdata-project
python src/iot_generator.py
```

После запуска в Kafka начнут отправляться события в topic `iot_events`.

### 7. Запустить Flink job

Во втором терминале:

```bash
conda activate bigdata-flink
cd ~/bigdata-project
python src/flink_job.py
```

Flink job начнет читать события из Kafka, обрабатывать их и писать агрегаты в topic `iot_aggregates`.

Чтобы посмотреть результат, нужно запустить в третьем терминале:

```bash
conda activate bigdata-flink
cd ~/bigdata-project
python src/read_results.py
```

Также результат можно посмотреть через Kafka UI:

```text
http://localhost:8080
```

Дальше нужно открыть:

```text
local-iot-kafka -> Topics -> iot_aggregates -> Messages
```

## Вывод

Таким образом, в проекте был реализован пайплайн для обработки IoT-событий. Генератор отправляет данные в Kafka, PyFlink job читает этот поток, обогащает события справочником из PostgreSQL и считает агрегаты по минутным окнам.

В результате для каждого типа устройства рассчитываются средняя температура и медиана влажности, после чего итоговые данные записываются обратно в Kafka в topic `iot_aggregates`.

В проекте также были использованы основные элементы, которые требовались в задании: Kafka source/sink, PostgreSQL-справочник, event time, watermarks, tumbling windows, DataStream API и переход из DataStream в Table API для записи результата.
