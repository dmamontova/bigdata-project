--скрипт для наполнения данными справочника IoT-устройств тестовыми значениями
INSERT INTO device_types (id, type_name) VALUES
(1, 'temperature_sensor'), --датчик температуры
(2, 'humidity_sensor'), --датчик влажности
(3, 'weather_station'), --погодная станция
(4, 'smart_thermostat'), --термостат
(5, 'air_quality_sensor'); --датчик качества воздуха
