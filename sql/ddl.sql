--скрипт для создания справочника типов IoT-устройств
DROP TABLE IF EXISTS device_types;

CREATE TABLE device_types (
    id INT PRIMARY KEY, --идентификатор устройства
    type_name VARCHAR(100) NOT NULL --тип устройства
);
