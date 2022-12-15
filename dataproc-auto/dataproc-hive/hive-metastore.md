# Настройка внешней базы данных Hive Metastore для сервися Yandex Data Proc

[Сервис Yandex Data Proc](https://cloud.yandex.ru/services/data-proc) включает в себя технологии [Apache Hive](https://hive.apache.org) и [Apache Spark](https://spark.apache.org), и использует компонент Hive Metastore для ведения информации о структуре таблиц, форматах хранения их данных, размещении файлов с данными.

Информация, с которой работает Hive Metastore, хранится в базе данных [одного из поддерживаемых типов](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration#AdminManualMetastoreAdministration-SupportedBackendDatabasesforMetastore). По умолчанию Yandex Data Proc размещает базу данных Hive Metastore под управлением PostgreSQL на мастер-сервере кластера Data Proc, но такая конфигурация не является полностью отказоустойчивой, имеет ограниченную масштабируемость и не позволяет организовать полноценное резервное копирование данных. Кроме того, использование отдельных копий данных Hive Metastore в каждом кластере Data Proc не позволяет организовать обработку единого массива данных с использованием нескольких кластеров.

Вместо использования встроенной базы данных Hive Metastore, на сегодня (до готовности в Yandex Cloud управляемого сервиса Hive Metastore) рекомендуется применять внешнюю базу данных, в виде сервиса [Yandex Managed Service for PostgreSQL](https://cloud.yandex.ru/services/managed-postgresql). Тем самым эффективно решаются вопросы отказоустойчивости, масштабирования и резервного копирования базы данных Hive Metastore. Время жизни метаданных в такой конфигурации превышает время существования отдельных кластеров Data Proc, и возникает возможность согласованного доступа из нескольких кластеров Data Proc к общему массиву данных, размещённому в [объектном хранилище Yandex Object Storage](https://cloud.yandex.ru/services/storage).

Настройка внешней базы данных Hive Metastore выполняется в следующем порядке:
1. Создание и настройка сервиса PostgreSQL
2. Инициализация базы данных Hive Metastore
3. Запуск кластеров Data Proc с использованием внешней базы данных Hive Metastore

## 1. Создание и настройка сервиса PostgreSQL

## 2. Инициализация базы данных Hive Metastore

## 3. Запуск кластеров Data Proc с использованием внешней базы данных Hive Metastore

# Настройка внешнего Hive Metastore

