# Настройка Yandex Data Proc для работы с Apache Kafka в заданиях Spark

В текущих версиях образов сервиса [Yandex Data Proc](https://cloud.yandex.ru/services/data-proc), а именно 2.0.61 и 2.1.3 (на февраль 2023 года), временно отсутствуют некоторые библиотеки, входящие в состав клиента [Apache Kafka](https://kafka.apache.org/). Для обеспечения работы с Kafka в заданиях [Spark](https://spark.apache.org/) на платформе Data Proc необходимо добавить недостающие библиотеки в состав образа при помощи скрипта инициализации.

Недостающие библиотеки:
* для образов Data Proc 2.0.x (Spark 3.0.3):
    * `commons-pool2-2.6.2.jar`
    * `kafka-clients-2.4.1.jar` (в составе образа есть `kafka-clients-0.8.2.1.jar`, который следует отключить)
* для образов Data Proc 2.1.0 - 2.1.3 (Spark 3.2.1):
    * `commons-pool2-2.6.2.jar`
    * `kafka-clients-2.8.2.jar` (в составе образа есть `kafka-clients-2.8.1.jar`, который следует отключить)
* для образов Data Proc 2.1.4+ (Spark 3.3.2):
    * `commons-pool2-2.11.1.jar`
    * `kafka-clients-3.5.0.jar` (в составе образа есть `kafka-clients-2.8.1.jar`, который следует отключить)

При этом включение [необходимого комплекта клиентских библиотек Kafka](https://spark.apache.org/docs/3.3.2/structured-streaming-kafka-integration.html) в зависимости на уровне задания Spark оказывается недостаточным, поскольку часть библиотек присутствует на системном уровне (в системных путях Spark), и цепочка зависимостей не позволяет загрузить необходимые классы в нужном контексте.

Для обеспечения работы заданий Spark, использующих сервисы Kafka, необходимо с помощью скрипта инициализации:
* добавить недостающие библиотеки в каталог `/usr/lib/spark/jars` (для образа 2.0 достаточно в `/usr/lib/spark/external/lib`);
* отключить (удалить или переименовать) старые версии библиотек.

Пример скрипта инициализации, выполняющего необходимые действия, [доступен в репозитории](init-kafka.sh). Недостающие библиотеки скрипт скачивает из бакета, имя которого передаётся в виде параметра. Для выбранного бакета у сервисной учётной записи, присвоенной кластеру Data Proc, должны быть права на чтение. Библиотеки должны быть размещены в следующих каталогах бакета:
* для Data Proc 2.0: `/jars/dp-2.0.x/depjars/`;
* для Data Proc 2.1.0 - 2.1.3: `/jars/dp-2.1.3/depjars/`.
* для Data Proc 2.1.4+: `/jars/dp-2.1.6/depjars/`.

Примеры команд для YC CLI, выполняющих создание кластеров Data Proc для работы с Kafka с использованием предлагаемого скрипта инициализации:
* [вариант для Data Proc 2.0](dp-kafka-20.sh)
* [вариант для Data Proc 2.1](dp-kafka-21.sh)

Предлагаемые примеры скриптов создания кластеров Data Proc расчитаны на наличие внешнего сервиса Hive Metastore. Описание порядка создания кластера Data Proc для работы в качестве Hive Metastore [есть в репозитории](../dataproc-hive/), и включает в себя [пример скрипта создания такого кластера](../dataproc-hive/dp-metastore.sh).

# Пример задания Spark для проверки работы с Kafka

В каталоге [sample-consumer](sample-consumer/) вы можете найти пример кода задания Spark, выполняющего чтение данных из указанной очереди и сохранение их в виде набора файлов формата Parquet в указанном каталоге. Задание принимает следующие настройки:
* `spark.dataproc.demo.kafka.file-prefix` - путь префикса для сохранения файлов, например, `s3a://bucketname/dir/demo-kafka_`
* `spark.dataproc.demo.kafka.bootstrap` - список bootstrap-узлов кластера Kafka, в формате `host1:9091,host2:9091,...`
* `spark.dataproc.demo.kafka.user` - логин пользователя для подключения к кластеру Kafka
* `spark.dataproc.demo.kafka.password` - пароль
* `spark.dataproc.demo.kafka.topic` - имя топика
* `spark.dataproc.demo.kafka.mode` - режим работы задания, BATCH - пакетное копирование всех записей, STREAM - потоковое копирование новых записей

В каталоге [sample-producer](sample-producer/) размещён простой генератор записей Kafka в формате, совместимом с примером задания Spark. В качестве параметра командной строки генератор записей принимает имя файла свойств в формате XML, в котором задаются настройки подключения к кластеру Kafka, имя топика, а также интенсивность генерации записей (в единицах в секунду). Пример файла настроек [есть в репозитории](sample-producer/scripts/producer-job-sample.xml).

Пример команды YC CLI для запуска задания в режиме потокового копирования новых записей:

```bash
CLUSTER=kafka21
KAFKA=rc1c-amg5f4q074e2hshf.mdb.yandexcloud.net:9091
yc dataproc job create-spark --cluster-name ${CLUSTER} \
  --main-class ru.yandex.cloud.dataproc.sample.consumer.SampleConsumer \
  --main-jar-file-uri s3a://dproc-code/shared-libs/sample-consumer-1.0-SNAPSHOT.jar \
  --properties spark.dataproc.demo.kafka.file-prefix=s3a://dproc-wh/kafka1/sample- \
  --properties spark.dataproc.demo.kafka.bootstrap=${KAFKA} \
  --properties spark.dataproc.demo.kafka.user=user1 \
  --properties spark.dataproc.demo.kafka.password=password \
  --properties spark.dataproc.demo.kafka.topic=topic1 \
  --properties spark.dataproc.demo.kafka.mode=STREAM
```
