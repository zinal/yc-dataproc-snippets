# Сбор логов Yandex Data Proc и доступ к логам

## 1. Логи, собираемые в Yandex Cloud Logging

Сервис [Yandex Data Proc](https://cloud.yandex.com/ru/services/data-proc) отправляет логи компонентов в [Yandex Cloud Logging](https://cloud.yandex.com/ru/services/logging). Каждая запись снабжается дополнительными атрибутами, позволяющими определить конкретный кластер Data Proc, компонент и другие характеристики источника логов.

[Для доступа к логам](https://cloud.yandex.ru/ru/docs/logging/operations/read-logs) можно использовать интерфейс Cloud Logging в Консоли Yandex Cloud, а также команду для доступа к логам в YC CLI.

При поиске информации важно задать корректные критерии фильтрации, чтобы не пропустить нужную информацию среди потока посторонних записей лога. Одним из инструментов первичной фильтрации является создание отдельной группы логирования для Data Proc, что позволяет сразу же отбросить логи других сервисов (например, бессерверных функций или контейнеров).

Для ограничения диапазона времени поиска поддерживаются фильтры на момент времени (значения задаются во временной зоне UTC), для YC CLI используются параметры `--since чч:мм:сс` и `--until чч:мм:сс` (метки времени указываются в UTC, для указания даты используется формат `гггг-мм-ддTчч:мм:ссZ`). При выводе логов по умолчанию применяются ограничения на количество записей. Для YC CLI есть возможность увеличить это ограничение с помощью парамера `--limit N`, где `N` - желаемое максимальное значение лимита.

При использовании YC CLI для вывода деталей по событиям необходимо указать параметр `--format json`. Этот режим вывода может быть неудобен для анализа логов от одного конкретного компонента, поэтому по умолчанию выводится только текстовая часть события лога.

Ниже приведены рецепты некоторых типовых фильтров для отбора данных о функционировании компонентов Data Proc и о выполнении заданий. Предполагаем, что `mzinal-dataproc1` - это название используемой группы логирования.

### 1.1. Инициализация кластера и его узлов

Логи работы сервиса `cloud-init`, осуществляющего инициализацию узлов кластера Data Proc, на конкретном узле кластера:

```bash
yc logging read mzinal-dataproc1 \
  --filter 'log_type: cloud-init AND hostname: "rc1d-dataproc-g-808029-ymyh.mdb.yandexcloud.net"'
```

Логи работы скриптов инициализации на конкретном узле кластера:

```bash
yc logging read mzinal-dataproc1 \
  --filter 'log_type: yandex-dataproc-init-actions AND hostname: "rc1d-dataproc-g-808029-omek.mdb.yandexcloud.net"'
```

### 1.2. Логи YARN

Логи ресурсного менеджера YARN (диагностика ситуаций вида "задание не запускается", "задание не получает нужного количества ресурсов"):

```bash
yc logging read mzinal-dataproc1 --since 08:50:00 --until 10:20:00 \
  --filter 'log_type: "hadoop-yarn-resourcemanager"'
```

Логи менеджера узла YARN (диагностика аварийных завершений заданий):

```bash
yc logging read mzinal-dataproc1 --since 08:50:00 --until 10:20:00 \
  --filter 'log_type: "hadoop-yarn-nodemanager" AND hostname: "rc1d-dataproc-g-808029-evyc.mdb.yandexcloud.net"'
```

### 1.3. Системные логи (syslog)

Записи, поступающие в `syslog` на конкретном узле кластера (обычно нужно при сборе данных на мастер-узле):

```bash
yc logging read mzinal-dataproc1 \
  --filter 'log_type: syslog AND hostname: "rc1d-dataproc-m-ici2e8n3pni3dwby.mdb.yandexcloud.net"' \
  --since 2024-01-23T08:00:00Z --limit 1000000
```

### 1.4. Логи контейнеров YARN

Вывод приложений, запускаемых в контейнерах YARN, включая задания Spark, фиксируются в виде событий Cloud Logging с установленным признаком `log_type: containers`. Вывод можно дополнительно отфильтровать по следуюшим атрибутам:

* `application_id` - идентификатор задания YARN;
* `container_id` - идентификатор контейнера YARN;
* `hostname` - имя хоста кластера Data Proc;
* `yarn_log_type` - имя выходного файла, например:
    *  `stdout` и `stderr` для стандартных потоков вывода и ошибок,
    *  `directory.info` для списка файлов контейнера,
    *  `launch_container.sh` скрипт запуска контейнера,
    *  `prelaunch.out` вывод скрипта подготовки к запуску.

Пример команды для выгрузки логов выполнения конкретного задания, отправляемых в выходные потоки stdout или stderr:

```bash
yc logging read mzinal-dataproc1 \
  --filter 'log_type: containers AND yarn_log_type: stdout stderr AND application_id: application_1705998482380_0013' \
  --since 2024-01-23T10:00:00Z --limit 1000000
```

### 1.5. Логи операций Zeppelin

Логи выполнения операций в ноутбуках Zeppelin можно выгрузить с помощью следующей команды:

```bash
yc logging read mzinal-dataproc1 \
  --filter 'log_type: zeppelin' \
  --since 2024-01-23T08:00:00Z --limit 1000000
```

yc logging read mzinal-dataproc1 --since 08:50:00 --until 10:20:00 --filter 'NOT log_type: "yandex-dataproc-agent" "syslog" "salt-minion" "containers" "livy-request" "zeppelin" "hadoop-yarn-resourcemanager" "hadoop-yarn-nodemanager" "yandex-dataproc-start" "cloud-init" "hadoop-yarn-timelineserver" "yandex-dataproc-init-actions"' --format json | less

### 1.6. Логи Livy



## 2. События Spark

События Spark накапливаются при работе Spark Master, после чего сохраняются в специальном каталоге в виде файлов формата JSON. Для каждого завершённого задания сохраняется отдельный файл. Отображение событий Spark для завершённых сессий осуществляется с помощью компонента Spark History Server. Детали по работе Spark History Server приведены в [документации на Apache Spark](https://spark.apache.org/docs/3.3.2/monitoring.html).

Каталог, в котором сохраняются файлы с событиями заданий, отображается в верхней части основной страницы интерфейса Spark History Server. Для кластеров Yandex Data Proc, в которых активна поддержка HDFS, файлы с событиями сохраняются в HDFS-каталоге `/var/log/spark/apps`. При использовании легковесного кластера Yandex Data Proc, файлы с событями сохраняются в каталоге `/dataproc/hadoop/var/log/spark/apps` бакета Object Storage, связанного с соответствующим кластером. Имена файлов соответствуют идентификаторам соответствующих заданий YARN.

