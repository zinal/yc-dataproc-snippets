# Использование Delta Lake в Yandex Data Proc

## 1. Что такое Delta Lake и зачем он нужен

[Delta Lake](https://delta.io) - программное обеспечение с открытым исходным кодом, расширяющее возможности платформы [Apache Spark](https://spark.apache.org) оптимизированным слоем хранения данных в формате таблиц с поддержкой ACID-транзакций, масштабируемой обработкой метаданных и управлением версиями данных.

Delta Lake решает задачу обновления данных в аналитических таблицах, хранимых в виде набора файлов в формате Parquet в HDFS или S3-совместимом хранилище. Таблица Delta Lake может одновременно использоваться пакетными запросами, а также в качестве источника и приемника операций потоковой передачи данных.

## 2. Delta Lake для стабильных образов Data Proc 2.0

Стабильные образы Data Proc (версии 2.0.x) основаны на Apache Spark 3.0.3. В соответствии с [официальной таблицей совместимости](https://docs.delta.io/latest/releases.html), в такой конфигурации может использоваться Delta Lake [версии 0.8.0](https://github.com/delta-io/delta/releases/tag/v0.8.0).

Основным ограничением этой версии Delta Lake является запрет на конкурентную модификацию таблиц в S3-совместимом хранилище (см. [примечание](https://docs.delta.io/0.8.0/delta-storage.html#amazon-s3) в официальной документации). Это ограничение означает, что при работе с таблицами Delta Lake необходимо координировать операции записи таким образом, чтобы исключить одновременную модификацию данных в одной таблице из разных кластеров Data Proc, а также из разных заданий Spark в рамках одного кластера. Нарушение этого требования может привести к неожиданной потере хранимой в таблице информации.

Для использования Delta Lake в сочетании с Data Proc 2.0 достаточно в зависимостях заданий добавить библиотеку `delta-core_2.12-0.8.0.jar`, а также установить следующие свойства Spark:
* `spark.sql.extensions` в значение `io.delta.sql.DeltaSparkSessionExtension`
* `spark.sql.catalog.spark_catalog` в значение `org.apache.spark.sql.delta.catalog.DeltaCatalog`

Библиотека `delta-core_2.12-0.8.0.jar` может быть добавлена в зависимости заданий любым из следующих способов:
1. через установку свойства `spark.jars` для конкретного задания либо для кластера в целом, значением является URL файла библиотеки, файл должен быть доступен по указанному URL;
2. через установку свойства `spark.jars.packages` в значение `io.delta:delta-core_2.12:0.8.0`, должен быть настроен сетевой доступ в Maven Central, либо сконфигурирован альтернативный репозиторий Maven;
3. через установку свойств `spark.driver.extraClassPath` и `spark.executor.extraClassPath` в полный путь к файлу библиотеки, файл должен быть [скопирован на все узлы кластера](../dataproc-copy-files/).

Если все данные заданий используют формат хранения Delta Lake, то настройка S3A Committers не требуется, поскольку Delta Lake реализует собственный алгоритм управления записью в S3-совместимые хранилища, функционально эквивалентный S3A Committers. Задание может одновременно работать с таблицами Delta Lake и "обычными" данными Spark в форматах Parquet, ORC, JSON или CSV; в этом случае для эффективной записи "обычных" данных необходимо [использовать S3A Committers](../dataproc-s3a-committers/).

Пример запуска и использования сессии Spark SQL для работы с таблицами Delta Lake:

```
$ spark-sql --executor-memory 20g --executor-cores 4 \
  --conf spark.executor.heartbeatInterval=10s \
  --conf spark.jars.packages=io.delta:delta-core_2.12:0.8.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
...
Ivy Default Cache set to: /home/ubuntu/.ivy2/cache
The jars for the packages stored in: /home/ubuntu/.ivy2/jars
:: loading settings :: url = jar:file:/usr/lib/spark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
io.delta#delta-core_2.12 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-d1295d4c-d10d-464c-aa11-e645cedd5da9;1.0
	confs: [default]
	found io.delta#delta-core_2.12;0.8.0 in central
	found org.antlr#antlr4;4.7 in central
...
downloading https://repo1.maven.org/maven2/io/delta/delta-core_2.12/0.8.0/delta-core_2.12-0.8.0.jar ...
	[SUCCESSFUL ] io.delta#delta-core_2.12;0.8.0!delta-core_2.12.jar (168ms)
...
23/02/26 14:57:37 INFO SharedState: Warehouse path is 's3a://dproc-wh/wh'.
23/02/26 14:57:38 INFO SessionState: Created HDFS directory: /tmp/hive/ubuntu
23/02/26 14:57:38 INFO SessionState: Created local directory: /tmp/ubuntu
23/02/26 14:57:38 INFO SessionState: Created HDFS directory: /tmp/hive/ubuntu/8e4fa862-2e74-4423-ad8a-241082ec2c88
23/02/26 14:57:38 INFO SessionState: Created local directory: /tmp/ubuntu/8e4fa862-2e74-4423-ad8a-241082ec2c88
23/02/26 14:57:38 INFO SessionState: Created HDFS directory: /tmp/hive/ubuntu/8e4fa862-2e74-4423-ad8a-241082ec2c88/_tmp_space.db
23/02/26 14:57:38 INFO SparkContext: Running Spark version 3.0.3
...
23/02/26 14:57:59 INFO metastore: Connected to metastore.
Spark master: yarn, Application Id: application_1677423247688_0004
23/02/26 14:58:00 INFO SparkSQLCLIDriver: Spark master: yarn, Application Id: application_1677423247688_0004
spark-sql> CREATE DATABASE testdelta;
Time taken: 4.248 seconds
spark-sql> USE testdelta;
Time taken: 0.056 seconds
spark-sql> CREATE TABLE tab1(a INTEGER NOT NULL, b VARCHAR(100)) USING DELTA;
23/02/26 15:04:07 INFO DeltaLog: Creating initial snapshot without metadata, because the directory is empty
...
23/02/26 15:04:09 INFO CreateDeltaTableCommand: Table is path-based table: false. Update catalog with mode: Create
23/02/26 15:04:09 WARN HiveExternalCatalog: Couldn't find corresponding Hive SerDe for data source provider delta. Persisting data source table `testdelta`.`tab1` into Hive metastore in Spark SQL specific format, which is NOT compatible with Hive.
Time taken: 3.286 seconds
spark-sql> INSERT INTO tab1 VALUES (1,'One'), (2,'Two'), (3,'Three');
...
Time taken: 3.684 seconds
spark-sql> UPDATE tab1 SET b=b || ' ** ' || CAST(a AS VARCHAR(10));
...
Time taken: 3.803 seconds
spark-sql> SELECT * FROM tab1;
3	Three ** 3
2	Two ** 2
1	One ** 1
Time taken: 1.166 seconds, Fetched 3 row(s)
spark-sql> 
```

При настройке перечисленных выше свойств Spark для использования Delta Lake на уровне кластера Data Proc, работа с таблицами Delta Lake может производиться через Spark Thrift Server, включение которого осуществляется свойством `dataproc:hive.thrift.impl=spark`, как указано в [документации Data Proc](https://cloud.yandex.ru/docs/data-proc/concepts/settings-list#spark-thrift-server). Для подключения к Spark Thrift Server может использоваться любая совместимая SQL IDE, например, [DBeaver](https://dbeaver.io/).

## 3. Расширенные возможности Delta Lake для бета-образов Data Proc 2.1

```bash
yc ydb database create delta1 --serverless
yc ydb database get delta1 --format json >info-delta1.json

yc iam service-account create delta1-sa1

cur_profile=`yc config profile list | grep -E ' ACTIVE$' | (read x y && echo $x)`
cur_folder=`yc config profile get ${cur_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`
yc resource-manager folder add-access-binding --id ${cur_folder} --service-account-name delta1-sa1 --role ydb.editor

yc iam access-key create --service-account-name delta1-sa1 --format json > info-delta1-sa1-key.json

cur_key_id=`cat info-delta1-sa1-key.json | jq -r .access_key.key_id`
cur_key_val=`cat info-delta1-sa1-key.json | jq -r .secret`
yc lockbox secret create --name delta1-lb1 \
  --payload '[{"key": "key-id", "text_value": "'${cur_key_id}'"}, {"key": "key-secret", "text_value": "'${cur_key_val}'"}]' \
  --format json > info-delta1-lb1.json

cur_lb_id=`cat info-delta1-lb1.json | jq -r .id`

```


```bash
ls -l /s3data/jars/yc-delta-multi-1.0-SNAPSHOT-fatjar.jar
```

```
spark-sql --executor-memory 20g --executor-cores 4 \
  --conf spark.executor.heartbeatInterval=10s \
  --conf spark.driver.extraClassPath=/s3data/jars/yc-delta-multi-1.0-SNAPSHOT-fatjar.jar \
  --conf spark.executor.extraClassPath=/s3data/jars/yc-delta-multi-1.0-SNAPSHOT-fatjar.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.databricks.delta.optimize.maxThreads=20 \
  --conf spark.delta.logStore.s3a.impl=yandex.cloud.custom.delta.YcS3YdbLogStore \
  --conf spark.io.delta.storage.S3DynamoDBLogStore.ddb.endpoint=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etngt3b6eh9qfc80vt54/ \
  --conf spark.io.delta.storage.S3DynamoDBLogStore.ddb.lockbox=e6qr20sbgn3ckpalh54p
```

```sql
USE demo2;

CREATE TABLE deltatab1 (
  num bigint not null,
  tv timestamp not null,
  a varchar(40) not null,
  b varchar(40) not null,
  c varchar(40) not null,
  d varchar(40) not null,
  tv_year INT not null,
  tv_month INT not null,
  tv_day INT not null
) USING DELTA
  PARTITIONED BY (tv_year, tv_month);
  

CREATE VIEW demo1_tab1k_v AS (
WITH tab100(a) AS (
  (SELECT 0 AS a) UNION ALL (SELECT 1) UNION ALL (SELECT 2) UNION ALL (SELECT 3) UNION ALL (SELECT 4) UNION ALL
  (SELECT 5) UNION ALL (SELECT 6) UNION ALL (SELECT 7) UNION ALL (SELECT 8) UNION ALL (SELECT 9) UNION ALL
  (SELECT 10) UNION ALL (SELECT 11) UNION ALL (SELECT 12) UNION ALL (SELECT 13) UNION ALL (SELECT 14) UNION ALL
  (SELECT 15) UNION ALL (SELECT 16) UNION ALL (SELECT 17) UNION ALL (SELECT 18) UNION ALL (SELECT 19) UNION ALL
  (SELECT 20) UNION ALL (SELECT 21) UNION ALL (SELECT 22) UNION ALL (SELECT 23) UNION ALL (SELECT 24) UNION ALL
  (SELECT 25) UNION ALL (SELECT 26) UNION ALL (SELECT 27) UNION ALL (SELECT 28) UNION ALL (SELECT 29) UNION ALL
  (SELECT 30) UNION ALL (SELECT 31) UNION ALL (SELECT 32) UNION ALL (SELECT 33) UNION ALL (SELECT 34) UNION ALL
  (SELECT 35) UNION ALL (SELECT 36) UNION ALL (SELECT 37) UNION ALL (SELECT 38) UNION ALL (SELECT 39) UNION ALL
  (SELECT 40) UNION ALL (SELECT 41) UNION ALL (SELECT 42) UNION ALL (SELECT 43) UNION ALL (SELECT 44) UNION ALL
  (SELECT 45) UNION ALL (SELECT 46) UNION ALL (SELECT 47) UNION ALL (SELECT 48) UNION ALL (SELECT 49) UNION ALL
  (SELECT 50) UNION ALL (SELECT 51) UNION ALL (SELECT 52) UNION ALL (SELECT 53) UNION ALL (SELECT 54) UNION ALL
  (SELECT 55) UNION ALL (SELECT 56) UNION ALL (SELECT 57) UNION ALL (SELECT 58) UNION ALL (SELECT 59) UNION ALL
  (SELECT 60) UNION ALL (SELECT 61) UNION ALL (SELECT 62) UNION ALL (SELECT 63) UNION ALL (SELECT 64) UNION ALL
  (SELECT 65) UNION ALL (SELECT 66) UNION ALL (SELECT 67) UNION ALL (SELECT 68) UNION ALL (SELECT 69) UNION ALL
  (SELECT 70) UNION ALL (SELECT 71) UNION ALL (SELECT 72) UNION ALL (SELECT 73) UNION ALL (SELECT 74) UNION ALL
  (SELECT 75) UNION ALL (SELECT 76) UNION ALL (SELECT 77) UNION ALL (SELECT 78) UNION ALL (SELECT 79) UNION ALL
  (SELECT 80) UNION ALL (SELECT 81) UNION ALL (SELECT 82) UNION ALL (SELECT 83) UNION ALL (SELECT 84) UNION ALL
  (SELECT 85) UNION ALL (SELECT 86) UNION ALL (SELECT 87) UNION ALL (SELECT 88) UNION ALL (SELECT 89) UNION ALL
  (SELECT 90) UNION ALL (SELECT 91) UNION ALL (SELECT 92) UNION ALL (SELECT 93) UNION ALL (SELECT 94) UNION ALL
  (SELECT 95) UNION ALL (SELECT 96) UNION ALL (SELECT 97) UNION ALL (SELECT 98) UNION ALL (SELECT 99)),
  tab1k(a) AS (SELECT x.a + 100*y.a AS a FROM tab100 x, tab100 y WHERE y.a<10)
SELECT CAST(x.a AS BIGINT) AS num FROM tab1k x);

CREATE VIEW demo1_tab1g_v AS (
  SELECT 1 + x.num + 1000*y.num + 1000000*z.num AS num 
  FROM demo1_tab1k_v x, demo1_tab1k_v y, demo1_tab1k_v z
);

CREATE VIEW demo1_uuid1g_v AS (
  SELECT num, tv, 
     extract(YEAR FROM tv) AS tv_year,
     extract(MONTH FROM tv) AS tv_month,
     extract(DAY FROM tv) AS tv_day,
     a, b, c, d
  FROM (SELECT x.num,
          y.tv_now - make_interval(0,0,0,0,0,0,x.num) AS tv,
          uuid() AS a, uuid() AS b, uuid() AS c, uuid() AS d 
        FROM demo1_tab1g_v x INNER JOIN (SELECT Now() AS tv_now) y ON 1=1)
);

INSERT INTO deltatab1
SELECT /*+ REPARTITION(50,tv_year,tv_month) */
  num,COALESCE(tv,TIMESTAMP '1980-01-01 00:00:00') AS tv,a,b,c,d,tv_year,tv_month,tv_day
FROM demo1_uuid1g_v;

optimize deltatab1;

select substring(a,1,1) as al, count(*) from deltatab1 group by substring(a,1,1) order by substring(a,1,1);

select substring(a,1,1) as al, count(*) from deltatab1 where b like '7f%' group by substring(a,1,1) order by substring(a,1,1);

convert to delta megatab_1g partitioned by (tv_year INT, tv_month INT);

```
