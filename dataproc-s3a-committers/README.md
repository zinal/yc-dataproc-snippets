# Настройка S3A коммиттеров в Yandex Data Proc для эффективной записи в Yandex Object Storage

S3A Committers - входящий в состав [Apache Hadoop](https://hadoop.apache.org/) набор алгоритмов для записи данных в объектное хранилище по протоколу S3, обеспечивающий эффективное и приближенное к атомарному подтверждение выполненных изменений. S3A Committers активируются специальными настройками для Hadoop и Spark, и рекомендованы к использованию в сервисе [Yandex Data Proc](https://cloud.yandex.ru/services/data-proc).

В современных условиях потребность в специальном алгоритме для записи в объектное хранилище возникает из-за особенностей протокола S3, который не поддерживает (и не может поддерживать) атомарную операцию переименования файлов и каталогов.

> *Примечание* Документация на S3A Committers дополнительно указывает на возможную неконсистентность листингов S3, и рекомендует применение компонента S3Guard для обеспечения стабильной работы. В современных S3 хранилищах (включая [Yandex Object Storage](https://cloud.yandex.ru/services/storage)) эта проблема не проявляется, и использование S3Guard не рекомендовано при работе с Yandex Data Proc.

Подробное описание логики работы и использования S3A Committers приведено в соответствующих разделах [документации Apache Hadoop](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html), а также [документации Apache Spark](https://spark.apache.org/docs/3.0.3/cloud-integration.html).

> *Примечание* S3A Committers не используются и не требуются для работы с таблицами, управляемыми средствами библиотеки [DeltaLake](https://delta.io/), которая реализует собственную логику для работы с данными в объектном хранилище.

## Условия применения и режимы работы S3A Committers

Существует три основных режима работы S3A Committers, пригодные к разным ситуациям. Сводка по возможностям разных режимов приведена ниже в таблице.

| Режим        | Среда           | Нужен HDFS для временных данных? | Запись в партиционированные таблицы | Скорость записи |
| ------------ | --------------- | -------------------------------- | ----------------------------------- | --------------- |
| magic        | Spark, MapRed   | Нет, запись напрямую в S3        | Не поддерживается                   | Максимальная    |
| directory    | Spark, MapRed   | Да                               | Полная перезапись                   | Обычная         |
| partitioned  | Spark           | Да                               | Замена и дополнение партиций        | Обычная         |

Можно видеть, что:
* режим magic обеспечивает самую быструю запись за счёт отсутствия промежуточных копий данных;
* режимам directory и partitioned требуется HDFS для размещения временных данных;
* для записи в партиционированные таблицы требуются режимы directory или partitioned;
* задания Spark могут обновлять отдельные партиции в партиционированных таблицах, используя режим partitioned. 

## Включение S3A Committers

Для включения S3A Committers необходимо установить ряд настроек на уровне кластера, как показано в таблице ниже.

| Уровень | Настройка | Значение |
| --------| --------- | -------- |
| core | `mapreduce.outputcommitter.factory.scheme.s3a` | `org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory` |
| core | `fs.s3a.committer.name` | Используемый режим по умолчанию: `magic`, `directory` либо `partitioned` |
| core | `fs.s3a.committer.magic.enabled` | true, если задания будут использовать режим `magic` |
| spark | `spark.hadoop.fs.s3a.committer.name` | Используемый режим по умолчанию: `magic`, `directory` либо `partitioned` |
| spark | `spark.sql.sources.commitProtocolClass` | `org.apache.spark.internal.io.cloud.PathOutputCommitProtocol` |
| spark | `spark.sql.parquet.output.committer.class` | `org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter` |

Используемый режим работы S3A Committers может переопределяться для конкретного задания путём установки настроек `fs.s3a.committer.name` и `spark.hadoop.fs.s3a.committer.name` в необходимое значение (`magic`, `directory` либо `partitioned`).

При использовании режима `partitioned` можно выбрать желаемый вариант обработки конфликтов, т.е. действия при обнаружении в целевой таблице уже существующих партиций с данными. Выбор осуществляется путём установки параметра `fs.s3a.committer.staging.conflict-mode` (глобально для кластера в целом либо для конкретного задания Spark) в одно из следующих значений:
* `fail` - при попытке перезаписи существующей партиции задание останавливается с ошибкой;
* `replace` - данные в существующей партиции заменяются на записываемые данные новой партиции;
* `append` - данные в существующей партиции дополняются новыми данными.

Не следует менять значение по умолчанию для настройки `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version` (использование алгоритма 1), поскольку хранилище Yandex Object Storage не поддерживает атомарные переименования каталогов.

При наличии нескольких параллельно работающих заданий, выполняющих запись информации в одну и ту же таблицу, необходимо установить параметр `fs.s3a.committer.staging.abort.pending.uploads` (для Hadoop 3.2.2 в составе образов Data Proc 2.0.x) либо `fs.s3a.committer.abort.pending.uploads` (для Hadoop 3.3.2 в составе экспериментальных образов Data Proc 2.1.x) в значение `false`.

## Настройки для оптимизации производительности записи в S3

При доступе к данным в объектном хранилище из заданий Spark рекомендованы следующие настройки:
* `spark.sql.hive.metastorePartitionPruning true`

Для работы в заданиях Spark с данными в формате Parquet:
* `spark.hadoop.parquet.enable.summary-metadata false`
* `spark.sql.parquet.mergeSchema false`
* `spark.sql.parquet.filterPushdown true`

Для работы в заданиях Spark с данными в формате Orc:
* `spark.sql.orc.filterPushdown true`
* `spark.sql.orc.splits.include.file.footer true`
* `spark.sql.orc.cache.stripe.details.size 10000`

