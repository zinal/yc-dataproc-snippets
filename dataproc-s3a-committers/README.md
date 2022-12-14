# Настройка S3A коммиттеров в Yandex Data Proc для эффективной записи в Yandex Object Storage

S3A Committers - входящий в состав [Apache Hadoop](https://hadoop.apache.org/) набор программных модулей для записи данных в объектное хранилище по протоколу S3, обеспечивающий эффективное и приближенное к атомарному подтверждение выполненных изменений. S3A Committers активируются специальными настройками для Hadoop и Spark, и рекомендованы к использованию в сервисе [Yandex Data Proc](https://cloud.yandex.ru/services/data-proc).

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

> *Примечание* При включении режимов `directory` и `partitioned` не производится никаких проверок на фактическое наличие HDFS для хранения промежуточных данных. Часть заданий может успешно отрабатывать при отсутствии HDFS и использовании этих режимов, а проблемы могут возникать в более сложных заданиях, и проявляться в виде ошибок "файл не найден", либо в форме неполной записи результатов задания в объектное хранилище.

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

## Настройки Apache Hadoop для оптимизации производительности записи в S3

Настройки по оптимизации производительности записи в объектные хранилища по протоколу S3 приведены в [документации Apache Hadoop](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html).

Для эффективной многопоточной работы необходимо увеличить количество разрешённых соединений с объектным хранилищем и количество рабочих потоков в менеджере загрузок (AWS Transfer Manager). Необходимо учитывать, что высокие значения, установленные для указанных ниже параметров, могут привести к увеличению потребления вычислительных ресурсов на рабочих узлах кластера Data Proc:
* `core:fs.s3a.threads.max 100`
* `core:fs.s3a.connection.maximum 200`

Операции над объектным хранилищем, которые не могут быть запущены на выполнение из-за исчерпания рабочих потоков, помещаются в очередь, размер которой ограничивается ещё одним параметром:
* `core:fs.s3a.max.total.tasks 100`

В некоторых случаях может потребоваться увеличить максимальный период ожидания соединения с сервисом объектного хранилища, особенно в заданиях, создающих высокую нагрузку. Параметр задаётся в миллисекундах, в примере ниже для него устанавливается значение в 2 минуты:
* `core:fs.s3a.connection.timeout 120000`

Для ускорения фиксации изменений в объектном хранилище, выполняемом в конце работы задания, бывает полезно увеличить количество потоков, выполняющих эту задачу:
* `core:fs.s3a.committer.threads 100` 

## Настройки Apache Spark для оптимизации производительности записи в S3

Для заданий Apache Spark есть [дополнительные рекомендации](https://spark.apache.org/docs/3.0.3/cloud-integration.html).

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

Задания, создающие или обновляющие большое количество (сотни и тысячи) партиций в партиционированных таблицах, иногда страдают от длительной актуализации записей о партициях в Hive Metastore. Для ускорения соответствующего процесса можно установить следующие настройки:
* `spark:spark.sql.addPartitionInBatch.size 1000` - количество партиций, актуализируемых за один вызов Hive Metastore, оптимально значение 10x или выше от количества рабочих потоков Hive Metastore (см. параметр `hive:hive.metastore.fshandler.threads`);
* `hive:hive.metastore.fshandler.threads 100` - количество рабочих потоков, выполняющих фоновые операции с файловой системой в рамках сервиса Hive Metastore;
* `hive:datanucleus.connectionPool.maxPoolSize 100` - лимит на размер пула соединений к БД Hive Metastore.

Необходимо учитывать, что бесконтрольное увеличение перечисленных выше параметров может привести к исчерпанию системных ресурсов Hive Metastore. Большой размер пула соединений к БД Hive Metastore может потребовать изменения настроек и выделение дополнительных мощностей на стороне сервиса СУБД, обслуживающего базу данных Hive Metastore.
