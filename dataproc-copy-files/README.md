# Автоматическое копирование дополнительных файлов на узлы Data Proc из объектного хранилища

> **Внимание!** В кластерах Data Proc с автомасштабированием использование скриптов инициализации для автоматического копирования файлов на узлы кластера в настоящее время не поддерживается. Это временное ограничение продукта Data Proc, которое будет устранено в будущем.

Необходимость в копировании на узлы кластера Data Proc дополнительных файлов обычно возникает в тех случаях, когда запускаемые задания обращаются к дополнительным программным библиотекам. Если такие библиотеки регулярно используются, то наличие локальной копии в файловой системе узлов кластера позволяет заметно ускорить запуск заданий.

При создании кластера Data Proc, а также при динамическом добавлении узлов в кластер автоматическое копирование на узлы кластера необходимых файлов может быть обеспечено с помощью скрипта инициализации. Необходимые файлы можно разместить в бакете объектного хранилища, и предоставить доступ к используемому бакету сервисной учётной записи кластера Data Proc. Скрипт инициализации должен обеспечить копирование файлов из объектного хранилища в локальную файловую систему узла Data Proc.

Предположим, что необходимые для запуска заданий Spark дополнительные библиотеки JAR размещены в каталоге `/depjars/` бакета `dproc-code`. Тогда скрипт инициализации, копирующий эти библиотеки в локальную файловую систему кластера Data Proc, будет выглядеть следующим образом:

```bash
#!/bin/bash

set -e

SRCDIR='s3a://dproc-code/depjars'
DSTDIR='/opt/depjars'
DSTBASE=`dirname ${DSTDIR}`

mkdir -p -v ${DSTDIR}
chown -R hdfs:hdfs ${DSTDIR}
sudo -u hdfs hdfs dfs -copyToLocal ${SRCDIR}/ ${DSTBASE}/
chown -R root:root ${DSTDIR}
```

Для копирования используется режим `copyToLocal` утилиты `dfs` программного обеспечения Hadoop. Это позволяет не устанавливать дополнительное программное обеспечение (клиента AWS CLI или утилиту `s3fs`) на кластер Data Proc без реальной необходимости.

Если разместить указанный выше скрипт инициализации в бакете `dproc-code` под именем `/init-scripts/init-depjars.sh`, то при создании кластера опцию для вызова скрипта инициализации необходимо будет указать через параметр `initialization-action`, как показано в примере ниже.

Для подключения необходимых JAR-библиотек во все задания Spark можно воспользоваться свойствами `spark.driver.extraClassPath` и `spark.executor.extraClassPath`.

```bash
yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services hdfs,yarn,tez,mapreduce,hive,spark,livy \
  --bucket ${YC_BUCKET} \
  ...
  --property spark:spark.driver.extraClassPath=/opt/depjars/delta-core_2.12-0.8.0.jar \
  --property spark:spark.executor.extraClassPath=/opt/depjars/delta-core_2.12-0.8.0.jar \
  --initialization-action 'uri=s3a://dproc-code/init-scripts/init-depjars.sh'
```

> **Примечание.** Свойство `spark.jars` может быть использовано вместо устаревших свойств `spark.*.extraClassPath`, но оно переопределяется при запуске заданий средствами Apache Livy, в результате чего централизованно установленное на уровне кластера значение этого параметра при запуске через Apache Livy не используется.
