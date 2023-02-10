#!/bin/bash

BUCKET=dproc-wh

export TERM=dumb
set -e
set -u

# Увеличиваем MTU
# Внимание! Увеличение MTU может снизить производительность при взаимодействии
# с сервисами, использующими малые значения MTU, включая Yandex Cloud MDB.
#ip link set eth0 mtu 8910

export DEBIAN_FRONTEND=noninteractive
apt-get install -y mc zip unzip screen

# Настройка логгинга для Spark
if [ ! -f /etc/spark/conf/log4j.properties ]; then
cat >/etc/spark/conf/log4j.properties <<EOF
# Set everything to be logged to the console
log4j.rootCategory=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
log4j.logger.org.apache.spark.repl.Main=WARN

# Settings to quiet third party logs that are too verbose
log4j.logger.org.sparkproject.jetty=WARN
log4j.logger.org.sparkproject.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR

# Extra loggers to avoid
log4j.logger.org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport=WARN
log4j.logger.org.apache.hadoop.metrics2.impl.MetricsSystemImpl=WARN
EOF
fi

# Загрузка и установка недостающих файлов JAR для Kafka
DESTDIR1=/usr/lib/spark/external/lib
DESTDIR2=/usr/lib/spark/jars
DESTDIR3=/usr/lib/hadoop-mapreduce

JARLIB=`ls ${DESTDIR1}/ | grep commons-pool2 | cat`
if [ -z "$JARLIB" ]; then
  # Не хватает библиотек для работы с Kafka
  mkdir -p -v /tmp/depjars
  chown -R hdfs:hdfs /tmp/depjars
  if [ -f ${DESTDIR1}/spark-sql-kafka-0-10_2.12-3.0.3.jar ]; then
    # Data Proc 2.0
    sudo -u hdfs hdfs dfs -copyToLocal "s3a://$BUCKET/dp20/depjars/" /tmp/

    (cd ${DESTDIR1} && \
      mv /tmp/depjars/commons-pool2-2.6.2.jar . && \
      mv /tmp/depjars/kafka-clients-2.4.1.jar . && \
      chown root:root kafka-clients-2.4.1.jar commons-pool2-2.6.2.jar && \
      chmod 644 kafka-clients-2.4.1.jar commons-pool2-2.6.2.jar)

    (cd ${DESTDIR3} && \
      mv kafka-clients-0.8.2.1.jar kafka-clients-0.8.2.1.jar-bak && \
      ln -s ${DESTDIR1}/kafka-clients-2.4.1.jar .)

  elif [ -f ${DESTDIR1}/spark-sql-kafka-0-10_2.12-3.2.1.jar ]; then
    # Data Proc 2.1
    sudo -u hdfs hdfs dfs -copyToLocal "s3a://$BUCKET/dp21/depjars/" /tmp/

    (cd ${DESTDIR1} && \
      mv /tmp/depjars/commons-pool2-2.6.2.jar . && \
      mv /tmp/depjars/kafka-clients-2.8.2.jar . && \
      chown root:root kafka-clients-2.8.2.jar commons-pool2-2.6.2.jar && \
      chmod 644 kafka-clients-2.8.2.jar commons-pool2-2.6.2.jar)

    (cd ${DESTDIR2} && \
      ln -s ${DESTDIR1}/commons-pool2-2.6.2.jar . && \
      ln -s ${DESTDIR1}/kafka-clients-2.8.2.jar . && \
      ln -s ${DESTDIR1}/spark-sql-kafka-0-10_2.12-3.2.1.jar . && \
      ln -s ${DESTDIR1}/spark-token-provider-kafka-0-10_2.12-3.2.1.jar .)

    (cd ${DESTDIR3} && \
      mv kafka-clients-2.8.1.jar kafka-clients-2.8.1.jar-bak && \
      ln -s ${DESTDIR1}/kafka-clients-2.8.2.jar .)

  fi
fi

# End Of File