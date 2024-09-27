#!/bin/bash

export TERM=dumb
set -e
set -u

# Увеличиваем MTU
# Внимание! Увеличение MTU может снизить производительность при взаимодействии
# с сервисами, использующими малые значения MTU, включая Yandex Cloud MDB.
ip link set eth0 mtu 8900

export DEBIAN_FRONTEND=noninteractive
apt-get install -y mc zip unzip screen

# Настройка логгинга для Spark
if [ ! -f /etc/spark/conf/log4j2.properties ]; then
cat >/etc/spark/conf/log4j2.properties <<EOF
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

# YDB Spark Connector Debugging
logger.ydb0.name = tech.ydb
logger.ydb0.level = debug
logger.ydb1.name = tech.ydb.core.impl.discovery
logger.ydb1.level = info
logger.ydb2.name = tech.ydb.spark
logger.ydb2.level = debug
logger.ydb3.name = tech.ydb.shaded
logger.ydb3.level = warn
EOF
fi

# End Of File