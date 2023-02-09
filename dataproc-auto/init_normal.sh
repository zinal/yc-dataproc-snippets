#!/bin/bash

set -e

# Увеличиваем MTU
ip link set eth0 mtu 8910

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

# **** Begin DISABLED GeeseFS
if false; then

BUCKET=$1
MOUNT_POINT=/s3data

# Загрузка GeeseFS
wget -nv https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64 -O /opt/geesefs
chmod a+rwx /opt/geesefs
mkdir -p "${MOUNT_POINT}"

# Подготовка скрипта, выполняющегося при каждой загрузке
BOOT_SCRIPT="/var/lib/cloud/scripts/per-boot/80-geesefs-mount.sh"
echo "#!/bin/bash" >> ${BOOT_SCRIPT}
echo "/opt/geesefs -o allow_other --iam ${BUCKET} ${MOUNT_POINT}" >> ${BOOT_SCRIPT}
chmod 755 ${BOOT_SCRIPT}

# Запуск скрипта
${BOOT_SCRIPT}

fi
# **** End GeeseFS

# Загрузка и установка недостающих файлов JAR
mkdir -p -v /tmp/depjars
chown -R hdfs:hdfs /tmp/depjars
DESTDIR=/usr/lib/spark/external/lib
if [ -f ${DESTDIR}/spark-sql-kafka-0-10_2.12-3.0.3.jar ]; then
  # Data Proc 2.0
  JARLIB=`ls ${DESTDIR}/commons-pool2-*.jar`
  if [ -z "$JARLIB" ]; then
    # Не хватает библиотек для работы с Kafka
    sudo -u hdfs hdfs dfs -copyToLocal 's3a://dproc-code/dp20/depjars/' /tmp/
    (cd ${DESTDIR} && \
      mv /tmp/depjars/commons-pool2-2.6.2.jar . && \
      mv /tmp/depjars/kafka-clients-2.4.1.jar . && \
      chown root:root kafka-clients-2.4.1.jar commons-pool2-2.6.2.jar && \
      chmod 644 kafka-clients-2.4.1.jar commons-pool2-2.6.2.jar)
  fi
fi
if [ -f ${DESTDIR}/spark-sql-kafka-0-10_2.12-3.2.1.jar ]; then
  # Data Proc 2.1
fi

# End Of File