#!/bin/bash

set -e
set -u

# Загрузка и установка недостающих файлов JAR для работы
# в заданиях Spark с источниками и получателями Kafka.

BUCKET=$1
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