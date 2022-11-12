#! /bin/sh

WORK_SHARED_DIR=/root/shared-libs

[ -d ${WORK_SHARED_DIR} ] || mkdir ${WORK_SHARED_DIR}
hdfs dfs -copyToLocal s3a://dproc-code/shared-libs/* ${WORK_SHARED_DIR}/

(cd ${WORK_SHARED_DIR} && ls) | while read fn; do
  cp -v ${WORK_SHARED_DIR}/"$fn" /usr/lib/hadoop/lib/
  cp -v ${WORK_SHARED_DIR}/"$fn" /usr/lib/spark/jars/
done


# Veto files: /usr/lib/hadoop/lib/protobuf-java-2.5.0.jar
