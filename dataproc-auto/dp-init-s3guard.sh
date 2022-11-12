#! /bin/sh

WORK_SHARED_DIR=/root/shared-libs
VETO_FILE=${WORK_SHARED_DIR}/00-veto.txt

[ -d ${WORK_SHARED_DIR} ] || mkdir ${WORK_SHARED_DIR}
hdfs dfs -copyToLocal s3a://dproc-code/shared-libs/* ${WORK_SHARED_DIR}/

if [ -f ${VETO_FILE} ]; then
  cat ${VETO_FILE} | while read fn; do
    [ -z "$fn" ] || rm -fv "$fn"
  done
fi

(cd ${WORK_SHARED_DIR} && ls) | grep -E '[.]jar$' | while read fn; do
  cp -v ${WORK_SHARED_DIR}/"$fn" /usr/lib/hadoop/lib/
  cp -v ${WORK_SHARED_DIR}/"$fn" /usr/lib/spark/jars/
done

# End Of File