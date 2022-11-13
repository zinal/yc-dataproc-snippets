#! /bin/sh

WORK_SHARED_DIR=/root/shared-libs
VETO_FILE=${WORK_SHARED_DIR}/00-veto.txt
S3GUARD_KEY_FILE=${WORK_SHARED_DIR}/01-s3guard-ydb-keyfile.xml
S3GUARD_PATCH_FILE=${WORK_SHARED_DIR}/02-yc-s3guard-jar.data

# Copy the extra libraries and configuration to the local directory
[ -d ${WORK_SHARED_DIR} ] || mkdir ${WORK_SHARED_DIR}
hdfs dfs -copyToLocal s3a://dproc-code/shared-libs/* ${WORK_SHARED_DIR}/

# If the veto file exists, drop some of the existing files
if [ -f ${VETO_FILE} ]; then
  cat ${VETO_FILE} | while read fn; do
    [ -z "$fn" ] || rm -fv "$fn"
  done
fi

# This function applies the patch to hadoop-aws-*.jar
applyS3guardPatch() {
  DEST=`find $2 -name 'hadoop-aws*.jar' -type f | sort | head -n 1`
  (cd $1 && jar uvf $DEST *)
}

# If the S3Guard patch exists, apply it
if [ -f ${S3GUARD_PATCH_FILE} ]; then
  PD=/tmp/s3guard-patch
  mkdir ${PD}
  cat ${S3GUARD_PATCH_FILE} | (cd ${PD} && jar x)
  rm -rf /tmp/s3guard-patch/META-INF
  NENTRIES=`(cd ${PD} && ls) | wc -l`
  if [ ${NENTRIES} -gt 0 ]; then # If the patch contains something
    applyS3guardPatch ${PD} /usr/lib/hadoop-mapreduce
    applyS3guardPatch ${PD} /usr/lib/spark/jars
    # Yarn Node Manager may be running, in that case it needs to be restarted.
    yarnpid=`ps -u yarn -f | grep nodemanager | (read x y z && echo $y)`
    if [ ! -z "$yarnpid" ]; then
      systemctl restart hadoop-yarn@nodemanager.service 
    fi
  fi
  rm -rf ${PD}
fi

# If the S3Guard key file exists, put it to the expected location
if [ -f ${S3GUARD_KEY_FILE} ]; then
  cp ${S3GUARD_KEY_FILE} /etc/s3guard-ydb-keyfile.xml
  chmod 444 /etc/s3guard-ydb-keyfile.xml
fi

# End Of File