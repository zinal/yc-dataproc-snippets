#! /bin/sh

YC_CLUSTER=dpx
YC_VERSION=2.1
YC_ZONE=ru-central1-c
YC_SUBNET=default-ru-central1-c
YC_BUCKET=dproc-wh
YC_SA=dp1
YC_MS_URI='thrift://rc1c-dataproc-m-yempltyygr9d8pjh.mdb.yandexcloud.net:9083'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,spark \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='computenode',resource-preset='s3-c16-m64',preemptible=false,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=3,max-hosts-count=3,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property yarn:yarn.node-labels.fs-store.root-dir=file:///hadoop/yarn/node-labels \
  --property yarn:yarn.node-labels.enabled=true \
  --property yarn:yarn.node-labels.configuration-type=centralized \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property core:yarn.system-metrics-publisher.enabled=true \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.hadoop.fs.s3a.committer.name=directory \
  --property spark:spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --property spark:spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --property spark:spark.driver.extraJavaOptions='-XX:+UseG1GC' \
  --property spark:spark.executor.extraJavaOptions='-XX:+UseG1GC' \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/dataproc-init-script.sh" \
  --async
