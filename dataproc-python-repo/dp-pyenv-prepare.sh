#! /bin/sh

# Кластер для подготовки окружения Python.

. ./dp-pyenv-options.sh

YC_CLUSTER=pyenv-prepare1

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services hdfs,yarn,tez,mapreduce,spark,livy \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="data",role='datanode',resource-preset='s2.large',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='s2.large',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=0,max-hosts-count=10,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=3600 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.hadoop.fs.s3a.committer.name=directory \
  --property spark:spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --property spark:spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --property spark:spark.driver.extraJavaOptions='-XX:+UseG1GC' \
  --property spark:spark.executor.extraJavaOptions='-XX:+UseG1GC' \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000

# End Of File