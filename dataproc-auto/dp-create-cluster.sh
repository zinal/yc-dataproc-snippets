#! /bin/sh

# Пример команд для создания кластера Data Proc.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * внешний сервис Hive Metastore

YC_CLUSTER=dproc1
YC_VERSION=2.0
YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=ms1
YC_SA=dp1
YC_METASTORE='thrift://rc1b-dataproc-m-vrgbjt4afo6zvrtj.mdb.yandexcloud.net:9083'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,hive,tez,spark,livy,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="${YC_CLUSTER}_master",role='masternode',resource-preset='m3-c2-m16',disk-type='network-hdd',disk-size=150,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="${YC_CLUSTER}_compute",role='computenode',resource-preset='s3-c4-m16',disk-type='network-ssd-nonreplicated',disk-size=93,hosts-count=1,max-hosts-count=3,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=magic \
  --property core:fs.s3a.committer.magic.enabled=true \
  --property core:fs.s3a.committer.staging.conflict-mode=fail \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property hive:hive.metastore.uris="${YC_METASTORE}" \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.hadoop.fs.s3a.committer.name=magic \
  --property spark:spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --property spark:spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --property spark:spark.executor.extraJavaOptions='-XX:+UseG1GC' \
  --property spark:spark.executor.cores=1 \
  --property spark:spark.executor.memory=3g \
  --property spark:spark.driver.cores=2 \
  --property spark:spark.driver.memory=4g
