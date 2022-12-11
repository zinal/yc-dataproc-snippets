#! /bin/sh

# Пример команд для создания кластера Data Proc.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * база данных PostgreSQL для Hive Metastore

YC_CLUSTER=dproc3
YC_VERSION=2.0
YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=ms1
YC_SA=dp1
YC_DBURL='jdbc:postgresql://rc1b-pajy1x3lh8tk04u4.mdb.yandexcloud.net:6432/hive?targetServerType=master&ssl=true&sslmode=require'
YC_DBUSER='hive'
YC_DBPASS='sai0Doh8Eib0iav'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,hive,tez,spark,livy,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="${YC_CLUSTER}_master",role='masternode',resource-preset='s3-c8-m32',disk-type='network-hdd',disk-size=150,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="${YC_CLUSTER}_compute",role='computenode',resource-preset='s3-c16-m64',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=8,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=magic \
  --property core:fs.s3a.committer.magic.enabled=true \
  --property core:fs.s3a.committer.staging.conflict-mode=fail \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.hadoop.fs.s3a.committer.name=magic \
  --property spark:spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --property spark:spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --property spark:spark.executor.extraJavaOptions='-XX:+UseG1GC' \
  --property hive:javax.jdo.option.ConnectionURL=${YC_DBURL} \
  --property hive:javax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  --property hive:javax.jdo.option.ConnectionPassword=${YC_DBPASS} \
  --property hive:javax.jdo.option.ConnectionUserName=${YC_DBUSER} \
  --property hive:hive.metastore.warehouse.dir=s3a://${YC_BUCKET}/warehouse
