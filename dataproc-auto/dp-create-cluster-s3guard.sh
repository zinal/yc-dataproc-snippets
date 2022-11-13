#! /bin/sh

# Пример команд для создания кластера Data Proc.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * бессерверная база данных YDB для работы S3Guard,
#  * права на доступ из сервисного аккаунта к базе YDB,
#  * статический ключ для созданного сервисного аккаунта,
#  * элемент хранения статического ключа в сервисе Lockbox,
#  * внешний сервис Hive Metastore.

# yc lockbox secret create --name ydb-aws-creds1 --payload '[{"key": "key-id", "text_value": "X"}, {"key": "key-secret", "text_value": "Y"}]'

YC_CLUSTER=dproc1
YC_VERSION=2.0
YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=ms1
YC_SA=dp1
YC_YDB='https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5'
YC_LOCKBOX_KEY=e6qkqma0dne53bjucgr2
YC_METASTORE='thrift://rc1b-dataproc-m-enwlteuuxbltzq8r.mdb.yandexcloud.net:9083'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBAcV0HrAheoHrN5ow3i6+3QN6TCyB7FNDsQbhK+i8aPNgYsdT4iWDIAllVGxUHRprkBSKSt5tzweXF48pztGTJo= mzinal@ydb1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,hive,tez,spark,livy,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s3-c4-m16',disk-type='network-ssd-nonreplicated',disk-size=93,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute1",role='computenode',resource-preset='s3-c8-m32',disk-type='network-ssd-nonreplicated',disk-size=93,hosts-count=1,max-hosts-count=3,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --initialization-action='uri=s3a://dproc-code/init-scripts/dp-init-s3guard.sh' \
  --property core:fs.s3a.metadatastore.impl=org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore \
  --property core:fs.s3a.bucket.dproc-code.metadatastore.impl=org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore \
  --property core:fs.s3a.s3guard.ddb.client.factory.impl=ru.yandex.cloud.s3guard.YcDynamoDBClientFactory \
  --property core:fs.s3a.s3guard.ddb.endpoint=${YC_YDB} \
  --property core:fs.s3a.s3guard.ddb.keyfile=/etc/s3guard-ydb-keyfile.xml \
  --property core:fs.s3a.s3guard.ddb.region=ru-central1 \
  --property core:fs.s3a.s3guard.ddb.table=dproc_s3guard \
  --property core:fs.s3a.s3guard.ddb.table.create=true \
  --property core:fs.s3a.committer.name=magic \
  --property core:fs.s3a.committer.magic.enabled=true \
  --property core:fs.s3a.committer.staging.conflict-mode=fail \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property hive:hive.metastore.uris="${YC_METASTORE}" \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,com.yandex.cloud,ru.yandex.cloud \
  --property spark:spark.hadoop.fs.s3a.committer.name=magic \
  --property spark:spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --property spark:spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --property spark:spark.executor.extraJavaOptions='-XX:+UseG1GC' \
  --property spark:spark.executor.cores=2 \
  --property spark:spark.executor.memory=4g \
  --property spark:spark.driver.cores=2 \
  --property spark:spark.driver.memory=4g
