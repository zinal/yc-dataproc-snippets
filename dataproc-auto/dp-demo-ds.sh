#! /bin/sh

# Комбинированный кластер для работы со Spark и Hive.
# Основной путь хранения установлен в каталог в S3.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * база данных PostgreSQL

YC_CLUSTER=dsdemo-1
YC_VERSION=2.0
YC_ZONE=ru-central1-c
YC_SUBNET=default-ru-central1-c
YC_BUCKET=dprocwh1
YC_SA=dp1
YC_MS_URL='jdbc:postgresql://gw0:5432/hivems?ssl=true&sslmode=require'
YC_MS_PASS='Saikeigoowik2Ahziejah5Eizu2Ooh'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBIskJBTih+KMzR4FfExdGej5fRBk9uBE6m+oTDEuHnwEBFuqIl6+1OaomTCJhKq8yqYSThIc86/CGIVTWvg7T8A= zinal@gw0" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services hdfs,yarn,tez,mapreduce,hive,spark,livy \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="data",role='datanode',resource-preset='s2.xlarge',disk-type='network-ssd-nonreplicated',disk-size=372,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='s2.xlarge',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=0,max-hosts-count=10,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=3600 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property dataproc:hive.thrift.impl=spark \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property hive:javax.jdo.option.ConnectionURL=${YC_MS_URL} \
  --property hive:javax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  --property hive:javax.jdo.option.ConnectionUserName=hive \
  --property hive:javax.jdo.option.ConnectionPassword=${YC_MS_PASS} \
  --property hive:hive.metastore.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property hive:hive.exec.compress.output=true \
  --property hive:hive.metastore.fshandler.threads=100 \
  --property hive:datanucleus.connectionPool.maxPoolSize=100 \
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
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --property spark:spark.driver.extraClassPath=s3a://dproc/jars/delta-core_2.12-0.8.0.jar \
  --property spark:spark.executor.extraClassPath=s3a://dproc/jars/delta-core_2.12-0.8.0.jar \
  --property spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --property spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
