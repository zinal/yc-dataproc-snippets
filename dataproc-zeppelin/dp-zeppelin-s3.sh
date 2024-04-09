#! /bin/sh

# Пример хранения ноутбуков Zeppelin в S3.
# Основной путь хранения установлен в каталог в S3.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * кластер Data Proc с сервисом Hive Metastore

# https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html

YC_CLUSTER=sample-zeppel21
YC_VERSION=2.1
YC_ZONE=ru-central1-d
YC_SUBNET=zinal-ru-central1-d
YC_BUCKET=mzinal-dproc1
YC_WH=mzinal-wh1
YC_ZEPPEL=mzinal-zeppelin1
YC_SA=dp1
YC_SECRET_ID=e6qml07583b8ojf5f9u2
YC_MS_URI='thrift://ms1.zonne:9083'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBCrnvdmnBQL0klOXlVy3ElRXdkmQbTmNIWyGnVYBS2ygZYrYEMiXXIStCxlrxu+1WXDLTlqGa9AZGyyjTfpP3Jk= demo@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,spark,livy,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='s3-c16-m64',preemptible=false,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=3,max-hosts-count=3,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=300 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_WH}/warehouse \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
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
  --property zeppelin:zeppelin.notebook.s3.bucket=${YC_ZEPPEL} \
  --property zeppelin:zeppelin.notebook.s3.user=${YC_CLUSTER} \
  --property zeppelin:zeppelin.notebook.storage=org.apache.zeppelin.notebook.repo.S3NotebookRepo \
  --property livy:livy.spark.deploy-mode=cluster \
  --initialization-action 'uri=s3a://'${YC_BUCKET}'/scripts/init_zeppelin.sh,args='${YC_SECRET_ID} \
  --async
