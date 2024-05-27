#! /bin/sh

# Пример кластера с HDFS.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * кластер Data Proc с сервисом Hive Metastore

YC_VERSION=2.1
YC_ZONE=ru-central1-d
YC_SUBNET=default-subnet-d
YC_BUCKET=mzinal-dproc1
YC_SA=mzinal-dp1
YC_MS_URI='thrift://10.129.0.18:9083'
YC_DDB_LOCKBOX=e6q7eqfqs962m6g9rs62
YC_DDB_ENDPOINT=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etnac7v7lqqiflor0sem

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBISp0QKehCPw5UfAC7d2HHZcijtChgBPNFdFLxYjvBufsTg/c1nbGNlBbZEx1HOee35TefBx7Mz49z/Iup+RGvs= demo@gw2" >ssh-keys.tmp

YC_CLUSTER=hdfs-1
yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,spark,livy,zeppelin,hdfs \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='m3-c8-m64',disk-type='network-ssd',disk-size=300,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="data",role='datanode',resource-preset='m3-c16-m128',preemptible=true,disk-type='network-ssd',disk-size=500,hosts-count=3,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='m3-c16-m128',preemptible=true,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=3600 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property core:yarn.system-metrics-publisher.enabled=true \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.jars=/s3data/jars/yc-delta-multi-dp21-1.0-fatjar.jar \
  --property spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --property spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --property spark:spark.delta.logStore.s3a.impl=ru.yandex.cloud.custom.delta.YcS3YdbLogStore \
  --property spark:spark.io.delta.storage.S3DynamoDBLogStore.ddb.endpoint=${YC_DDB_ENDPOINT} \
  --property spark:spark.io.delta.storage.S3DynamoDBLogStore.ddb.lockbox=${YC_DDB_LOCKBOX} \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --initialization-action 'uri=s3a://'${YC_BUCKET}'/init-scripts/init_geesefs.sh,args=['${YC_BUCKET}',/s3data]' \
  --async
