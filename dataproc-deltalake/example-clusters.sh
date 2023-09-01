#! /bin/sh

# Пример трёх кластеров для конкурентного доступа к данным в формате DeltaLake.
# Основной путь хранения установлен в каталог в S3.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * кластер Data Proc с сервисом Hive Metastore

YC_ZONE=ru-central1-c
YC_SUBNET=default-ru-central1-c
YC_BUCKET=dproc-wh
YC_SA=dp1
YC_MS_URI='thrift://rc1c-dataproc-m-yempltyygr9d8pjh.mdb.yandexcloud.net:9083'
YC_DDB_LOCKBOX=e6qr20sbgn3ckpalh54p
YC_DDB_ENDPOINT=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etngt3b6eh9qfc80vt54/

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp

if false; then
for YC_CLUSTER in dl21_1; do
yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version 2.1 --ui-proxy \
  --services yarn,spark,livy,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='computenode',resource-preset='m3-c16-m128',preemptible=false,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="dynamic",role='computenode',resource-preset='m3-c16-m128',preemptible=true,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=5,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=300 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property yarn:yarn.node-labels.fs-store.root-dir=file:///hadoop/yarn/node-labels \
  --property yarn:yarn.node-labels.enabled=true \
  --property yarn:yarn.node-labels.configuration-type=centralized \
  --property capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=1.00 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels=SPARKAM \
  --property capacity-scheduler:yarn.scheduler.capacity.root.accessible-node-labels.SPARKAM.capacity=100 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels.SPARKAM.capacity=100 \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.jars=s3a://${YC_BUCKET}/jars/yc-delta23-multi-dp21-1.1-fatjar.jar \
  --property spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --property spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.YcDeltaCatalog \
  --property spark:spark.delta.logStore.s3a.impl=ru.yandex.cloud.custom.delta.YcS3YdbLogStore \
  --property spark:spark.io.delta.storage.S3DynamoDBLogStore.ddb.endpoint=${YC_DDB_ENDPOINT} \
  --property spark:spark.io.delta.storage.S3DynamoDBLogStore.ddb.lockbox=${YC_DDB_LOCKBOX} \
  --property spark:spark.databricks.delta.snapshotCache.storageLevel=MEMORY_ONLY_SER_2 \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --property livy:livy.spark.deploy-mode=cluster \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/init_nodelabels.sh,args=static" \
  --async
done
fi

if true; then
for YC_CLUSTER in dl20_1; do
yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version 2.0 --ui-proxy \
  --services yarn,spark,livy,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='computenode',resource-preset='m3-c16-m128',preemptible=false,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="dynamic",role='computenode',resource-preset='m3-c16-m128',preemptible=true,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=5,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=300 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property yarn:yarn.node-labels.fs-store.root-dir=file:///hadoop/yarn/node-labels \
  --property yarn:yarn.node-labels.enabled=true \
  --property yarn:yarn.node-labels.configuration-type=centralized \
  --property capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=1.00 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels=SPARKAM \
  --property capacity-scheduler:yarn.scheduler.capacity.root.accessible-node-labels.SPARKAM.capacity=100 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels.SPARKAM.capacity=100 \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --property livy:livy.spark.deploy-mode=cluster \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/init_nodelabels.sh,args=static" \
  --async
done
fi
