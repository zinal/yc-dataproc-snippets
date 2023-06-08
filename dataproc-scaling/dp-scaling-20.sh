#! /bin/sh

# Комбинированный кластер для работы со Spark, с разметкой узлов для Spark AM.
# Основной путь хранения установлен в каталог в S3.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * кластер Data Proc с сервисом Hive Metastore
# Вариант для образа 2.0 (Hive в составе сервисов)

YC_VERSION=2.0
YC_ZONE=ru-central1-c
YC_SUBNET=default-ru-central1-c
YC_BUCKET=dproc-wh
YC_SA=dp1
YC_MS_URI='thrift://rc1c-dataproc-m-yempltyygr9d8pjh.mdb.yandexcloud.net:9083'
YC_CLUSTER=scaling20

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,spark,livy,hive,zeppelin \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='computenode',resource-preset='m3-c16-m128',preemptible=false,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="dynamic",role='computenode',resource-preset='m3-c16-m128',preemptible=true,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=8,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=300 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property yarn:yarn.node-labels.fs-store.root-dir=file:///hadoop/yarn/node-labels \
  --property yarn:yarn.node-labels.enabled=true \
  --property yarn:yarn.node-labels.configuration-type=centralized \
  --property capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=1.00 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels=SPARKAM \
  --property capacity-scheduler:yarn.scheduler.capacity.root.accessible-node-labels.SPARKAM.capacity=100 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels.SPARKAM.capacity=100 \
  --property capacity-scheduler:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET}/wh \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.jars.packages=io.delta:delta-core_2.12:0.8.0 \
  --property spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --property spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --property spark:spark.databricks.delta.snapshotCache.storageLevel=MEMORY_AND_DISK_SER_2 \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --property livy:livy.spark.deploy-mode=cluster \
  --property dataproc:hive.thrift.impl=spark \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/init_nodelabels.sh,args=static" \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/init_normal.sh" \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/init_geesefs.sh,args=${YC_BUCKET}" \
  --async
