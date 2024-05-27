#! /bin/sh

# Пример кластера для SQL-аналитики.
# Основной путь хранения установлен в каталог в S3.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * экземпляр Hive Metastore,
#  * группа безопасности с необходимыми настройками.
# В кластере используется автоскейлинг, но настроен он только для варианта
# исполнения Spark Thrift Server - другие сервисы могут завершаться аварийно
# при срабатывании автоскейлера.

YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=mzinal-dproc1
YC_WH=s3a://mzinal-wh1/warehouse
YC_SA=mzinal-dp1
YC_MS_URI='thrift://metastore-1.zonne:9083'
YC_DDB_LOCKBOX=e6q7eqfqs962m6g9rs62
YC_DDB_ENDPOINT='https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etnac7v7lqqiflor0sem'
YC_SG=enpechrqs0fia7vfm2u6
YC_CLUSTER=sqlan1

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBFDUWfFLBZ/OdeL7geZhiaIeMVY6AsJgFe/xQI2c1NMH2HJPh0GMhxkLGJKv4U5YxCrwrFRZb+dRnlzt7L4T9FM= demo@run0" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version 2.1 --ui-proxy \
  --services yarn,spark \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='computenode',resource-preset='s3-c16-m64',preemptible=false,disk-type='network-ssd',disk-size=200,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="dynamic",role='computenode',resource-preset='s3-c16-m64',preemptible=true,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=0,max-hosts-count=5,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=600,stabilization-duration=600s \
  --ssh-public-keys-file ssh-keys.tmp \
  --security-group-ids ${YC_SG} \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property core:yarn.system-metrics-publisher.enabled=true \
  --property spark:spark.executor.memory=8g \
  --property spark:spark.sql.catalogImplementation=hive \
  --property spark:spark.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=${YC_WH} \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.driver.extraClassPath=/s3data/jars/yc-delta23-multi-dp21-1.1-fatjar.jar \
  --property spark:spark.executor.extraClassPath=/s3data/jars/yc-delta23-multi-dp21-1.1-fatjar.jar \
  --property spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --property spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.YcDeltaCatalog \
  --property spark:spark.delta.logStore.s3a.impl=ru.yandex.cloud.custom.delta.YcS3YdbLogStore \
  --property spark:spark.io.delta.storage.S3DynamoDBLogStore.ddb.endpoint=${YC_DDB_ENDPOINT} \
  --property spark:spark.io.delta.storage.S3DynamoDBLogStore.ddb.lockbox=${YC_DDB_LOCKBOX} \
  --property spark:spark.databricks.delta.snapshotCache.storageLevel=MEMORY_ONLY_SER_2 \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --property dataproc:hive.thrift.impl=spark \
  --initialization-action "uri=s3a://${YC_BUCKET}/init-scripts/init_geesefs.sh,args=[${YC_BUCKET},/s3data]" \
  --async

# End Of File