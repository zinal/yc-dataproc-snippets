#! /bin/sh

# Пример автомасштабируемого кластера для доступа к данным в формате DeltaLake.
# Основной путь хранения установлен в каталог в S3.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * экземпляр Managed Metastore и DNS-имя для доступа к нему (ms1.zonne ниже).
#
# Зависимости:
#  * файлы jq.gz, yq.gz в бакете mycop1 (https://mycop1.website.yandexcloud.net/utils/...)
#  * скрипт инициализации init_nodelabels.sh в каталоге scripts основного бакета
#  * созданная лог-группа

YC_VERSION=2.1
YC_ZONE=ru-central1-d
YC_SUBNET=zinal-ru-central1-d
YC_BUCKET=mzinal-dproc1
YC_BUCKET_WH=mzinal-wh1
YC_SA=zinal-dp1
YC_MS_URI='thrift://ms1.zonne:9083'
YC_LOGGROUP_ID=e23itcj83v2r9o51llld

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBCrnvdmnBQL0klOXlVy3ElRXdkmQbTmNIWyGnVYBS2ygZYrYEMiXXIStCxlrxu+1WXDLTlqGa9AZGyyjTfpP3Jk= demo@gw1" >ssh-keys.tmp

# для присвоения группы безопасности добавьте опцию:
#   --security-group-ids <sg-id>

YC_CLUSTER=delta-2
yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,spark,livy,hdfs \
  --bucket ${YC_BUCKET} \
  --log-group-id ${YC_LOGGROUP_ID} \
  --subcluster name="master",role='masternode',resource-preset='m3-c4-m32',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='datanode',resource-preset='m3-c16-m128',preemptible=false,disk-type='network-ssd',disk-size=500,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="dynamic",role='computenode',resource-preset='m3-c16-m128',preemptible=true,disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=9,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=3600 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property yarn:yarn.node-labels.fs-store.root-dir=file:///hadoop/yarn/node-labels \
  --property yarn:yarn.node-labels.enabled=true \
  --property yarn:yarn.node-labels.configuration-type=centralized \
  --property capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=1.00 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels=SPARKAM \
  --property capacity-scheduler:yarn.scheduler.capacity.root.accessible-node-labels.SPARKAM.capacity=100 \
  --property capacity-scheduler:yarn.scheduler.capacity.root.default.accessible-node-labels.SPARKAM.capacity=100 \
  --property capacity-scheduler:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator \
  --property spark:spark.yarn.am.nodeLabelExpression=SPARKAM \
  --property livy:livy.spark.deploy-mode=client \
  --property core:fs.s3a.committer.threads=100 \
  --property core:fs.s3a.connection.maximum=1000 \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property core:yarn.system-metrics-publisher.enabled=true \
  --property spark:spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --property spark:spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --property spark:spark.hadoop.hive.metastore.uris=${YC_MS_URI} \
  --property spark:spark.sql.warehouse.dir=s3a://${YC_BUCKET_WH}/warehouse \
  --property spark:spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --property spark:spark.kryoserializer.buffer=32m \
  --property spark:spark.kryoserializer.buffer.max=256m \
  --property spark:spark.jars=s3a://${YC_BUCKET}/jars/yc-delta23-multi-dp21-1.1-fatjar.jar,s3a://${YC_BUCKET}/jars/ydb-spark-connector-1.1.jar \
  --property spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --property spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --property spark:spark.sql.addPartitionInBatch.size=1000 \
  --initialization-action 'uri=s3a://'${YC_BUCKET}'/scripts/init_nodelabels.sh,args=static' \
  --async
