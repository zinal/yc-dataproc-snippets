#! /bin/sh

# Пример кластера с HDFS.

YC_VERSION=2.0
YC_ZONE=ru-central1-d
YC_SUBNET=zinal-ru-central1-d
YC_BUCKET=mzinal-dproc1
YC_BUCKET_WH=mzinal-wh1
YC_SA=dp1
YC_LOGGROUP_ID=e23itcj83v2r9o51llld

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBCrnvdmnBQL0klOXlVy3ElRXdkmQbTmNIWyGnVYBS2ygZYrYEMiXXIStCxlrxu+1WXDLTlqGa9AZGyyjTfpP3Jk= demo@gw1" >ssh-keys.tmp

YC_CLUSTER=hdfs-1
yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,hdfs,mapreduce,spark,livy \
  --bucket ${YC_BUCKET} \
  --log-group-id ${YC_LOGGROUP_ID} \
  --subcluster name="master",role='masternode',resource-preset='m3-c4-m32',disk-type='network-ssd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="static",role='datanode',resource-preset='m3-c16-m128',preemptible=false,disk-type='network-ssd',disk-size=300,hosts-count=3,max-hosts-count=3,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=3600 \
  --ssh-public-keys-file ssh-keys.tmp \
  --property livy:livy.spark.deploy-mode=client \
  --property spark:spark.sql.hive.metastore.sharedPrefixes=com.amazonaws,ru.yandex.cloud \
  --async
