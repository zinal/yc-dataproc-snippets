#! /bin/sh

# Пример команд для создания кластера Data Proc под выделенный Metastore.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * база данных PostgreSQL

YC_CLUSTER=ms1
YC_VERSION=2.0
YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=ms1
YC_SA=dp1
YC_DBURL='jdbc:postgresql://rc1b-pajy1x3lh8tk04u4.mdb.yandexcloud.net:6432/hive?targetServerType=master&ssl=true&sslmode=require'
YC_DBUSER='hive'
YC_DBPASS='sai0Doh8Eib0iav'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBAcV0HrAheoHrN5ow3i6+3QN6TCyB7FNDsQbhK+i8aPNgYsdT4iWDIAllVGxUHRprkBSKSt5tzweXF48pztGTJo= mzinal@ydb1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,hive,tez \
  --bucket ${YC_BUCKET} \
  --subcluster name="${YC_CLUSTER}_master",role='masternode',resource-preset='s3-c2-m8',disk-type='network-hdd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="${YC_CLUSTER}_compute",role='computenode',resource-preset='s3-c2-m8',disk-type='network-hdd',disk-size=100,hosts-count=0,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property hive:javax.jdo.option.ConnectionURL=${YC_DBURL} \
  --property hive:javax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  --property hive:javax.jdo.option.ConnectionPassword=${YC_DBPASS} \
  --property hive:javax.jdo.option.ConnectionUserName=${YC_DBUSER} \
  --property hive:hive.metastore.warehouse.dir=s3a://${YC_BUCKET}/warehouse
