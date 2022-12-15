#! /bin/sh

# Пример команд для создания кластера Data Proc под выделенный Metastore.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * база данных PostgreSQL

YC_CLUSTER=metastore-1
YC_VERSION=2.0.58
YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=dproc1
YC_SA=dp1
YC_MS_URL='jdbc:postgresql://rc1a-i9gzawc9db9a516g.mdb.yandexcloud.net:6432,rc1b-aohkuuhqimb7cjty.mdb.yandexcloud.net:6432,rc1c-d9lg5uu7nc2duwx1.mdb.yandexcloud.net:6432/hive?targetServerType=master&ssl=true&sslmode=require'
YC_MS_PASS='chahle1Eiqu5BukieZoh'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,tez,hive \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-hdd',disk-size=150,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='s2.medium',disk-type='network-hdd',disk-size=150,hosts-count=0,max-hosts-count=8,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property hive:javax.jdo.option.ConnectionURL=${YC_MS_URL} \
  --property hive:javax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  --property hive:javax.jdo.option.ConnectionUserName=hive \
  --property hive:javax.jdo.option.ConnectionPassword=${YC_MS_PASS} \
  --property hive:hive.metastore.warehouse.dir=s3a://${YC_BUCKET}/warehouse \
  --property hive:hive.exec.compress.output=true
