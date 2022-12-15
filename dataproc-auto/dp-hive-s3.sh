#! /bin/sh

# На сегодня кластер с Hive без HDFS и MapReduce может работать только как Hive Metastore!

# Пример команд для создания кластера Data Proc под выделенный Metastore.
# Предполагается, что уже созданы
#  * бакет Object Storage,
#  * сервисный аккаунт с доступом к бакету,
#  * база данных PostgreSQL

YC_CLUSTER=hive-s3-1
YC_VERSION=2.0.58
YC_ZONE=ru-central1-b
YC_SUBNET=default-ru-central1-b
YC_BUCKET=dproc1
YC_SA=dp1

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEe2Kv7hXKJHip4bvawkQ0n6/L6JcTrzUeZ4QqGGxm0bLe6hecoWZ68J9r5umgvFqNHGCEO868TAlxQKOf+aNus= zinal@gw1" >ssh-keys.tmp

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,hive,tez \
  --bucket ${YC_BUCKET} \
  --subcluster name="${YC_CLUSTER}_master",role='masternode',resource-preset='s3-c2-m8',disk-type='network-hdd',disk-size=100,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="${YC_CLUSTER}_compute",role='computenode',resource-preset='s3-c4-m16',disk-type='network-hdd',disk-size=100,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property hive:hive.metastore.warehouse.dir=s3a://${YC_BUCKET}/warehouse \
  --property hive:hive.exec.compress.output=true
