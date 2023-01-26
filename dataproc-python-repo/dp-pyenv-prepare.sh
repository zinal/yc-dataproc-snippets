#! /bin/sh

# Кластер для подготовки окружения Python.

. ./dp-pyenv-options.sh

YC_CLUSTER=pyenv-prepare1

yc dataproc cluster create ${YC_CLUSTER} \
  --zone ${YC_ZONE} \
  --service-account-name ${YC_SA} \
  --version ${YC_VERSION} --ui-proxy \
  --services yarn,tez,spark,livy \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.xlarge',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='s2.large',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=0,max-hosts-count=1,subnet-name=${YC_SUBNET},autoscaling-decommission-timeout=3600 \
  --ssh-public-keys-file ssh-keys.tmp

# End Of File