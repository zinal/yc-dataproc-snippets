
yc dataproc cluster create dataproc-course-21 \
  --zone=ru-central1-a \
  --service-account-name=zinal-dp1 \
  --bucket=mzinal-dproc1 \
  --version=2.1 \
  --services=SPARK,YARN,LIVY \
  --security-group-ids=enpplq4lur4u4ldbcfgm \
  --ssh-public-keys-file=./ssh-keys.tmp \
  --subcluster name=master,role=masternode,resource-preset=s3-c2-m8,disk-type=network-ssd,disk-size=50,subnet-name=zinal-ru-central1-a,assign-public-ip=false \
  --subcluster name=compute,role=computenode,resource-preset=s3-c2-m8,disk-type=network-ssd,disk-size=50,subnet-name=zinal-ru-central1-a,hosts-count=1,assign-public-ip=false \
  --property livy:livy.spark.deploy-mode=client \
  --deletion-protection=false --ui-proxy=true \
  --async
