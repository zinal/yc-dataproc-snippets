#! /bin/sh

cd /Mirror
sudo -u hdfs hdfs dfs -mkdir s3a://dproc-repo/repos/
sudo -u hdfs hdfs dfs -copyFromLocal -d -t 20 conda1/ s3a://dproc-repo/repos/

# End Of File