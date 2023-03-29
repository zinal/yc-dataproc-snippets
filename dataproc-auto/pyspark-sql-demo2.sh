#! /bin/sh

yc dataproc job create-pyspark --cluster-name delta-1 \
  --name pyspark-sql-demo2 \
  --jar-file-uris=s3a://dproc-wh/tmp/mysql-connector-j-8.0.32.jar,s3a://dproc-wh/jars/yc-delta-multi-dp21-1.0-fatjar.jar \
  --main-python-file-uri=s3a://dproc-wh/tmp/pyspark-sql-demo2.py
