#! /bin/sh

yc dataproc job create-pyspark --cluster-name delta-1 \
  --name pyspark-sql-demo1 --main-python-file-uri=s3a://dproc-wh/tmp/pyspark-sql-demo1.py
