#! /bin/sh

yc dataproc job create-pyspark --cluster-name sqlan1 --name datagen1 \
  --main-python-file-uri s3a://mzinal-dproc1/jobs/datagen.py
