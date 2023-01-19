#!/bin/bash

set -e

SRCDIR='s3a://dproc-code/depjars'
DSTDIR='/opt/depjars'
DSTBASE=`dirname ${DSTDIR}`

mkdir -p -v ${DSTDIR}
chown -R hdfs:hdfs ${DSTDIR}
sudo -u hdfs hdfs dfs -copyToLocal ${SRCDIR}/ ${DSTBASE}/
chown -R root:root ${DSTDIR}
