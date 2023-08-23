#! /bin/sh
# DeltaLake DocAPI table maintenance script removal

set -e
set -u

. ./ddb-maint-config.sh

echo `date`" - Deleting trigger ${cf_name}..."
yc serverless trigger delete --name ${cf_name}

echo `date`" - Deleting function ${cf_name}..."
yc serverless function delete ${cf_name}

echo `date`" - Deleting lockbox secret ${sa_name}..."
yc lockbox secret delete --name ${sa_name}

echo `date`" - Deleting service account ${sa_name}..."
yc iam service-account delete --name ${sa_name}

echo `date`" - Cleanup completed!"

# End Of File