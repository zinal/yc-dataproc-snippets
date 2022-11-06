#! /bin/sh

. ./options.sh

echo `date`" - Deleting trigger ${cf_name}-scan..."
yc serverless trigger delete --name ${cf_name}-scan

echo `date`" - Deleting function ${cf_name}-scan..."
yc serverless function delete ${cf_name}-scan

echo `date`" - Deleting trigger ${cf_name}-xtr..."
yc serverless trigger delete --name ${cf_name}-xtr

echo `date`" - Deleting function ${cf_name}-xtr..."
yc serverless function delete ${cf_name}-xtr

echo `date`" - Deleting lockbox secret ${sa_name}..."
yc lockbox secret delete --name ${sa_name}

echo `date`" - Deleting service account ${sa_name}..."
yc iam service-account delete --name ${sa_name}

echo `date`" - Cleanup completed!"

# End Of File
