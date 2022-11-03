#! /bin/sh

. options.sh

yc serverless trigger delete --name ${cf_name}-scan
yc serverless function delete ${cf_name}-scan

yc serverless trigger delete --name ${cf_name}-xtr
yc serverless function delete ${cf_name}-xtr

yc iam service-account delete --name ${sa_name}

# End Of File
