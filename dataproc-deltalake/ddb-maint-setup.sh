#! /bin/sh
# DeltaLake DocAPI table maintenance script installation

set -e
set -u

echo `date`" - Generating ZIP archives for cloud functions..."
(cd cf-cleanup && ./pack.sh)

. ./ddb-maint-config.sh

# Service account with the required permissions.
#    We expect that YDB database is created in the same YC folder,
#    or the permissions need to be configured manually.

echo `date`" - Creating service account ${sa_name}..."
yc iam service-account create --name ${sa_name}

echo `date`" - Setting permissions for service account ${sa_name}..."
yc resource-manager folder add-access-binding --role ydb.editor \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role storage.editor \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role serverless.functions.invoker \
   --id ${yc_folder} --service-account-name ${sa_name}

sa_id=`yc iam service-account get ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`

echo `date`" - Generating static key for service account ${sa_name}..."
kf=/tmp/${sa_name}_key.txt
yc iam access-key create --service-account-name ${sa_name} >${kf}
yc_key_id=`cat ${kf} | grep 'key_id:' |  (read x y && echo $y)`
yc_key_key=`cat ${kf} | grep 'secret:' |  (read x y && echo $y)`

echo `date`" - Putting the static key into lockbox for service account ${sa_name}..."
yc lockbox secret create --name ${sa_name} --payload \
  "[{'key': 'key-id', 'text_value': '$yc_key_id'}, {'key': 'key-secret', 'text_value': '$yc_key_key'}]"
yc_secret_id=`yc lockbox secret get --name ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`
yc lockbox secret add-access-binding --role lockbox.payloadViewer --name ${sa_name} --service-account-name ${sa_name}


echo `date`" - Creating the function ${cf_ddb_name}..."
yc serverless function create --name=${cf_ddb_name}

echo `date`" - Creating the function ${cf_s3_name}..."
yc serverless function create --name=${cf_s3_name}

echo `date`" - Creating the function version ${cf_ddb_name}..."
yc serverless function version create \
  --function-name=${cf_ddb_name} \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 128m \
  --execution-timeout 300s \
  --service-account-id ${sa_id} \
  --environment MODE=ddb,LBX_SECRET_ID=${yc_secret_id},DOCAPI_ENDPOINT=${docapi_endpoint},TABLE_NAME=${docapi_table} \
  --source-path cf-cleanup/cf-delta-cleanup.zip

echo `date`" - Creating the function version ${cf_s3_name}..."
yc serverless function version create \
  --function-name=${cf_s3_name} \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 128m \
  --execution-timeout 600s \
  --service-account-id ${sa_id} \
  --environment MODE=s3,LBX_SECRET_ID=${yc_secret_id},PREFIX_FILE=${s3_prefix_file} \
  --source-path cf-cleanup/cf-delta-cleanup.zip

# Run once per hour
echo `date`" - Scheduling the function ${cf_ddb_name}..."
yc serverless trigger create timer \
  --name ${cf_ddb_name} \
  --cron-expression '0 * * * ? *' \
  --invoke-function-name ${cf_ddb_name} \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

# Run once per day at 3:00 AM
echo `date`" - Scheduling the function ${cf_s3_name}..."
yc serverless trigger create timer \
  --name ${cf_s3_name} \
  --cron-expression '0 3 * * ? *' \
  --invoke-function-name ${cf_s3_name} \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

echo `date`" - Installation completed!"

# End Of File