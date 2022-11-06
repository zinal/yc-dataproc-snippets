#! /bin/sh

set -e
set -u

echo `date`" - Generating ZIP archives for cloud functions..."
(cd cf-scan && make)
(cd cf-extract && make)

. ./options.sh

# Service account with the required permissions.
#    We expect that YDB database is created in the same YC folder,
#    or the permissions need to be configured manually.

echo `date`" - Creating service account ${sa_name}..."
yc iam service-account create --name ${sa_name}

echo `date`" - Setting permissions for service account ${sa_name}..."
yc resource-manager folder add-access-binding --role viewer \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role ydb.editor \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role serverless.functions.invoker \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role storage.uploader \
   --id ${yc_folder} --service-account-name ${sa_name}

sa_id=`yc iam service-account get ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`

echo `date`" - Generating static key for service account ${sa_name}..."
kf=/tmp/${sa_name}_key.txt
yc iam access-key create --service-account-name ${sa_name} >${kf}
yc_key_id=`cat ${kf} | grep 'key_id:' |  (read x y && echo $y)`
yc_key_key=`cat ${kf} | grep 'secret:' |  (read x y && echo $y)`

echo `date`" - Putting the static key into lockbox for service account ${sa_name}..."
yc lockbox secret create --name ${sa_name} --payload \
  "[{'key': 'ACCESS_KEY_ID', 'text_value': '$yc_key_id'}, {'key': 'SECRET_ACCESS_KEY', 'text_value': '$yc_key_key'}]"
yc_secret_id=`yc lockbox secret get --name ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`
yc lockbox secret add-access-binding --role lockbox.payloadViewer --name ${sa_name} --service-account-name ${sa_name}


#yc iam key create --service-account-name dp-compute-colorizer --output keys/dp-compute-colorizer.json

echo `date`" - Creating the function ${cf_name}-scan..."
yc serverless function create --name=${cf_name}-scan

echo `date`" - Creating the function version ${cf_name}-scan..."
yc serverless function version create \
  --function-name=${cf_name}-scan \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 128m \
  --execution-timeout 20s \
  --service-account-id ${sa_id} \
  --environment YDB_ENDPOINT=${ydb_endpoint},YDB_DATABASE=${ydb_database},YDB_PATH=${ydb_path} \
  --source-path cf-scan/dp-compute-scan.zip

# Run each minute
echo `date`" - Scheduling the function ${cf_name}-scan..."
yc serverless trigger create timer \
  --name ${cf_name}-scan \
  --cron-expression '* * * * ? *' \
  --invoke-function-name ${cf_name}-scan \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

echo `date`" - Creating the function ${cf_name}-xtr..."
yc serverless function create --name=${cf_name}-xtr

echo `date`" - Creating the function version ${cf_name}-xtr..."
yc serverless function version create \
  --function-name=${cf_name}-xtr \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 128m \
  --execution-timeout 20s \
  --service-account-id ${sa_id} \
  --environment YDB_ENDPOINT=${ydb_endpoint},YDB_DATABASE=${ydb_database},YDB_PATH=${ydb_path},S3_BUCKET=${s3_bucket},S3_PREFIX=${s3_prefix},SECRET_ID=${yc_secret_id} \
  --source-path cf-extract/dp-compute-extract.zip

# Run each hour
echo `date`" - Scheduling the function ${cf_name}-xtr..."
yc serverless trigger create timer \
  --name ${cf_name}-xtr \
  --cron-expression '0 * ? * * *' \
  --invoke-function-name ${cf_name}-xtr \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

echo `date`" - Installation completed!"

# End Of File
