#! /bin/sh

(cd cf-scan && make)
(cd cf-extract && make)

. options.sh

# Service account with the required permissions
yc iam service-account create --name ${sa_name}
yc resource-manager folder add-access-binding --role viewer \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role ydb.editor \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role serverless.functions.invoker \
   --id ${yc_folder} --service-account-name ${sa_name}

sa_id=`yc iam service-account get ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`

#yc iam key create --service-account-name dp-compute-colorizer --output keys/dp-compute-colorizer.json

yc serverless function create --name=${cf_name}-scan

yc serverless function version create \
  --function-name=${cf_name}-scan \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 64m \
  --execution-timeout 20s \
  --service-account-id ${sa_id} \
  --source-path cf-scan/dp-compute-scan.zip

yc serverless trigger create timer \
  --name ${cf_name}-scan \
  --cron-expression '* * * * ? *' \
  --invoke-function-name ${cf_name}-scan \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

yc serverless function create --name=${cf_name}-xtr

yc serverless function version create \
  --function-name=${cf_name}-xtr \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 64m \
  --execution-timeout 20s \
  --service-account-id ${sa_id} \
  --source-path cf-extract/dp-compute-extract.zip

yc serverless trigger create timer \
  --name ${cf_name}-xtr \
  --cron-expression '* * * * ? *' \
  --invoke-function-name ${cf_name}-xtr \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

# End Of File
