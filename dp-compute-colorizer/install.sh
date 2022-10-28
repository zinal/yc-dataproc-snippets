#! /bin/sh

# Default folder as configured in the default profile
yc_profile=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
yc_folder=`yc config profile get ${yc_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`
sa_name=dp-compute-colorizer
cf_name=dp-compute-colorizer

# Service account with the required permissions
yc iam service-account create --name ${sa_name}
yc resource-manager folder add-access-binding --role viewer \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role compute.admin \
   --id ${yc_folder} --service-account-name ${sa_name}
yc resource-manager folder add-access-binding --role serverless.functions.invoker \
   --id ${yc_folder} --service-account-name ${sa_name}

sa_id=`yc iam service-account get ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`

#yc iam key create --service-account-name dataproc-colorizer --output dp-colorizer-key.json

yc serverless function create --name=${cf_name}

yc serverless function version create \
  --function-name=${cf_name} \
  --runtime python39 \
  --entrypoint cfunc.handler \
  --memory 128m \
  --execution-timeout 20s \
  --service-account-id ${sa_id} \
  --source-path cf/dp-compute-colorizer.zip


yc serverless trigger create timer \
  --name ${cf_name} \
  --cron-expression '* * * * ? *' \
  --invoke-function-name ${cf_name} \
  --invoke-function-service-account-id ${sa_id} \
  --retry-attempts 0

# End Of File
