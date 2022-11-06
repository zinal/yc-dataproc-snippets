#! /bin/sh

# Default folder as configured in the default profile.
#  `yc_profile` variable is not used in the `install.sh` and `cleanup.sh` scripts
yc_profile=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
yc_folder=`yc config profile get ${yc_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`
# Service account and cloud function names
sa_name=dp-compute-colorizer
cf_name=dp-compute-colorizer
# Destination S3 bucket name and path prefix (cannot be empty)
s3_bucket=billing1
s3_prefix=dp-compute-colorizer
# YDB database, endpoint and path
ydb_endpoint=grpcs://ydb.serverless.yandexcloud.net:2135
ydb_database=/ru-central1/b1g1hfek2luako6vouqb/etno6m1l1lf4ae3j01ej
ydb_path=billing1

# End Of File
