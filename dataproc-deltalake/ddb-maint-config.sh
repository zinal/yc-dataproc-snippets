# DeltaLake DocAPI table maintenance scripts installation configuration.

# Default folder as configured in the default profile.
#  `yc_profile` variable is not used in the `install.sh` and `cleanup.sh` scripts
yc_profile=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
yc_folder=`yc config profile get ${yc_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`
# Service account and cloud function names
sa_name=delta-sa1
cf_ddb_name=delta-ddb-cleanup
cf_s3_name=delta-s3-cleanup
# YDB DocAPI endpoint and table name
docapi_endpoint=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etngt3b6eh9qfc80vt54
docapi_table=delta_log
# S3 file with the list of prefixes to be cleaned up
s3_prefix_file=s3://dproc-wh/config/delta-prefixes.txt

# End Of File