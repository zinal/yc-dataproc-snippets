# DeltaLake DocAPI table maintenance script configuration

# Default folder as configured in the default profile.
#  `yc_profile` variable is not used in the `install.sh` and `cleanup.sh` scripts
yc_profile=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
yc_folder=`yc config profile get ${yc_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`
# Service account and cloud function names
sa_name=delta-sa1
cf_name=delta-ddb-cleanup
# YDB database, endpoint and path
docapi_endpoint=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etngt3b6eh9qfc80vt54
docapi_table=delta_log

# End Of File