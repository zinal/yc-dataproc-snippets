#! /bin/sh

# Default folder as configured in the default profile
yc_profile=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
yc_folder=`yc config profile get ${yc_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`

# Service account with the required permissions
yc iam service-account create --name dataproc-binder
yc resource-manager folder add-access-binding --role viewer \
   --id ${yc_folder} --service-account-name dataproc-binder
yc resource-manager folder add-access-binding --role compute.admin \
   --id ${yc_folder} --service-account-name dataproc-binder

#yc iam key create --service-account-name dataproc-binder --output dataproc-binder-key.json

# End Of File
