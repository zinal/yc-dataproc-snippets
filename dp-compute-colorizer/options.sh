#! /bin/sh

# Default folder as configured in the default profile
yc_profile=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
yc_folder=`yc config profile get ${yc_profile} | grep -E '^folder-id: ' | (read x y && echo $y)`
# Service account and cloud function names
sa_name=dp-compute-colorizer
cf_name=dp-compute-colorizer

# End Of File
