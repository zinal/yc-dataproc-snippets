#! /bin/sh

set -e
set -u

sa_reader_name=dp1
sa_name=mzinal-dp-zeppelin1
bucket_name=mzinal-zeppelin1

yc iam service-account create --name ${sa_name}
sa_id=`yc iam service-account get ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`

yc storage bucket update ${bucket_name} \
   --grants grant-type=GRANT_TYPE_ACCOUNT,permission=PERMISSION_READ,grantee-id=${sa_id} \
   --grants grant-type=GRANT_TYPE_ACCOUNT,permission=PERMISSION_WRITE,grantee-id=${sa_id}

kf=`mktemp`
trap "rm -f ${kf}" EXIT
yc iam access-key create --service-account-name ${sa_name} >${kf}
yc_key_id=`cat ${kf} | grep 'key_id:' |  (read x y && echo $y)`
yc_key_key=`cat ${kf} | grep 'secret:' |  (read x y && echo $y)`

yc lockbox secret create --name ${sa_name} --payload \
  "[{'key': 'ACCESS_KEY_ID', 'text_value': '$yc_key_id'}, {'key': 'SECRET_ACCESS_KEY', 'text_value': '$yc_key_key'}]"
yc_secret_id=`yc lockbox secret get --name ${sa_name} | grep -E '^id: ' | (read x y && echo $y)`

yc lockbox secret add-access-binding --role lockbox.payloadViewer --name ${sa_name} --service-account-name ${sa_reader_name}

# End Of File