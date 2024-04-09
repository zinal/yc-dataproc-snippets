#! /bin/sh

set -e
set -u

secretId=$1

TOKEN=`curl -f -s -H Metadata-Flavor:Google http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token | jq -r .access_token`

KEYFILE=`mktemp`
trap "rm -f ${KEYFILE}" EXIT

curl -f -s -H "Authorization: Bearer ${TOKEN}" \
  "https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/${secretId}/payload" >${KEYFILE}

KEY_ID=`cat ${KEYFILE} | jq -r '.entries[0].textValue'`
KEY_VALUE=`cat ${KEYFILE} | jq -r '.entries[1].textValue'`

tee -a /etc/zeppelin/conf.dist/zeppelin-env.sh <<EOF

# Key data for accessing S3 buckets
export AWS_ACCESS_KEY_ID=${KEY_ID}
export AWS_SECRET_ACCESS_KEY=${KEY_VALUE}
EOF

# End Of File