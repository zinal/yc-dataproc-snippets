#! /bin/sh

set -e
set -u

secretId=$1

# Download the JQ and YQ pre-built utilities.
if [ ! -f /usr/local/bin/jq ]; then
  wget -q -O - https://mycop1.website.yandexcloud.net/utils/jq.gz | gzip -dc >/tmp/jq
  mv /tmp/jq /usr/local/bin/
  chown root:bin /usr/local/bin/jq
  chmod 555 /usr/local/bin/jq
fi
if [ ! -f /usr/local/bin/yq ]; then
  wget -q -O - https://mycop1.website.yandexcloud.net/utils/yq.gz | gzip -dc >/tmp/yq
  mv /tmp/yq /usr/local/bin/
  chown root:bin /usr/local/bin/yq
  chmod 555 /usr/local/bin/yq
fi

# Access token for Lockbox access
TOKEN=`curl -f -s -H Metadata-Flavor:Google http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token | jq -r .access_token`

# Grab key from Lockbox to file
KEYFILE=`mktemp`
trap "rm -f ${KEYFILE}" EXIT
curl -f -s -H "Authorization: Bearer ${TOKEN}" \
  "https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/${secretId}/payload" >${KEYFILE}

KEY_ID=`cat ${KEYFILE} | jq -r '.entries[0].textValue'`
KEY_VALUE=`cat ${KEYFILE} | jq -r '.entries[1].textValue'`

# Configure Zeppelin with the key data
tee -a /etc/zeppelin/conf.dist/zeppelin-env.sh <<EOF

# Key data for accessing S3 buckets
export AWS_ACCESS_KEY_ID=${KEY_ID}
export AWS_SECRET_ACCESS_KEY=${KEY_VALUE}
EOF

# Restart Zeppelin if already started
ZC=`ps -ef | grep zeppelin | grep java | wc -l`
if [ $ZC -gt 0 ]; then
  sleep 2 # To allow Zeppelin startup to complete
  systemctl restart zeppelin.service
fi

# End Of File