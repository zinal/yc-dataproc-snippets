#! /bin/sh

set -e
set +u

SUBCLUSTER_NAME="$1"

# Download the JQ and YQ pre-built utilities.
wget -qO -O - https://mycop1.website.yandexcloud.net/utils/jq.gz | gzip -dc >/tmp/jq
wget -qO -O - https://mycop1.website.yandexcloud.net/utils/yq.gz | gzip -dc >/tmp/yq
sudo mv /tmp/jq /usr/local/bin/
sudo mv /tmp/yq /usr/local/bin/
sudo chown root:bin /usr/local/bin/jq /usr/local/bin/yq
sudo chmod 555 /usr/local/bin/jq /usr/local/bin/yq

# Check if the label must be set.
MUSTLABEL=N
if [ "${ROLE}" = "masternode" ]; then
  # On master init, we add the cluster node labels
  sudo -u yarn yarn rmadmin -addToClusterNodeLabels 'SPARKAM'
elif [ "${ROLE}" = "datanode" ]; then
  # Datanodes are good for running Spark AM containers.
  # Note that setting the label means that "regular" containers will not go to datanodes.
  MUSTLABEL=Y
elif [ ! -z "$SUBCLUSTER_NAME" ]; then
  # Let's retrieve and check the subcluster name
  curl -H Metadata-Flavor:Google 'http://169.254.169.254/computeMetadata/v1/instance/?alt=json&recursive=true' -o /tmp/host-meta.json
  jq -r .attributes.'"user-data"' /tmp/host-meta.json >/tmp/host-userdata.yaml
  subcid=`jq -r '.vendor.labels.subcluster_id' /tmp/host-meta.json`
  subname=`yq -r ".data.topology.subclusters.${subcid}.name" /tmp/host-userdata.yaml`
  if [ "${subname}" = "${SUBCLUSTER_NAME}" ]; then
    MUSTLABEL=Y
  fi
fi

if [ "${MUSTLABEL}" = "Y" ]; then
  # Set the SPARKAM label on the current host
  MYHOST=`hostname -f`
  sudo -u yarn yarn rmadmin -replaceLabelsOnNode "${MYHOST}=SPARKAM" -failOnUnknownNodes
fi

# End Of File
