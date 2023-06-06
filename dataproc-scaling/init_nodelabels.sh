#! /bin/sh

set -e
set +u

SUBCLUSTER_NAME="$1"

sudo apt-get install -y jq
sudo wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64
sudo chmod aoug+x /usr/local/bin/yq

MUSTLABEL=N
if [ "${ROLE}" = "masternode" ]; then
  # On master init, we add the cluster node labels
  sudo -u yarn yarn rmadmin -addToClusterNodeLabels 'sparkam'
elif [ "${ROLE}" = "datanode" ]; then
  # Datanodes are good for running Spark AM containers
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
  MYHOST=`hostname -f`
  sudo -u yarn yarn rmadmin -replaceLabelsOnNode "${MYHOST}=sparkam" -failOnUnknownNodes
fi

# End Of File
