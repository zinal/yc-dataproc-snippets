#! /bin/sh

set -e
set +u

SUBCLUSTER_NAME="$1"
BUCKET_PATH="$2"

# Download the JQ and YQ pre-built utilities.
sudo -u hdfs hdfs dfs -copyToLocal ${BUCKET_PATH}/jq.gz /tmp/jq.gz
sudo -u hdfs hdfs dfs -copyToLocal ${BUCKET_PATH}/yq.gz /tmp/yq.gz
gzip -d /tmp/jq.gz /tmp/yq.gz
mv /tmp/jq /usr/local/bin/
mv /tmp/yq /usr/local/bin/
chown root:bin /usr/local/bin/jq /usr/local/bin/yq
chmod 555 /usr/local/bin/jq /usr/local/bin/yq

# Check if the label must be set.
MUSTLABEL=N
if [ "${ROLE}" = "masternode" ]; then
  # On master init, we add the cluster node labels
  sudo -u yarn yarn rmadmin -addToClusterNodeLabels 'SPARKAM' || true
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
  while true; do
    # Check that the label is already defined
    foundya=`sudo -u yarn yarn cluster --list-node-labels 2>/dev/null | grep -E '^Node Labels' | grep '<SPARKAM:' | wc -l | (read x && echo $x)`
    if [ $foundya -gt 0 ]; then
      break
    fi
    # Add the label if missing
    sudo -u yarn yarn rmadmin -addToClusterNodeLabels 'SPARKAM' || true
    sleep 1
  done
  sudo -u yarn yarn rmadmin -replaceLabelsOnNode "${MYHOST}=SPARKAM"
fi

# End Of File
