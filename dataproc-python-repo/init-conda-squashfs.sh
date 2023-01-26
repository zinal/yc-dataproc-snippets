#! /bin/sh
# Copy and mount a pre-created squashfs /opt/conda image

IMAGE="$1"

if [ -z "$IMAGE" ]; then
    echo "NOTE: image was not specified, /opt/conda left as is"
    exit 0
fi

set -e
set -u

# Remove the current conda files in background, to decrease startup time
rm -rf /opt/conda/* &
# Download the base image into the temp directory
fname=`basename "$IMAGE"`
sudo -u hdfs hdfs dfs -copyToLocal "$IMAGE" /tmp/"$fname"
# Move the base image into the filesystem root, and adjust the file owner.
mv -v /tmp/"$fname" /"$fname"
chown root:root /"$fname"
# Wait for the removal task to complete
wait
# Setup the mount point
echo "/$fname    /opt/conda    squashfs    ro,defaults    0 0" >>/etc/fstab
# Mount the new readonly /opt/conda
mount -a
# Validate the mount
if [ ! -f /opt/conda/pkgs/urls.txt ]; then
    echo "FATAL: /opt/conda not mounted properly, aborting..."
    exit 1
fi

# Download and replace the zeppelin_python.py file.
IMAGE2=`dirname "$IMAGE"`/zeppelin_python.py
DEST_PY=/usr/lib/zeppelin/interpreter/python/python/zeppelin_python.py
sudo -u hdfs hdfs dfs -copyToLocal "$IMAGE2" /tmp/zeppelin_python.py
mv ${DEST_PY} ${DEST_PY}-orig
mv /tmp/zeppelin_python.py ${DEST_PY}
chown root:root ${DEST_PY}
chmod 644 ${DEST_PY}
# Replace the copy in the jar library
cd /usr/lib/zeppelin/interpreter/spark
cp ${DEST_PY} python/
jar -uf spark-interpreter-0.9.0.jar python/zeppelin_python.py &
cd /usr/lib/zeppelin/interpreter/python
jar -uf python-interpreter-with-py4j-0.9.0.jar python/zeppelin_python.py
# Wait for completion
wait

# End Of File
