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

# End Of File