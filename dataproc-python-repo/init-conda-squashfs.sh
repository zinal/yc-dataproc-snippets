#! /bin/sh
# Copy and mount a pre-created squashfs /opt/conda image

IMAGE="$1"

if [ -z "$IMAGE" ]; then
    echo "NOTE: image was not specified, /opt/conda left as is" >&2
    exit 0
fi

set -e
set -u

echo "NOTE: Loading Conda image from $IMAGE ..." >&2

# Remove the current conda files in background, to decrease startup time
rm -rf /opt/conda/* &
# Download the base image into the temp directory
fname=`basename "$IMAGE"`
sudo -u hdfs hdfs dfs -copyToLocal "$IMAGE" /tmp/"$fname"
# Move the base image into the filesystem root, and adjust the file owner.
mv -v /tmp/"$fname" /"$fname"
chown root:root /"$fname"
chmod 444 /"$fname"
# Wait for the removal task to complete
wait
echo "NOTE: Mounting Conda image ..." >&2
# Setup the mount point
echo "/$fname    /opt/conda    squashfs    ro,defaults    0 0" >>/etc/fstab
# Mount the new readonly /opt/conda
mount /opt/conda
# Validate the mount
if [ ! -f /opt/conda/pkgs/urls.txt ]; then
    echo "FATAL: /opt/conda not mounted properly, aborting..."
    exit 1
fi

echo "NOTE: Conda image replaced!" >&2

# End Of File