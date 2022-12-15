#!/bin/bash

set -e

BUCKET=dproc1
MOUNT_POINT=/s3data

# Загрузка GeeseFS
wget -nv https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64 -O /opt/geesefs
chmod a+rwx /opt/geesefs
mkdir -p "${MOUNT_POINT}"

# Подготовка скрипта, выполняющегося при каждой загрузке
BOOT_SCRIPT="/var/lib/cloud/scripts/per-boot/80-geesefs-mount.sh"
echo "#!/bin/bash" >> ${BOOT_SCRIPT}
echo "/opt/geesefs -o allow_other --iam ${BUCKET} ${MOUNT_POINT}" >> ${BOOT_SCRIPT}
chmod 755 ${BOOT_SCRIPT}

# Запуск скрипта
${BOOT_SCRIPT}