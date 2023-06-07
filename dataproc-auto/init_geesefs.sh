#! /bin/bash

BUCKET=$1
MOUNT_POINT=/s3data

# Загрузка GeeseFS
#wget -nv https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64 -O /usr/local/sbin/geesefs
wget -nv https://mycop1.website.yandexcloud.net/utils/geesefs-linux-amd64 -O /usr/local/sbin/geesefs
chmod a+rwx /usr/local/sbin/geesefs
mkdir -p "${MOUNT_POINT}"

# Подготовка скрипта, выполняющегося при каждой загрузке
BOOT_SCRIPT="/var/lib/cloud/scripts/per-boot/80-geesefs-mount.sh"
echo "#!/bin/bash" >> ${BOOT_SCRIPT}
echo "/usr/local/sbin/geesefs -o allow_other --iam ${BUCKET} ${MOUNT_POINT}" >> ${BOOT_SCRIPT}
chmod 755 ${BOOT_SCRIPT}

# Запуск скрипта
${BOOT_SCRIPT}

# End Of File