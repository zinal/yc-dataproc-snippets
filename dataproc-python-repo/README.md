
https://pypi.org/project/python-pypi-mirror/

При обновлении до текущей версии временно требуется обновить файл 
/usr/lib/zeppelin/interpreter/python/python/zeppelin_python.py
на версию из Github
https://github.com/apache/zeppelin/blob/master/python/src/main/resources/python/zeppelin_python.py

```bash
conda update -c conda-forge --all --yes
conda install -c conda-forge -n base --yes conda-libmamba-solver
conda config --set solver libmamba
conda install -c conda-forge --yes conda-build
conda install -c conda-forge --yes \
  'catboost==1.0.6' \
  'lightgbm==3.2.1' \
  'nltk==3.7' \
  'prophet==1.1.2' \
  'seaborn==0.12.2' \
  'unidecode==1.2.0' \
  'psycopg2==2.9.3'
```

```bash
REPO=/Mirror/conda1
mkdir -pv ${REPO}
(cd /opt/conda/pkgs && ls *.{conda,bz2}) | while read fname; do
  url=`grep "$fname" /opt/conda/pkgs/urls.txt`
  if [ ! -z "$url" ]; then
    part1=`dirname $url`
    arch=`basename $part1`
    mkdir -pv ${REPO}/$arch
    cp -v /opt/conda/pkgs/$fname ${REPO}/$arch/
  fi
done
conda index ${REPO}
find ${REPO} -type d -name .cache | sort -u | while read x; do rm -rf $x; done
```

```bash
sudo -u hdfs hdfs dfs -mkdir s3a://dproc-repo/repos/
sudo -u hdfs hdfs dfs -copyFromLocal -d -t 20 conda1/ s3a://dproc-repo/repos/
```

```bash
CHANNEL='https://dproc-repo.website.yandexcloud.net/repos/conda1'
conda update -c ${CHANNEL} --override-channels --all --yes
conda install -c ${CHANNEL} --override-channels --yes conda-libmamba-solver
conda config --set solver libmamba
conda install -c ${CHANNEL} --override-channels --yes \
  'catboost==1.0.6' \
  'lightgbm==3.2.1' \
  'nltk==3.7' \
  'prophet==1.1.2' \
  'seaborn==0.12.2' \
  'unidecode==1.2.0' \
  'psycopg2==2.9.3'
```

```bash
apt install squashfs-tools
mksquashfs /opt/conda /CondaImage1.squashfs
sudo -u hdfs hdfs dfs -copyFromLocal -d /CondaImage1.squashfs s3a://dproc-repo/repos/
```

```bash
sudo -u hdfs hdfs dfs -copyToLocal s3a://dproc-repo/repos/CondaImage1.squashfs /tmp/
mv /tmp/CondaImage1.squashfs /
chown root:root /CondaImage1.squashfs
rm -rf /opt/conda/*
echo '/CondaImage1.squashfs    /opt/conda    squashfs    ro,defaults    0 0' >>/etc/fstab
mount -a
```

Ориентировочное время выполнения - 1 минута.