
https://pypi.org/project/python-pypi-mirror/


Временно: установить `conda-mirror` из исходных кодов с наложенным патчем для поддержки опции ограничения количества извлекаемых версий конкретного пакета.

```bash
git clone https://github.com/zinal/conda-mirror.git
cd conda-mirror
pip3 install -r requirements.txt
python3 setup.py install
conda-mirror --help
```

Проверка актуальной версии пакета, которая может быть установлена с учетом особенностей окружения Data Proc:

```
root@rc1c-dataproc-g-1157fe-irow:~# conda install unidecode
Collecting package metadata (current_repodata.json): done
Solving environment: done

## Package Plan ##

  environment location: /opt/conda

  added / updated specs:
    - unidecode


The following packages will be downloaded:

    package                    |            build
    ---------------------------|-----------------
    ca-certificates-2023.01.10 |       h06a4308_0         120 KB
    unidecode-1.2.0            |     pyhd3eb1b0_0         155 KB
    ------------------------------------------------------------
                                           Total:         275 KB

The following NEW packages will be INSTALLED:

  unidecode          pkgs/main/noarch::unidecode-1.2.0-pyhd3eb1b0_0 

The following packages will be UPDATED:

  ca-certificates                     2022.10.11-h06a4308_0 --> 2023.01.10-h06a4308_0 


Proceed ([y]/n)? ^C
CondaSystemExit: 
Operation aborted.  Exiting.
```

Сделать файл настроек для работы `conda-mirror` с указанием перечня пакетов и их версий:

```yaml
blacklist:
    - name: "*"
whitelist:
  - name: botocore
    version: "1.29.55"
  - name: catboost
    version: "1.1.1"
  - name: lightgbm
    version: "3.2.1"
  - name: nltk
    version: "3.7"
  - name: prophet
    version: "1.1.2"
  - name: psycopg2
    version: "2.9.3"
  - name: seaborn
    version: "0.12.2"
  - name: unidecode
    version: "1.2.0"
include_depends: True
```

Скачать нужные пакеты вместе с зависимостями и сформировать служебные файлы репозитория:

```bash
mkdir conda1
conda-mirror --upstream-channel conda-forge --target-directory conda1 --config conda-mirror-conf.yaml --platform linux-64,noarch --num-threads 50 --latest 10
cp conda1/linux-64/repodata.json conda1/linux-64/current_repodata.json
cp conda1/linux-64/repodata.json.bz2 conda1/linux-64/current_repodata.json.bz2
cp conda1/noarch/repodata.json conda1/noarch/current_repodata.json
cp conda1/noarch/repodata.json.bz2 conda1/noarch/current_repodata.json.bz2
```

Скопировать скачанные пакеты в каталог бакета объектного хранилища:

```bash
sudo -u hdfs hdfs dfs -mkdir s3a://dproc-repo/repos/
sudo -u hdfs hdfs dfs -copyFromLocal -d -t 20 conda1/ s3a://dproc-repo/repos/
```

Проверить наличие одного из пакетов в новом репозитории:

```bash
conda clean -i -y
conda search -c https://dproc-repo.website.yandexcloud.net/repos/conda1 --override-channels catboost
```


Установить пакеты:

```bash
conda clean -i -y
conda install -c https://dproc-repo.website.yandexcloud.net/repos/conda1 --override-channels catboost lightgbm nltk prophet psycopg2 seaborn unidecode
```


cp -rlp conda conda.orig
