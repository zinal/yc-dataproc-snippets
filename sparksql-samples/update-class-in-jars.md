# Процедура замены конкретных классов в jar-архивах

```bash
scp ./src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java demo@gw1:.
ssh demo@gw1
scp HiveMetaStore.java ubuntu@rc1b-dataproc-m-zwuhxlohumq0v1yy.mdb.yandexcloud.net:.
ssh ubuntu@rc1b-dataproc-m-zwuhxlohumq0v1yy.mdb.yandexcloud.net
mkdir -p org/apache/hadoop/hive/metastore
mv HiveMetaStore.java org/apache/hadoop/hive/metastore

javac -g -classpath /usr/lib/hive/lib/'*':/usr/lib/hadoop/'*' org/apache/hadoop/hive/metastore/HiveMetaStore.java

sudo jar ufv /usr/lib/hive/lib/hive-exec-3.1.2.jar org/apache/hadoop/hive/metastore/*
sudo jar ufv /usr/lib/hive/lib/hive-metastore-3.1.2.jar org/apache/hadoop/hive/metastore/*
```
