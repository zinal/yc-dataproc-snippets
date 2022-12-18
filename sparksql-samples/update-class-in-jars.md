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

Статистика на фазе создания партиций в Hive Metastore:
```
2022-12-18T16:19:02,255  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:19:28,868  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 26613, push=1569, fetch=17631, addParts=6833
2022-12-18T16:19:29,564  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:19:52,310  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 22746, push=1086, fetch=14491, addParts=6630
2022-12-18T16:19:53,004  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:20:17,169  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 24165, push=913, fetch=16231, addParts=6485
2022-12-18T16:20:17,835  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:20:32,769  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 14934, push=2241, fetch=5043, addParts=7133
2022-12-18T16:20:33,420  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:20:52,609  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 19189, push=2465, fetch=9759, addParts=6456
2022-12-18T16:20:53,246  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:21:12,639  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 19393, push=2412, fetch=9448, addParts=7013
2022-12-18T16:21:13,273  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:21:32,508  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 19235, push=1852, fetch=9520, addParts=7341
2022-12-18T16:21:33,163  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:21:51,659  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 18496, push=1844, fetch=9622, addParts=6510
2022-12-18T16:21:52,282  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:22:16,876  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 24594, push=1905, fetch=15081, addParts=7097
2022-12-18T16:22:17,507  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:22:36,237  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 18730, push=1738, fetch=9607, addParts=6875
2022-12-18T16:22:36,852  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T16:22:51,604  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 14752, push=2182, fetch=5016, addParts=7038
2022-12-18T16:22:52,003  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total partitions to be created: 575
2022-12-18T16:23:02,079  INFO [pool-6-thread-4] metastore.HiveMetaStore: MVZ total method time: 10076, push=1205, fetch=4471, addParts=4072
```
