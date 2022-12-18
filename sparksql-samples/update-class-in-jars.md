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

Расширенная статистика, второй прогон:
```
2022-12-18T17:06:13,833  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:06:38,984  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 25151, push=1452, fetch=15918, addParts=7182, future.cr=411260, future.it=902169
2022-12-18T17:06:39,721  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:07:03,053  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 23332, push=1104, fetch=14075, addParts=7604, future.cr=309308, future.it=784154
2022-12-18T17:07:03,763  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:07:26,114  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 22351, push=908, fetch=14421, addParts=6479, future.cr=350760, future.it=788525
2022-12-18T17:07:26,801  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:07:49,761  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 22960, push=861, fetch=14910, addParts=6631, future.cr=345149, future.it=792183
2022-12-18T17:07:50,468  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:08:12,158  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 21690, push=1338, fetch=13760, addParts=6063, future.cr=351311, future.it=725624
2022-12-18T17:08:12,836  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:08:35,090  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 22254, push=856, fetch=14383, addParts=6469, future.cr=364344, future.it=761272
2022-12-18T17:08:35,828  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:08:57,862  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 22034, push=861, fetch=14481, addParts=6177, future.cr=320521, future.it=785614
2022-12-18T17:08:58,529  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:09:13,172  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 14643, push=2343, fetch=5155, addParts=6609, future.cr=224379, future.it=177753
2022-12-18T17:09:13,824  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:09:33,165  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 19341, push=2213, fetch=9972, addParts=6623, future.cr=207543, future.it=326638
2022-12-18T17:09:33,855  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:09:52,885  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 19030, push=2419, fetch=9757, addParts=6324, future.cr=224478, future.it=142353
2022-12-18T17:09:53,556  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 1000
2022-12-18T17:10:09,854  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 16298, push=2386, fetch=4870, addParts=8519, future.cr=102590, future.it=108391
2022-12-18T17:10:10,270  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total partitions to be created: 575
2022-12-18T17:10:19,634  INFO [pool-6-thread-57] metastore.HiveMetaStore: MVZ total method time: 9364, push=1287, fetch=4288, addParts=3483, future.cr=30510, future.it=39485
```
