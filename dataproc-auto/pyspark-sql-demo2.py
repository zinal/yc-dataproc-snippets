from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("pyspark-sql-demo1") \
      .enableHiveSupport() \
      .getOrCreate()

spark.catalog.listDatabases()

df = spark.sql("SELECT num, CAST(tv AS VARCHAR(50)) AS tv, a, b, c, d, tv_year, tv_month, tv_day FROM demo1.deltatab0 WHERE tv_year=2001 AND tv_month IN (1,2,3,4)")
df.write.format('jdbc').options(
  url='jdbc:mysql://c-c9qun220hulvcq00gtl3.rw.mdb.yandexcloud.net:3306/db1?useSSL=true',
  driver='com.mysql.jdbc.Driver',
  user='user1',
  password='passw0rd',
  dbtable='deltatab0',
  numPartitions=45,
).mode('overwrite').save()