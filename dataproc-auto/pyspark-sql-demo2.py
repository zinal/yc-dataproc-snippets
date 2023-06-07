from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("pyspark-sql-demo1") \
      .enableHiveSupport() \
      .getOrCreate()

spark.catalog.listDatabases()

df = spark.sql("SELECT num, CAST(tv AS VARCHAR(50)) AS tv, a, b, c, d, tv_year, tv_month, tv_day FROM demo1.deltatab0 WHERE tv_year=2001 AND tv_month=4 AND SUBSTR(a,1,1)='0'")
df.write.format('jdbc').options(
  url='jdbc:mysql://c-c9qqt68q14f6fdr4s1o9.rw.mdb.yandexcloud.net:3306/db1?useSSL=true',
  driver='com.mysql.jdbc.Driver',
  user='user1',
  password='passw0rd',
  dbtable='deltatab0',
  numPartitions=45,
).mode('overwrite').save()
