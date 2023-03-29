from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("pyspark-sql-demo1") \
      .enableHiveSupport() \
      .getOrCreate()

spark.catalog.listDatabases()

df = spark.sql("SELECT * FROM demo1.megatab_1m LIMIT 10")
df.printSchema()
df.show()
