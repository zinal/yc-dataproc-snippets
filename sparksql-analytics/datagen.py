from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType,BinaryType
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import md5

spark = SparkSession.builder \
      .enableHiveSupport() \
      .getOrCreate()

BUCKET_NAME = "mzinal-dproc1"
OUTPUT_PATH = "s3a://" + BUCKET_NAME + "/s3measure/INPUT/input_wide/"
numPart = 200
rowsPerPart = 1000000 # 100Mb each file, 20G total


df1 = spark.createDataFrame(data=list(map(lambda x: [x], list(range(1, numPart+1)))), schema = ["id_part"])

def genPartition(part):
  for row in part:
    for val in range(0, rowsPerPart):
      yield [val]

df2 = df1.repartition(numPart).rdd.mapPartitions(genPartition).toDF(["val"])
df3 = df2.withColumn("val",df2.val.cast(IntegerType()))
df4 = df3.withColumn("spark_partition_id",spark_partition_id()) \
  .withColumn("int_1",col("val")+20) \
  .withColumn("int_2",col("val")+30) \
  .withColumn("int_3",col("val")+33) \
  .withColumn("int_4",col("val")+37) \
  .withColumn("int_5",col("val")+1000000000) \
  .withColumn("int_6",col("val")+2000000000) \
  .withColumn("long_1",col("val")+3000000000) \
  .withColumn("long_2",col("val")+4100000000) \
  .withColumn("long_3",col("val")+4200000000) \
  .withColumn("long_4",col("val")+4300000000) \
  .withColumn("long_5",col("val")+4400000000) \
  .withColumn("long_6",col("val")+4500000000) \
  .withColumn("double_1", expr("CAST(int_6/8 AS double)")) \
  .withColumn("double_2", expr("CAST(int_6/10 AS double)")) \
  .withColumn("decimal_1", expr("CAST(int_6/10 AS DECIMAL(10,2))")) \
  .withColumn("decimal_2", expr("CAST(int_3/10 AS DECIMAL(10,2))")) \
  .withColumn("decimal_3", expr("CAST(int_2/10 AS DECIMAL(10,2))")) \
  .withColumn("str_1",md5(col("int_1").cast(BinaryType()))) \
  .withColumn("str_2",col("str_1")) \
  .withColumn("str_3",col("str_1")) \
  .withColumn("str_4",col("str_1"))

df4.write.option("compression", "gzip").mode("overwrite").parquet(OUTPUT_PATH)

