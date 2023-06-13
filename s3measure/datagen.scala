import org.apache.spark.sql.types._

val SYNTHETICS_OUTPUT_PATH = "s3a://dproc-wh/s3measure/INPUT/input_wide/"
val numPart = 200
val rowsPerPart = 1000000 // 100Mb each file, 20G total

val df1 = 1.to(numPart).toDF("id_part").repartition(numPart)
val df2 = df1.as[Int].mapPartitions(c=>1.to(rowsPerPart).toIterator)
val df3 = df2.
  withColumn("spark_partition_id",spark_partition_id()).
  withColumn("int_1",col("value")+20).
  withColumn("int_2",col("value")+30).
  withColumn("int_3",col("value")+33).
  withColumn("int_4",col("value")+37).
  withColumn("int_5",col("value")+1000000000).
  withColumn("int_6",col("value")+2000000000).
  withColumn("long_1",col("value")+3000000000L).
  withColumn("long_2",col("value")+4100000000L).
  withColumn("long_3",col("value")+4200000000L).
  withColumn("long_4",col("value")+4300000000L).
  withColumn("long_5",col("value")+4400000000L).
  withColumn("long_6",col("value")+4500000000L).
  withColumn("double_1", expr("CAST(int_6/8 AS double)")).
  withColumn("double_2", expr("CAST(int_6/10 AS double)")).
  withColumn("decimal_1", expr("CAST(int_6/10 AS DECIMAL(10,2))")).
  withColumn("decimal_2", expr("CAST(int_3/10 AS DECIMAL(10,2))")).
  withColumn("decimal_3", expr("CAST(int_2/10 AS DECIMAL(10,2))")).
  withColumn("str_1",md5(col("int_1").cast(BinaryType))).
  withColumn("str_2",col("str_1")).
  withColumn("str_3",col("str_1")).
  withColumn("str_4",col("str_1"))

df3.write.
  option("mapreduce.fileoutputcommitter.algorithm.version", "2").
  option("compression", "gzip").
  mode("overwrite").
  //csv(SYNTHETICS_OUTPUT_PATH)
  parquet(SYNTHETICS_OUTPUT_PATH)  

System.exit(0)
