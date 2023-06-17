
val inputPath = "s3a://dproc-zeppelin/s3measure/INPUT/input_wide/part-*"
val outputPath = "s3a://dproc-zeppelin/s3measure/OUTPUT/output_wide"

val rawDF = spark.read.parquet(inputPath)

rawDF.write.
  option("compression", "gzip").
  mode("overwrite").
  parquet(outputPath)

System.exit(0)
