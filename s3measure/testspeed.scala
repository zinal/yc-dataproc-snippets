
val inputPath = "s3a://dproc-wh/s3measure/INPUT/input_wide/part-*"
val outputPath = "s3a://dproc-wh/s3measure/OUTPUT/output_wide"

val rawDF = spark.read.parquet(inputPath)

rawDF.write.
  option("mapreduce.fileoutputcommitter.algorithm.version", "2").
  option("compression", "gzip").
  mode("overwrite").
  parquet(outputPath)

System.exit(0)
