CREATE EXTERNAL TABLE dset1 (
  spark_partition_id integer,
  int_1 integer,
  int_2 integer,
  int_3 integer,
  int_4 integer,
  int_5 integer,
  int_6 integer,
  long_1 long,
  long_2 long,
  long_3 long,
  long_4 long,
  long_5 long,
  long_6 long,
  double_1 double,
  double_2 double,
  decimal_1 DECIMAL(10,2),
  decimal_2 DECIMAL(10,2),
  decimal_3 DECIMAL(10,2),
  str_1 binary,
  str_2 text,
  str_3 text,
  str_4 text
) STORED AS PARQUET
  LOCATION "s3a://mzinal-dproc1/s3measure/INPUT/input_wide/";

SELECT COUNT(*) FROM dset1;

SELECT int_2, SUM(long_1) FROM dset1 GROUP BY int_2;

CREATE TABLE dset2 STORED AS PARQUET AS SELECT int_1 AS a, str_3 AS b FROM dset1 ;

