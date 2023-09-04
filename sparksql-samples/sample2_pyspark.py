#!/usr/bin/env python
# coding: utf-8

from datetime import date, timedelta
 
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel


spark = SparkSession.builder                               \
     .appName("sample2_pyspark")                           \
     .getOrCreate()

# spark.sql("SELECT COUNT(*) FROM mytest1.orderlines").show(false)

spark.sql("SELECT prod_id, COUNT(*) FROM mytest1.orderlines GROUP BY prod_id ORDER BY COUNT(*) DESC LIMIT 10").show()
