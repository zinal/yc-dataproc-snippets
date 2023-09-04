#!/usr/bin/env python
# coding: utf-8

from datetime import date, timedelta
 
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel


spark = SparkSession.builder                               \
     .appName("sample1_pyspark")                           \
     .config("spark.dynamicAllocation.enabled", "false")   \
     .config("spark.shuffle.service.enabled", "false")     \
     .config("spark.executor.cores", "3")                  \
     .config("spark.executor.memoryOverhead", "1g")        \
     .config("spark.executor.instances", "4")              \
     .config("spark.executor.memory", "3g")                \
     .config("spark.driver.memory", "12g")                 \
     .config("spark.sql.autoBroadcastJoinThreshold", "20971520") \
     .config("spark.sql.broadcastTimeout", "36000")        \
     .getOrCreate()

# spark.sql("SELECT COUNT(*) FROM mytest1.orderlines").show(false)

spark.sql("SELECT prod_id, COUNT(*) FROM mytest1.orderlines GROUP BY prod_id ORDER BY COUNT(*) DESC LIMIT 10").show()
