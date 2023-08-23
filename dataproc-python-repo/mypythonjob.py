from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("mypythonjob")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

import catboost
print(catboost.__version__)
