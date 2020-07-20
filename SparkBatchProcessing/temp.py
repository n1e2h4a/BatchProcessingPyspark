from pyspark.sql import SparkSession
from datetime import datetime

from pyspark.sql.functions import col, udf, avg
from pyspark.sql.types import DateType

import pyspark.sql.functions as sql_functions
import os
import time
os.environ["PYSPARK_PYTHON"]='/usr/bin/python3'
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext
time.sleep(10)
# Load data from a CSV
file_location = "/home/niharika/PycharmProjects/spark/venv/include/SparkBatchProcessing/data/temperature.csv"
df = spark.read.format("CSV").option("inferSchema", True).option("header", True).load(file_location)
time.sleep(40)
#print(df.show())

firstdataset = df.filter(sql_functions.col('date') <= '2014-01-08').select('date', 'wind_speed')
firstdataset.select(avg('wind_speed')).show()
time.sleep(10)

seconddataset = df.filter(sql_functions.col('meantemp') <= '10').select('date', 'meanpressure','meantemp')
time.sleep(10)

dropped_df = seconddataset.dropDuplicates(subset = ['meantemp'])
dropped_df.show(5)
