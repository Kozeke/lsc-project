## Import
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.sql.functions as func
import time


# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Group_25") \
    .getOrCreate()

# The Group Members: Kozy-Korpesh Tolep (S5302354) and Urwa Fatima (s5156692)
# Read the files
datasetPath  = "hdfs:/user/user_lsc_25/BigMac.csv"

time_1 = time.time()

datasetDF = spark.read.csv(datasetPath, header=True, inferSchema=True)
datasetDF.printSchema()


print("Q1: Find top three countries with most expensive BigMac in 2011")

data = datasetDF.where(year('date') == 2011).select('name').sort(desc('dollar_price'))

data.show(3)

print("Q2: Find average dollar_price for BigMac for each year and sort by year.")

data = datasetDF.selectExpr('year(date) as year', 'dollar_price').groupBy('year').agg(F.avg('dollar_price').alias('avg_price')).sort('year')
data.show()

print("Q3: Find the difference between maximum value and minimum value for dollar exchange for each country and sort by difference in descending order")

data = datasetDF.groupBy('name').agg(F.max('dollar_ex').alias('max_dollar_ex'), F.min('dollar_ex').alias('min_dollar_ex'))
data = data.selectExpr('name', 'max_dollar_ex - min_dollar_ex as difference_of_dollar_ex').sort(desc('difference_of_dollar_ex'))
data.show()

print("Q4: Show average local price of Bigmac for each year and find the difference with the previous year's average local price and sort difference in descending order")

window = Window.partitionBy('name').orderBy('name')
data = datasetDF.selectExpr('name', 'local_price', 'year(date) as year').groupBy('name', 'year').agg(F.avg('local_price').alias('avg_local_price')).sort('name', 'year')
data = data.withColumn('prev_year_local_price', func.lag('avg_local_price').over(window))
data = data.selectExpr('name', 'year', 'avg_local_price', 'avg_local_price - prev_year_local_price as difference_local_price_with_previous_year')
data = data.sort(desc('difference_local_price_with_previous_year'))
data.show()

print("Q5: Compare dollar_ex with initial and final measurements for each year and find largest difference for each country and order by descending.")

window = Window.partitionBy('year', 'name').orderBy('year')
data = datasetDF.selectExpr('name', 'dollar_ex', 'year(date) as year', 'month(date) as month').withColumn('initial_dollar_ex', func.lag('dollar_ex').over(window)).sort('name', 'year')
data = data.filter('month != 1 and year != 2011').selectExpr('name', 'year', 'dollar_ex', 'initial_dollar_ex', 'dollar_ex - initial_dollar_ex as difference_dollar_ex')
data = data.groupBy('name').agg(F.max('difference_dollar_ex')).sort(desc('max(difference_dollar_ex)'))
data.show()

print("Q6: For each year give a rank for countries average dollar_price for BigMac.")

window = Window.partitionBy('name').orderBy('avg_dollar_price')
data = datasetDF.selectExpr('name', 'dollar_price', 'year(date) as year').groupBy('name','year').agg(F.avg('dollar_price').alias('avg_dollar_price')).withColumn('rank', rank().over(window)).sort('name','rank')
data.show()

time_2 = time.time()
time_interval = time_2 - time_1
