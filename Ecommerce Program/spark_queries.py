#
# Project 2 Technology: PySpark SQL Queries
# Group 2 (Ruth, Precious, Rian, Delia, Evelyn)
#

# Imports
import findspark
findspark.init()

from pyspark import SparkContext, sql, SparkConf
from pyspark.sql import SparkSession, Row, HiveContext, SQLContext
import pyspark
from pyspark.sql.functions import col, quarter, year, hour
import os
import sys

# Set Up PySpark Environment Variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create SparkSession Variable
spark = SparkSession.builder\
    .appName("Toy_Ecommerce")\
    .master("local[*]")\
    .getOrCreate()

# Initialize SparkContext as sc
sc = spark.sparkContext
# Initialize SQLContext as sqlcontext
sqlcontext = SQLContext(spark)

# Create dataframe from csv file
toydf = spark.read.option("header", "true").csv("/user/ruth/created_data.csv")

# Change Datatypes of all fields that need to be integer
toydf = toydf.withColumn("order_id", col("order_id").cast("int"))
toydf = toydf.withColumn("customer_id", col("customer_id").cast("int"))
toydf = toydf.withColumn("qty", col("qty").cast("int"))
toydf = toydf.withColumn("price", col("price").cast("int"))
toydf = toydf.withColumn("payment_txn_id", col("payment_txn_id").cast("int"))
toydf = toydf.withColumn("product_id", col("product_id").cast("int"))

# # MARKETING Q1: What is the top selling category of items? Per Country?
print("QUESTION 1: What is the top selling category of items? Per Country?")
toydf.select("country", "product_category", "qty").where(toydf.payment_txn_success=="Y")\
.groupBy("country", "product_category").agg({"qty":"sum"}).orderBy(col("country").desc(), col("sum(qty)").desc(), col("product_category").desc()).show()

# MARKETING Q2: How does the popularity of products change throughout the year? Per Country?
print("QUESTION 2: How does the popularity of products change throughout the year? Per Country?")
marketQ2 = toydf.withColumn("quarter", quarter(toydf.datetime)).withColumn("year", year(toydf.datetime))
# Year 2020
marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2020)\
.groupBy("year", "quarter", "country", "product_name").agg({"qty":"sum"})\
.orderBy(col("quarter").asc(), col("sum(qty)").desc()).show(truncate=False)
# Year 2021
marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2021)\
.groupBy("year", "quarter", "country", "product_name").agg({"qty":"sum"})\
.orderBy(col("quarter").asc(), col("sum(qty)").desc()).show(truncate=False)
# Year 2022
marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2022)\
.groupBy("year", "country", "product_name", "quarter").agg({"qty":"sum"})\
.orderBy(col("quarter").asc(), col("sum(qty)").desc()).show(truncate=False)

# MARKETING Q3: Which locations see the highest traffic of sales?
print("QUESTION 3: Which locations see the highest traffic of sales?")
marketQ3 = toydf.withColumn("year", year(toydf.datetime))
# Year 2020
print("Year 2020:")
marketQ3.select("country","qty").where(marketQ3.payment_txn_success=="Y").where(marketQ3.year=="2020")\
.groupBy("country").agg({"qty":"sum"}).orderBy(col("sum(qty)").desc()).show(truncate=False)
# Year 2021
print("Year 2021:")
marketQ3.select("country","qty").where(marketQ3.payment_txn_success=="Y").where(marketQ3.year=="2021")\
.groupBy("country").agg({"qty":"sum"}).orderBy(col("sum(qty)").desc()).show(truncate=False)
# Year 2022
print("Year 2022:")
marketQ3.select("country","qty").where(marketQ3.payment_txn_success=="Y").where(marketQ3.year=="2022")\
.groupBy("country").agg({"qty":"sum"}).orderBy(col("sum(qty)").desc()).show(truncate=False)

# MARKETING Q4: Which website had the most sales?
print("QUESTION 4: Which website had the most sales?")
toydf.select("ecommerce_website_name","qty",).where(toydf.payment_txn_success=="Y")\
.groupBy("ecommerce_website_name").agg({"qty":"sum"}).orderBy(col("sum(qty)").desc()).show(1, truncate=False)

# MARKETING Q5: Which the most common form of payment failure?
print("QUESTION 5: Which the most common form of payment failure?")
toydf.select("failure_reason").where(toydf.payment_txn_success=="N")\
.groupBy("failure_reason").agg({"failure_reason":"count"})\
.orderBy(col("count(failure_reason)").desc()).show(1)

# MARKETING Q6: Which is the most common form of payment method?
print("QUESTION 6: Which is the most common form of payment method?")
toydf.select("payment_type","qty",).where(toydf.payment_txn_success=="Y")\
.groupBy("payment_type").agg({"qty":"sum"}).orderBy(col("sum(qty)").desc()).show()

# MARKETING Q7: What times have the highest traffic of sales? Per Country?
print("QUESTION 7: What times have the highest traffic of sales? Per Country?")
marketQ7 = toydf.withColumn("hour", hour(toydf.datetime)).withColumn("year", year(toydf.datetime))
# Year 2020
print("Year 2020:")
marketQ7.select("country", "hour", "qty").where(marketQ7.payment_txn_success=="Y").where(marketQ7.year==2020)\
.groupBy("country", "hour").agg({"qty":"sum"}).orderBy(col("country").desc(), col("sum(qty)").desc(), col("hour").asc()).show(50, truncate=False)
# Year 2021
print("Year 2021:")
marketQ7.select("country", "hour", "qty").where(marketQ7.payment_txn_success=="Y").where(marketQ7.year==2021)\
.groupBy("country", "hour").agg({"qty":"sum"}).orderBy(col("country").desc(), col("sum(qty)").desc(), col("hour").asc()).show(50, truncate=False)
# Year 2022
print("Year 2022:")
marketQ7.select("country", "hour", "qty").where(marketQ7.payment_txn_success=="Y").where(marketQ7.year==2022)\
.groupBy("country", "hour").agg({"qty":"sum"}).orderBy(col("country").desc(), col("sum(qty)").desc(), col("hour").asc()).show(50, truncate=False)