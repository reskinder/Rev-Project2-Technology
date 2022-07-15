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
from pyspark.sql.functions import col, quarter, year
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
# toydf.select("country", "product_category", "qty").where(toydf.payment_txn_success=="Y")\
# .groupBy("country", "product_category").agg({"qty":"sum"}).orderBy(col("country").desc(), col("sum(qty)").desc(), col("product_category").desc()).show()


# topSaleQ1 = toydf.select(col("country").alias("Country"), col("product_category").alias("Category"), "payment_txn_success", col("qty").alias("Qty")).where(toydf.payment_txn_success!="NULL")
# topSaleQ1.show()
# topSaleQ2 = topSaleQ1.select(col("Country"), col("Category"), col("Qty"))

# topSellCategory.select("*").groupBy("country", "product_category").agg({"Qty":"max"}).alias("Qty").filter(col("Qty")<2).orderBy(col("product_category"), col("Qty").desc()).show()
# toydf.createOrReplaceTempView("topSales")
# topCategory = spark.sql("SELECT country, product_category, payment_txn_success, SUM(qty) AS Qty FROM topSales WHERE payment_txn_success != NULL HAVING MAX(Qty) GROUP BY country, product_category")
# topCategory.show()
# topSellCategory.spark.sql("SELECT country, product_category, ")

# MARKETING Q2: How does the popularity of products change throughout the year? Per Country?
# marketQ2 = toydf.withColumn("quarter", quarter(toydf.datetime)).withColumn("year", year(toydf.datetime))
# # Year 2020
# marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2020)\
# .groupBy("year", "quarter", "country", "product_name").agg({"qty":"sum"})\
# .orderBy(col("quarter").asc(), col("sum(qty)").desc()).show(truncate=False)
# # Year 2021
# marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2021)\
# .groupBy("year", "quarter", "country", "product_name").agg({"qty":"sum"})\
# .orderBy(col("quarter").asc(), col("sum(qty)").desc()).show(truncate=False)
# # Year 2022
# marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2022)\
# .groupBy("year", "country", "product_name", "quarter").agg({"qty":"sum"})\
# .orderBy(col("quarter").asc(), col("sum(qty)").desc()).show(truncate=False)

# df1 = df_student.withColumn('quarter',quarter(df_student.birthday))

# MARKETING Q3: Which locations see the highest traffic of sales?
# toydf.select("country", "")

# MARKETING Q4: Which website had the most sales?
toydf.select("ecommerce_website_name","qty",).where(toydf.payment_txn_success=="Y")\
.groupBy("ecommerce_website_name").agg({"qty":"sum"}).orderBy(col("sum(qty)").desc()).show(1, truncate=False)

# MARKETING Q5: Which the most common form of payment failure?

# MARKETING Q6: Which is the most common form of payment method?

# MARKETING Q7: What times have the highest traffic of sales? Per Country?