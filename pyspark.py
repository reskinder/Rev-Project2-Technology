
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark import SparkContext, sql, SparkConf
from pyspark.sql import SparkSession, SQLContext
# import pyspark
from pyspark.sql.functions import col
import os
import sys

## Set up PySpark Environment Variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

## Create a SparkSession variable
spark = SparkSession\
    .builder\
    .appName("App Name")\
    .master("local[*]")\
    .getOrCreate()

## .getOrCreate will either create a new SparkSession or search for the current one.
## SparkContext is a part of SparkSession, so we can initialize SparkContext and call it 'sc':
sc = spark.sparkContext

## If we wish to initialize SQLContext:
sqlcontext = SQLContext(spark)

## We can use 'sc' to create RDDs and DataFrames
# rdd = sc.parallelize([(1,"Jacob","IT"),(2,"Bob","Sales"),(3,"Sam","IT"),(4,"Cindy","HR")])
# df = rdd.toDF(["ID","Name","Dept"])
#df.show()
#df2 = spark.read.option('header','true').csv('Notes/Week8/sample.csv')
#df2.show()

## And we can query the DataFrame
#df.select("*").where(col("ID")=="1").show()

## Query using Spark SQL
# Spark SQL cannot query DataFrames directly, we must create a Temporary View
#df2.createOrReplaceTempView("NamesDF")
# df.createOrReplaceTempView("Employees")
# Creates a temporary view that gets saved in Hive Metastore
# spark.sql("SELECT Dept, count(*) FROM Employees GROUP BY Dept").show()


toydf = spark.read.option("header", "true").csv("created_data.csv")



toydf = toydf.withColumn("order_id", col("order_id").cast("int"))
toydf = toydf.withColumn("customer_id", col("customer_id").cast("int"))
toydf = toydf.withColumn("qty", col("qty").cast("int"))
toydf = toydf.withColumn("price", col("price").cast("int"))
toydf = toydf.withColumn("payment_txn_id", col("payment_txn_id").cast("int"))
toydf = toydf.withColumn("product_id", col("product_id").cast("int"))

# What is the top selling category of items? Per Country?
toydf.select("country","product_category","payment_txn_success","qty").where(toydf.payment_txn_success!="NULL")\
.groupBy("country").agg({"qty":"sum"})


#How does the popularity of products change throughout the year? Per Country?



# Which locations see the highest traffic of sales?



# What times have the highest traffic of sales? Per Country?



#Which website had the most sales?



#Which the most common form of payment failure?



#Which is the most common form of payment method?

# SELECT "created_data.csv"."country"AS"country","created_data.csv".
# "product_category"AS"product_category",SUM("created_data.csv"."qty")AS s
# um:qty:ok"FROM"TableauTemp"."created_data#csv" 
# "created_data.csv"WHERE(NOT("created_data.csv"."payment_txn_success"IS NULL)) GROUP BY 1,2

#     dfemp.join(dfdept, dfemp.Dept == dfdept.Dept, "inner").show()