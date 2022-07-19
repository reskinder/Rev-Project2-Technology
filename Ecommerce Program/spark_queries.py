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
from pyspark.sql.functions import col, quarter, year, hour, sum, count
import os
import sys

# Menu Function
def mainMenu():
    while (True):
        print("Marketing Questions:")
        print("1. What is the top selling category of items? Per Country?")
        print("2. How does the popularity of products change throughout the year? Per Country?")
        print("3. Which locations see the highest traffic of sales?")
        print("4. Which website had the most sales?")
        print("5. Which the most common form of payment failure?")
        print("6. Which is the most common form of payment method?")
        print("7. What times have the highest traffic of sales? Per Country?")
        print("8. Exit")
        
        try:
            menuChoice = int(input("\nSelect a question (1 - 8): "))
        except:
            print("Invalid selection. Must enter 1 - 8.\n")
            mainMenu()
        else:
            if (menuChoice > 8 or menuChoice < 1):
                print("Invalid selection. Must enter 1 - 8.\n")
        
        if (menuChoice == 8):
            print("I hope we've answered all your questions. Have a nice day!")
            exit()
        
        # Call Market Queries Function
        marketQueries(menuChoice)

# Queries Function
def marketQueries(menuChoice):
    if (menuChoice == 1):
        # MARKETING Q1: What is the top selling category of items? Per Country?
        print("\nQUESTION 1: What is the top selling category of items? Per Country?")
        marketQ1 = toydf.withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ1.select("country", "product_category", "qty").where(marketQ1.payment_txn_success=="Y").where(marketQ1.year==2020)\
        .groupBy(marketQ1.country.alias("Country"), marketQ1.product_category.alias("Product Category")).agg(sum(marketQ1.qty).alias("Quantity"))\
        .orderBy(col("Country").desc(), col("Quantity").desc(), col("Product Category").desc()).show()
        # Year 2021
        print("Year 2021:")
        marketQ1.select("country", "product_category", "qty").where(marketQ1.payment_txn_success=="Y").where(marketQ1.year==2021)\
        .groupBy(marketQ1.country.alias("Country"), marketQ1.product_category.alias("Product Category")).agg(sum(marketQ1.qty).alias("Quantity"))\
        .orderBy(col("Country").desc(), col("Quantity").desc(), col("Product Category").desc()).show()
        # Year 2022
        print("Year 2022:")
        marketQ1.select("country", "product_category", "qty").where(marketQ1.payment_txn_success=="Y").where(marketQ1.year==2022)\
        .groupBy(marketQ1.country.alias("Country"), marketQ1.product_category.alias("Product Category")).agg(sum(marketQ1.qty).alias("Quantity"))\
        .orderBy(col("Country").desc(), col("Quantity").desc(), col("Product Category").desc()).show()
    elif (menuChoice == 2):
        # MARKETING Q2: How does the popularity of products change throughout the year? Per Country?
        print("\nQUESTION 2: How does the popularity of products change throughout the year? Per Country?")
        marketQ2 = toydf.withColumn("quarter", quarter(toydf.datetime)).withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2020)\
        .groupBy(marketQ2.quarter.alias("Year Quarter"), marketQ2.country.alias("Country"), marketQ2.product_name.alias("Product Name"))\
        .agg(sum(marketQ2.qty).alias("Quantity")).orderBy(col("Year Quarter").asc(), col("Quantity").desc()).show(truncate=False)
        # Year 2021
        print("Year 2021:")
        marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2021)\
        .groupBy(marketQ2.quarter.alias("Year Quarter"), marketQ2.country.alias("Country"), marketQ2.product_name.alias("Product Name"))\
        .agg(sum(marketQ2.qty).alias("Quantity")).orderBy(col("Year Quarter").asc(), col("Quantity").desc()).show(truncate=False)
        # Year 2022
        print("Year 2022:")
        marketQ2.select("country", "product_name", "qty", "quarter", "year").where(marketQ2.payment_txn_success=="Y").where(marketQ2.year==2022)\
        .groupBy(marketQ2.quarter.alias("Year Quarter"), marketQ2.country.alias("Country"), marketQ2.product_name.alias("Product Name"))\
        .agg(sum(marketQ2.qty).alias("Quantity")).orderBy(col("Year Quarter").asc(), col("Quantity").desc()).show(truncate=False)
    elif (menuChoice == 3):
        # MARKETING Q3: Which locations see the highest traffic of sales?
        print("\nQUESTION 3: Which locations see the highest traffic of sales?")
        marketQ3 = toydf.withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ3.select("country","qty").where(marketQ3.payment_txn_success=="Y").where(marketQ3.year=="2020")\
        .groupBy(marketQ3.country.alias("Country")).agg(sum(marketQ3.qty).alias("Quantity")).orderBy(col("Quantity").desc()).show(truncate=False)
        # Year 2021
        print("Year 2021:")
        marketQ3.select("country","qty").where(marketQ3.payment_txn_success=="Y").where(marketQ3.year=="2021")\
        .groupBy(marketQ3.country.alias("Country")).agg(sum(marketQ3.qty).alias("Quantity")).orderBy(col("Quantity").desc()).show(truncate=False)
        # Year 2022
        print("Year 2022:")
        marketQ3.select("country","qty").where(marketQ3.payment_txn_success=="Y").where(marketQ3.year=="2022")\
        .groupBy(marketQ3.country.alias("Country")).agg(sum(marketQ3.qty).alias("Quantity")).orderBy(col("Quantity").desc()).show(truncate=False)
    elif (menuChoice == 4):
        # MARKETING Q4: Which website had the most sales?
        print("\nQUESTION 4: Which website had the most sales?")
        marketQ4 = toydf.withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ4.select(marketQ4.ecommerce_website_name.alias("Ecommerce Website"),"qty",).where(marketQ4.payment_txn_success=="Y").where(marketQ4.year==2020)\
        .groupBy("Ecommerce Website").agg(sum(marketQ4.qty).alias("Quantity")).orderBy(col("Quantity").desc()).show(1, truncate=False)
        # Year 2021
        print("Year 2021:")
        marketQ4.select(marketQ4.ecommerce_website_name.alias("Ecommerce Website"),"qty",).where(marketQ4.payment_txn_success=="Y").where(marketQ4.year==2021)\
        .groupBy("Ecommerce Website").agg(sum(marketQ4.qty).alias("Quantity")).orderBy(col("Quantity").desc()).show(1, truncate=False)
        # Year 2022
        print("Year 2022:")
        marketQ4.select(marketQ4.ecommerce_website_name.alias("Ecommerce Website"),"qty",).where(marketQ4.payment_txn_success=="Y").where(marketQ4.year==2022)\
        .groupBy("Ecommerce Website").agg(sum(marketQ4.qty).alias("Quantity")).orderBy(col("Quantity").desc()).show(1, truncate=False)
    elif (menuChoice == 5):
        # MARKETING Q5: Which is the most common form of payment failure?
        print("\nQUESTION 5: Which is the most common form of payment failure?")
        marketQ5 = toydf.withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ5.select("failure_reason").where(marketQ5.payment_txn_success=="N").where(marketQ5.year==2020)\
        .groupBy(marketQ5.failure_reason.alias("Failure Reason")).agg(count(marketQ5.failure_reason).alias("Count")).orderBy(col("Count").desc()).show(1)
        # Year 2021
        print("Year 2021:")
        marketQ5.select("failure_reason").where(marketQ5.payment_txn_success=="N").where(marketQ5.year==2021)\
        .groupBy(marketQ5.failure_reason.alias("Failure Reason")).agg(count(marketQ5.failure_reason).alias("Count")).orderBy(col("Count").desc()).show(1)
        # Year 2022
        print("Year 2022:")
        marketQ5.select("failure_reason").where(marketQ5.payment_txn_success=="N").where(marketQ5.year==2022)\
        .groupBy(marketQ5.failure_reason.alias("Failure Reason")).agg(count(marketQ5.failure_reason).alias("Count")).orderBy(col("Count").desc()).show(1)
    elif (menuChoice == 6):
        # MARKETING Q6: Which is the most common form of payment method?
        print("\nQUESTION 6: Which is the most common form of payment method?")
        marketQ6 = toydf.withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ6.select("payment_type").where(marketQ6.payment_txn_success=="Y").where(marketQ6.year==2020)\
        .groupBy(marketQ6.payment_type.alias("Payment Type")).agg(count(marketQ6.payment_type).alias("Count")).orderBy(col("Count").desc()).show()
        # Year 2021
        print("Year 2021:")
        marketQ6.select("payment_type","qty",).where(marketQ6.payment_txn_success=="Y").where(marketQ6.year==2021)\
        .groupBy(marketQ6.payment_type.alias("Payment Type")).agg(count(marketQ6.payment_type).alias("Count")).orderBy(col("Count").desc()).show()
        # Year 2022
        print("Year 2022:")
        marketQ6.select("payment_type","qty",).where(marketQ6.payment_txn_success=="Y").where(marketQ6.year==2022)\
        .groupBy(marketQ6.payment_type.alias("Payment Type")).agg(count(marketQ6.payment_type).alias("Count")).orderBy(col("Count").desc()).show()
    elif (menuChoice == 7):
        # MARKETING Q7: What times have the highest traffic of sales? Per Country?
        print("\nQUESTION 7: What times have the highest traffic of sales? Per Country?")
        marketQ7 = toydf.withColumn("hour", hour(toydf.datetime)).withColumn("year", year(toydf.datetime))
        # Year 2020
        print("Year 2020:")
        marketQ7.select("country", "hour", "qty").where(marketQ7.payment_txn_success=="Y").where(marketQ7.year==2020)\
        .groupBy(marketQ7.country.alias("Country"), marketQ7.hour.alias("Hour")).agg(sum(marketQ7.qty).alias("Quantity"))\
        .orderBy(col("Country").desc(), col("Quantity").desc(), col("Hour").asc()).show(50, truncate=False)
        # Year 2021
        print("Year 2021:")
        marketQ7.select("country", "hour", "qty").where(marketQ7.payment_txn_success=="Y").where(marketQ7.year==2021)\
        .groupBy(marketQ7.country.alias("Country"), "hour").agg(sum(marketQ7.qty).alias("Quantity"))\
        .orderBy(col("Country").desc(), col("Quantity").desc(), col("Hour").asc()).show(50, truncate=False)
        # Year 2022
        print("Year 2022:")
        marketQ7.select("country", "hour", "qty").where(marketQ7.payment_txn_success=="Y").where(marketQ7.year==2022)\
        .groupBy(marketQ7.country.alias("Country"), marketQ7.hour.alias("Hour")).agg(sum(marketQ7.qty).alias("Quantity"))\
        .orderBy(col("Country").desc(), col("Quantity").desc(), col("Hour").asc()).show(50, truncate=False)

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

# Call Main Menu Function
mainMenu()