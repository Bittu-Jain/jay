from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, IntegerType, StructType, TimestampType, DoubleType
from pyspark.sql.types import *
import configparser

from src.main.python.gkfunctions import read_schema
from datetime import datetime, date, timedelta, time
from pyspark.sql import functions
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# reading the configs

config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingFileSchemaFromConf = config.get('schema', 'landingFileSchema')
holdFileSchemaFromConf = config.get('schema', 'holdFileSchema')

landingFileSchema = read_schema(landingFileSchemaFromConf)
holdFileSchema = read_schema(holdFileSchemaFromConf)


# Initiating Spark Session
spark = SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()

# reading landing zone

# landingFileSchema = StructType([
#     StructField('Sale_ID', StringType(), True),
#     StructField('Product_ID', StringType(), True),
#     StructField('Quantity_Sold', IntegerType(), True),
#     StructField('Vendor_ID', StringType(), True),
#     StructField('Sale_Date', TimestampType(), True),
#     StructField('Sale_Amount', DoubleType(), True),
#     StructField('Sale_Currency', StringType(), True)
# ])

# handling dates

today = datetime.now()
yesterdayDate = today - timedelta(1)


# DDMMYYYY

# currentDaySuffix = "_" + today.strftime("%d%m%Y")
# print(currentDaySuffix)
# previousDaySuffix = "_" + yesterdayDate.strftime("%d%m%Y")
# print(previousDaySuffix)

currentDaySuffix = "_05062020"
previousDaySuffix = "_04062020"


landingFileDF = spark.read.schema(landingFileSchema).option("delimiter", "|").format("csv")\
        .load(inputLocation + "Sales_Landing\SalesDump" + currentDaySuffix)

landingFileDF.createOrReplaceTempView("landingFileDF")

# reading previous Hold data
previousHoldDF = spark.read.schema(holdFileSchema).option("delimiter", "|").option("header", True).format("csv")\
        .load(outputLocation + "Hold/HoldData" + previousDaySuffix)

previousHoldDF.createOrReplaceTempView("previousHoldDF")

refreshedLandingData = spark.sql("""
                        select a.Sale_ID, a.Product_ID,
                        CASE WHEN (a.Quantity_Sold Is Null) THEN b.Quantity_Sold
                        ELSE a.Quantity_Sold END as Quantity_Sold,
                        CASE WHEN (a.Vendor_ID Is Null) THEN b.Vendor_ID
                        ELSE a.Vendor_ID END as Vendor_ID,
                        a.Sale_Date, a.Sale_Amount, a.Sale_Currency
                        from landingFileDF a left outer join 
                        previousHoldDF b on a.Sale_ID = b.Sale_Id
        """)

validLandingDF = refreshedLandingData.filter(functions.col("Quantity_Sold").isNotNull() & functions.col("Vendor_ID").isNotNull())

validLandingDF.createOrReplaceTempView("validLandingDF")

releasedFromHold = spark.sql("""
                select vd.Sale_ID FROM validLandingDF vd INNER JOIN previousHoldDF phd on vd.Sale_ID = phd.Sale_ID
""")

releasedFromHold.createOrReplaceTempView("releasedFromHold")

notReleasedFromHold = spark.sql("""
                select * from previousHoldDF where Sale_ID not in (select Sale_ID from releasedFromHold)
""")
notReleasedFromHold.createOrReplaceTempView("notReleasedFromHold")


invalidLandingDF = refreshedLandingData.filter(functions.col("Quantity_Sold").isNull() | functions.col("Vendor_ID").isNull())\
        .withColumn("Hold_Reason", when(functions.col("Quantity_Sold").isNull(), "Quantity_Sold is missing")
                    .otherwise(when(functions.col("Vendor_ID").isNull(), "Vendor_ID is missing")))\
        .union(notReleasedFromHold)

invalidLandingDF.show()


validLandingDF.write\
        .mode("overwrite")\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(outputLocation + "Valid/ValidData" + currentDaySuffix)

invalidLandingDF.write\
        .mode("overwrite")\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(outputLocation + "Hold/HoldData" + currentDaySuffix)
