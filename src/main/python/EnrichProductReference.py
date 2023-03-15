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

# Initiating Spark Session
spark = SparkSession.builder.appName("EnrichProductreference").master("local").getOrCreate()

# reading the configs
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingFileSchemaFromConf = config.get('schema', 'landingFileSchema')

validFileSchema = read_schema(landingFileSchemaFromConf)

currentDaySuffix = "_05062020"
previousDaySuffix = "_04062020"

# Reading Valid data
validDataDf = spark.read\
        .schema(validFileSchema)\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(outputLocation + "Valid/ValidData" + currentDaySuffix)

validDataDf.createOrReplaceTempView("validDataDf")

productPriceReferenceSchema = StructType([
    StructField('Product_ID', StringType(), True),
    StructField('Product_Name', StringType(), True),
    StructField('Product_Price', IntegerType(), True),
    StructField('Product_Price_Currency', StringType(), True),
    StructField('Product_updated_date', TimestampType(), True)
])

# R eading Project Reference
productPriceReferenceDF = spark.read\
        .schema(productPriceReferenceSchema)\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(inputLocation + "Products")

productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

productEnrichedDF = spark.sql("""
            select v.Sale_ID, v.Product_ID, p.Product_Name, v.Vendor_ID, v.Quantity_Sold, v.Sale_Date,
            p.Product_Price * v.Quantity_Sold as Sale_Amount, v.Sale_Currency
            from validDataDf v inner join productPriceReferenceDF p on v.Product_ID = p.Product_ID
""")

productEnrichedDF.show()

productEnrichedDF.write\
        .mode("overwrite")\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currentDaySuffix)