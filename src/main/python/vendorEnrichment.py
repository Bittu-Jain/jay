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


productEnrichmentInputSchema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Product_Name', StringType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Quantity_Sold', IntegerType(), True),
    StructField('Sale_Date', TimestampType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True)
])

vendorReferenceSchema = StructType([
    StructField('Vendor_ID', StringType(), True),
    StructField('Vendor_Name', StringType(), True),
    StructField('Vendor_Add_Street', StringType(), True),
    StructField('Vendor_Add_City', StringType(), True),
    StructField('Vendor_Add_State', StringType(), True),
    StructField('Vendor_Add_Country', StringType(), True),
    StructField('Vendor_Add_Zip', StringType(), True),
    StructField('Vendor_Update_Date', TimestampType(), True)
])

usdReferenceSchema = StructType([
    StructField('Currency', StringType(), True),
    StructField('Currency_Code', StringType(), True),
    StructField('Exchange_Rate', FloatType(), True),
    StructField('Currency_Update_Date', TimestampType(), True)
])

productEnrichedDF = spark.read\
        .schema(productEnrichmentInputSchema)\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(outputLocation + "Enriched\SaleAmountEnrichment\SaleAmountEnriched" + currentDaySuffix)

productEnrichedDF.createOrReplaceTempView("productEnrichedDF")


usdReferenceDf = spark.read\
        .schema(usdReferenceSchema)\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(inputLocation + "USD_Rates")

usdReferenceDf.createOrReplaceTempView("usdReferenceDf")

vendorReferenceDf = spark.read\
        .schema(vendorReferenceSchema)\
        .option("delimiter", "|")\
        .option("header", True)\
        .csv(inputLocation + "Vendors")

vendorReferenceDf.createOrReplaceTempView("vendorReferenceDf")

vendorEnrichedDF = spark.sql("""
            select p.*, v.Vendor_Name
            from productEnrichedDF p join vendorReferenceDf v on p.Vendor_ID = v.Vendor_ID
""")

vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

usdEnrichDF = spark.sql("""
        select *, round(v.Sale_Amount / u.Exchange_Rate,2) as Amount_USD
        from vendorEnrichedDF v join usdReferenceDf u ON v.Sale_Currency = u.Currency_Code
""")

usdEnrichDF.show()

usdEnrichDF.write\
    .option("delimiter", "|")\
    .option("header", True)\
    .mode("overwrite")\
    .csv(outputLocation + "Enriched\Vendor_USD_Enriched\Vendor_USD_Enriched" + currentDaySuffix)


# MySql Connectivity

 