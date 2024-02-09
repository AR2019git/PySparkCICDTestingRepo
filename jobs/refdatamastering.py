# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

def mastereferencedata(df):
    
    spark = SparkSession.builder \
        .appName("DataFrame Transformation") \
        .getOrCreate()

    df2 = df.withColumn("COUNTRY", when(df["COUNTRY"]=="Korea" ,"KOR")
                        .when(df["COUNTRY"].isin(["US","United States", "United states of America"]) ,"USA")
                        .when(df["COUNTRY"]=="France" ,"FRA")
                        .otherwise(df["COUNTRY"])) \
            .withColumn("POLICY_HOLDER_GENDER", when(df["POLICY_HOLDER_GENDER"].isin(["Male", "0", "M"]) ,"M")
                        .when(df["POLICY_HOLDER_GENDER"].isin(["Female", "1", "F"]) ,"F")
                        .otherwise(df["POLICY_HOLDER_GENDER"])) \
            .withColumn("CURRENCY", when(df["CURRENCY"]=="US Dollar" ,"USD")
                        .when(df["CURRENCY"]=="Korea Won" ,"KRW")
                        .when(df["CURRENCY"].isin(["Australian Dollar", "Aussie Dollar"]) ,"AUD")
                        .when(df["CURRENCY"].isin(["Indian Rupee","Rupee"]) ,"INR")
                        .otherwise(df["CURRENCY"])) \
            .withColumn("PROD_CODE", when(df["PROD_CODE"].isin(["TL", "Term", "Life"]) ,"Term Life")
                        .when(df["PROD_CODE"].isin(["UL","Universal"]) ,"Universal Life")
                        .when(df["PROD_CODE"].isin(["WL","Whole"]) ,"Whole Life")
                        .otherwise(df["PROD_CODE"])) \
            .withColumn("TRANS_CODE", when(df["TRANS_CODE"].isin(["PREMIUM PAYMENT", "PREM", "PRM"]) ,"PEX")
                        .when(df["TRANS_CODE"].isin(["CLAIMS PAYMENT","CLM", "CLAIM"]) ,"LDD")
                        .otherwise(df["TRANS_CODE"])) \
            .withColumn("POLICY_STATUS", when(df["POLICY_STATUS"].isin(["Active", "In force"]) ,"A")
                        .when(df["POLICY_STATUS"].isin(["Cancellation","Terminated"]) ,"I")
                        .otherwise(df["POLICY_STATUS"]))


    df2.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("Sample DataFrame Creation") \
    .getOrCreate()

data = [
    Row(COUNTRY='Korea', POLICY_HOLDER_GENDER='Male', CURRENCY='Korea Won', PROD_CODE='TL', TRANS_CODE='PREMIUM PAYMENT', POLICY_STATUS='Active'),
    Row(COUNTRY='US', POLICY_HOLDER_GENDER='Female', CURRENCY='US Dollar', PROD_CODE='UL', TRANS_CODE='CLAIMS PAYMENT', POLICY_STATUS='Terminated'),
    Row(COUNTRY='France', POLICY_HOLDER_GENDER='0', CURRENCY='Euro', PROD_CODE='WL', TRANS_CODE='PREM', POLICY_STATUS='In force'),
    # Add more rows as needed
]


df = spark.createDataFrame(data)
print("Raw Data values")
df.display()

print("Data values post mastering")
mastereferencedata(df)


# COMMAND ----------


