# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

def mastereferencedata(df):
    
    spark = SparkSession.builder \
        .appName("DataFrame Transformation") \
        .getOrCreate()

    df2 = df.withColumn("Country", when(df["Country"] == "KOREA", "KOR")
                        .when(df["Country"].isin(["US", "United States", "United states of America"]), "USA")
                        .when(df["Country"] == "France", "FRA")
                        .otherwise(df["Country"])) \
            .withColumn("POLICY_HOLDER_GENDER", 
                        when(df["POLICY_HOLDER_GENDER"].isin(["Male", "0", "M","male"]), "M")
                        .when(df["POLICY_HOLDER_GENDER"].isin(["Female", "1", "F","female"]), "F")
                        .otherwise(df["POLICY_HOLDER_GENDER"])) \
            .withColumn("CURRENCY", 
                        when(df["CURRENCY"].isin(["US DOLLAR","US Dollar"]), "USD")
                        .when(df["CURRENCY"].isin(["Korea Won", "KR"]), "KRW")
                        .when(df["CURRENCY"].isin(["Australian Dollar", "Aussie Dollar"]), "AUD")
                        .when(df["CURRENCY"].isin(["Indian Rupee", "Rupee"]), "INR")
                        .otherwise(df["CURRENCY"])) \
            .withColumn("PROD_CODE", 
                        when(df["PROD_CODE"].isin(["TL", "Term", "Life","TERM","TERM LIFE"]), "Term Life")
                        .when(df["PROD_CODE"].isin(["UL", "Universal"]), "Universal Life")
                        .when(df["PROD_CODE"].isin(["WL", "Whole","LIFE"]), "Whole Life")
                        .otherwise(df["PROD_CODE"])) \
            .withColumn("TRANS_CODE", 
                        when(df["TRANS_CODE"].isin(["PREMIUM PAYMENT", "PREM", "PRM"]), "PEX")
                        .when(df["TRANS_CODE"].isin(["CLAIMS PAYMENT", "CLM", "CLAIMS"]), "LDD")
                        .otherwise(df["TRANS_CODE"])) \
            .withColumn("POLICY_STATUS", 
                        when(df["POLICY_STATUS"].isin(["ACTIVE", "In force","Active","0"]), "A")
                        .when(df["POLICY_STATUS"].isin(["Cancellation", "Terminated","INFORCE","1"]), "I")
                        .otherwise(df["POLICY_STATUS"]))


    return df2

# COMMAND ----------


