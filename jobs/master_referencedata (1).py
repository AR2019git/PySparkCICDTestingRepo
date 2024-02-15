import conftest
from pyspark.sql.functions import when

def master_referencedata_extract(spark, input_file):

    in_df1 = spark.read.csv(input_file, header=True)
    return (in_df1)

def master_referencedata_transform(in_df1):
    in_df1 = (in_df1
              .withColumn("Country",
                          when(in_df1["Country"] == "KOREA", "KOR")
                          .when(in_df1["Country"].isin(["US", "United States", "United states of America"]), "USA")
                          .when(in_df1["Country"] == "France", "FRA")
                          .otherwise(in_df1["Country"]))
              .withColumn("POLICY_HOLDER_GENDER",
                          when(in_df1["POLICY_HOLDER_GENDER"].isin(["Male", "0", "M", "male"]), "M")
                          .when(in_df1["POLICY_HOLDER_GENDER"].isin(["Female", "1", "F", "female"]), "F")
                          .otherwise(in_df1["POLICY_HOLDER_GENDER"]))
              .withColumn("CURRENCY",
                          when(in_df1["CURRENCY"].isin(["US DOLLAR", "US Dollar"]), "USD")
                          .when(in_df1["CURRENCY"].isin(["Korea Won", "KR ", "KR"]), "KRW")
                          .when(in_df1["CURRENCY"].isin(["Australian Dollar", "AUSSIE DOLLAR"]), "AUD")
                          .when(in_df1["CURRENCY"].isin(["Indian Rupee", "Rupee"]), "INR")
                          .otherwise(in_df1["CURRENCY"]))
              .withColumn("PROD_CODE",
                          when(in_df1["PROD_CODE"].isin(["TL", "Term", "Life", "TERM", "TERM LIFE"]), "Term Life")
                          .when(in_df1["PROD_CODE"].isin(["UL", "Universal"]), "Universal Life")
                          .when(in_df1["PROD_CODE"].isin(["WL", "Whole", "LIFE"]), "Whole Life")
                          .otherwise(in_df1["PROD_CODE"]))
              .withColumn("TRANS_CODE",
                          when(in_df1["TRANS_CODE"].isin(["PREMIUM PAYMENT", "PREM", "PRM"]), "PEX")
                          .when(in_df1["TRANS_CODE"].isin(["CLAIMS PAYMENT", "CLM", "CLAIMS"]), "LDD")
                          .otherwise(in_df1["TRANS_CODE"]))
              .withColumn("POLICY_STATUS",
                          when(in_df1["POLICY_STATUS"].isin(["ACTIVE", "In force", "Active", "0"]), "A")
                          .when(in_df1["POLICY_STATUS"].isin(["Cancellation", "Terminated", "INFORCE", "1"]), "I")
                          .otherwise(in_df1["POLICY_STATUS"])))

    return (in_df1)