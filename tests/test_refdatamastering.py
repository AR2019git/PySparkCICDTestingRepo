# Databricks notebook source


# COMMAND ----------

# MAGIC %run "./refdatamastering"
# MAGIC
# MAGIC

# COMMAND ----------

import pytest
#from pyspark.testing.utils import assertDataFrameEqual
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

# COMMAND ----------




def test_mastereferencedata_positive(spark_fixture):

    sample_data = [
    {
        "Country": "KOR",
        "PatientID": 1,
        "age": 39,
        "POLICY_HOLDER_GENDER": "male",
        "bmi": 23.2,
        "bloodpressure": 91,
        "region": "southeast",
        "TRAN_AMT": 1121.87,
        "CURRENCY": "KRW",
        "PROD_CODE": "TL",
        "TRANS_CODE": "PRM",
        "POLICY_STATUS": "ACTIVE"
    },
    {
        "Country": "KOR",
        "PatientID": 2,
        "age": 24,
        "POLICY_HOLDER_GENDER": "M",
        "bmi": 30.1,
        "bloodpressure": 87,
        "region": "southeast",
        "TRAN_AMT": 1131.51,
        "CURRENCY": "KR",
        "PROD_CODE": "TERM",
        "TRANS_CODE": "PREM",
        "POLICY_STATUS": "INFORCE"
    },
    {
        "Country": "US",
        "PatientID": 3,
        "age": None,
        "POLICY_HOLDER_GENDER": "O",
        "bmi": 33.3,
        "bloodpressure": 82,
        "region": "southeast",
        "TRAN_AMT": 1135.94,
        "CURRENCY": "USD",
        "PROD_CODE": "TERM LIFE",
        "TRANS_CODE": "PREMIUM PAYMENT",
        "POLICY_STATUS": "1"
    },
    {
        "Country": "USA",
        "PatientID": 4,
        "age": None,
        "POLICY_HOLDER_GENDER": "female",
        "bmi": 33.7,
        "bloodpressure": 80,
        "region": "northwest",
        "TRAN_AMT": 1136.4,
        "CURRENCY": "US DOLLAR",
        "PROD_CODE": "LIFE",
        "TRANS_CODE": "CLAIMS",
        "POLICY_STATUS": "1"}]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = mastereferencedata(original_df)

    expected_data = [
    {
        "Country": "KOR",
        "PatientID": 1,
        "age": 39,
        "POLICY_HOLDER_GENDER": "M",
        "bmi": 23.2,
        "bloodpressure": 91,
        "region": "southeast",
        "TRAN_AMT": 1121.87,
        "CURRENCY": "KRW",
        "PROD_CODE": "Term Life",
        "TRANS_CODE": "PEX",
        "POLICY_STATUS": "A"
    },
    {
        "Country": "KOR",
        "PatientID": 2,
        "age": 24,
        "POLICY_HOLDER_GENDER": "M",
        "bmi": 30.1,
        "bloodpressure": 87,
        "region": "southeast",
        "TRAN_AMT": 1131.51,
        "CURRENCY": "KRW",
        "PROD_CODE": "Term Life",
        "TRANS_CODE": "PEX",
        "POLICY_STATUS": "I"
    },
    {
        "Country": "USA",
        "PatientID": 3,
        "age": None,
        "POLICY_HOLDER_GENDER": "O",
        "bmi": 33.3,
        "bloodpressure": 82,
        "region": "southeast",
        "TRAN_AMT": 1135.94,
        "CURRENCY": "USD",
        "PROD_CODE": "Term Life",
        "TRANS_CODE": "PEX",
        "POLICY_STATUS": "I"
    },
    {
        "Country": "USA",
        "PatientID": 4,
        "age": None,
        "POLICY_HOLDER_GENDER": "F",
        "bmi": 33.7,
        "bloodpressure": 80,
        "region": "northwest",
        "TRAN_AMT": 1136.4,
        "CURRENCY": "USD",
        "PROD_CODE": "Whole Life",
        "TRANS_CODE": "LDD",
        "POLICY_STATUS": "I"
    }
]

    print("POSITIVE CASE")
    expected_df = spark.createDataFrame(expected_data)

    #assertDataFrameEqual(transformed_df, expected_df)           #use the version Spark 3.5.0


    actual_pdf = transformed_df.toPandas()
    expected_pdf = expected_df.toPandas()
    assert_frame_equal(actual_pdf, expected_pdf)


    print("Pytest has worked successfully.")
    print("Sample data matches the expected data.")

def test_mastereferencedata_negative(spark_fixture):

    sample_data = [
    {
        "Country": "KOR",
        "PatientID": 1,
        "age": 39,
        "POLICY_HOLDER_GENDER": "male",
        "bmi": 23.2,
        "bloodpressure": 91,
        "region": "southeast",
        "TRAN_AMT": 1121.87,
        "CURRENCY": "KRW",
        "PROD_CODE": "TL",
        "TRANS_CODE": "PRM",
        "POLICY_STATUS": "ACTIVE"
    },
    {
        "Country": "KOREA",
        "PatientID": 2,
        "age": 24,
        "POLICY_HOLDER_GENDER": "M",
        "bmi": 30.1,
        "bloodpressure": 87,
        "region": "southeast",
        "TRAN_AMT": 1131.51,
        "CURRENCY": "KR",
        "PROD_CODE": "TERM",
        "TRANS_CODE": "PREM",
        "POLICY_STATUS": "INFORCE"
    },
    {
        "Country": "US",
        "PatientID": 3,
        "age": None,
        "POLICY_HOLDER_GENDER": "O",
        "bmi": 33.3,
        "bloodpressure": 82,
        "region": "southeast",
        "TRAN_AMT": 1135.94,
        "CURRENCY": "USD",
        "PROD_CODE": "TERM LIFE",
        "TRANS_CODE": "PREMIUM PAYMENT",
        "POLICY_STATUS": "1"
    },
    {
        "Country": "USA",
        "PatientID": 4,
        "age": None,
        "POLICY_HOLDER_GENDER": "female",
        "bmi": 33.7,
        "bloodpressure": 80,
        "region": "northwest",
        "TRAN_AMT": 1136.4,
        "CURRENCY": "US DOLLAR",
        "PROD_CODE": "LIFE",
        "TRANS_CODE": "CLAIMS",
        "POLICY_STATUS": "1"}]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = mastereferencedata(original_df)

    expected_data = [
    {
        "Country": "KOREA",
        "PatientID": 1,
        "age": 39,
        "POLICY_HOLDER_GENDER": "M",
        "bmi": 23.2,
        "bloodpressure": 91,
        "region": "southeast",
        "TRAN_AMT": 1121.87,
        "CURRENCY": "KRW",
        "PROD_CODE": "Term Life",
        "TRANS_CODE": "PEX",
        "POLICY_STATUS": "A"
    },
    {
        "Country": "KOR",
        "PatientID": 2,
        "age": 24,
        "POLICY_HOLDER_GENDER": "MALE",
        "bmi": 30.1,
        "bloodpressure": 87,
        "region": "southeast",
        "TRAN_AMT": 1131.51,
        "CURRENCY": "KRW",
        "PROD_CODE": "Term Life",
        "TRANS_CODE": "PEX",
        "POLICY_STATUS": "I"
    },
    {
        "Country": "USA",
        "PatientID": 3,
        "age": None,
        "POLICY_HOLDER_GENDER": "O",
        "bmi": 33.3,
        "bloodpressure": 82,
        "region": "southeast",
        "TRAN_AMT": 1135.94,
        "CURRENCY": "USD",
        "PROD_CODE": "Term Life",
        "TRANS_CODE": "PEX",
        "POLICY_STATUS": "I"
    },
    {
        "Country": "USA",
        "PatientID": 4,
        "age": None,
        "POLICY_HOLDER_GENDER": "F",
        "bmi": 33.7,
        "bloodpressure": 80,
        "region": "northwest",
        "TRAN_AMT": 1136.4,
        "CURRENCY": "USD",
        "PROD_CODE": "Whole Life",
        "TRANS_CODE": "LDD",
        "POLICY_STATUS": "I"
    }
]
    print("NEGATIIVE CASE")
    expected_df = spark.createDataFrame(expected_data)
    #transformed_df.display()
    #expected_df.display()

    
    actual_pdf = transformed_df.toPandas()
    expected_pdf = expected_df.toPandas()
    assert_frame_equal(actual_pdf, expected_pdf)


    #assertDataFrameEqual(transformed_df, expected_df)           #use the version Spark 3.5.0


    print("Pytest has worked successfully.")
    print("Sample data matches the expected data.")

# COMMAND ----------

test_mastereferencedata_positive(spark_fixture)
test_mastereferencedata_negative(spark_fixture)

# COMMAND ----------


