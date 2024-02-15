import os
import conftest
import jobs.master_referencedata
from jobs.master_referencedata import *
from jobs.apply_dobrules import validate_dob, override_dob_values

spark = conftest.setUp()

# if __name__ == "__main__":
def test_apply_dobrules():

    test_data = [
        ("21927ZZZ0", "01-01-1976", "LTD", 100000),
        ("7329379AA", "02-04-1954", "STD", 300000),
        ("7397397AQ", "08/21/1982", "Whole Life", 250000),
        ("39376937A", "11/23/1995", "Traditional Life", 10000000),
        ("32937DFFF", "05/17/1962", "Critical Illness", 200000),
        ("7391197AL", "02/29/1982", "Whole Life", 250000),
        ("7391197AL", "27-Mar-23", "Whole Life", 250000),
        ("7391197AL", "01/01/1899", "Whole Life", 250000),
        ("7391197AL", "01/01/2025", "Whole Life", 250000)
    ]

    test_data_header = ["Policy_Number", "DOB", "Product", "MaturityAmount"]
    in_df1 = spark.createDataFrame(test_data, test_data_header)

    # print("Input dataframe")
    # print(in_df1.show())

    spark.udf.register("validate_dob", validate_dob)
    # print("Input performing DOB validation...")
    # print(' ')

    # print('Output dataframe')
    out_df1 = override_dob_values(in_df1)
    # print(out_df1.show())

