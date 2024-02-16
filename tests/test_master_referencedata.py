import os
import conftest
import jobs.master_referencedata
from jobs.master_referencedata import *

spark = conftest.setUp()

def test_master_referencedata():

    test_df1_raw = (master_referencedata_extract
                    (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Input_data.csv'))
    transformed_dataframe1 = (master_referencedata_transform(test_df1_raw))

    expected_df1_raw = (master_referencedata_extract
                        (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Output_data.csv'))

    assert transformed_dataframe1.collect() == expected_df1_raw.collect()

def test_master_neg_referencedata():

    test_df2_raw = (master_referencedata_extract
                    (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Input_data.csv'))
    transformed_dataframe2 = (master_referencedata_transform(test_df2_raw))

    expected_df2_raw = (master_referencedata_extract
                        (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Output_neg_data.csv'))

    assert transformed_dataframe2.collect() == expected_df2_raw.collect()
