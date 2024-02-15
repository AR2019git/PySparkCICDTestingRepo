import os
import conftest
import jobs.master_referencedata
from jobs.master_referencedata import *

spark = conftest.setUp()

def test_master_referencedata():

    test_df1_raw = (master_referencedata_extract
                    (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Input_data.csv'))
    test_df1_transformed = (master_referencedata_transform(test_df1_raw))

    expected_df1_raw = (master_referencedata_extract
                        (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Output_data.csv'))
    expected_df1_transformed = (master_referencedata_transform(expected_df1_raw))

    assert test_df1_transformed.collect() == expected_df1_transformed.collect()

def test_master_neg_referencedata():

    test_df2_raw = (master_referencedata_extract
                    (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Input_data.csv'))
    test_df2_transformed = (master_referencedata_transform(test_df2_raw))

    expected_df2_raw = (master_referencedata_extract
                        (spark, conftest.TEST_FILE_PATH + '/' + 'Policy_Output_neg_data.csv'))
    expected_df2_transformed = (master_referencedata_transform(expected_df2_raw))

    assert test_df2_transformed.collect() == expected_df2_transformed.collect()