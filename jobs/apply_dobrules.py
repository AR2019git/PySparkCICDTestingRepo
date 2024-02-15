from datetime import datetime
import conftest
from pyspark.sql.functions import when, col, expr


# spark = conftest.setUp()
def validate_dob(dob):
    try:
        parsed_date = datetime.strptime(dob, "%m/%d/%Y")
        if 1900 <= parsed_date.year <= 2024:
            return dob
    except ValueError:
        pass
    return "01/01/1980"

def override_dob_values(in_df):
    return in_df.withColumn("DOB", when(col("DOB").isNull(), "01/01/1980").
                            otherwise(expr("validate_dob(DOB)")))
