""" 
Description: 
    This file contains the data validation functions for the rebalancing tool. 
"""

import datetime


#----------------------------- Data Validation ---------------------------------------#
def validate_columns(df, columns):
    for column, dtype in columns.items():
        assert column in df.columns, f"Expected column '{column}' not found in DataFrame"
        assert df[column].dtype == dtype, f"Expected '{column}' to have dtype '{dtype}'"
        # assert df[column].isnull().sum() == 0, f"Found null values in '{column}'"
        #assert df[column].str.strip().str.len().eq(0).sum() == 0, f"Found empty values in '{column}'"
        # assert not df[column].eq('').any(), f"Found empty strings in '{column}'"
        # assert not df[column].eq(' ').any(), f"Found whitespace strings in '{column}'"
        # assert not df[column].eq('nan').any(), f"Found 'nan' strings in '{column}'"
        


def test_no_duplicates(df):
    duplicates = df.duplicated(keep=False)
    if duplicates.any():
        assert not duplicates.any(), "Found duplicate transactions"
        print(df[duplicates])
    
def test_no_invalid_dates(df,column):
    current_year = datetime.datetime.today().year 
    invalid_dates = (df[column].dt.year < current_year - 1) | (df[column].dt.year > current_year + 8)
    if invalid_dates.any():
        print(df[invalid_dates])
        assert not invalid_dates.any(), f"Dates are out of range for range {current_year - 1} to {current_year + 8}"

def check_no_negative_value(df, column):
    negative_value = df[column] < 0
    if negative_value.any():
        raise ValueError(f"Negative values found in column {column}")


# def test_no_empty_descriptions(df):
#     empty_descriptions = df['Description'].str.strip() == ''
#     print(df[empty_descriptions])
#     assert not empty_descriptions.any(), "Found empty descriptions"

# def test_no_invalid_types(df):
#     invalid_types = ~df['Type'].isin(['Income', 'Expense'])
#     print(df[invalid_types])
#     assert not invalid_types.any(), "Found invalid types"


