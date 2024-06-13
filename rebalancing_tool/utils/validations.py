
## This file contains the data validation functions for the rebalancing tool. 


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
        print(f'Validation Test: Column {column} has been validated')
        


def test_no_duplicates(df):
    duplicates = df.duplicated(keep=False)
    if duplicates.any():
        assert not duplicates.any(), "Validation Test: Found duplicate transactions"
        print(df[duplicates])
    else:
        print('Validation Test: No duplicates found')
        print('')
    
def test_no_invalid_dates(df):
    current_year = datetime.datetime.today().year 
    extracted_years = df['fund_label'].str.extract('(\d{4})').astype(int)
    invalid_dates = (extracted_years < current_year - 1) | (extracted_years > current_year + 8)
    
    if invalid_dates.any().any():
        assert not invalid_dates.any().any(), f"Validation Test: Unexpected TRFs - Dates are out of range for {current_year - 1} to {current_year + 8}"
    else:
        print('Validation Test: Dates are in range ')
    

# def check_no_negative_value(df):
#     negative_value = df['target_val'] < 0
#     if negative_value.any():
#         raise ValueError(f"Negative values found in column {df[negative_value]}")
#     else:
#         print('Validation Result: No negative values found')
#         print('')


# def test_no_empty_descriptions(df):
#     empty_descriptions = df['Description'].str.strip() == ''
#     print(df[empty_descriptions])
#     assert not empty_descriptions.any(), "Found empty descriptions"

# def test_no_invalid_types(df):
#     invalid_types = ~df['Type'].isin(['Income', 'Expense'])
#     print(df[invalid_types])
#     assert not invalid_types.any(), "Found invalid types"


