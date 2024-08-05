
## This file contains the data validation functions for the rebalancing tool. 
import datetime

#----------------------------- Data Validation ---------------------------------------#
def validate_columns_check1(before_cols, after_cols):
    assert len(before_cols) == len(after_cols)-2, "Validation Test: Columns do not match"

def validate_columns_check2(df, columns):
    for column in columns:
        assert column in df.columns, f"Expected column '{column}' not found in DataFrame"

    print(f'Validation Test: All columns type has been validated')
    print('')
        
def validate_no_duplicates(df):
    duplicates = df.duplicated(keep=False)
    if duplicates.any():
        print(df[duplicates])
        assert not duplicates.any(), "Validation Test: Found duplicate transactions. See above for details"
    else:
        print('Validation Test: No duplicates found')
        print('')
    
def validate_no_invalid_dates(df):
    current_year = datetime.datetime.today().year 
    extracted_years = df['fund_label'].str.extract('(\d{4})').astype(float)
    extracted_years = extracted_years.dropna().astype(int)
    invalid_dates = (extracted_years < current_year - 1) | (extracted_years > current_year + 8)
    
    if invalid_dates.any().any():
        invalid_dates_list = extracted_years[invalid_dates].dropna().values.tolist()
        assert not invalid_dates.any().any(), f"Validation Test: Unexpected TRFs - Dates are out of range. Invalid dates: {invalid_dates_list}"
    else:
        print('Validation Test: Dates are in range ')
        print('')



#############################################################################
# from pydantic import BaseModel, Field
# from datetime import datetime
# from typing import Dict

# class PreColumn(BaseModel):
#     date: datetime
#     fund_label: str
#     fund_underlying: str
#     fund_key: str
#     fund_glidepath: str
#     valuation: float
#     actual_weight: float
#     target_weight: float

# class PostColumn(BaseModel):
#     date: datetime
#     fund_label: str
#     fund_underlying: str
#     fund_key: str
#     fund_glidepath: str
#     glidepath: str
#     actual_weight: float
#     target_weight: float
#     valuation: float
#     year: float
#     month: float
#     glidepath_lookup_value: float
#     static_target_lookup_value: float
#     target_val: float
#     difference: float

# pre_columns: Dict[str, PreColumn] = {
#     'date': PreColumn,
#     'fund_label': PreColumn,
#     'fund_underlying': PreColumn,
#     'fund_key': PreColumn,
#     'fund_glidepath': PreColumn,
#     'valuation': PreColumn,
#     'actual_weight': PreColumn,
#     'target_weight': PreColumn,
# }

# post_columns: Dict[str, PostColumn] = {
#     'date': PostColumn,
#     'fund_label': PostColumn,
#     'fund_underlying': PostColumn,
#     'fund_key': PostColumn,
#     'fund_glidepath': PostColumn,
#     'glidepath': PostColumn,
#     'actual_weight': PostColumn,
#     'target_weight': PostColumn,
#     'valuation': PostColumn,
#     'year': PostColumn,
#     'month': PostColumn,
#     'glidepath_lookup_value': PostColumn,
#     'static_target_lookup_value': PostColumn,
#     'target_val': PostColumn,
#     'difference': PostColumn,
# }

# from pydantic import ValidationError

# def validate_pre_columns(data):
#     try:
#         for key, value in data.items():
#             PreColumn(**value)
#     except ValidationError as e:
#         raise ValueError(f"Validation error in pre_columns: {e}")

# def validate_post_columns(data):
#     try:
#         for key, value in data.items():
#             PostColumn(**value)
#     except ValidationError as e:
#         raise ValueError(f"Validation error in post_columns: {e}")