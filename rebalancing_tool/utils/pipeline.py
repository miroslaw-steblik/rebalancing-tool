import pandas as pd
import datetime
import numpy as np
import os


#----------------------------- Processed Columns ---------------------------------------#
pre_columns = {
    'date': 'datetime64[ns]',
    'fund_label': 'object',
    'fund_underlying': 'object',
    'fund_key': 'object',
    'fund_glidepath': 'object',
    'valuation': 'float64',
    'actual_weight': 'float64',
    'target_weight': 'float64',
}
post_columns = {
    'date': 'datetime64[ns]',
    'fund_label': 'object',
    'fund_underlying': 'object',
    'fund_key': 'object',
    'fund_glidepath': 'object',
    'glidepath': 'object',
    'actual_weight': 'float64',
    'target_weight': 'float64',
    'valuation': 'float64',
    'year': 'float64',
    'month': 'float64',
    'glidepath_lookup_value': 'float64',
    'static_target_lookup_value': 'float64',
    'target_val': 'float64',
    'difference': 'float64',
}



sw_read_columns = {
                'Fund Name': 'fund_label',
                'SW_Code': 'fund_key',
                'Current Val': 'valuation',
                'Mix At Date %': 'actual_weight',
                'Allocation %': 'target_weight',
                'Date': 'date'
                }

av_read_columns = {
                'Date': 'date',
                'Fund Name': 'fund_label',
                'Description': 'fund_key',
                'Holding Value': 'valuation',
                'Weighting': 'actual_weight',
                'Target Weight': 'target_weight'
                }

# ----------------------------- Functions ---------------------------------------#

def load_and_preprocess_data(folder_path, reference, read_columns, percent):
    reference = reference.drop_duplicates(subset='fund_key')
    reference_subset = reference[['fund_key', 'fund_glidepath','fund_underlying']]

    # Get list of all files, and their full paths
    files = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith(".xls") or filename.endswith(".xlsx")]
    # Find the latest file
    latest_file = max(files, key=os.path.getmtime)

    # Load the latest file
    try:
        weekly_file = pd.read_excel(latest_file, usecols=read_columns.keys())
        print(f"File '{latest_file}' loaded successfully.")
    except Exception as e:
        raise ValueError(f"Error loading file '{latest_file}': {e}")

    # Rename columns
    weekly_file = weekly_file.rename(columns=read_columns)

    if percent == 'YES':
        weekly_file['actual_weight'] = weekly_file['actual_weight'] / 100
        weekly_file['target_weight'] = weekly_file['target_weight'] / 100

    # Convert 'date' column to datetime
    weekly_file['date'] = pd.to_datetime(weekly_file['date'], dayfirst=True)

    if weekly_file['valuation'].dtype == 'O': # if 'valuation' column is an object
        weekly_file['valuation'] = weekly_file['valuation'].str.replace(',', '').astype(float)

    df = weekly_file.merge(reference_subset, on='fund_key', how='left')
    return df  



def add_glidepath_data(df, glidepath_lookback):
    # add 'fund_glidepath' column
    conditions = [
        df['fund_label'].str.contains('Target Cash'),
        df['fund_label'].str.contains('Trgt Cash'),
        df['fund_label'].str.contains('Trgt Annuity'),
        df['fund_label'].str.contains('Target Annuity'),
        df['fund_label'].str.contains('Target Drawdown'),
        df['fund_label'].str.contains('Trgt Drwdwn')
    ]
    values = [
        'cash_glidepath', 
        'cash_glidepath', 
        'annuity_glidepath', 
        'annuity_glidepath', 
        'drawdown_glidepath', 
        'drawdown_glidepath']
    df['glidepath'] = np.select(conditions, values, default='other')

    # add year column
    df['year'] = df['fund_label'].str.extract('(\d{4})')

    # add month column
    current_year = datetime.datetime.today().year 
    current_month_number = df['date'].dt.month   
    df['year'] = df['year'].astype(float)
    add_value = 1 if glidepath_lookback == 'YES' else 0
    df.loc[df['year'].notnull(), 'month'] = ((df['year']-current_year ) * 12 - current_month_number + add_value).clip(lower=0)  # original statement considers values from 1 month ago
    return df


def add_lookup_values(df, all_glidepaths):
    all_glidepaths.set_index('month', inplace=True)

    def glidepath_lookup_values(row):
        fund = row['fund_glidepath']
        month = row['month']
        if pd.isnull(month):
            return np.nan
        elif fund in all_glidepaths.columns:
            return all_glidepaths.loc[month, fund]
        else:
            return np.nan
    try:
        df['glidepath_lookup_value'] = df.apply(glidepath_lookup_values, axis=1) / 100
        assert df['glidepath_lookup_value'].count() == df['fund_glidepath'].count(), "Count mismatch between 'glidepath_lookup_value' and 'fund_glidepath'"
        print("Lookup values added successfully and count test is passed.")
        print('')
    except Exception as e:
        raise Exception(str(e) + " - Please check.")

    return df


def add_static_target_values(df, static_funds_targets):
    lookup_dict = static_funds_targets.set_index('fund_key')['static_target'].to_dict()
    df['static_target_lookup_value'] = df['fund_key'].map(lookup_dict)
    df['static_target_lookup_value'] = pd.to_numeric(df['static_target_lookup_value'], errors='coerce')
    return df


def calculate_difference_final(df):
    df['target_val'] = np.where(
        df['glidepath'] == 'other',
        (df['static_target_lookup_value']),
        (df['glidepath_lookup_value'])
    )
    # Check for negative values in the 'difference' column
    if (df['target_val'] < 0).any():
        raise ValueError("Validation Test: Negative value found in 'target_val' column")
    else:
        print('Validation Test: No negative values found in the column "target_val"')
        print('')
    
    df['difference'] = df['actual_weight'] - df['target_val']
    df['difference'] = df['difference'].round(4)
    return df


def check_range(df, MIN_TEST, MAX_TEST):
    mask = (df['difference'] < MIN_TEST) | (df['difference'] > MAX_TEST)
    out_of_range_df = df[mask]
    if out_of_range_df.empty:
        date = df['date'].iloc[0]  # Access the date from the first row of the original DataFrame
    else:
        date = out_of_range_df['date'].iloc[0]  # Access the date from the first row of the filtered DataFrame
    # Format the date as 'YYYY-MM-DD'
    date = date.strftime('%Y-%m-%d')
    return out_of_range_df, date


def post_transformations(df):
    order_columns = [
        'date', 
        'fund_label',
        'fund_underlying', 
        'actual_weight', 
        'target_weight', 
        'target_val', 
        'difference']
    
    # using .loc[] to apply the formatting to the DataFrame to avoid SettingWithCopyWarning
    df.loc[:, 'actual_weight'] = df['actual_weight'].apply(lambda x: '{:.1%}'.format(x))
    df.loc[:, 'target_weight'] = df['target_weight'].apply(lambda x: '{:.1%}'.format(x))
    df.loc[:, 'target_val'] = df['target_val'].apply(lambda x: '{:.1%}'.format(x))
    df.loc[:, 'difference'] = df['difference'].apply(lambda x: '{:.1%}'.format(x))

    df = df[order_columns]
    df = df.sort_values(by='difference')

    print('Post-transformations completed successfully.')
    print('')

    return df

 