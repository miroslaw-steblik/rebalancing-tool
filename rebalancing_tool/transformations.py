import pandas as pd
import datetime
import numpy as np
from tabulate import tabulate





def add_glidepath_data(df, PROVIDER):
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
    add_value = 1 if PROVIDER == 'AV' else 0
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
    except Exception as e:
        raise Exception(str(e) + " - Please check.")

    return df


def add_static_target_values(df, static_funds_targets):
    lookup_dict = static_funds_targets.set_index('fund_key')['static_target'].to_dict()
    df['static_target_lookup_value'] = df['fund_key'].map(lookup_dict)
    return df


def calculate_difference_final(df):
    df['target_val'] = np.where(
        df['glidepath'] == 'other',
        (df['static_target_lookup_value']),
        (df['glidepath_lookup_value'])
    )
    # Check for negative values in the 'diff' column
    if (df['target_val'] < 0).any():
        raise ValueError("Negative value found in 'target_val' column")
    
    df['diff'] = df['actual_weight'] - df['target_val']
    df['diff'] = df['diff'].round(4)
    return df


def check_range(df, MIN_TEST, MAX_TEST):
    mask = (df['diff'] < MIN_TEST) | (df['diff'] > MAX_TEST)
    out_of_range_df = df[mask]
    return out_of_range_df


def post_transformations(df):
    df = df.drop(columns=[
        'fund_glidepath', 
        'glidepath',
        'valuation',
        'year',
        'month',
        'fund_key',
        'glidepath_lookup_value',
        'static_target_lookup_value'
        ])
    df['actual_weight'] = df['actual_weight'].apply(lambda x: '{:.1%}'.format(x))
    df['target_weight'] = df['target_weight'].apply(lambda x: '{:.1%}'.format(x))
    df['target_val'] = df['target_val'].apply(lambda x: '{:.1%}'.format(x))
    df['diff'] = df['diff'].apply(lambda x: '{:.1%}'.format(x))
    df = df.sort_values(by='diff')
    return df
    

def print_message(df, date, PROVIDER):

    date_str = date.strftime('%Y-%m-%d')
    message_df = pd.DataFrame({'Auto Generated Message': ['Rebalancing Monitoring Report for ' f"{PROVIDER}, "  + date_str]})
 
    if df.empty:
        empty_df = pd.DataFrame({'Auto Generated Message': ['-> All funds are within the tolerance range of +/- 3%']})
        empty_line = pd.DataFrame({'Auto Generated Message': [' ']})
        message_df = pd.concat([message_df, empty_df], ignore_index=True)
        message_df = pd.concat([message_df, empty_line], ignore_index=True)
    else:
        errors_df = pd.DataFrame({'Auto Generated Message': ['-> Please see below funds out of the tolerance range of +/- 3%']})
        empty_line = pd.DataFrame({'Auto Generated Message': [' ']})
        message_df = pd.concat([message_df, errors_df], ignore_index=True)
        message_df = pd.concat([message_df, empty_line], ignore_index=True)
    
    message = tabulate(message_df, headers='keys', tablefmt='simple', showindex=False)

    df = df.drop(columns=['date'])

    if not df.empty:
        message += "\n" + tabulate(df, headers='keys', tablefmt='simple', showindex=False)

    return message



