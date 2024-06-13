import pandas as pd
import datetime
import numpy as np
import json
import requests
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
    'diff': 'float64',
}

# ----------------------------- Functions ---------------------------------------#
def load_and_preprocess_data_sw(folder_path, reference):
    reference = reference.drop_duplicates(subset='fund_key')
    reference_subset = reference[['fund_key', 'fund_glidepath']]

    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):  # only process Excel files
            file_path = os.path.join(folder_path, filename)
            weekly_file = pd.read_excel(file_path)

            weekly_file = weekly_file.drop(columns=[
                'MCH Code',
                'Asset Pool',
                'Code',
                'Previous Val',
                'Current Price',
                'Previous Price',
                'Val % Mvmt',
                'Price % Mvmt'
            ])
            weekly_file = weekly_file.rename(columns={
                                        'Fund Name': 'fund_label',
                                        'Component Fund': 'fund_underlying',
                                        'SW Codes': 'fund_key',
                                        'Current Val': 'valuation',
                                        'Mix At Date%': 'actual_weight',
                                        'Allocation%': 'target_weight'
                                        })
            # date_input = input("Please enter a date (YYYY-MM-DD): ")
            date_input = '2024-05-31'  # replace with your date input

            weekly_file = weekly_file.assign(date=date_input)
            weekly_file['date'] = pd.to_datetime(weekly_file['date'])
            df = weekly_file.merge(reference_subset, on='fund_key', how='left')
            return df  


def load_and_preprocess_data_av(folder_path, reference):
    reference = reference.drop_duplicates(subset='fund_key')
    reference_subset = reference[['fund_key', 'fund_glidepath','fund_underlying']]

    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):  # only process Excel files
            file_path = os.path.join(folder_path, filename)
            weekly_file = pd.read_excel(file_path)


            weekly_file = weekly_file.drop(columns=['Fund Code','Asset Code'])
            weekly_file = weekly_file.rename(columns={
                                            'Date': 'date', 
                                            'Fund Name': 'fund_label',
                                            'Description': 'fund_key',
                                            'Holding Value': 'valuation',
                                            'Weighting': 'actual_weight',
                                            'Target Weight': 'target_weight'
                                            })
            weekly_file['date'] = pd.to_datetime(weekly_file['date'], dayfirst=True)
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
    # Check for negative values in the 'diff' column
    if (df['target_val'] < 0).any():
        raise ValueError("Validation Test: Negative value found in 'target_val' column")
    else:
        print('Validation Test: No negative values found in the column "target_val"')
        print('')
    
    df['diff'] = df['actual_weight'] - df['target_val']
    df['diff'] = df['diff'].round(4)
    return df


def check_range(df, MIN_TEST, MAX_TEST):
    mask = (df['diff'] < MIN_TEST) | (df['diff'] > MAX_TEST)
    out_of_range_df = df[mask]
    if out_of_range_df.empty:
        date = df['date'].iloc[0]  # Access the date from the first row of the original DataFrame
    else:
        date = out_of_range_df['date'].iloc[0]  # Access the date from the first row of the filtered DataFrame
    # Format the date as 'YYYY-MM-DD'
    date = date.strftime('%Y-%m-%d')
    return out_of_range_df, date


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
    print('Post-transformations completed successfully.')
    return df

 

# def print_message(df, date, PROVIDER):

#     date_str = date.strftime('%Y-%m-%d')
#     message_df = pd.DataFrame({'Auto Generated Message': ['Rebalancing Monitoring Report for ' f"{PROVIDER}, "  + date_str]})
 
#     if df.empty:
#         empty_df = pd.DataFrame({'Auto Generated Message': ['-> All funds are within the tolerance range of +/- 3%']})
#         empty_line = pd.DataFrame({'Auto Generated Message': [' ']})
#         message_df = pd.concat([message_df, empty_df], ignore_index=True)
#         message_df = pd.concat([message_df, empty_line], ignore_index=True)
#     else:
#         errors_df = pd.DataFrame({'Auto Generated Message': ['-> Please see below funds out of the tolerance range of +/- 3%']})
#         empty_line = pd.DataFrame({'Auto Generated Message': [' ']})
#         message_df = pd.concat([message_df, errors_df], ignore_index=True)
#         message_df = pd.concat([message_df, empty_line], ignore_index=True)
    
#     message = tabulate(message_df, headers='keys', tablefmt='simple', showindex=False)

#     df = df.drop(columns=['date'])

#     if not df.empty:
#         message += "\n" + tabulate(df, headers='keys', tablefmt='simple', showindex=False)

#     return message




def dataframe_to_adaptivecard_table(df):
    # Define the columns
    columns = [{"width": "stretch"} for _ in df.columns]

    # Create header row
    header_row = {
        "type": "TableRow",
        "cells": [
            {"type": "TableCell", "items": [{"type": "TextBlock", "text": col, "wrap": True, "weight": "Bolder"}]}
            for col in df.columns
        ]
    }
   
    # Create data rows
    data_rows = [
        {
            "type": "TableRow",
            "cells": [
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": str(val), "wrap": True}]}
                for val in row
            ]
        }
        for row in df.values
    ]

    # Create the table
    table = {
        "type": "Table",
        "columns": columns,
        "rows": [header_row] + data_rows
    }
    return table

 
def send_dataframe_to_teams(df, webhook_url, provider,date_result):
    # Store 'date' column in a variable
    #date = df['date'].iloc[0]

    # Drop 'date' column from DataFrame
    df = df.drop(columns=['date'])
    
    # Create the Adaptive Card message
    message = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.2",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "Auto Generated Report",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Rebalancing Monitoring Report for {provider}, {date_result}",
                        "wrap": True
                    },
                    dataframe_to_adaptivecard_table(df)
                ],
                "msteams": {
                    "width": "Full"
                }
            }
        }]
    }



    # Convert the message to a JSON string
    message_json = json.dumps(message)

    # Post the message to the webhook URL
    response = requests.post(webhook_url, data=message_json, headers={'Content-Type': 'application/json'})

    
    #Check the response
    if response.status_code == 200:
        print('Message posted successfully')
    else:
        print(f'Failed to post message: {response.status_code} - {response.text}')


