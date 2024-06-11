import pandas as pd
import sys
import os


from utils.glidepath import merged_glidepaths
import utils.transformations as tr
import utils.validations as val
import utils.message as msg

#----------------------------- Pipeline ---------------------------------------#
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

#----------------------------- Read data ---------------------------------------#
sw_weekly = 'data/sw_weekly'

sw_reference = pd.read_csv('data/reference/sw_reference.csv')
sw_static_funds_targets = pd.read_csv('data/reference/sw_static_funds_targets.csv')

MIN_TEST = -0.03
MAX_TEST = 0.03

TEAMS_WEBHOOK_URL = "https://discord.com/api/webhooks/1249053406245425163/5ouTsWiWmP_v8aDsH0aujjjOA2OG7wdX56CyK389TuN92TTTirzMu0hyCjqXcJYotRCw"  # replace with your actual webhook URL
PROVIDER = 'SW'
GLIDEPATH_LOOKBACK = 'YES'


#----------------------------- Process ---------------------------------------#



def process_data(folder_path, reference):
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

            return df  # yield the processed DataFrame



#----------------------------- Main ---------------------------------------#


def process():
    all_glidepaths = merged_glidepaths()
    df = process_data(sw_weekly, sw_reference)
    date = df['date'].iloc[0]

    test = val.validate_columns(df, pre_columns)
    test = val.test_no_duplicates(df)  

    df = tr.add_glidepath_data(df, GLIDEPATH_LOOKBACK)
    df = tr.add_lookup_values(df, all_glidepaths)
    df = tr.add_static_target_values(df, sw_static_funds_targets)
    df = tr.calculate_difference_final(df)

    df.to_csv('data/output/sw_output.csv', index=False)
    print(df.info())

    validated_df = tr.check_range(df, MIN_TEST, MAX_TEST)
    validated_df = tr.post_transformations(validated_df)

    #test = val.check_no_negative_value(validated_df, 'target_val')
    test = val.test_no_invalid_dates(validated_df, 'date')

    message = tr.print_message(validated_df, date, PROVIDER)
    print(message)
    #msg.send_teams_message(TEAMS_WEBHOOK_URL,message)
    msg.send_message_to_teams(message, TEAMS_WEBHOOK_URL)

    message_size = sys.getsizeof(message)
    print(f"Size of message: {message_size} bytes")





if __name__ == "__main__":
    process()
