import pandas as pd
import sys

from glidepath import merged_glidepaths
import transformations as tr


#----------------------------- Read data ---------------------------------------#
sw_weekly = pd.read_excel('data/sw_weekly/sw_weekly_file.xlsx')

sw_reference = pd.read_csv('data/reference/sw_reference.csv')
sw_static_funds_targets = pd.read_csv('data/reference/sw_static_funds_targets.csv')

MIN_TEST = -0.03
MAX_TEST = 0.03

TEAMS_WEBHOOK_URL = "https://discord.com/api/webhooks/1249053406245425163/5ouTsWiWmP_v8aDsH0aujjjOA2OG7wdX56CyK389TuN92TTTirzMu0hyCjqXcJYotRCw"  # replace with your actual webhook URL

PROVIDER = 'SW'

""" columns_needed = ['date', 'fund_label', 'fund_underlying', 'fund_key', 'valuation', 'actual_weight', 'target_weight'] """

#----------------------------- Functions ---------------------------------------#

def process_data(weekly_file, reference):
    reference = sw_reference.drop_duplicates(subset='fund_key')
    reference_subset = reference[['fund_key', 'fund_glidepath']]
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
    date_input = '2024-05-31'

    weekly_file = weekly_file.assign(date=date_input)
    weekly_file['date'] = pd.to_datetime(weekly_file['date'])
    df = weekly_file.merge(reference_subset, on='fund_key', how='left')


    return df



#----------------------------- Main ---------------------------------------#


def main():
    all_glidepaths = merged_glidepaths()

    df = process_data(sw_weekly, sw_reference)
    df = tr.add_glidepath_data(df, PROVIDER)
    df = tr.add_lookup_values(df, all_glidepaths)
    df = tr.add_static_target_values(df, sw_static_funds_targets)
    df = tr.calculate_difference_final(df)

    out_of_range_df = tr.check_range(df, MIN_TEST, MAX_TEST)
    message = tr.print_message(df, out_of_range_df, PROVIDER)
    print(message)
    #tr.send_teams_message(TEAMS_WEBHOOK_URL,message)

    message_size = sys.getsizeof(message)
    print(f"Size of message: {message_size} bytes")

    df.to_csv('data/output/sw_output.csv', index=False)
    print(df.info())

if __name__ == "__main__":
    main()