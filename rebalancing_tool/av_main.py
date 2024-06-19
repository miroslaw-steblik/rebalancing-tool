import pandas as pd

from utils.glidepath import merged_glidepaths
import utils.pipeline as pl
import utils.validations as val
import utils.config as config

# set option to display all columns
pd.set_option('display.max_columns', None)
#----------------------------- Read data ---------------------------------------#
PROVIDER = 'Aviva'

av_weekly = 'rebalancing_tool/data/av_weekly'

av_reference = pd.read_csv('rebalancing_tool/data/reference/av_reference.csv')
av_static_funds_targets = pd.read_csv('rebalancing_tool/data/reference/av_static_funds_targets.csv')

av_output = 'rebalancing_tool/data/output/av_output.csv'

MIN_TEST = -0.03
MAX_TEST = 0.03

webhook_url = config.webhook_url

glidepath_lookback = 'YES'



#----------------------------- Main ---------------------------------------#

def process():
    # Load data
    all_glidepaths = merged_glidepaths()
    df = pl.load_and_preprocess_data(av_weekly, av_reference, pl.av_read_columns)
    #pl.check_columns

    # Validations
    val.validate_columns(df, pl.pre_columns)
    val.test_no_duplicates(df)  

    # Main processing and calculations
    df = pl.add_glidepath_data(df, glidepath_lookback)
    df = pl.add_lookup_values(df, all_glidepaths)
    df = pl.add_static_target_values(df, av_static_funds_targets)
    df = pl.calculate_difference_final(df)

    # Save output
    df.to_csv(av_output, index=False)

    # Post processing and validations
    validated_df, date_result = pl.check_range(df, MIN_TEST, MAX_TEST)
    validated_df = pl.post_transformations(validated_df)
    val.test_no_invalid_dates(validated_df)

    # Send to Teams
    pl.send_dataframe_to_teams(validated_df, webhook_url, provider=PROVIDER, date_result=date_result)

    print('Done!')


if __name__ == "__main__":
    process()