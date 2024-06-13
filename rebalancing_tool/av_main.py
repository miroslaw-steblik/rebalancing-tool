import pandas as pd

from utils.glidepath import merged_glidepaths
import utils.pipeline as pl
import utils.validations as val
import utils.config as config

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
PROVIDER = 'AV'
av_weekly = ('rebalancing_tool/data/av_weekly')

av_reference = pd.read_csv('rebalancing_tool/data/reference/av_reference.csv')
av_static_funds_targets = pd.read_csv('rebalancing_tool/data/reference/av_static_funds_targets.csv')

av_output = 'rebalancing_tool/data/output/av_output.csv'

MIN_TEST = -0.03
MAX_TEST = 0.03

webhook_url = config.webhook_url

glidepath_lookback = 'YES'



#----------------------------- Main ---------------------------------------#

def process():
    all_glidepaths = merged_glidepaths()
    df = pl.load_and_preprocess_data_av(av_weekly, av_reference)

    val.validate_columns(df, pre_columns)
    val.test_no_duplicates(df)  

    df = pl.add_glidepath_data(df, glidepath_lookback)
    df = pl.add_lookup_values(df, all_glidepaths)
    df = pl.add_static_target_values(df, av_static_funds_targets)
    df = pl.calculate_difference_final(df)
    
    df.to_csv(av_output, index=False)
    print(df.info())

    validated_df, date_result = pl.check_range(df, MIN_TEST, MAX_TEST)
    validated_df = pl.post_transformations(validated_df)
    print(date_result)

    test = val.test_no_invalid_dates(validated_df)

    #pl.send_dataframe_to_teams(validated_df, webhook_url, provider=PROVIDER)

    print('Done!')


if __name__ == "__main__":
    process()