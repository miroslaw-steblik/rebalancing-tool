import pandas as pd

from utils.glidepath import merged_glidepaths
import utils.pipeline as pl
import utils.validations as val
import utils.config as config


#----------------------------- Settings ---------------------------------------#
PROVIDER = 'SW'

sw_weekly = 'rebalancing_tool/data/sw_weekly'

sw_reference = pd.read_csv('rebalancing_tool/data/reference/sw_reference.csv')
sw_static_funds_targets = pd.read_csv('rebalancing_tool/data/reference/sw_static_funds_targets.csv')

sw_output = 'rebalancing_tool/data/output/sw_output.csv'

MIN_TEST = -0.03
MAX_TEST = 0.03

webhook_url = config.webhook_url

glidepath_lookback = 'YES'


#----------------------------- Main ---------------------------------------#


def process():
    all_glidepaths = merged_glidepaths()
    df = pl.load_and_preprocess_data_sw(sw_weekly, sw_reference)

    val.validate_columns(df, pl.pre_columns)
    val.test_no_duplicates(df)  

    df = pl.add_glidepath_data(df, glidepath_lookback)
    df = pl.add_lookup_values(df, all_glidepaths)
    df = pl.add_static_target_values(df, sw_static_funds_targets)
    df = pl.calculate_difference_final(df)

    df.to_csv(sw_output, index=False)

    validated_df, date_result = pl.check_range(df, MIN_TEST, MAX_TEST)
    validated_df = pl.post_transformations(validated_df)

    val.test_no_invalid_dates(validated_df)

    #pl.send_dataframe_to_teams(validated_df, webhook_url, provider=PROVIDER)

    print('Done!')


if __name__ == "__main__":
    process()
