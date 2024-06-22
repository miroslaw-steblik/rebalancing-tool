import pandas as pd

from utils.glidepath import merged_glidepaths
import utils.pipeline as pl
import utils.validations as val
import utils.config as config
import utils.message as msg


#----------------------------- Config ---------------------------------------#
provider = config.provider2

provider_weekly = config.sw_weekly
provider_reference = pd.read_csv(config.sw_reference)
provider_static_funds_targets = pd.read_csv(config.sw_static_funds_targets)
provider_output = config.sw_output
# Teams webhook
webhook_url = config.webhook_url

# Rebalancing thresholds
MIN_TEST = -0.03
MAX_TEST = 0.03

glidepath_lookback = 'YES'

percent = 'YES'


#----------------------------- Main ---------------------------------------#


def process():
    # Load data
    all_glidepaths = merged_glidepaths()
    df = pl.load_and_preprocess_data(provider_weekly, provider_reference, pl.sw_read_columns, percent=percent)
    #pl.check_columns

    # Validations
    val.validate_columns(df, pl.pre_columns)
    val.test_no_duplicates(df)  

    # Main processing and calculations
    df = pl.add_glidepath_data(df, glidepath_lookback)
    df = pl.add_lookup_values(df, all_glidepaths)
    df = pl.add_static_target_values(df, provider_static_funds_targets)
    df = pl.calculate_difference_final(df)

    # Save output
    df.to_csv(provider_output, index=False)

    # Post processing and validations
    validated_df, date_result = pl.check_range(df, MIN_TEST, MAX_TEST)
    validated_df = pl.post_transformations(validated_df)
    #val.test_no_invalid_dates(validated_df)

    # Send to Teams
    msg.send_dataframe_to_teams(validated_df, webhook_url, provider=provider, date_result=date_result)

    print('Done!')


if __name__ == "__main__":
    process()
