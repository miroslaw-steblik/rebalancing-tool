import argparse
from models.pipeline import RebalancingPipeline
from scripts.glidepath import  get_glidepaths
import scripts.message as msg
from utils.common import timer
from utils.config import Config
from my_loggerr import CustomLoggerr



@timer
def run_pipeline(client_name):

    config = Config(client_name)
    logger = CustomLoggerr(config.client_name)

    try:
        all_glidepaths = get_glidepaths()

        pipeline = RebalancingPipeline(client_name)
        process_pipeline  = (
                pipeline
                .extract_data(config.provider_weekly_file, key_word=config.key_word)
                .standardise_columns()
                .add_glidepath_reference_file(config.provider_reference_file)
                .transform_data(config.in_percent, config.date_format)
                .custom_transform_data()
                .add_glidepath_data(config.add_extra_month)
                .add_lookup_values(all_glidepaths)
                .add_static_target_values(config.provider_static_funds_targets_file)
                .validate_static_targets(config.provider_static_funds_targets_file)
                .calculate_difference_final()
                .save_data(config.provider_output_file)
                .testing_tolerance_range(config.range_min, config.range_max)
                .apply_formatting()
        )
        final_dataframe = process_pipeline.get_dataframe()
        stored_date = pipeline.store_date()
        msg.send_dataframe_to_teams(final_dataframe, config.webhook_url, client_name, stored_date, config.output_table_file)
        #msg.test_file_naming(final_dataframe, client_name, stored_date, config.output_table_file)
        logger.success(f"Rebalancing process for {client_name} completed successfully")
    except Exception as e:
        logger.error(f"Rebalancing process for {client_name} failed")
        logger.error(e)
        raise e

if __name__ == '__main__':

    # Parse the client name from the command line
    # Example: python src/rebalancing/main.py --client ScottishWidows
    # .bat file example in /docs folder
    parser = argparse.ArgumentParser(description="Run data pipeline for specific client")
    parser.add_argument('--client', type=str, required=True, help='Client name to run the pipeline for')
    args = parser.parse_args()
    
    run_pipeline(args.client)
    


