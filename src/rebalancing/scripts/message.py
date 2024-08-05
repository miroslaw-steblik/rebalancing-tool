
import json
import math
import requests

from scripts.adaptive_table import AdaptiveCardTableGenerator

# Create an instance of the AdaptiveCardTableGenerator class
generator = AdaptiveCardTableGenerator()


def send_dataframe_to_teams(df, webhook_url, provider, date_result, file_path):
    # Check if DataFrame is empty
    if df.empty:
        # Create a different message for empty DataFrame
        empty_payload = generator.empty_dataframe_payload(provider, date_result)

        # Convert the message to a JSON string
        message_json = json.dumps(empty_payload)
        # Post the message to the webhook URL
        response = requests.post(webhook_url, data=message_json, headers={'Content-Type': 'application/json'})

        # Check the response
        if response.status_code == 200:
            print(f'Message posted: {response.text}')
            print('')
        else:
            print(f'Failed to post message: {response.status_code} - {response.text}')
    else:
        # Define the maximum number of rows per message
        max_rows_per_message = 10

        generator.save_dataset_to_csv(df, provider, date_result, file_path)

        #drop 'date' column 
        df = df.drop(columns=['date'])

        # Split the DataFrame into chunks
        num_chunks = math.ceil(len(df) / max_rows_per_message)
        print(f"Splitting the DataFrame into {num_chunks} chunks")

        # Split the DataFrame into chunks
        for i in range(num_chunks):
            # Get the start and end index of the chunk
            start = i * max_rows_per_message
            end = (i + 1) * max_rows_per_message
            df_chunk = df.iloc[start:end]
            print(f"Processing chunk {i+1} with {len(df_chunk)} rows")

            # Create the Adaptive Card message for the chunk
            full_payload = generator.full_dataframe_payload(df_chunk, provider, date_result, i+1, num_chunks, file_path)

            # Convert the message to a JSON string
            message_json = json.dumps(full_payload)
            # Post the message to the webhook URL
            response = requests.post(webhook_url, data=message_json, headers={'Content-Type': 'application/json'})

            # Check the response
            if response.status_code == 200:
                print(f'Message posted: {response.text}')
                print('')
            else:
                print(f'Failed to post message: {response.status_code} - {response.text}')

def test_file_naming(df, provider, date_result, file_path):
    # Test the file naming function
    generator.save_dataset_to_csv(df, provider, date_result, file_path) 
    print(f"File saved as {file_path}")


