from tabulate import tabulate


class AdaptiveCardTableGenerator:
    @staticmethod
    def dataframe_to_adaptivecard_table(df):
        # Define the columns
        columns = [{"width": "stretch"} for _ in df.columns]

        # Create header row
        header_row = {
            "type": "TableRow",
            "cells": [
                {
                    "type": "TableCell",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": col,
                            "wrap": True,
                            "weight": "Bolder"
                        }
                    ]
                }
                for col in df.columns
            ]
        }

        # Create data rows
        data_rows = [
            {
                "type": "TableRow",
                "cells": [
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": str(val),
                                "wrap": True
                            }
                        ]
                    }
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

        # Print the table using tabulate
        print(tabulate(df, headers='keys', tablefmt='pretty'))

        return table

    def save_dataset_to_csv(self, df, provider, date_result, file_path):


        # Generate the filename with the provider and date
        filename = f"{file_path}_{provider}_{date_result}.csv"

        # Save the dataset to the CSV file
        df.to_csv(filename, index=False)

        return filename

    @staticmethod
    def empty_dataframe_payload(provider, date_result):
        # Create a different message for empty DataFrame
        payload = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.2",
                    "msteams": {
                        "width": "full"
                    },
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "Auto Generated Report",
                            "weight": "Bolder",
                            "size": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"{date_result} Rebalancing Monitoring Report for {provider}",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": "All funds are within the tolerance range of +/- 3%",
                            "wrap": True
                        }
                    ]
                }
            }]
        }
        return payload

    @staticmethod
    def full_dataframe_payload(df_chunk, provider, date_result, chunk_num, num_chunks,file_path):
        # Convert the chunk to an Adaptive Card table
        table_df = AdaptiveCardTableGenerator.dataframe_to_adaptivecard_table(df_chunk)


        # Create the Adaptive Card message
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.2",
                        "msteams": {
                            "width": "Full"
                        },
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": "Auto Generated Report",
                                "weight": "Bolder",
                                "size": "Medium"
                            },
                            {
                                "type": "TextBlock",
                                "text": f"{date_result} Rebalancing Monitoring Report for {provider}",
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": f"Page {chunk_num} of {num_chunks}",
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": "Please see below funds out of the tolerance range of +/- 3%",
                                "wrap": True
                            },
                            table_df,
                            {
                                "type": "TextBlock",
                                "text": "The dataset has been saved to the following location:",
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": f"{file_path}_{provider}_{date_result}.csv",
                                "wrap": True
                            }
                        ]
   
                    }
                }
            ]
        }
        return payload
    
    