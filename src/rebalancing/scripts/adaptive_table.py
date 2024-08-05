from tabulate import tabulate

text_line1      = "Auto Generated Report"
text_line2      = "Weekly Rebalancing and Tolerance Range Monitoring Report for"
text_line3      = "All funds are within the tolerance range of +/- 3%"
text_line4      = "Please see below funds out of the tolerance range of +/- 3% with valuation above £ 1000"
text_line5      = "The dataset has been saved to the following location:"
text_insert     = "rebalancing_monitoring"
suffix          = ".csv"

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
        filename = f"{file_path}{text_insert}_{provider.lower()}_{date_result}{suffix}"
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
                            "text": text_line1,
                            "weight": "Bolder",
                            "size": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"{date_result} {text_line2} {provider}",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": text_line3,
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
                                "text": text_line1,
                                "weight": "Bolder",
                                "size": "Medium"
                            },
                            {
                                "type": "TextBlock",
                                "text": f"{date_result} {text_line2} {provider}",
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": f"Page {chunk_num} of {num_chunks}",
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": text_line4 ,
                                "wrap": True
                            },
                            table_df,
                            {
                                "type": "TextBlock",
                                "text": text_line5,
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": f"{file_path}{text_insert}_{provider.lower()}_{date_result}{suffix}",
                                "wrap": True
                            }
                        ]
   
                    }
                }
            ]
        }
        return payload
    
    