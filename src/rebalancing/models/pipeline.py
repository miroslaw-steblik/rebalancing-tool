import pandas as pd
import datetime
import numpy as np
import os
from utils.common import timer
from utils.mapping import column_mapping 


# ----------------------------- SETTINGS ---------------------------------------#
#pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# ----------------------------- MAIN ---------------------------------------#
class RebalancingPipeline:
    def __init__(self, provider):
        self.provider = provider
        self.data = None
        self.column_mapping = column_mapping

        
    def extract_data(self, folder_path, key_word=None):
        # Get list of all files, and their full paths
        files = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith(".xls") or filename.endswith(".xlsx")]
        # Find the latest file
        if key_word is not None:
            files = [file for file in files if key_word in file]
        if not files:
            raise ValueError(f"No files found in '{folder_path}' with keyword '{key_word}'")
        latest_file = max(files, key=os.path.getmtime)

        column_names = self.column_mapping.get(self.provider, {}).values()
        self.data = pd.read_excel(latest_file, usecols=column_names)
        print(f"\nFile '{latest_file}' loaded successfully.")
        return self
    
    def standardise_columns(self):
        column_mapping = self.column_mapping.get(self.provider, {})
        reversed_mapping = {v: k for k, v in column_mapping.items()}
        self.data = self.data.rename(columns=reversed_mapping)
        print(f"\nColumns standardised successfully.")
        return self
            

    def add_glidepath_reference_file(self, glidepath_reference_file,):
        glidepath_reference_df = pd.read_csv(glidepath_reference_file)
        glidepath_reference_df = glidepath_reference_df.drop_duplicates(subset='fund_key')
        self.data = self.data.merge(glidepath_reference_df, on='fund_key', how='left')
        self.data['fund_glidepath'] = self.data['fund_glidepath'].replace(' ', np.nan)
        return self
    
    def transform_data(self, percent, date_format):
        if percent == 'YES':
            self.data['provider_actual_weight'] = pd.to_numeric(self.data['provider_actual_weight'], errors='coerce')
            self.data['provider_actual_weight'] = self.data['provider_actual_weight'] / 100
            self.data['provider_target_weight'] = pd.to_numeric(self.data['provider_target_weight'], errors='coerce')
            self.data['provider_target_weight'] = self.data['provider_target_weight'] / 100

        self.data['date'] = pd.to_datetime(self.data['date'],format=date_format, dayfirst=True)

        if self.data['valuation'].dtype == 'O': # if 'valuation' column is an object
            self.data['valuation'] = self.data['valuation'].str.replace(',', '').astype(float)

        print(f"\nData transformation completed successfully.")
        return self
    
    def custom_transform_data(self):
        funds_to_remove = ['-> CASH','CS5F -> ICS']
        pattern = '|'.join(funds_to_remove)
        if self.provider == 'Aviva':
            self.data['fund_underlying'] = self.data['fund_key']
            self.data = self.data[~self.data['fund_key'].str.contains(pattern)]     
            print(f"\nCustom data transformation completed successfully.")
            return self
        else:  
            return self

    def add_glidepath_data(self, add_extra_month):
        conditions = [
            self.data['fund_label'].str.contains('Target Cash'),
            self.data['fund_label'].str.contains('Trgt Cash'),
            self.data['fund_label'].str.contains('Trgt Annuity'),
            self.data['fund_label'].str.contains('Target Annuity'),
            self.data['fund_label'].str.contains('Target Drawdown'),
            self.data['fund_label'].str.contains('Trgt Drwdwn')
        ]
        values = [
            'cash_glidepath',
            'cash_glidepath',
            'annuity_glidepath',
            'annuity_glidepath',
            'drawdown_glidepath',
            'drawdown_glidepath']
        
        # add 'glidepath' column
        self.data['glidepath_type'] = np.select(conditions, values, default='other')

        # add year column
        self.data['year'] = self.data['fund_label'].str.extract('(\d{4})')

        # add month column
        current_year = datetime.datetime.today().year
        current_month_number = self.data['date'].dt.month
        self.data['year'] = self.data['year'].astype(float)
        extra_month = 1 if add_extra_month == 'YES' else 0
        self.data.loc[self.data['fund_glidepath'].notnull(), 'month'] = ((self.data['year'] - current_year) * 12 - current_month_number + extra_month).clip(lower=0)

        print(f"\nGlidepath data added successfully.")
        return self
    
    def add_lookup_values(self, all_glidepaths_df):
        self.data = self.data.merge(all_glidepaths_df, how='left', on=['month', 'fund_glidepath'])
        self.data['weight_glidepath'] = self.data['weight_glidepath'] / 100
        print(f'\nMerged with all_glidepaths_df: ')
        return self


    def add_static_target_values(self, provider_static_funds_targets_file):
        provider_static_funds_targets_file = pd.read_csv(provider_static_funds_targets_file)
        # Create a dictionary from the 'static_funds_targets' DataFrame
        lookup_dict = provider_static_funds_targets_file.set_index('fund_key')['static_target'].to_dict()
        self.data['static_target_lookup_value'] = self.data['fund_key'].map(lookup_dict)
        self.data['static_target_lookup_value'] = pd.to_numeric(self.data['static_target_lookup_value'], errors='coerce')
        return self
        
    def validate_static_targets(self, provider_static_funds_targets_file):   
        try:
            provider_static_funds_targets_file = pd.read_csv(provider_static_funds_targets_file)
            lookup_keys = provider_static_funds_targets_file['fund_key'].unique()
            fund_label_values = self.data.loc[self.data['glidepath_type'] == 'other', 'fund_key'].unique()
            missing_values =  set(fund_label_values) - set(lookup_keys)
            if missing_values:
                raise ValueError(f"\nMissing funds on static_targets_file: {missing_values}")
            else:
                print(f"\nValidation Test: All Static Targets values included")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{provider_static_funds_targets_file}' not found") from e
        except Exception as e:
            raise Exception(f"An error occurred: {e}") from e
        return self
    
    def calculate_difference_final(self):
        """ Calculate the difference between 'provider_actual_weight' and 'internal_target_weight' """
        self.data['internal_target_weight'] = np.where(
            self.data['glidepath_type'] == 'other',
            # if 'glidepath' is 'other', assign the value from 'static_target_lookup_value'
            (self.data['static_target_lookup_value']),
            # if 'glidepath' is not 'other', assign the value from 'glidepath_lookup_value'
            (self.data['weight_glidepath'])
        )
        # Check for negative values in the 'difference' column
        if (self.data['internal_target_weight'] < 0).any():
            raise ValueError("Validation Test: Negative value found in 'internal_target_weight' column")
        else:
            print(f'\nValidation Test: No negative values found in the column "internal_target_weight"')
        self.data['difference'] = self.data['provider_actual_weight'] - self.data['internal_target_weight']
        self.data['difference'] = self.data['difference'].round(4)
        return self
    
    def save_data(self, file_path):
        self.data.to_csv(file_path, index=False)
        return self
    
    def store_date(self):
        date_result = self.data['date'].iloc[0]
        date_result = date_result.strftime('%Y-%m-%d')
        return date_result

    
    def testing_tolerance_range(self, MIN_RANGE, MAX_RANGE):
        mask = (self.data['difference'] < MIN_RANGE) | (self.data['difference'] > MAX_RANGE)
        self.data = self.data[mask]
        print(f'\nTesting tolerance range completed successfully.')
        return self
        
    def apply_formatting(self):
        self.data = self.data.copy()
        #filter out values lower then 1000
        self.data = self.data[self.data['valuation'] > 1000]
        #formatting
        self.data['valuation']               = self.data['valuation'].apply(lambda x: f'{x:,.0f}')
        self.data['provider_actual_weight']  = self.data['provider_actual_weight'].apply(lambda x: f'{x * 100:.1f}%')
        self.data['provider_target_weight']  = self.data['provider_target_weight'].apply(lambda x: f'{x * 100:.1f}%')
        self.data['internal_target_weight']  = self.data['internal_target_weight'].apply(lambda x: f'{x * 100:.1f}%')
        self.data['difference']              = self.data['difference'].apply(lambda x: f'{x * 100:.1f}%')

        order_columns = [
            'date', 'fund_label', 'fund_underlying', 'valuation', 'provider_actual_weight', 'provider_target_weight', 'internal_target_weight', 'difference'
            ]
        self.data = self.data[order_columns]
        self.data = self.data.sort_values(by='difference')

        print(f'\nFormatting completed successfully.')
        print(self.data)
        return self
    
    def get_dataframe(self):
        return self.data
    
    

