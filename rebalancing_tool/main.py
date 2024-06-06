import pandas as pd
import datetime
import numpy as np
from glidepath import monthly_annuity_glidepath, monthly_cash_glidepath, monthly_drawdown_glidepath, all_glidepaths

# Set pandas display options
pd.set_option('display.max_rows', 800)
pd.set_option('display.max_columns', 8)

#TODO:
#rename columns
#test if all the columns are being read in

# Read in the weekly files
av_weekly = pd.read_excel('data/av_weekly/av_weekly_file.xlsx')
sw_weekly = pd.read_excel('data/sw_weekly/sw_weekly_file.xlsx')

# Read in the reference file
av_reference = pd.read_csv('data/reference/av_reference.csv')

av_reference = av_reference.drop_duplicates(subset='av_fund_name')

# Merge
merged_df = av_weekly.merge(av_reference, 
                            left_on='Description', 
                            right_on='av_fund_name', 
                            how='left')

merged_df['Date'] = pd.to_datetime(merged_df['Date'])

# add glidepath column
conditions = [
    merged_df['Fund Name'].str.contains('Mercer Target Cash'),
    merged_df['Fund Name'].str.contains('Mercer Target Annuity'),
    merged_df['Fund Name'].str.contains('Mercer Target Drawdown')
]
values = ['cash_glidepath', 'annuity_glidepath', 'drawdown_glidepath']
merged_df['glidepath'] = np.select(conditions, values, default='other')

# add year column
merged_df['year'] = merged_df['Fund Name'].str.extract('(\d{4})')

# add month column
current_year = datetime.datetime.today().year 
current_month_number = merged_df['Date'].dt.month   

merged_df['year'] = merged_df['year'].astype(float)
merged_df.loc[merged_df['year'].notnull(), 'month'] = (merged_df['year']-current_year ) * 12 - current_month_number+1  # original statement considers values from 1 month ago

print(merged_df.head(200))

# Set 'month' as index in monthly_annuity_glidepath
monthly_annuity_glidepath.set_index('month', inplace=True)

# Define a function to look up values in monthly_annuity_glidepath
def lookup_values(row):
    fund = row['underlying_fund']
    month = row['month']
    if pd.isnull(month):
        return np.nan
    elif fund in monthly_annuity_glidepath.columns:
        return monthly_annuity_glidepath.loc[month, fund]
    else:
        return np.nan

# Add new column 'lookup_value' to merged_df by applying the function
merged_df['lookup_value'] = merged_df.apply(lookup_values, axis=1) / 100

print(merged_df.head(200))

merged_df.to_csv('data/output/av_output.csv', index=False)

#TODO
#add the other glidepaths
# current LOOKUP NOT WORKING, it includes all glidepaths, but set to annuity, references has to be changed
#possible solution to merge all glidepaths into one dataframe and then lookup from there
#change reference on 'underlying_fund' in av_reference to match the glidepath columns