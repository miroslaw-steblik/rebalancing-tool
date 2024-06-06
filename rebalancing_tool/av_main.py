import pandas as pd
import datetime
import numpy as np
from glidepath import monthly_cash_glidepath, monthly_annuity_glidepath, adjusted_drawdown_monthly_glidepath, monthly_drawdown_glidepath


#----------------------------- Read data ---------------------------------------#
av_weekly = pd.read_excel('data/av_weekly/av_weekly_file.xlsx')

av_reference = pd.read_csv('data/reference/av_reference.csv')
av_static_funds_targets = pd.read_csv('data/reference/av_static_funds_targets.csv')


#----------------------------- Functions ---------------------------------------#

def load_and_preprocess_data(av_weekly, av_reference):
    av_reference = av_reference.drop_duplicates(subset='av_fund_name')

    merged_df = av_weekly.merge(av_reference, 
                                left_on='Description', 
                                right_on='av_fund_name', 
                                how='left')

    merged_df['Date'] = pd.to_datetime(merged_df['Date'], dayfirst=True)

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
    return merged_df

def merge_glidepaths():
    cash_glidepath_df = monthly_cash_glidepath()
    annuity_glidepath_df = monthly_annuity_glidepath()
    drawdown_glidepath_df = adjusted_drawdown_monthly_glidepath(monthly_drawdown_glidepath)
    all_glidepaths_df = cash_glidepath_df.merge(annuity_glidepath_df, on='month', how='left').merge(drawdown_glidepath_df, on='month', how='left')
    return all_glidepaths_df

def add_lookup_values(df, all_glidepaths):
    all_glidepaths.set_index('month', inplace=True)

    def glidepath_lookup_values(row):
        fund = row['underlying_fund']
        month = row['month']
        if pd.isnull(month):
            return np.nan
        elif fund in all_glidepaths.columns:
            return all_glidepaths.loc[month, fund]
        else:
            return np.nan

    df['glidepath_lookup_value'] = df.apply(glidepath_lookup_values, axis=1) / 100
    return df

def add_static_target_values(df, av_static_funds_targets):
    lookup_dict = av_static_funds_targets.set_index('av_fund_name')['static_target'].to_dict()
    df['static_target_lookup_value'] = df['Description'].map(lookup_dict)
    return df

def calculate_diff_test(df):
    df['diff_test'] = np.where(
        df['glidepath'] == 'other',
        (df['Weighting'] - df['static_target_lookup_value']),
        (df['Weighting'] - df['glidepath_lookup_value'])
    )
    df['diff_test'] = df['diff_test'].round(4)
    return df

def main():
    all_glidepaths = merge_glidepaths()
    merged_df = load_and_preprocess_data(av_weekly, av_reference)
    merged_df = add_lookup_values(merged_df, all_glidepaths)
    merged_df = add_static_target_values(merged_df, av_static_funds_targets)
    merged_df = calculate_diff_test(merged_df)
    print(merged_df.head(10))
    merged_df.to_csv('data/output/av_output.csv', index=False)

if __name__ == "__main__":
    main()