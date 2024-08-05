import pandas as pd
import datetime


glidepaths_output = "G:\\Eworking\\MWS\\New Investments\\2. Operations & Implementation\\Platforms\\Rebalancing-Monitoring\\config\\reference\\glidepaths.csv"

# funds
fund_1 = 'MAG'
fund_2 = 'DRF'
fund_3 = 'cash'
fund_4 = 'pre_ret_fund'

cash_glidepath = {
                'year':             [    8,    7,    6,    5,    4,    3,    2,    1,     0], 
                fund_1:             [100.0, 87.5, 75.0, 62.5, 50.0, 37.5, 25.0, 12.5,   0.0],
                fund_2:             [  0.0, 12.5, 25.0, 37.5, 50.0, 37.5, 25.0, 12.5,   0.0],
                fund_3:             [  0.0,  0.0,  0.0,  0.0,  0.0, 25.0, 50.0, 75.0, 100.0]
                }

annuity_glidepath = {
                'year':             [    8,    7,    6,    5,    4,    3,    2,    1,    0], 
                fund_1:             [100.0, 87.5, 75.0, 62.5, 50.0, 37.5, 25.0, 12.5,  0.0],
                fund_4:             [  0.0, 12.5, 25.0, 37.5, 50.0, 62.5, 67.0, 71.5, 75.0],
                fund_3:             [  0.0,  0.0,  0.0,  0.0,  0.0,  0.0,  8.0, 16.0, 25.0]
                }

drawdown_glidepath = {
                'year':             [    8,    7,    6,    5,    4,    3,    2,    1,    0], 
                fund_1:             [100.0, 87.5, 75.0, 62.5, 50.0, 37.5, 25.0, 12.5,  0.0],
                fund_2:             [  0.0, 12.5, 25.0, 37.5, 50.0, 62.5, 75.0, 82.5, 75.0],
                fund_3:             [  0.0,  0.0,  0.0,  0.0,  0.0,  0.0,  0.0,  5.0, 25.0]
                }

adjusted_drawdown_glidepath = {
                'month':                      [    12,    11,    10,     9,     8,     7,     6,     5,     4,     3,     2,     1,     0 ],
                'fund1_drawdown_glidepath':   [ 12.50, 11.46, 10.42,  9.38,  8.33,  7.29,  6.25,  5.21,  4.17,  3.13,  2.08,  1.04,  0.00 ],
                'fund2_drawdown_glidepath':   [ 82.50, 71.79, 72.08, 72.37, 72.67, 72.96, 73.25, 73.54, 73.83, 74.12, 74.42, 74.71, 75.00 ],
                'fund3_drawdown_glidepath':   [  5.00, 16.75, 17.50, 18.25, 19.00, 19.75, 20.50, 21.25, 22.00, 22.75, 23.50, 24.25, 25.00 ]
                }

current_year = datetime.datetime.today().year

cash_glidepath_df = pd.DataFrame(cash_glidepath)
cash_glidepath_df['total'] = cash_glidepath_df[fund_1]+ cash_glidepath_df[fund_2]  + cash_glidepath_df[fund_3]

annuity_glidepath_df = pd.DataFrame(annuity_glidepath)
annuity_glidepath_df['total'] = annuity_glidepath_df[fund_1]+ annuity_glidepath_df[fund_4]  + annuity_glidepath_df[fund_3]

drawdown_glidepath_df = pd.DataFrame(drawdown_glidepath)
drawdown_glidepath_df['total'] = drawdown_glidepath_df[fund_1]+ drawdown_glidepath_df[fund_2]  + drawdown_glidepath_df[fund_3]


def monthly_cash_glidepath():
    data = []
    for year in range(9):  # We stop at 9 to include month 96
        for month in range(12):
            if year == 8 and month > 0:  # We only want the first month of the 9th year
                break
            month_number = 96 - (year*12 + month)
            mag = cash_glidepath_df.iloc[year][fund_1] - month * ((cash_glidepath_df.iloc[year][fund_1] - cash_glidepath_df.iloc[min(year+1, 8)][fund_1]) / 12)
            drf = cash_glidepath_df.iloc[year][fund_2] - month * ((cash_glidepath_df.iloc[year][fund_2] - cash_glidepath_df.iloc[min(year+1, 8)][fund_2]) / 12)
            cash = cash_glidepath_df.iloc[year][fund_3] - month * ((cash_glidepath_df.iloc[year][fund_3] - cash_glidepath_df.iloc[min(year+1, 8)][fund_3]) / 12)
            total = cash_glidepath_df.iloc[year]["total"] - month * ((cash_glidepath_df.iloc[year]["total"] - cash_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
            data.append([month_number, mag, drf, cash, total])
    
    df = pd.DataFrame(data, columns=['month', 'fund1_cash_glidepath', 'fund2_cash_glidepath', 'fund3_cash_glidepath', 'total_cash_glidepath'])
    return df


def monthly_annuity_glidepath():
    data = []
    for year in range(9):  # We stop at 9 to include month 96
        for month in range(12):
            if year == 8 and month > 0:  # We only want the first month of the 9th year
                break
            month_number = 96 - (year*12 + month)
            mag = annuity_glidepath_df.iloc[year][fund_1] - month * ((annuity_glidepath_df.iloc[year][fund_1] - annuity_glidepath_df.iloc[min(year+1, 8)][fund_1]) / 12)
            pre_ret_fund = annuity_glidepath_df.iloc[year][fund_4] - month * ((annuity_glidepath_df.iloc[year][fund_4] - annuity_glidepath_df.iloc[min(year+1, 8)][fund_4]) / 12)
            cash = annuity_glidepath_df.iloc[year][fund_3] - month * ((annuity_glidepath_df.iloc[year][fund_3] - annuity_glidepath_df.iloc[min(year+1, 8)][fund_3]) / 12)
            total = annuity_glidepath_df.iloc[year]["total"] - month * ((annuity_glidepath_df.iloc[year]["total"] - annuity_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
            data.append([month_number, mag, pre_ret_fund, cash, total])
    
    df = pd.DataFrame(data, columns=['month', 'fund1_annuity_glidepath', 'fund4_annuity_glidepath', 'fund3_annuity_glidepath', 'total_annuity_glidepath'])
    return df


def monthly_drawdown_glidepath():
    data = []
    for year in range(9):  # We stop at 9 to include month 96
        for month in range(12):
            if year == 8 and month > 0:  # We only want the first month of the 9th year
                break
            month_number = 96 - (year*12 + month)
            mag = drawdown_glidepath_df.iloc[year][fund_1] - month * ((drawdown_glidepath_df.iloc[year][fund_1] - drawdown_glidepath_df.iloc[min(year+1, 8)][fund_1]) / 12)
            drf = drawdown_glidepath_df.iloc[year][fund_2] - month * ((drawdown_glidepath_df.iloc[year][fund_2] - drawdown_glidepath_df.iloc[min(year+1, 8)][fund_2]) / 12)
            cash = drawdown_glidepath_df.iloc[year][fund_3] - month * ((drawdown_glidepath_df.iloc[year][fund_3] - drawdown_glidepath_df.iloc[min(year+1, 8)][fund_3]) / 12)
            total = drawdown_glidepath_df.iloc[year]["total"] - month * ((drawdown_glidepath_df.iloc[year]["total"] - drawdown_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
            data.append([month_number, mag, drf, cash, total])
    
    df = pd.DataFrame(data, columns=['month', 'fund1_drawdown_glidepath', 'fund2_drawdown_glidepath', 'fund3_drawdown_glidepath', 'total_drawdown_glidepath'])
    return df



def adjusted_drawdown_monthly_glidepath(monthly_drawdown_glidepath_func):
    monthly_drawdown_glidepath_df = monthly_drawdown_glidepath_func()
    adjusted_drawdown_glidepath_df = pd.DataFrame(adjusted_drawdown_glidepath)

    # Set 'month' as the index in both DataFrames
    monthly_drawdown_glidepath_df.set_index('month', inplace=True)
    adjusted_drawdown_glidepath_df.set_index('month', inplace=True)

    # Update the rows in monthly_drawdown_glidepath_df with the corresponding rows in adjusted_drawdown_glidepath_df
    monthly_drawdown_glidepath_df.update(adjusted_drawdown_glidepath_df)

    monthly_drawdown_glidepath_df.reset_index(inplace=True)
    return monthly_drawdown_glidepath_df


def merged_glidepaths():
    cash_glidepath_df = monthly_cash_glidepath()
    annuity_glidepath_df = monthly_annuity_glidepath()
    drawdown_glidepath_df = adjusted_drawdown_monthly_glidepath(monthly_drawdown_glidepath)
    all_glidepaths_df = cash_glidepath_df.merge(annuity_glidepath_df, on='month', how='left').merge(drawdown_glidepath_df, on='month', how='left')
    return all_glidepaths_df


def melt_glidepaths():
    all_glidepaths_df = merged_glidepaths()
    melted_glidepaths_df = pd.melt(all_glidepaths_df, id_vars=['month'], var_name='fund_glidepath', value_name='weight_glidepath')
    return melted_glidepaths_df

def get_glidepaths():
    all_glidepaths_df = melt_glidepaths()
    all_glidepaths_df.to_csv(glidepaths_output, index=False)
    return all_glidepaths_df



# merged_glidepaths()
# melt_glidepaths()

