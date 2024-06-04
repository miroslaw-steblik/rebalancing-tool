import pandas as pd
import datetime
import calendar

# Set pandas display options
pd.set_option('display.max_rows', 100)


cash_glidepath = {
                'year':             [8,7,6,5,4,3,2,1,0], 
                'MAG':              [100,87.50,75.0,62.5,50.0,37.5,25.0,12.5,0],
                'DRF':              [0,12.50,25.0,37.5,50.0,37.5,25.0,12.5,0],
                'cash':             [0,0,0,0,0,25.0,50.0,75.0,100]
                }

annuity_glidepath = {
                'year':             [8,7,6,5,4,3,2,1,0], 
                'MAG':              [100,87.50,75.0,62.5,50.0,37.5,25.0,12.5,0],
                'pre_ret_fund':     [0,12.50,25.0,37.5,50.0,62.5,67.0,71.5,75.0],
                'cash':             [0,0,0,0,0,0,8.0,16.0,25.0]
                }

drawdown_glidepath = {
                'year':             [8,7,6,5,4,3,2,1,0], 
                'MAG':              [100,87.50,75.0,62.5,50.0,37.5,25.0,12.5,0],
                'DRF':              [0,12.50,25.0,37.5,50.0,62.5,67.0,71.5,75.0],
                'cash':             [0,0,0,0,0,0,8.0,16.0,25.0]
                }

current_year = datetime.datetime.today().year
print(current_year)

cash_glidepath_df = pd.DataFrame(cash_glidepath)
print(cash_glidepath_df)

cash_glidepath_df['total'] = cash_glidepath_df['MAG']+ cash_glidepath_df['DRF']  + cash_glidepath_df['cash']
print('Cash glidepath: ')
print(cash_glidepath_df)

annuity_glidepath_df = pd.DataFrame(annuity_glidepath)
print(annuity_glidepath_df)

annuity_glidepath_df['total'] = annuity_glidepath_df['MAG']+ annuity_glidepath_df['pre_ret_fund']  + annuity_glidepath_df['cash']
print('Annuity glidepath: ')
print( annuity_glidepath_df)

drawdown_glidepath_df = pd.DataFrame(drawdown_glidepath)
print(drawdown_glidepath_df)

drawdown_glidepath_df['total'] = drawdown_glidepath_df['MAG']+ drawdown_glidepath_df['DRF']  + drawdown_glidepath_df['cash']
print('Drawdown glidepath: ')
print(drawdown_glidepath_df)




def monthly_cash_glidepath():
    data = []
    for year in range(9):  # We stop at 9 to include month 96
        for month in range(12):
            if year == 8 and month > 0:  # We only want the first month of the 9th year
                break
            month_number = 96 - (year*12 + month)
            mag = cash_glidepath_df.iloc[year]["MAG"] - month * ((cash_glidepath_df.iloc[year]["MAG"] - cash_glidepath_df.iloc[min(year+1, 8)]["MAG"]) / 12)
            drf = cash_glidepath_df.iloc[year]["DRF"] - month * ((cash_glidepath_df.iloc[year]["DRF"] - cash_glidepath_df.iloc[min(year+1, 8)]["DRF"]) / 12)
            cash = cash_glidepath_df.iloc[year]["cash"] - month * ((cash_glidepath_df.iloc[year]["cash"] - cash_glidepath_df.iloc[min(year+1, 8)]["cash"]) / 12)
            total = cash_glidepath_df.iloc[year]["total"] - month * ((cash_glidepath_df.iloc[year]["total"] - cash_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
            data.append([month_number, mag, drf, cash, total])
    
    df = pd.DataFrame(data, columns=['Month', 'MAG', 'DRF', 'Cash', 'Total'])
    return df

monthly_cash_glidepath = monthly_cash_glidepath()
print(monthly_cash_glidepath.head(100))

def monthly_annuity_glidepath():
    data = []
    for year in range(9):  # We stop at 9 to include month 96
        for month in range(12):
            if year == 8 and month > 0:  # We only want the first month of the 9th year
                break
            month_number = 96 - (year*12 + month)
            mag = annuity_glidepath_df.iloc[year]["MAG"] - month * ((annuity_glidepath_df.iloc[year]["MAG"] - annuity_glidepath_df.iloc[min(year+1, 8)]["MAG"]) / 12)
            pre_ret_fund = annuity_glidepath_df.iloc[year]["pre_ret_fund"] - month * ((annuity_glidepath_df.iloc[year]["pre_ret_fund"] - annuity_glidepath_df.iloc[min(year+1, 8)]["pre_ret_fund"]) / 12)
            cash = annuity_glidepath_df.iloc[year]["cash"] - month * ((annuity_glidepath_df.iloc[year]["cash"] - annuity_glidepath_df.iloc[min(year+1, 8)]["cash"]) / 12)
            total = annuity_glidepath_df.iloc[year]["total"] - month * ((annuity_glidepath_df.iloc[year]["total"] - annuity_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
            data.append([month_number, mag, pre_ret_fund, cash, total])
    
    df = pd.DataFrame(data, columns=['Month', 'MAG', 'Pre_Ret_Fund', 'Cash', 'Total'])
    return df

monthly_annuity_glidepath = monthly_annuity_glidepath()
print(monthly_annuity_glidepath.head(100))

def monthly_drawdown_glidepath():
    data = []
    for year in range(9):  # We stop at 9 to include month 96
        for month in range(12):
            if year == 8 and month > 0:  # We only want the first month of the 9th year
                break
            month_number = 96 - (year*12 + month)
            mag = drawdown_glidepath_df.iloc[year]["MAG"] - month * ((drawdown_glidepath_df.iloc[year]["MAG"] - drawdown_glidepath_df.iloc[min(year+1, 8)]["MAG"]) / 12)
            drf = drawdown_glidepath_df.iloc[year]["DRF"] - month * ((drawdown_glidepath_df.iloc[year]["DRF"] - drawdown_glidepath_df.iloc[min(year+1, 8)]["DRF"]) / 12)
            cash = drawdown_glidepath_df.iloc[year]["cash"] - month * ((drawdown_glidepath_df.iloc[year]["cash"] - drawdown_glidepath_df.iloc[min(year+1, 8)]["cash"]) / 12)
            total = drawdown_glidepath_df.iloc[year]["total"] - month * ((drawdown_glidepath_df.iloc[year]["total"] - drawdown_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
            data.append([month_number, mag, drf, cash, total])
    
    df = pd.DataFrame(data, columns=['Month', 'MAG', 'DRF', 'Cash', 'Total'])
    return df

monthly_drawdown_glidepath = monthly_drawdown_glidepath()
print(monthly_drawdown_glidepath.head(100))



# def monthly_cash_glidepath():
#     data = []
#     current_year = datetime.datetime.today().year
#     for year in range(9):  # We stop at 9 to include month 96
#         for month in range(12):
#             if year == 8 and month > 0:  # We only want the first month of the 9th year
#                 break
#             month_number = (year*12 + month)
#             month_name = calendar.month_name[((month_number) % 12)+1 ]
#             year_value = current_year + 8 - year
#             mag = cash_glidepath_df.iloc[year]["MAG"] - month * ((cash_glidepath_df.iloc[year]["MAG"] - cash_glidepath_df.iloc[min(year+1, 8)]["MAG"]) / 12)
#             drf = cash_glidepath_df.iloc[year]["DRF"] - month * ((cash_glidepath_df.iloc[year]["DRF"] - cash_glidepath_df.iloc[min(year+1, 8)]["DRF"]) / 12)
#             cash = cash_glidepath_df.iloc[year]["cash"] - month * ((cash_glidepath_df.iloc[year]["cash"] - cash_glidepath_df.iloc[min(year+1, 8)]["cash"]) / 12)
#             total = cash_glidepath_df.iloc[year]["total"] - month * ((cash_glidepath_df.iloc[year]["total"] - cash_glidepath_df.iloc[min(year+1, 8)]["total"]) / 12)
#             data.append([month_number, month_name, year_value, mag, drf, cash, total])
    
#     df = pd.DataFrame(data, columns=['Month', 'Month_Name', 'Year', 'MAG', 'DRF', 'Cash', 'Total'])
#     return df

# df = monthly_cash_glidepath()
# print(df.head(100))

# def monthly_annuity_glidepath():
