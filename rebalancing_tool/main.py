import pandas as pd
from glidepath import cash_glidepath, annuity_glidepath, drawdown_glidepath


aviva_weekly = pd.read_excel('data/aviva_weekly/aviva_weekly_file.xlsx')
sw_weekly = pd.read_excel('data/sw_weekly/sw_weekly_file.xlsx')

# process the data
print(aviva_weekly.head())
print(sw_weekly.head())

def check_tool(df, cash_glidepath, annuity_glidepath, drawdown_glidepath):
    for index, row in df.iterrows():
        # Assuming 'value' is the column to compare
        cash_value = cash_glidepath(row['MAG'])
        annuity_value = annuity_glidepath(row['MAG'])
        drawdown_value = drawdown_glidepath(row['MAG'])

        print(f"Row {index}:")
        print(f"Cash glidepath: {cash_value}")
        print(f"Annuity glidepath: {annuity_value}")
        print(f"Drawdown glidepath: {drawdown_value}")
        print()

# Use the check tool
check_tool(aviva_weekly, cash_glidepath, annuity_glidepath, drawdown_glidepath)