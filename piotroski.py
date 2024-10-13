import pandas as pd
import numpy as np
import wrds
import matplotlib.pyplot as plt
from tqdm import tqdm
import yfinance as yf
import csv
from dotenv import load_dotenv
import os


# Load environment variables from the .env file
load_dotenv()
wrds_username = os.getenv('WRDS_USERNAME')
wrds_password = os.getenv('WRDS_PASSWORD')

# db = wrds.Connection(wrds_username=wrds_username, wrds_password=wrds_password)

# Define the time period
start_date = '2013-01-01'
end_date = '2024-07-20'

# Fetch Piotroski F-scores and stock prices
# Assume Compustat data for Piotroski scores and CRSP data for stock prices

# Piotroski F-scores (Fundamental Data)
#lt - liabilities
#comp.funda name of compustat database

# funda_query = f"""
#     SELECT gvkey, datadate, fyear, fic, tic,
#            at, lt, pstkl, txditc, pstkrv,
#            txdb, pstk, ni, oancf, dltt,
#            dlc, ivst, che, re,
#            sale, cogs, xsga, xint, xrd, dp, cusip
#     FROM comp.funda
#     WHERE indfmt = 'INDL'
#     AND datafmt = 'STD'
#     AND popsrc = 'D'
#     AND consol = 'C'
#     AND datadate BETWEEN '{start_date}' AND '{end_date}'
# """
# funda = db.raw_sql(funda_query)
#
# # Stock Prices (CRSP Data)
# crsp_query = f"""
#     SELECT permno, date, ret, prc, shrout, cusip
#     FROM crsp.dsf
#     WHERE date BETWEEN '{start_date}' AND '{end_date}'
# """
# crsp = db.raw_sql(crsp_query)
#
# # # Disconnect from WRDS
# db.close()
#
# crsp.to_csv("crsp_data.csv")
# Process Fundamental Data to Calculate Piotroski Score
def calc_piotroski(df):
    df['roa'] = df['ni'] / df['at'].shift(1)
    df['cfo'] = df['oancf'] / df['at'].shift(1)
    df['delta_roa'] = df['roa'] - df['roa'].shift(1)
    df['accrual'] = df['cfo'] - df['roa']
    df['delta_leverage'] = df['dltt'] / df['at'].shift(1) - df['dltt'].shift(1) / df['at'].shift(2)
    df['delta_margin'] = (df['sale'] - df['cogs']) / df['sale'] - (df['sale'].shift(1) - df['cogs'].shift(1)) / df[
        'sale'].shift(1)
    df['delta_turn'] = df['sale'] / df['at'] - df['sale'].shift(1) / df['at'].shift(1)

    # Initialize the piotroski column with zeros
    df['Score'] = 0

    # Add 1 for each criterion satisfied
    df['Score'] += (df['roa'] > 0).astype(int)
    df['Score'] += (df['cfo'] > 0).astype(int)
    df['Score'] += (df['delta_roa'] > 0).astype(int)
    df['Score'] += (df['accrual'] > 0).astype(int)
    df['Score'] += (df['delta_leverage'] <= 0).astype(int)
    df['Score'] += (df['delta_margin'] > 0).astype(int)
    df['Score'] += (df['delta_turn'] > 0).astype(int)

    return df
# funda = funda.groupby('cusip').apply(calc_piotroski).reset_index(drop=True)
#funda.to_csv('funda.csv')
# The DeprecationWarning you mentioned relates to how groupby().apply() behaves. Pandas will soon exclude the grouping columns by default. You can fix this by either:
#	•	Adding include_group_columns=False in groupby(), or
#	•	Explicitly including the grouping columns after groupby.

#Read from saved csv file
funda = pd.read_csv('data/funda.csv')
crsp = pd.read_csv('data/crsp_data.csv')
# Debug: Check Piotroski scores calculation
print("Calculated Piotroski Scores Head:")
print(funda[['cusip', 'fyear', 'Score', 'tic']].head())

unique_dates = funda["datadate"].unique()
unique_dates = sorted(unique_dates)
print(len(unique_dates))
# Write to CSV
with open("data/dates_so_far.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(unique_dates)

# Read from CSV
with open('data/dates_so_far.csv', 'r') as file:
    reader = csv.reader(file)
    dates_so_far = next(reader)

print(dates_so_far)

def portfolio_sort(funda,dates_so_far, num_quantiles = 4):
    quantile_companies = {q: [] for q in range(1, num_quantiles + 1)}
    for date in dates_so_far:
        funda = funda.loc[funda['datadate'] == date] #Selects companies that published data on that date
        funda["quantile"] = pd.qcut(funda["Score"], q=num_quantiles, labels=False, duplicates="drop") + 1
        for q in range(1, num_quantiles + 1):
            gvkeys_in_quantile = funda.loc[funda['quantile'] == q, 'cusip'].tolist()
            # Append the gvkeys to the corresponding quantile in the dictionary
            quantile_companies[q].extend(gvkeys_in_quantile)
    return quantile_companies


def portfolio_nsort(funda, dates_so_far, n=15, max_size=20):
    quantile_companies = {}

    for date in dates_so_far:
        # Select companies that published data on that date
        funda_date = funda.loc[funda['datadate'] == date]

        # Initialize short and long lists for this date
        quantile_companies[date] = {'short': [], 'long': []}

        # Get the 15 companies with the smallest scores (short) and largest scores (long)
        short_companies = funda_date.nsmallest(n, 'Score')['cusip'].tolist()
        long_companies = funda_date.nlargest(n, 'Score')['cusip'].tolist()

        # Add only unique cusips to the 'short' group
        for cusip in short_companies:
            if cusip not in quantile_companies[date]['short']:
                quantile_companies[date]['short'].append(cusip)

        # Add only unique cusips to the 'long' group
        for cusip in long_companies:
            if cusip not in quantile_companies[date]['long']:
                quantile_companies[date]['long'].append(cusip)

        # Limit the number of companies in the short list to max_size (e.g., 20)
        if len(quantile_companies[date]['short']) > max_size:
            # Get the lowest scores again to keep only the best-performing companies
            short_cusips = funda.loc[funda['cusip'].isin(quantile_companies[date]['short'])].nsmallest(max_size, 'Score')['cusip'].tolist()
            quantile_companies[date]['short'] = short_cusips

        # Limit the number of companies in the long list to max_size (e.g., 20)
        if len(quantile_companies[date]['long']) > max_size:
            # Get the highest scores again to keep only the worst-performing companies
            long_cusips = funda.loc[funda['cusip'].isin(quantile_companies[date]['long'])].nlargest(max_size, 'Score')['cusip'].tolist()
            quantile_companies[date]['long'] = long_cusips

    return quantile_companies

quantile_companies = portfolio_nsort(funda, dates_so_far)
def calculate_strategy_returns(crsp, quantile_companies):
    # Initialize empty DataFrames to store cumulative returns
    strategy_returns = {'long': [], 'short': [], 'long_short': [], 'date': []}

    for date, companies in quantile_companies.items():
        print(date)
        print(companies)
        # Filter CRSP data for the current date and cusips in 'long' and 'short' lists
        crsp_date = crsp.loc[crsp['date'] == date]
        long_crsp = crsp_date.loc[crsp_date['cusip'].isin(companies['long'])]
        short_crsp = crsp_date.loc[crsp_date['cusip'].isin(companies['short'])]

        # Calculate the average return for long and short companies on this date
        long_return = long_crsp['ret'].mean() if not long_crsp.empty else 0
        short_return = short_crsp['ret'].mean() if not short_crsp.empty else 0

        # Calculate the long-short strategy return (long - short)
        long_short_return = long_return - short_return

        # Append the returns to the respective lists
        strategy_returns['date'].append(date)
        strategy_returns['long'].append(long_return)
        strategy_returns['short'].append(short_return)
        strategy_returns['long_short'].append(long_short_return)

    # Convert to DataFrame for easier manipulation
    strategy_returns_df = pd.DataFrame(strategy_returns)

    return strategy_returns_df


def plot_strategy_returns(strategy_returns_df):
    # Calculate cumulative returns
    strategy_returns_df['long_cum'] = (1 + strategy_returns_df['long']).cumprod() - 1
    strategy_returns_df['short_cum'] = (1 + strategy_returns_df['short']).cumprod() - 1
    strategy_returns_df['long_short_cum'] = (1 + strategy_returns_df['long_short']).cumprod() - 1

    # Plot cumulative returns
    plt.figure(figsize=(10, 6))
    plt.plot(strategy_returns_df['date'], strategy_returns_df['long_cum'], label='Long Strategy', color='blue')
    plt.plot(strategy_returns_df['date'], strategy_returns_df['short_cum'], label='Short Strategy', color='red')
    plt.plot(strategy_returns_df['date'], strategy_returns_df['long_short_cum'], label='Long-Short Strategy',
             color='green')

    plt.title('Cumulative Returns of Long, Short, and Long-Short Strategies')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Returns')
    plt.legend()
    plt.grid(True)
    plt.show()


# Calculate and plot returns
strategy_returns_df = calculate_strategy_returns(crsp, quantile_companies)
plot_strategy_returns(strategy_returns_df)


# piotroski_scores = funda[['cusip', 'datadate', 'Score']].drop_duplicates()
#
# # Filter out rows with missing years
# piotroski_scores = piotroski_scores[piotroski_scores[''].notna()]
#
# # Convert date to string and extract year for merging
# crsp['year'] = crsp['date'].astype(str).str[:4].astype(int)
#
# # Debug: Check for missing years
# print("Years in Piotroski Scores:")
# print(piotroski_scores['fyear'].unique())
#
# print("Years in CRSP Data:")
# print(crsp['year'].unique())
#
# # Ensure CUSIP is string type and clean
# crsp['cusip'] = crsp['cusip'].astype(str).str.strip()
# piotroski_scores['cusip'] = piotroski_scores['cusip'].astype(str).str.strip()
#
# # Ensure all CUSIPs are 8 characters long
# crsp['cusip'] = crsp['cusip'].str[:8]
# piotroski_scores['cusip'] = piotroski_scores['cusip'].str[:8]
#
# # Check the overlap in CUSIP between the two datasets
# common_cusips = set(crsp['cusip']).intersection(set(piotroski_scores['cusip']))
# print(f"Number of common CUSIPs: {len(common_cusips)}")
# print("Sample common CUSIPs:", list(common_cusips)[:5])
#
# # Merge Piotroski Scores with Stock Prices by CUSIP
# merged_data = pd.merge(crsp, piotroski_scores, how='left', left_on=['cusip', 'year'], right_on=['cusip', 'fyear'])
#
# # Debug: Check merged data for any NaNs
# print("Merged Data Head:")
# print(merged_data.head())
#
# print("Missing values in Merged Data:")
# print(merged_data.isna().sum())
#
# ### Here, some critical issues might arise:
#
# #	•	CUSIP Handling: Ensure that all CUSIPs are 8 characters long and correctly match between datasets. This is done correctly with the cleaning steps, but merging based on CUSIP and year can still lead to missing values if the data does not align perfectly.
# #	•	Missing Data: You handle missing Piotroski scores by filling them with the mean, which might distort the results. Instead, consider filtering out rows with missing values, or performing an imputation based on more contextually relevant statistics.
# # Handle any missing Piotroski scores
# merged_data['piotroski'] = merged_data['piotroski'].ffill()
#
# # Debug: Print merged data summary
# print("Merged Data Summary:")
# print(merged_data.describe())
#
# portfolio_returns = []
# portfolio_long = []
# portfolio_short = []
# dates = []
# # Function to calculate daily returns for the portfolio with progress bar
# def calculate_daily_portfolio_returns(df):
#     # Initialize lists to store daily returns
#     portfolio_returns = []
#     portfolio_long = []
#     portfolio_short = []
#     dates = []
#
#     # Precompute top and bottom quintiles for each year
#     df['quintile'] = df.groupby('year')['piotroski'].transform(lambda x: pd.qcut(x, 5, labels=False, duplicates='drop'))
#
#     # Precompute long and short portfolios for each year
#     long_portfolios = {}
#     short_portfolios = {}
#
#     for year in df['year'].unique():
#         yearly_data = df[df['year'] == year]
#         max_score =  yearly_data['piotroski'].max()
#         # Define long and short portfolios
#         # long_portfolios[year] = yearly_data[yearly_data['quintile'] == 4]['cusip'].unique().tolist()
#         long_portfolios[year] = yearly_data[yearly_data['piotroski'] == max_score]['cusip'].unique().tolist()
#         short_portfolios[year] = yearly_data[yearly_data['quintile'] == 0]['cusip'].unique().tolist()
#
#     # Print out the long and short portfolios for each year
#     print("Long and Short Portfolios by Year:")
#     for year in sorted(long_portfolios.keys()):
#         print(f"\nYear: {year}")
#         print(f"Long Portfolios: {long_portfolios[year]}")
#         print(f"Short Portfolios: {short_portfolios[year]}")
#
#     # Calculate daily returns based on precomputed portfolios
#     for date in tqdm(df['date'].unique(), desc="Processing Dates"):
#         daily_data = df[df['date'] == date]
#         year = pd.to_datetime(date).year
#
#         long_cusips = long_portfolios.get(year, [])
#         short_cusips = short_portfolios.get(year, [])
#
#         # Calculate mean returns for long and short positions
#         long_returns = daily_data[daily_data['cusip'].isin(long_cusips)]['ret'].mean()
#         short_returns = daily_data[daily_data['cusip'].isin(short_cusips)]['ret'].mean()
#
#         # Handle potential NaNs in returns
#         long_returns = 0 if np.isnan(long_returns) else long_returns
#         short_returns = 0 if np.isnan(short_returns) else short_returns
#
#         # Calculate the daily portfolio return
#         daily_portfolio_return = (1 + long_returns) - (1 + short_returns)
#
#         # Append daily returns to the lists
#         portfolio_long.append(1 + long_returns)
#         portfolio_short.append(1 + short_returns)
#         portfolio_returns.append(1 + daily_portfolio_return)
#         dates.append(date)
#
#     # Convert to cumulative returns (product instead of sum to avoid overflow)
#     portfolio_returns = np.cumprod(portfolio_returns)
#     portfolio_long = np.cumprod(portfolio_long)
#     portfolio_short = np.cumprod(portfolio_short)
#
#     return dates, portfolio_returns, portfolio_long, portfolio_short
#
# dates, portfolio_returns, portfolio_long, portfolio_short = calculate_daily_portfolio_returns(merged_data)
#
# # Plot the results
# plt.figure(figsize=(10, 6))
# plt.plot(dates, portfolio_returns, label='Portfolio Returns', color='blue')
# plt.plot(dates, portfolio_long, label='Long Positions', color='green')
# plt.plot(dates, portfolio_short, label='Short Positions', color='red')
#
# # Adding title and labels
# plt.title('Cumulative Log Returns of Long-Short Piotroski Strategy (Efficient Looping)')
# plt.xlabel('Date')
# plt.ylabel('Cumulative Log Return')
#
# # Adding grid and legend
# plt.grid(True)
# plt.legend()
#
# plt.savefig('portfolio_returns.png')
# plt.show()

