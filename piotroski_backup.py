import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm

from DataHandler import DataHandler
from EnvironmentLoader import EnvironmentLoader
from WRDSConnection import WRDSConnection

START_DATE = '2013-01-01'
END_DATE = '2023-12-29'
PORTFOLIO_SIZE = 30  # Maximum number of positions in each portfolio
INACTIVITY_THRESHOLD = 30  # Number of consecutive days without trading
GET_NEW_DATA = False

if __name__ == "__main__":
    wrds_credentials = EnvironmentLoader.load_wrds_credentials()

    if GET_NEW_DATA:
        # Initialize WRDS connection and fetch data
        wrds_connection = WRDSConnection(wrds_credentials['wrds_username'], wrds_credentials['wrds_password'])
        funda = wrds_connection.fetch_fundamental_data(START_DATE, END_DATE)
        crsp = wrds_connection.fetch_crsp_data(START_DATE, END_DATE)
        wrds_connection.close()

        DataHandler.add_piotroski_column_to_funda(funda)

        funda.to_csv("data/funda.csv")
        crsp.to_csv("data/crsp.csv")
    else:
        #Read from saved csv file
        funda = pd.read_csv('data/funda.csv')
        funda = DataHandler.filter_duplicates(funda)
        funda = DataHandler.filter_missing_years(funda)
        # Ensure CUSIPs are strings and 8 character long
        funda['cusip'] = funda['cusip'].astype(str).str.strip()
        funda['cusip'] = funda['cusip'].str[:8]

        crsp = pd.read_csv('data/crsp.csv')
        # Ensure CUSIPs are strings and 8 character long
        crsp['cusip'] = crsp['cusip'].astype(str).str.strip()
        crsp['cusip'] = crsp['cusip'].str[:8]

piotroski_scores = funda[['cusip', 'datadate', 'Score']].drop_duplicates()

# Filter out rows with missing years
piotroski_scores = piotroski_scores[piotroski_scores[''].notna()]

# Convert date to string and extract year for merging
crsp['year'] = crsp['date'].astype(str).str[:4].astype(int)

# Debug: Check for missing years
print("Years in Piotroski Scores:")
print(piotroski_scores['fyear'].unique())

print("Years in CRSP Data:")
print(crsp['year'].unique())

# Ensure CUSIP is string type and clean
crsp['cusip'] = crsp['cusip'].astype(str).str.strip()
piotroski_scores['cusip'] = piotroski_scores['cusip'].astype(str).str.strip()

# Ensure all CUSIPs are 8 characters long
crsp['cusip'] = crsp['cusip'].str[:8]
piotroski_scores['cusip'] = piotroski_scores['cusip'].str[:8]

# Check the overlap in CUSIP between the two datasets
common_cusips = set(crsp['cusip']).intersection(set(piotroski_scores['cusip']))
print(f"Number of common CUSIPs: {len(common_cusips)}")
print("Sample common CUSIPs:", list(common_cusips)[:5])

# Merge Piotroski Scores with Stock Prices by CUSIP
merged_data = pd.merge(crsp, piotroski_scores, how='left', left_on=['cusip', 'year'], right_on=['cusip', 'fyear'])

# Debug: Check merged data for any NaNs
print("Merged Data Head:")
print(merged_data.head())

print("Missing values in Merged Data:")
print(merged_data.isna().sum())

### Here, some critical issues might arise:

#	•	CUSIP Handling: Ensure that all CUSIPs are 8 characters long and correctly match between datasets. This is done correctly with the cleaning steps, but merging based on CUSIP and year can still lead to missing values if the data does not align perfectly.
#	•	Missing Data: You handle missing Piotroski scores by filling them with the mean, which might distort the results. Instead, consider filtering out rows with missing values, or performing an imputation based on more contextually relevant statistics.
# Handle any missing Piotroski scores
merged_data['piotroski'] = merged_data['piotroski'].ffill()

# Debug: Print merged data summary
print("Merged Data Summary:")
print(merged_data.describe())

portfolio_returns = []
portfolio_long = []
portfolio_short = []
dates = []
# Function to calculate daily returns for the portfolio with progress bar
def calculate_daily_portfolio_returns(df):
    # Initialize lists to store daily returns
    portfolio_returns = []
    portfolio_long = []
    portfolio_short = []
    dates = []

    # Precompute top and bottom quintiles for each year
    df['quintile'] = df.groupby('year')['piotroski'].transform(lambda x: pd.qcut(x, 5, labels=False, duplicates='drop'))

    # Precompute long and short portfolios for each year
    long_portfolios = {}
    short_portfolios = {}

    for year in df['year'].unique():
        yearly_data = df[df['year'] == year]
        max_score =  yearly_data['piotroski'].max()
        # Define long and short portfolios
        # long_portfolios[year] = yearly_data[yearly_data['quintile'] == 4]['cusip'].unique().tolist()
        long_portfolios[year] = yearly_data[yearly_data['piotroski'] == max_score]['cusip'].unique().tolist()
        short_portfolios[year] = yearly_data[yearly_data['quintile'] == 0]['cusip'].unique().tolist()

    # Print out the long and short portfolios for each year
    print("Long and Short Portfolios by Year:")
    for year in sorted(long_portfolios.keys()):
        print(f"\nYear: {year}")
        print(f"Long Portfolios: {long_portfolios[year]}")
        print(f"Short Portfolios: {short_portfolios[year]}")

    # Calculate daily returns based on precomputed portfolios
    for date in tqdm(df['date'].unique(), desc="Processing Dates"):
        daily_data = df[df['date'] == date]
        year = pd.to_datetime(date).year

        long_cusips = long_portfolios.get(year, [])
        short_cusips = short_portfolios.get(year, [])

        # Calculate mean returns for long and short positions
        long_returns = daily_data[daily_data['cusip'].isin(long_cusips)]['ret'].mean()
        short_returns = daily_data[daily_data['cusip'].isin(short_cusips)]['ret'].mean()

        # Handle potential NaNs in returns
        long_returns = 0 if np.isnan(long_returns) else long_returns
        short_returns = 0 if np.isnan(short_returns) else short_returns

        # Calculate the daily portfolio return
        daily_portfolio_return = (1 + long_returns) - (1 + short_returns)

        # Append daily returns to the lists
        portfolio_long.append(1 + long_returns)
        portfolio_short.append(1 + short_returns)
        portfolio_returns.append(1 + daily_portfolio_return)
        dates.append(date)

    # Convert to cumulative returns (product instead of sum to avoid overflow)
    portfolio_returns = np.cumprod(portfolio_returns)
    portfolio_long = np.cumprod(portfolio_long)
    portfolio_short = np.cumprod(portfolio_short)

    return dates, portfolio_returns, portfolio_long, portfolio_short

dates, portfolio_returns, portfolio_long, portfolio_short = calculate_daily_portfolio_returns(merged_data)

# Plot the results
plt.figure(figsize=(10, 6))
plt.plot(dates, portfolio_returns, label='Portfolio Returns', color='blue')
plt.plot(dates, portfolio_long, label='Long Positions', color='green')
plt.plot(dates, portfolio_short, label='Short Positions', color='red')

# Adding title and labels
plt.title('Cumulative Log Returns of Long-Short Piotroski Strategy (Efficient Looping)')
plt.xlabel('Date')
plt.ylabel('Cumulative Log Return')

# Adding grid and legend
plt.grid(True)
plt.legend()

plt.savefig('portfolio_returns.png')
plt.show()