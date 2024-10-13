import pandas as pd

class PortfolioManager:
    def __init__(self, funda, crsp):
        self.funda = funda
        self.crsp = crsp

    def portfolio_nsort(self, dates_so_far, n=15, max_size=20):
        quantile_companies = {}

        for date in dates_so_far:
            funda_date = self.funda.loc[self.funda['datadate'] == date]
            quantile_companies[date] = {'short': [], 'long': []}

            # Get the 15 companies with the smallest scores (short) and largest scores (long)
            short_companies = funda_date.nsmallest(n, 'Score')['cusip'].tolist()
            long_companies = funda_date.nlargest(n, 'Score')['cusip'].tolist()

            quantile_companies[date]['short'].extend(short_companies)
            quantile_companies[date]['long'].extend(long_companies)

            quantile_companies[date]['short'] = list(set(quantile_companies[date]['short']))[:max_size]
            quantile_companies[date]['long'] = list(set(quantile_companies[date]['long']))[:max_size]

        return quantile_companies

    def calculate_strategy_returns(self, quantile_companies):
        strategy_returns = {'long': [], 'short': [], 'long_short': [], 'date': []}
        for date, companies in quantile_companies.items():
            crsp_date = self.crsp.loc[self.crsp['date'] == date]
            long_crsp = crsp_date.loc[crsp_date['cusip'].isin(companies['long'])]
            short_crsp = crsp_date.loc[crsp_date['cusip'].isin(companies['short'])]

            long_return = long_crsp['ret'].mean() if not long_crsp.empty else 0
            short_return = short_crsp['ret'].mean() if not short_crsp.empty else 0

            long_short_return = long_return - short_return
            strategy_returns['date'].append(date)
            strategy_returns['long'].append(long_return)
            strategy_returns['short'].append(short_return)
            strategy_returns['long_short'].append(long_short_return)

        return pd.DataFrame(strategy_returns)