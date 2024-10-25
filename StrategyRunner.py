import pandas as pd
from tqdm import tqdm

from PortfolioManager import PortfolioManager


class StrategyRunner:
    def __init__(self, funda, crsp, inactivity_threshold, portfolio_size):
        self.funda = funda
        self.crsp = crsp
        self.inactivity_threshold = inactivity_threshold
        self.portfolio_size = portfolio_size
        self.portfolio_manager = PortfolioManager(inactivity_threshold, portfolio_size)

        # Create a list of all trading dates
        self.trading_dates = crsp['date'].sort_values().unique()

        # Initialize DataFrame to store cumulative returns
        self.strategy_returns = pd.DataFrame(index=self.trading_dates, columns=['long_return', 'short_return', 'long_short_return'])

    def run_strategy(self):
        for current_date in tqdm(self.trading_dates, desc="Processing Trading Dates"):
            # Check if any new reports were released on this date
            new_reports = self.funda[self.funda['datadate'] == current_date]

            # Update company_scores with new reports
            if not new_reports.empty:
                self.portfolio_manager.update_company_scores(new_reports)

            # Build the portfolios based on current company_scores
            self.portfolio_manager.build_portfolios(current_date)

            # Remove inactive holdings from portfolios
            self.portfolio_manager.remove_inactive_holdings_from_portfolios(current_date)

            # Calculate daily returns for current portfolios
            self.calculate_daily_returns(current_date)

    def calculate_daily_returns(self, current_date):
        crsp_today = self.crsp[self.crsp['date'] == current_date]

        # Get current portfolios
        long_cusips, short_cusips = self.portfolio_manager.get_current_portfolios()

        # Long positions
        long_returns = crsp_today[crsp_today['cusip'].isin(long_cusips)]['ret']
        avg_long_return = long_returns.mean() if not long_returns.empty else 0

        # Short positions
        short_returns = crsp_today[crsp_today['cusip'].isin(short_cusips)]['ret']
        avg_short_return = short_returns.mean() if not short_returns.empty else 0

        # Update last traded date for companies that traded today
        traded_cusips = crsp_today['cusip'].unique()
        self.portfolio_manager.update_last_traded_date(traded_cusips, current_date)

        # Calculate net portfolio return (difference between long and short returns)
        avg_long_short_return = avg_long_return - avg_short_return

        # Store returns
        self.strategy_returns.loc[current_date, 'long_return'] = avg_long_return + 1
        self.strategy_returns.loc[current_date, 'short_return'] = avg_short_return + 1
        self.strategy_returns.loc[current_date, 'long_short_return'] = avg_long_short_return + 1

    def process_returns(self):
        # Fill any missing values with 1
        self.strategy_returns = self.strategy_returns.fillna(1)

        # Calculate cumulative returns
        self.strategy_returns['long_cum'] = self.strategy_returns['long_return'].cumprod()
        self.strategy_returns['short_cum'] = self.strategy_returns['short_return'].cumprod()
        self.strategy_returns['long_short_cum'] = self.strategy_returns['long_short_return'].cumprod()

        # Convert cumulative returns to percentages
        self.strategy_returns['long_cum_pct'] = self.strategy_returns['long_cum'] * 100
        self.strategy_returns['short_cum_pct'] = self.strategy_returns['short_cum'] * 100
        self.strategy_returns['long_short_cum_pct'] = self.strategy_returns['long_short_cum'] * 100

        return self.strategy_returns