import pandas as pd
from tqdm import tqdm

from PortfolioManager import PortfolioManager


class StrategyRunner:
    def __init__(self, funda, crsp, inactivity_threshold, long_portfolio_size, short_portfolio_size, start_date, end_date, portfolio_update_delay):
        self.funda = funda
        self.crsp = crsp
        self.inactivity_threshold = inactivity_threshold
        self.portfolio_update_delay = portfolio_update_delay
        self.long_portfolio_size = long_portfolio_size
        self.short_portfolio_size = short_portfolio_size
        self.portfolio_manager = PortfolioManager(inactivity_threshold, long_portfolio_size, short_portfolio_size)

        self.all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        self.trading_dates = crsp['date'].sort_values().unique()
        self.rebalancing_dates = funda['datadate'].sort_values().unique()
        # Initialize DataFrame to store cumulative returns
        self.strategy_returns = pd.DataFrame(index=self.trading_dates,
                                             columns=['long_return', 'short_return', 'long_short_return'])
        # Initialize DataFrame to store long and short cusips at each rebalancing date
        self.portfolio_tickers = pd.DataFrame(columns=['long_tickers', 'short_tickers'], index=self.rebalancing_dates)
        # Create a dictionary mapping cusip to ticker (tic)
        self.cusip_to_tic = funda[['cusip', 'tic']].drop_duplicates().set_index('cusip')['tic']

    def run_strategy(self, lookback_days=60, weight_method="inverse_volatility"):
        """
        Execute the strategy with optional efficient weight assignment.
        """
        for current_date in tqdm(self.all_dates, desc="Processing Trading Dates"):
            # Check if any new reports were released on this date
            lagged_date = current_date - pd.Timedelta(days=self.portfolio_update_delay)
            new_reports = self.funda[self.funda['datadate'] == lagged_date]

            if not new_reports.empty:
                # Update company_scores with new reports as of the lagged date
                self.portfolio_manager.update_company_scores(new_reports, lagged_date)

                # Build portfolios using specified weight method
                self.portfolio_manager.build_portfolios(
                    current_date=current_date,
                    long_portfolio_size=self.long_portfolio_size,
                    short_portfolio_size=self.short_portfolio_size,
                    crsp=self.crsp,
                    lookback_days=lookback_days,
                    method=weight_method
                )

                # Get current portfolios (long and short cusips with weights)
                long_portfolio, short_portfolio = self.portfolio_manager.get_current_portfolios()

                # Map cusips to tickers for long and short portfolios
                long_tickers = [self.cusip_to_tic[cusip] for cusip in long_portfolio.keys() if cusip in self.cusip_to_tic]
                short_tickers = [self.cusip_to_tic[cusip] for cusip in short_portfolio.keys() if cusip in self.cusip_to_tic]

                # Store portfolios for the current rebalancing date
                self.portfolio_tickers.loc[lagged_date] = [long_tickers, short_tickers]

            # Remove inactive holdings from portfolios
            self.portfolio_manager.remove_inactive_holdings_from_portfolios(current_date)

            # Calculate daily returns for the current portfolios
            self.calculate_daily_returns(current_date)

    def calculate_daily_returns(self, current_date):
        """
        Calculate daily returns for the current portfolios using weights.
        """
        crsp_today = self.crsp[self.crsp['date'] == current_date]

        # Get current portfolios with weights
        long_portfolio, short_portfolio = self.portfolio_manager.get_current_portfolios()

        # Calculate weighted returns for long positions
        long_returns = sum(
            long_portfolio[cusip] * crsp_today.loc[crsp_today['cusip'] == cusip, 'ret'].values[0]
            for cusip in long_portfolio if cusip in crsp_today['cusip'].values
        )

        # Calculate weighted returns for short positions
        short_returns = sum(
            short_portfolio[cusip] * crsp_today.loc[crsp_today['cusip'] == cusip, 'ret'].values[0]
            for cusip in short_portfolio if cusip in crsp_today['cusip'].values
        )

        # Update last traded date for companies that traded today
        traded_cusips = crsp_today['cusip'].unique()
        self.portfolio_manager.update_last_traded_date(traded_cusips, current_date)

        # Calculate net portfolio return (difference between long and short returns)
        long_short_return = long_returns - short_returns

        # Store returns
        self.strategy_returns.loc[current_date, 'long_return'] = long_returns + 1
        self.strategy_returns.loc[current_date, 'short_return'] = short_returns + 1
        self.strategy_returns.loc[current_date, 'long_short_return'] = long_short_return + 1

    def process_returns(self):
        """
        Calculate cumulative returns from daily returns.
        """
        # Fill missing values with 1
        self.strategy_returns = self.strategy_returns.fillna(1)

        # Calculate cumulative returns
        self.strategy_returns['long_cum'] = self.strategy_returns['long_return'].cumprod()
        self.strategy_returns['short_cum'] = self.strategy_returns['short_return'].cumprod()
        self.strategy_returns['long_short_cum'] = self.strategy_returns['long_short_return'].cumprod()

        return self.strategy_returns

    def save_portfolio_tickers(self, file_path='portfolio_tickers.csv'):
        """
        Save the long and short portfolios (tickers) with rebalancing dates.
        """
        self.portfolio_tickers.to_csv(file_path)