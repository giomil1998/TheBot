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
        #TODO: Move to data
        # Create a dictionary mapping cusip to tic (ticker)
        self.cusip_to_tic = funda[['cusip', 'tic']].drop_duplicates().set_index('cusip')['tic']

    def run_strategy(self):
        for current_date in tqdm(self.all_dates, desc="Processing Trading Dates"):
            # Check if any new reports were released on this date
            lagged_date = current_date - pd.Timedelta(days=self.portfolio_update_delay)
            new_reports = self.funda[self.funda['datadate'] == lagged_date]

            if not new_reports.empty:
                # Update company_scores with new reports as of the lagged date
                self.portfolio_manager.update_company_scores(new_reports, lagged_date)

                # Build the portfolios as of the lagged date
                self.portfolio_manager.build_portfolios(current_date, self.long_portfolio_size,
                                                        self.short_portfolio_size)

                # Get current portfolios (long and short cusips)
                long_cusips, short_cusips = self.portfolio_manager.get_current_portfolios()

                # Map cusips to tickers using the cusip_to_tic dictionary
                long_tickers = self.cusip_to_tic.loc[long_cusips].dropna().tolist()
                short_tickers = self.cusip_to_tic.loc[short_cusips].dropna().tolist()

                # Store the portfolios for the current rebalancing date
                self.portfolio_tickers.loc[lagged_date] = [long_tickers, short_tickers]

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

        return self.strategy_returns

    def save_portfolio_tickers(self, file_path='portfolio_tickers.csv'):
        """Save the long and short portfolios (tickers) with rebalancing dates."""
        self.portfolio_tickers.to_csv(file_path)
