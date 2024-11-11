from DataHandler import DataHandler
from Plotter import Plotter
from StrategyRunner import StrategyRunner

if __name__ == "__main__":
    START_DATE = '2020-01-01'
    END_DATE = '2024-11-01'
    long_portfolio_size = 20  # Maximum number of positions in long portfolio
    short_portfolio_size = 10  # Maximum number of positions in short portfolio
    INACTIVITY_THRESHOLD = 30  # Number of consecutive days below market cap threshold
    MARKET_CAP_THRESHOLD = 2_000_000_000  # Minimum market cap in dollars
    PORTFOLIO_UPDATE_DELAY = 1  # Number of days before the data in a report is used to update portfolios
    GET_NEW_DATA = True

    # Fetch or read data
    if GET_NEW_DATA:
        funda, crsp = DataHandler.fetch_and_save_new_data(START_DATE, END_DATE)
    else:
        funda, crsp = DataHandler.read_data('data/funda.csv', 'data/crsp.csv')

    # Clean fundamental data
    funda = DataHandler.clean_funda(funda, START_DATE, END_DATE)

    # Clean CRSP data with market cap filter
    crsp = DataHandler.clean_crsp(
        crsp,
        START_DATE,
        END_DATE,
        market_cap_threshold=MARKET_CAP_THRESHOLD,
        inactivity_threshold=INACTIVITY_THRESHOLD
    )

    # Initialize and run the strategy
    strategy_runner = StrategyRunner(
        funda=funda,
        crsp=crsp,
        inactivity_threshold=INACTIVITY_THRESHOLD,
        long_portfolio_size=long_portfolio_size,
        short_portfolio_size=short_portfolio_size,
        start_date=START_DATE,
        end_date=END_DATE,
        portfolio_update_delay=PORTFOLIO_UPDATE_DELAY
    )

    strategy_runner.run_strategy()
    strategy_runner.save_portfolio_tickers('long_short_portfolios.csv')
    strategy_returns = strategy_runner.process_returns()

    # Plot the strategy returns
    Plotter.plot_strategy_returns(strategy_returns)
    print("done")