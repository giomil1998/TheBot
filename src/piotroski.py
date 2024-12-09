from DataHandler import DataHandler
from Plotter import Plotter
from StrategyRunner import StrategyRunner

if __name__ == "__main__":
    START_DATE = '2015-01-01'
    END_DATE = '2024-11-01'
    LONG_PORTFOLIO_SIZE = 20  # Maximum number of positions in long portfolio
    SHORT_PORTFOLIO_SIZE = 10  # Maximum number of positions in short portfolio
    INACTIVITY_THRESHOLD = 30  # Number of consecutive days below market cap threshold
    MARKET_CAP_THRESHOLD = 2_000_000_000  # Minimum market cap in dollars
    PORTFOLIO_UPDATE_DELAY = 60  # Number of days before the data in a report is used to update portfolios
    GET_NEW_DATA = False

    funda, crsp = DataHandler.fetch_or_read_data(GET_NEW_DATA, START_DATE, END_DATE)

    funda = DataHandler.clean_funda(funda, START_DATE, END_DATE, MARKET_CAP_THRESHOLD)

    crsp = DataHandler.clean_crsp(crsp, START_DATE, END_DATE)

    strategy_runner = StrategyRunner(funda, crsp, INACTIVITY_THRESHOLD, LONG_PORTFOLIO_SIZE, SHORT_PORTFOLIO_SIZE, START_DATE, END_DATE, PORTFOLIO_UPDATE_DELAY)
    strategy_runner.run_strategy()
    strategy_runner.save_portfolios_to_csv()
    cumulative_strategy_returns = strategy_runner.process_returns()

    Plotter.plot_strategy_returns(cumulative_strategy_returns)
    print("done")