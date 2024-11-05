from DataHandler import DataHandler
from Plotter import Plotter
from StrategyRunner import StrategyRunner

if __name__ == "__main__":
    START_DATE = '2015-01-01'
    END_DATE = '2024-07-31'
    PORTFOLIO_SIZE_LONG = 30  # Maximum number of positions in long portfolio
    PORTFOLIO_SIZE_SHORT = 15  #Maximum number of positions in short portfolio
    INACTIVITY_THRESHOLD = 30  # Number of consecutive days without trading
    PORTFOLIO_UPDATE_DELAY = 45  # Number of days before the data in a report is used to update portfolios
    GET_NEW_DATA = False

    if GET_NEW_DATA:
        funda, crsp = DataHandler.fetch_and_save_new_data(START_DATE, END_DATE)
    else:
        funda, crsp = DataHandler.read_data('data/funda.csv', 'data/crsp.csv')

    funda = DataHandler.clean_funda(funda, START_DATE, END_DATE)
    crsp = DataHandler.clean_crsp(crsp, START_DATE, END_DATE)

    # Initialize and run the strategy
    strategy_runner = StrategyRunner(funda, crsp, INACTIVITY_THRESHOLD, PORTFOLIO_SIZE_LONG, PORTFOLIO_SIZE_SHORT, START_DATE, END_DATE, PORTFOLIO_UPDATE_DELAY)
    strategy_runner.run_strategy()
    strategy_runner.save_portfolio_tickers('long_short_portfolios.csv')
    strategy_returns = strategy_runner.process_returns()

    Plotter.plot_strategy_returns(strategy_returns)
    print("done")
