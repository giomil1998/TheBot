from DataHandler import DataHandler
from Plotter import Plotter
from StrategyRunner import StrategyRunner

if __name__ == "__main__":
    START_DATE = '2013-01-01'
    END_DATE = '2023-12-29'
    PORTFOLIO_SIZE = 30  # Maximum number of positions in each portfolio
    INACTIVITY_THRESHOLD = 30  # Number of consecutive days without trading
    GET_NEW_DATA = False


    if GET_NEW_DATA:
        funda, crsp = DataHandler.fetch_and_save_new_data(START_DATE, END_DATE)
    else:
        funda, crsp = DataHandler.read_data('data/funda.csv', 'data/crsp.csv')

    funda = DataHandler.clean_funda(funda)
    crsp = DataHandler.clean_crsp(crsp)

    # Initialize and run the strategy
    strategy_runner = StrategyRunner(funda, crsp, INACTIVITY_THRESHOLD, PORTFOLIO_SIZE)
    strategy_runner.run_strategy()
    strategy_returns = strategy_runner.process_returns()

    Plotter.plot_strategy_returns(strategy_returns)