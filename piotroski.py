import pandas as pd
from tqdm import tqdm

from DataHandler import DataHandler
from Plotter import Plotter



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



    # Initialize portfolios
    long_portfolio = {}  # {cusip: {'score': Score, 'entry_date': datadate, 'last_traded_date': date}}
    short_portfolio = {}
    portfolio_returns = []
    dates = []

    # Create a list of all trading dates
    trading_dates = crsp['date'].sort_values().unique()

    # Initialize DataFrame to store cumulative returns
    strategy_returns = pd.DataFrame(index=trading_dates, columns=['long_return', 'short_return', 'long_short_return'])

    # Initialize a DataFrame to keep track of current scores for all companies
    company_scores = pd.DataFrame(columns=['cusip', 'score', 'datadate'])
    company_scores.set_index('cusip', inplace=True)


    # Function to remove inactive holdings
    def remove_inactive_holdings(portfolio, current_date):
        to_remove = []
        for cusip, info in portfolio.items():
            days_inactive = (current_date - info['last_traded_date']).days
            if days_inactive > INACTIVITY_THRESHOLD:
                to_remove.append(cusip)
        for cusip in to_remove:
            del portfolio[cusip]


    # Process each trading date
    for current_date in tqdm(trading_dates, desc="Processing Trading Dates"):
        # Check if any new reports were released on this date
        new_reports = funda[funda['datadate'] == current_date]

        # Update company_scores with new reports
        if not new_reports.empty:
            for idx, row in new_reports.iterrows():
                cusip = row['cusip']
                score = row['Score']
                datadate = row['datadate']
                company_scores.loc[cusip] = {'score': score, 'datadate': datadate}

        # Build the portfolios based on current company_scores
        if not company_scores.empty:
            # Get the top 20 and bottom 20 companies by score
            sorted_scores = company_scores.sort_values(by=['score', 'datadate'], ascending=[False, True])
            top_companies = sorted_scores.head(PORTFOLIO_SIZE)
            bottom_companies = sorted_scores.tail(PORTFOLIO_SIZE)

            # Update or add companies to long portfolio
            for cusip in top_companies.index:
                score = top_companies.loc[cusip, 'score']
                datadate = top_companies.loc[cusip, 'datadate']
                if cusip not in long_portfolio:
                    long_portfolio[cusip] = {
                        'score': score,
                        'entry_date': current_date,
                        'last_traded_date': current_date  # Initialize with current date
                    }
                else:
                    long_portfolio[cusip]['score'] = score  # Update score if it has changed

            # Update or add companies to short portfolio
            for cusip in bottom_companies.index:
                score = bottom_companies.loc[cusip, 'score']
                datadate = bottom_companies.loc[cusip, 'datadate']
                if cusip not in short_portfolio:
                    short_portfolio[cusip] = {
                        'score': score,
                        'entry_date': current_date,
                        'last_traded_date': current_date  # Initialize with current date
                    }
                else:
                    short_portfolio[cusip]['score'] = score  # Update score if it has changed
        else:
            long_portfolio = {}
            short_portfolio = {}

        # Remove inactive holdings from portfolios
        remove_inactive_holdings(long_portfolio, current_date)
        remove_inactive_holdings(short_portfolio, current_date)

        # Calculate daily returns for current portfolios
        crsp_today = crsp[crsp['date'] == current_date]

        # Long positions
        long_cusips = list(long_portfolio.keys())
        long_returns = crsp_today[crsp_today['cusip'].isin(long_cusips)]['ret']
        avg_long_return = long_returns.mean() if not long_returns.empty else 0

        # Short positions
        short_cusips = list(short_portfolio.keys())
        short_returns = crsp_today[crsp_today['cusip'].isin(short_cusips)]['ret']
        avg_short_return = short_returns.mean() if not short_returns.empty else 0

        # Update last traded date for companies that traded today
        traded_cusips = crsp_today['cusip'].unique()

        for cusip in long_portfolio.keys():
            if cusip in traded_cusips:
                long_portfolio[cusip]['last_traded_date'] = current_date

        for cusip in short_portfolio.keys():
            if cusip in traded_cusips:
                short_portfolio[cusip]['last_traded_date'] = current_date

        # Calculate net portfolio return (sum of long and short profits)
        net_return = avg_long_return - avg_short_return

        # Store returns
        strategy_returns.loc[current_date, 'long_return'] = avg_long_return + 1
        strategy_returns.loc[current_date, 'short_return'] = avg_short_return + 1  # This is already profit from shorts
        strategy_returns.loc[current_date, 'long_short_return'] = net_return + 1

    # Fill any missing values with 1
    strategy_returns = strategy_returns.fillna(1)

    # Calculate cumulative returns
    strategy_returns['long_cum'] = (strategy_returns['long_return']).cumprod()
    strategy_returns['short_cum'] = (strategy_returns['short_return']).cumprod()
    strategy_returns['long_short_cum'] = (strategy_returns['long_short_return']).cumprod()

    # Convert cumulative returns to percentages
    strategy_returns['long_cum_pct'] = strategy_returns['long_cum'] * 100
    strategy_returns['short_cum_pct'] = strategy_returns['short_cum'] * 100
    strategy_returns['long_short_cum_pct'] = strategy_returns['long_short_cum'] * 100

    # Plot cumulative returns
    Plotter.plot_strategy_returns(strategy_returns)