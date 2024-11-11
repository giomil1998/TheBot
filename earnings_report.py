import pandas as pd
from finance_calendars import finance_calendars as fc
from datetime import datetime, timedelta
import yfinance as yf
import matplotlib.pyplot as plt

# Sample earnings data from the finance calendar (8x8 DataFrame)
def fetch_earnings_dates_by_dataframe(sample_date):
    earnings = fc.get_earnings_by_date(sample_date)
    return earnings  # This returns the earnings DataFrame for the specified date

# Fetch historical price data for given tickers
def fetch_price_data(tickers, start_date, end_date):
    price_data = {}
    for ticker in tickers:
        try:
            price_data[ticker] = yf.download(ticker, start=start_date, end=end_date)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    return price_data

# Calculate 20-day returns prior to the earnings date
def calculate_20d_returns(price_data, earnings_df):
    returns_data = []
    for index, row in earnings_df.iterrows():
        ticker = row['symbol']
        report_date = row['time']  # Use the report date from the DataFrame
        if ticker in price_data:
            pre_earnings_data = price_data[ticker][price_data[ticker].index < report_date].iloc[-20:]
            if not pre_earnings_data.empty:
                return_20d = (pre_earnings_data['Close'].iloc[-1] - pre_earnings_data['Close'].iloc[0]) / pre_earnings_data['Close'].iloc[0]
                returns_data.append((ticker, report_date, return_20d))
    return pd.DataFrame(returns_data, columns=['Ticker', 'EarningsDate', '20dReturn'])

# Adjusted strategy: Long/Short based on 20-day returns
def run_strategy_adjusted(sample_date):
    # Fetch earnings data for the specific date
    earnings_df = fetch_earnings_dates_by_dataframe(sample_date)

    # Fetch historical price data for the tickers in earnings_df
    tickers = earnings_df['symbol'].unique()
    start_date = sample_date - timedelta(days=40)
    end_date = sample_date + timedelta(days=10)
    price_data = fetch_price_data(tickers, start_date, end_date)

    # Calculate 20-day returns prior to earnings dates
    returns_df = calculate_20d_returns(price_data, earnings_df)

    # Create long and short portfolios based on returns
    long_portfolio = returns_df[returns_df['20dReturn'] > 0.05]
    short_portfolio = returns_df[returns_df['20dReturn'] < -0.05]

    # Calculate portfolio returns
    def calculate_portfolio_returns(portfolio, price_data):
        daily_returns = []
        for _, row in portfolio.iterrows():
            ticker = row['Ticker']
            report_date = row['EarningsDate']
            post_earnings_data = price_data[ticker][price_data[ticker].index > report_date]
            if not post_earnings_data.empty:
                daily_return = post_earnings_data['Close'].pct_change().dropna()
                daily_returns.append(daily_return)
        if daily_returns:
            return pd.concat(daily_returns, axis=1).mean(axis=1)
        else:
            return pd.Series(dtype=float)

    long_returns = calculate_portfolio_returns(long_portfolio, price_data)
    short_returns = calculate_portfolio_returns(short_portfolio, price_data)
    long_short_returns = long_returns - short_returns

    # Plot results
    long_cum = (1 + long_returns).cumprod()
    short_cum = (1 + short_returns).cumprod()
    long_short_cum = (1 + long_short_returns).cumprod()

    plt.figure(figsize=(12, 6))
    plt.plot(long_cum, label='Long Portfolio', color='blue')
    plt.plot(short_cum, label='Short Portfolio', color='red')
    plt.plot(long_short_cum, label='Long-Short Portfolio', color='green')
    plt.title(f'Strategy Cumulative Returns for {sample_date.date()}')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Return')
    plt.legend()
    plt.grid(True)
    plt.show()

# Example Usage
sample_date = datetime(2022, 1, 5)
run_strategy_adjusted(sample_date)