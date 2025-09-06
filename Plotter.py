import matplotlib.pyplot as plt
import yfinance as yf

class Plotter:
    @staticmethod
    def plot_strategy_returns(cumulative_strategy_returns, start_date):
        spy = yf.download('^GSPC', start=start_date,
                          end=cumulative_strategy_returns.index.max())

        # Calculate S&P 500 returns
        spy['daily_return'] = spy['Close'].pct_change()
        spy['cumulative_return'] = (1 + spy['daily_return']).cumprod()

        plt.figure(figsize=(12, 6))
        plt.plot(cumulative_strategy_returns.index, cumulative_strategy_returns['long'], label='Long Portfolio', color='blue')
        plt.plot(cumulative_strategy_returns.index, cumulative_strategy_returns['short'], label='Short Portfolio', color='red')
        plt.plot(cumulative_strategy_returns.index, cumulative_strategy_returns['long_short'], label='Long-Short Portfolio',
                 color='green')
        # Plot S&P 500 cumulative returns
        plt.plot(spy.index, spy['cumulative_return'], label='S&P 500', color='orange', linestyle='--')

        plt.title('Cumulative Returns of Long, Short, and Long-Short Portfolios vs Benchmark')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Return')
        plt.legend()
        plt.grid(True)

        plt.show()
        print(f"long cum. ret. is : {cumulative_strategy_returns['long'][-1]}")
        print(f"short cum. ret. is : {cumulative_strategy_returns['short'][-1]}")
        print(f"long-short cum. ret. is : {cumulative_strategy_returns['long_short'][-1]}")