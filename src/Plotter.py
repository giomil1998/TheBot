import matplotlib.pyplot as plt


class Plotter:
    @staticmethod
    def plot_strategy_returns(cumulative_strategy_returns):
        plt.figure(figsize=(12, 6))
        plt.plot(cumulative_strategy_returns.index, cumulative_strategy_returns['long'], label='Long Portfolio', color='blue')
        plt.plot(cumulative_strategy_returns.index, cumulative_strategy_returns['short'], label='Short Portfolio', color='red')
        plt.plot(cumulative_strategy_returns.index, cumulative_strategy_returns['long_short'], label='Long-Short Portfolio',
                 color='green')

        plt.title('Cumulative Returns of Long, Short, and Long-Short Portfolios')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Return')
        plt.legend()
        plt.grid(True)

        plt.show()