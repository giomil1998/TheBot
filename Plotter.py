import matplotlib.pyplot as plt

class Plotter:
    @staticmethod
    def plot_strategy_returns(strategy_returns_df):
        strategy_returns_df['long_cum'] = (1 + strategy_returns_df['long']).cumprod() - 1
        strategy_returns_df['short_cum'] = (1 + strategy_returns_df['short']).cumprod() - 1
        strategy_returns_df['long_short_cum'] = (1 + strategy_returns_df['long_short']).cumprod() - 1

        plt.figure(figsize=(10, 6))
        plt.plot(strategy_returns_df['date'], strategy_returns_df['long_cum'], label='Long Strategy', color='blue')
        plt.plot(strategy_returns_df['date'], strategy_returns_df['short_cum'], label='Short Strategy', color='red')
        plt.plot(strategy_returns_df['date'], strategy_returns_df['long_short_cum'], label='Long-Short Strategy', color='green')

        plt.title('Cumulative Returns of Long, Short, and Long-Short Strategies')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Returns')
        plt.legend()
        plt.grid(True)
        plt.show()