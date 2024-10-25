import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

class Plotter:
    @staticmethod
    def plot_strategy_returns(strategy_returns_df):
        plt.figure(figsize=(12, 6))
        plt.plot(strategy_returns_df.index, strategy_returns_df['long_cum_pct'], label='Long Portfolio', color='blue')
        plt.plot(strategy_returns_df.index, strategy_returns_df['short_cum_pct'], label='Short Portfolio', color='red')
        plt.plot(strategy_returns_df.index, strategy_returns_df['long_short_cum_pct'], label='Long-Short Portfolio',
                 color='green')

        plt.title('Cumulative Returns of Long, Short, and Long-Short Portfolios')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Return (%)')
        plt.legend()
        plt.grid(True)

        # Format Y-axis as percentage
        plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())
        plt.show()