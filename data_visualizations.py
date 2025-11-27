import logging
import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

logging.basicConfig(
    filename='./logs/data_visualizations.log',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger(__name__)


def data_vis():
    
    con = None

    try:
        # Connect to DuckDB instance
        con = duckdb.connect(database='./coinbase.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")


        # Extract and plot Bitcoin price over time
        bitcoin_timeseries = con.execute("""
            SELECT price, time FROM bitcoin;
        """).df()

        plt.plot(bitcoin_timeseries['time'], bitcoin_timeseries['price'], color='#f7931a')
        plt.title("Bitcoin Ticker Timeseries")
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.savefig("./plots/bitcoin_timeseries.png")
        plt.close()
        logger.info("Produced Bitcoin timeseries plot")

        # Extract and plot Ethereum price over time
        ethereum_timeseries = con.execute("""
            SELECT price, time FROM ethereum;
        """).df()

        plt.plot(ethereum_timeseries['time'], ethereum_timeseries['price'], color='#4043AE')
        plt.title("Ethereum Ticker Timeseries")
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.savefig("./plots/ethereum_timeseries.png")
        plt.close()
        logger.info("Produced Ethereum timeseries plot")


        # Extract and plot Solana price over time
        solana_timeseries = con.execute("""
            SELECT price, time FROM solana;
        """).df()

        plt.plot(solana_timeseries['time'], solana_timeseries['price'], color='#14F195')
        plt.title("Solana Ticker Timeseries")
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.savefig("./plots/solana_timeseries.png")
        plt.close()
        logger.info("Produced Solana timeseries plot")


        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = ax1.twinx()
        ax3 = ax1.twinx()
        ax3.spines['right'].set_position(('outward', 60))
        sns.lineplot(data=bitcoin_timeseries, x='time', y='price', ax=ax1, color='#f7931a', label='Bitcoin', estimator=None)
        sns.lineplot(data=ethereum_timeseries, x='time', y='price', ax=ax2, color='#4043AE', label='Ethereum', estimator=None)
        sns.lineplot(data=solana_timeseries, x='time', y='price', ax=ax3, color='#14F195', label='Solana', estimator=None)
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Bitcoin", color='#f7931a')
        ax2.set_ylabel("Ethereum", color='#4043AE')
        ax3.set_ylabel("Solana", color='#14F195')
        ax1.tick_params(axis='y', labelcolor='#f7931a')
        ax2.tick_params(axis='y', labelcolor='#4043AE')
        ax3.tick_params(axis='y', labelcolor='#14F195')
        plt.title("Coinbase Product Timeseries")
        plt.legend()
        fig.tight_layout()
        plt.savefig("./plots/coinbase_timeseries.png")
        plt.close()


        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")



if __name__ == "__main__":
    data_vis()