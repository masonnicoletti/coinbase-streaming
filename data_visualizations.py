import logging
import duckdb
import pandas as pd
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
            SELECT price, time 
            FROM bitcoin
            ORDER BY time DESC;
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
            SELECT price, time 
            FROM ethereum
            ORDER BY time DESC;
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
            SELECT price, time 
            FROM solana
            ORDER BY time DESC;
        """).df()

        plt.plot(solana_timeseries['time'], solana_timeseries['price'], color='#14F195')
        plt.title("Solana Ticker Timeseries")
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.savefig("./plots/solana_timeseries.png")
        plt.close()
        logger.info("Produced Solana timeseries plot")


        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")



if __name__ == "__main__":
    data_vis()