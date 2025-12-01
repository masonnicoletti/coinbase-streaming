import time
import logging
import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from dash import Dash, html, dcc

logging.basicConfig(
    filename='./logs/daily_dashboard.log',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger(__name__)


def produce_daily_dash():

    con = None

    try:
        # Connect to DuckDB instance
        con = duckdb.connect(database='./coinbase.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")


        #daily_summary_df = con.execute("""
        #""").df()
    


        # Close DuckDB connection
        con.close()
        logger.info("Closed DuckDB connection")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")



if __name__ == "__main__":
    produce_daily_dash()