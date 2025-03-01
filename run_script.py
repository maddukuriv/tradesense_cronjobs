import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import stock_data_scraping

def main():
    stock_data_scraping.main()

if __name__ == "__main__":
    main()