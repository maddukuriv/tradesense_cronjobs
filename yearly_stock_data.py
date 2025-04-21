## Code to load yearly data of a stock
import yfinance as yf
import pandas as pd
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client
from datetime import datetime, timedelta

# Supabase credentials
supabase = create_client(url, key)

# List of stock tickers (modify to fetch only specific tickers)
#tickers_to_fetch = Smallcap

# Define date range
today = (datetime.now()).strftime('%Y-%m-%d')
one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ **Fetch existing tickers & dates from database**
try:
    db_data = supabase.table("stock_data").select("ticker", "date").execute()
    existing_records = {(row['ticker'], row['date']) for row in db_data.data}
    logging.info(f"Fetched {len(existing_records)} records from database.")
except Exception as e:
    logging.error(f"Error fetching existing tickers: {e}")
    existing_records = set()

# ✅ **Filter out already existing data**
tickers_to_fetch_filtered = []
for ticker in tickers_to_fetch:
    for date in pd.date_range(one_year_ago, today):
        date_str = date.strftime('%Y-%m-%d')
        if (ticker, date_str) not in existing_records:
            tickers_to_fetch_filtered.append(ticker)
            break  # Only add if at least one date is missing

if not tickers_to_fetch_filtered:
    logging.info("No missing data. Exiting script.")
    exit()

logging.info(f"Fetching data for missing tickers: {tickers_to_fetch_filtered}")

# ✅ **Function to fetch stock data**
def fetch_stock_data(ticker, start_date, end_date):
    retries = 3
    wait_time = 2  # Initial wait time
    for _ in range(retries):
        try:
            time.sleep(1)  # Rate limit handling
            stock = yf.Ticker(ticker)
            stock_data = stock.history(start=start_date, end=end_date)

            if stock_data.empty:
                logging.warning(f"No data found for {ticker}")
                return None

            stock_data['ticker'] = ticker
            stock_data.columns = [col.lower() for col in stock_data.columns]
            stock_data.reset_index(inplace=True)
            stock_data.rename(columns={"Date": "date"}, inplace=True)
            stock_data['date'] = stock_data['date'].astype(str)

            return stock_data[['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']].to_dict(orient="records")

        except Exception as e:
            logging.error(f"Error fetching data for {ticker}: {e}")
            time.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
    return None

# ✅ **Multi-threaded fetching**
def fetch_data_multithreaded(ticker_list, start_date, end_date):
    results = []
    batch_size = min(10, max(1, len(ticker_list) // 20))  # Adjust batch size dynamically

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        future_to_ticker = {executor.submit(fetch_stock_data, ticker, start_date, end_date): ticker for ticker in ticker_list}
        for future in as_completed(future_to_ticker):
            data = future.result()
            if data:
                results.extend(data)

    return results

# ✅ **Fetch only missing data**
new_data = fetch_data_multithreaded(tickers_to_fetch_filtered, one_year_ago, today)

# ✅ **Insert only new data into the database**
if new_data:
    try:
        supabase.table("stock_data").insert(new_data).execute()
        logging.info(f"Inserted {len(new_data)} new records into the database.")
    except Exception as e:
        logging.error(f"Error inserting new data: {e}")
else:
    logging.info("No new data fetched.")
