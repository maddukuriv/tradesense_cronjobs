import yfinance as yf
import pandas as pd
import threading
import time
from supabase import create_client
from datetime import datetime, timedelta

# Supabase credentials
supabase = create_client(url, key)

# List of stock tickers
#tickers = Smallcap
batch_size = 50  # Adjust batch size to optimize performance without hitting API limits

# Get the last recorded date in the database
latest_date_query = supabase.table("stock_data").select("date").order("date", desc=True).limit(1).execute()
latest_date = latest_date_query.data[0]['date'] if latest_date_query.data else None

# Define today’s date
today = datetime.now().strftime('%Y-%m-%d')

# Determine the start date (fetch only missing days)
if latest_date:
    last_fetched_date = datetime.strptime(latest_date, '%Y-%m-%d')
    start_date = (last_fetched_date + timedelta(days=1)).strftime('%Y-%m-%d')  # Start from next missing date
else:
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')  # Fetch last 1 year if no data exists

# Ensure we only fetch data if needed
if start_date >= today:
    print("No new stock data to fetch.")
    exit()

# Function to fetch historical stock data with rate limit handling
def fetch_stock_data(tickers_batch, start_date, end_date, results):
    for ticker in tickers_batch:
        retries = 3  # Retry up to 3 times if request fails
        while retries > 0:
            try:
                time.sleep(1.5)  # Avoid hitting API rate limits
                stock = yf.Ticker(ticker)
                stock_data = stock.history(start=start_date, end=end_date)

                if stock_data.empty:
                    print(f"Warning: No data found for {ticker}")
                    break  # No need to retry if no data exists

                stock_data['ticker'] = ticker
                stock_data.columns = [col.lower() for col in stock_data.columns]
                stock_data = stock_data.reset_index()
                stock_data.rename(columns={"Date": "date"}, inplace=True)
                stock_data['date'] = stock_data['date'].astype(str)

                required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
                results.extend(stock_data[required_columns].to_dict(orient="records"))

                print(f"✅ Fetched data for {ticker} ({start_date} to {end_date})")
                break  # Success, exit retry loop

            except Exception as e:
                print(f"⚠️ Failed to fetch data for {ticker}: {e}")
                retries -= 1
                time.sleep(3)  # Wait 3 seconds before retrying

# ✅ Fetch & update missing stock data using multithreading
print(f"Fetching missing stock data from {start_date} to {today}")

updated_data = []
threads = []

for i in range(0, len(tickers), batch_size):
    batch = tickers[i:i + batch_size]
    thread = threading.Thread(target=fetch_stock_data, args=(batch, start_date, today, updated_data))
    threads.append(thread)
    thread.start()
    time.sleep(2)  # Short delay to prevent excessive API calls

for thread in threads:
    thread.join()

# Insert the fetched data into the database
if updated_data:
    try:
        supabase.table("stock_data").insert(updated_data).execute()
        print("✅ Stock data successfully updated in Supabase.")
    except Exception as e:
        print(f"⚠️ Error inserting stock data: {e}")

# ✅ Delete data older than 1 year
one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

try:
    supabase.table("stock_data").delete().filter("date", "lt", one_year_ago).execute()
    print("✅ Old data deleted (older than 1 year).")
except Exception as e:
    print(f"⚠️ Error deleting old data: {e}")
