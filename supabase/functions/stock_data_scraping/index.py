import json
import os
from supabase import create_client, Client
import yfinance as yf
import pandas as pd
import threading
import time
import math
from datetime import datetime, timedelta
import configparser

# ✅ Load Supabase credentials from config.ini
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config.ini')
config.read(config_path)

url = config.get('DEFAULT', 'URL', fallback=None)
key = config.get('DEFAULT', 'KEY', fallback=None)

if not url or not key:
    raise ValueError(f"Supabase URL or Key is missing. Check your config.ini file at {config_path}.")

print(f"Supabase URL: {url}")
print(f"Supabase Key: {key}")

supabase: Client = create_client(url, key)

# ✅ Load stock tickers from JSON files
def load_tickers(file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path, 'r') as file:
        return json.load(file)

Largecap = load_tickers('largecap.json')
Midcap = load_tickers('midcap.json')
Smallcap = load_tickers('smallcap.json')
Indices = load_tickers('indices.json')

# ✅ Today's date
today = datetime.now().strftime('%Y-%m-%d')

# ✅ Get last available date from Supabase for a given ticker
def get_last_date_for_ticker(ticker):
    try:
        response = supabase.table("stock_data").select("date").eq("ticker", ticker).order("date", desc=True).limit(1).execute()
        if response.data:
            return response.data[0]['date']
        else:
            return None
    except Exception as e:
        print(f"⚠️ Error fetching last date for {ticker}: {e}")
        return None

# ✅ Sanitize NaN to None for JSON compatibility
def sanitize_json(data):
    def sanitize_value(val):
        if isinstance(val, float) and math.isnan(val):
            return None
        return val

    if isinstance(data, list):
        return [sanitize_json(item) for item in data]
    elif isinstance(data, dict):
        return {k: sanitize_value(v) for k, v in data.items()}
    else:
        return sanitize_value(data)

# ✅ Fetch missing data for tickers
def fetch_stock_data(tickers_batch, results):
    for ticker in tickers_batch:
        last_date = get_last_date_for_ticker(ticker)

        if last_date:
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')

        end_date = today

        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.now():
            print(f"✅ No new data needed for {ticker}. Skipping...")
            continue

        retries = 3
        while retries > 0:
            try:
                time.sleep(1.5)  # Rate limiting
                stock = yf.Ticker(ticker)
                stock_data = stock.history(start=start_date, end=end_date)

                if stock_data.empty:
                    print(f"⚠️ No data for {ticker} from {start_date} to {end_date}")
                    break

                stock_data.reset_index(inplace=True)
                stock_data.rename(columns={'Date': 'date'}, inplace=True)
                stock_data['ticker'] = ticker
                stock_data['date'] = stock_data['date'].dt.strftime('%Y-%m-%d')
                stock_data.columns = [col.lower() for col in stock_data.columns]

                required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
                stock_data = stock_data[required_columns]
                stock_data = stock_data.where(pd.notnull(stock_data), None)

                results.extend(stock_data.to_dict(orient="records"))
                print(f"✅ {ticker} updated from {start_date} to {end_date}")
                break

            except Exception as e:
                print(f"⚠️ Error fetching data for {ticker}: {e}")
                retries -= 1
                time.sleep(3)

# ✅ Process a set of tickers
def process_tickers(tickers):
    batch_size = 50
    insert_batch_size = 100

    updated_data = []
    threads = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        thread = threading.Thread(target=fetch_stock_data, args=(batch, updated_data))
        threads.append(thread)
        thread.start()
        time.sleep(2)

    for thread in threads:
        thread.join()

    if updated_data:
        try:
            for i in range(0, len(updated_data), insert_batch_size):
                batch = sanitize_json(updated_data[i:i + insert_batch_size])
                supabase.table("stock_data").upsert(batch).execute()
                print(f"✅ Batch upserted, records {i+1} to {i+len(batch)}")

            print("✅ Missing stock data successfully updated in Supabase.")
        except Exception as e:
            print(f"⚠️ Error inserting stock data: {e}")

    # ✅ Delete data older than 5 years
    five_year_ago = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    try:
        supabase.table("stock_data").delete().filter("date", "lt", five_year_ago).execute()
        print("✅ Old data deleted (older than 5 years).")
    except Exception as e:
        print(f"⚠️ Error deleting old data: {e}")

# ✅ Run the process for each cap category
for cap_name, tickers in [("Largecap", Largecap), ("Midcap", Midcap), ("Smallcap", Smallcap), ("Indices", Indices)]:
    print(f"🚀 Processing {cap_name} tickers...")
    process_tickers(tickers)
