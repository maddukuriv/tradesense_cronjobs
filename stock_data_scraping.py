import yfinance as yf
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta
import os

# Supabase credentials
URL = os.getenv('SUPABASE_URL')
KEY = os.getenv('SUPABASE_KEY')

if not URL or not KEY:
  raise ValueError("Supabase URL and KEY must be set in environment variables.")

supabase = create_client(URL, KEY)

# List of stock tickers
tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']

# Define date range
today = datetime.today().strftime('%Y-%m-%d')
one_year_ago = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')

# Fetch existing tickers from database
try:
    existing_tickers_query = supabase.table("stock_data").select("ticker").execute()
    existing_tickers = [row['ticker'] for row in existing_tickers_query.data]
    print("Existing tickers in database:", existing_tickers)
except Exception as e:
    print(f"Error fetching existing tickers: {e}")
    existing_tickers = []

# Identify new and removed tickers
new_tickers = list(set(tickers) - set(existing_tickers))
removed_tickers = list(set(existing_tickers) - set(tickers))

# ✅ 1️⃣ Fetch 1-year data for new tickers
if new_tickers:
    print(f"Fetching 1-year historical data for new tickers: {new_tickers}")
    new_data = []
    for ticker in new_tickers:
        stock = yf.Ticker(ticker)
        stock_data = stock.history(start=one_year_ago, end=today)

        if stock_data.empty:
            print(f"Warning: No data found for {ticker}")
            continue

        stock_data['ticker'] = ticker
        stock_data.columns = [col.lower() for col in stock_data.columns]  # Convert to lowercase
        stock_data = stock_data.reset_index()  # Convert index to column
        stock_data.rename(columns={"Date": "date"}, inplace=True)  # Rename column
        stock_data['date'] = stock_data['date'].astype(str)  # Convert to string

        # 🚀 **Fix: Keep only required columns**
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
        stock_data = stock_data[required_columns]

        new_data.append(stock_data)

    if new_data:
        df_new = pd.concat(new_data)

        print("Columns in new data before inserting:", df_new.columns)  # Debugging

        try:
            supabase.table("stock_data").insert(df_new.to_dict(orient="records")).execute()
            print("New tickers' historical data added.")
        except Exception as e:
            print(f"Error saving new tickers' data: {e}")

# ✅ 2️⃣ Update today's data for existing tickers
if existing_tickers:
    print(f"Updating stock data for existing tickers: {existing_tickers}")
    updated_data = []
    for ticker in existing_tickers:
        stock = yf.Ticker(ticker)
        stock_data = stock.history(start=today, end=today)

        if stock_data.empty:
            print(f"Warning: No data found for {ticker}")
            continue

        stock_data['ticker'] = ticker
        stock_data.columns = [col.lower() for col in stock_data.columns]  # Convert to lowercase
        stock_data = stock_data.reset_index()  # Convert index to column
        stock_data.rename(columns={"Date": "date"}, inplace=True)  # Rename column
        stock_data['date'] = stock_data['date'].astype(str)  # Convert to string

        # 🚀 **Fix: Keep only required columns**
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
        stock_data = stock_data[required_columns]

        updated_data.append(stock_data)

    if updated_data:
        df_update = pd.concat(updated_data)

        print("Columns in updated data before inserting:", df_update.columns)  # Debugging

        try:
            supabase.table("stock_data").insert(df_update.to_dict(orient="records")).execute()
            print("Stock data updated for existing tickers.")
        except Exception as e:
            print(f"Error updating existing tickers' data: {e}")

# ✅ 3️⃣ Delete data older than 1 year
try:
    supabase.table("stock_data").delete().filter("date", "lt", one_year_ago).execute()
    print("Old data deleted to save space.")
except Exception as e:
    print(f"Error deleting old data: {e}")

# ✅ 4️⃣ Delete data for removed tickers
if removed_tickers:
    print(f"Removing data for tickers no longer in the list: {removed_tickers}")
    try:
        supabase.table("stock_data").delete().in_("ticker", removed_tickers).execute()
        print("Removed tickers deleted from database.")
    except Exception as e:
        print(f"Error removing old tickers: {e}")
