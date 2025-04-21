# filepath: /Users/gautamgaddam/Downloads/office works/tradesense_database/deploy.sh
#!/bin/bash

# Activate the virtual environment
source venv/bin/activate

# Check if supabase CLI is installed
if ! command -v supabase &> /dev/null
then
    echo "supabase CLI could not be found. Please install it first."
    exit 1
fi

# Set Supabase environment variables
export SUPABASE_URL='https://kbhdeynmboawkjtxvlek.supabase.co'
export SUPABASE_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtiaGRleW5tYm9hd2tqdHh2bGVrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDA2NDg3NjAsImV4cCI6MjA1NjIyNDc2MH0.T3L5iIn1FiBlBo5HZMqysgokD8cfOw2n3u_YCJV0DkQ'
export SUPABASE_ACCESS_TOKEN='sbp_b8954d7a10835083ae6f2ad2bfd9912ee3908422'

# Deploy the edge function
supabase functions deploy stock_data_scraping

echo "Deployment complete."