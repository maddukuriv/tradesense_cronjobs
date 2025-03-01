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
export SUPABASE_ACCESS_TOKEN='your_supabase_access_token_here'

# Deploy the edge function
supabase functions deploy stock_data_scraping

echo "Deployment complete."