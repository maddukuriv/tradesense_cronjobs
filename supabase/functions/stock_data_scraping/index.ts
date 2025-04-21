import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import { DateTime } from "luxon";

// Supabase credentials
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

if (!supabaseUrl || !supabaseKey) {
  throw new Error("Supabase URL and KEY must be set in environment variables.");
}

const supabase = createClient(supabaseUrl, supabaseKey);

// List of stock tickers
const tickers = ["AAPL", "GOOGL", "MSFT"]; // Example tickers, replace with your list
const batchSize = 50; // Adjust batch size to optimize performance without hitting API limits

// Function to fetch historical stock data with rate limit handling
async function fetchStockData(
  tickersBatch: string[],
  startDate: string,
  endDate: string,
  results: any[]
) {
  for (const ticker of tickersBatch) {
    let retries = 3; // Retry up to 3 times if request fails
    while (retries > 0) {
      try {
        await new Promise((resolve) => setTimeout(resolve, 1500)); // Avoid hitting API rate limits
        const response = await axios.get(
          `https://query1.finance.yahoo.com/v7/finance/download/${ticker}`,
          {
            params: {
              period1: DateTime.fromISO(startDate).toSeconds(),
              period2: DateTime.fromISO(endDate).toSeconds(),
              interval: "1d",
              events: "history",
              includeAdjustedClose: "true",
            },
          }
        );

        const stockData = response.data;

        if (!stockData) {
          console.warn(`Warning: No data found for ${ticker}`);
          break; // No need to retry if no data exists
        }

        const parsedData = stockData
          .split("\n")
          .slice(1)
          .map((row: string) => {
            const [date, open, high, low, close, adjClose, volume] =
              row.split(",");
            return {
              date,
              open: parseFloat(open),
              high: parseFloat(high),
              low: parseFloat(low),
              close: parseFloat(close),
              volume: parseInt(volume, 10),
              ticker,
            };
          });

        results.push(...parsedData);

        console.log(
          `✅ Fetched data for ${ticker} (${startDate} to ${endDate})`
        );
        break; // Success, exit retry loop
      } catch (error) {
        console.error(`⚠️ Failed to fetch data for ${ticker}: ${error}`);
        retries -= 1;
        await new Promise((resolve) => setTimeout(resolve, 3000)); // Wait 3 seconds before retrying
      }
    }
  }
}

// Main function to fetch and update missing stock data using multithreading
export default async function handler(req: any, res: any) {
  try {
    // Get the last recorded date in the database
    const { data: latestDateData, error: latestDateError } = await supabase
      .from("stock_data")
      .select("date")
      .order("date", { ascending: false })
      .limit(1)
      .single();

    if (latestDateError) throw latestDateError;

    const latestDate = latestDateData ? latestDateData.date : null;

    // Define today’s date
    const today = DateTime.now().toISODate();

    // Determine the start date (fetch only missing days)
    let startDate;
    if (latestDate) {
      const lastFetchedDate = DateTime.fromISO(latestDate);
      startDate = lastFetchedDate.plus({ days: 1 }).toISODate(); // Start from next missing date
    } else {
      startDate = DateTime.now().minus({ years: 1 }).toISODate(); // Fetch last 1 year if no data exists
    }

    // Ensure we only fetch data if needed
    if (DateTime.fromISO(startDate) >= DateTime.fromISO(today)) {
      console.log("No new stock data to fetch.");
      res.status(200).send("No new stock data to fetch.");
      return;
    }

    console.log(`Fetching missing stock data from ${startDate} to ${today}`);

    const updatedData: any[] = [];
    const promises: Promise<void>[] = [];

    for (let i = 0; i < tickers.length; i += batchSize) {
      const batch = tickers.slice(i, i + batchSize);
      promises.push(fetchStockData(batch, startDate, today, updatedData));
    }

    await Promise.all(promises);

    // Insert the fetched data into the database
    if (updatedData.length > 0) {
      const { error: insertError } = await supabase
        .from("stock_data")
        .insert(updatedData);

      if (insertError) throw insertError;

      console.log("✅ Stock data successfully updated in Supabase.");
    }

    // Delete data older than 1 year
    const oneYearAgo = DateTime.now().minus({ years: 1 }).toISODate();

    const { error: deleteError } = await supabase
      .from("stock_data")
      .delete()
      .lt("date", oneYearAgo);

    if (deleteError) throw deleteError;

    console.log("✅ Old data deleted (older than 1 year).");

    res.status(200).send("Stock data updated and old data deleted.");
  } catch (error) {
    console.error(error);
    res.status(500).send("An error occurred.");
  }
}
