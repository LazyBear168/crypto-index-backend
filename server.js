// File name: server.js
// use terminal by psql "postgresql://crypto_data_db_user:Ab7BqcyTVswXoiAEERrGKKnUaB57LliQ@dpg-d1bo9e3e5dus73eq88pg-a.oregon-postgres.render.com:5432/crypto_data_db?sslmode=require"
// Author: Sunny1

require('dotenv').config();
const express = require("express");
const cors = require("cors"); 
const axios = require("axios");
const db = require("./db");

const app = express(); 
app.use(cors()); 

const PORT = process.env.PORT || 3001;
const delay = ms => new Promise(res => setTimeout(res, ms));

// Fetch hourly K-line data from CoinGecko
const fetchHourlyKline = async () => {
  const url = "https://api.coingecko.com/api/v3/coins/bitcoin/ohlc";
  let retries = 3;

  while (retries > 0) {
    try {
      // Add delay before each request to avoid rate limiting
      await delay(2000); // 2 second delay
      
      const res = await axios.get(url, {
        params: {
          vs_currency: 'usd',
          days: 1 // Reduced to 1 day to minimize API calls
        },
        timeout: 10000, // 10 second timeout
        headers: {
          'User-Agent': 'CryptoBackend/1.0'
        }
      });

      const ohlcData = res.data;
      if (!ohlcData || ohlcData.length === 0) throw new Error("No OHLC data");

      // Get the latest OHLC entry
      const [timestampMs, open, high, low, close] = ohlcData[ohlcData.length - 1];
      const timestamp = new Date(timestampMs);

      // Check if this timestamp already exists
      const check = await db.query(
        "SELECT 1 FROM btc_kline WHERE timestamp = $1",
        [timestamp]
      );

      if (check.rowCount > 0) {
        console.log(`⚠️ Duplicate skipped for ${timestamp.toISOString()}`);
      } else {
        await db.query(
          "INSERT INTO btc_kline (timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6)",
          [timestamp, open, high, low, close, 0] // Volume is not available in free CoinGecko API
        );
        console.log(`✅ Inserted BTC K-line at ${timestamp.toISOString()} - O:${open} H:${high} L:${low} C:${close}`);
      }

      break; // exit while loop after success
    } catch (err) {
      if (err.response?.status === 429) {
        console.warn(`⏳ Rate limit hit. Retrying in 30s... (${retries} retries left)`);
        retries--;
        await delay(30000); // Increased delay to 30 seconds
      } else if (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT') {
        console.warn(`⏳ Request timeout. Retrying in 15s... (${retries} retries left)`);
        retries--;
        await delay(15000);
      } else {
        console.error("❌ Error inserting hourly K-line:", err.response?.data || err.message);
        break;
      }
    }
  }
  
  if (retries === 0) {
    console.error("❌ Failed to fetch after all retries. Will try again in next cycle.");
  }
};

// Run every 15 minutes instead of 10 to reduce API pressure
const scheduleFifteenMinuteFetch = () => {
  // Don't fetch immediately, wait for first scheduled time
  console.log("🔄 Scheduling first fetch...");

  const now = new Date();
  const minutes = now.getMinutes();
  const nextFifteenMinuteMark = Math.ceil(minutes / 15) * 15;
  const delayToNextRun =
    (nextFifteenMinuteMark - minutes) * 60 * 1000 -
    now.getSeconds() * 1000 -
    now.getMilliseconds();

  console.log(`⏰ Waiting ${Math.round(delayToNextRun / 1000)} seconds to start 15-minute interval job`);

  setTimeout(() => {
    fetchHourlyKline(); // Run at the next 15-minute mark

    // Then run every 15 minutes
    setInterval(fetchHourlyKline, 15 * 60 * 1000);
  }, delayToNextRun);
};

scheduleFifteenMinuteFetch();

// Health check
app.get('/', (req, res) => {
  res.send('✅ Crypto backend is running!');
});

// Get latest hourly K-line data
app.get('/kline/hourly', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM btc_kline ORDER BY timestamp DESC LIMIT 100');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// Serve historical K-line data with optional start/end (updated to use new table)
app.get('/kline', async (req, res) => {
  const { start, end } = req.query;

  try {
    let result;

    if (start && end) {
      result = await db.query(
        `SELECT * FROM btc_kline
         WHERE timestamp BETWEEN $1::timestamptz AND $2::timestamptz
         ORDER BY timestamp ASC`,
        [start, end]
      );
    } else {
      // fallback: latest 200 entries, still ordered by ASC
      result = await db.query(
        `SELECT * FROM (
           SELECT * FROM btc_kline
           ORDER BY timestamp DESC
           LIMIT 200
         ) AS sub
         ORDER BY timestamp ASC`
      );
    }

    res.json(result.rows);
  } catch (err) {
    console.error("❌ /kline error:", err);
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));