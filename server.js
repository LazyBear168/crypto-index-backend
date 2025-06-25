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

// Fetch hourly BTC price from CoinGecko (latest)
const fetchHourlyPrice = async () => {
  const url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart";
  let retries = 3;

  while (retries > 0) {
    try {
      const res = await axios.get(url, {
        params: {
          vs_currency: 'usd',
          days: 2
        },
      });

      const prices = res.data.prices;
      if (!prices || prices.length === 0) throw new Error("No price data");

      // Get the latest price entry
      const [timestampMs, close] = prices[prices.length - 1];
      const timestamp = new Date(timestampMs);
      // timestamp.setMinutes(0, 0, 0); // Round to top of the hour

      // Check if this timestamp already exists
      const check = await db.query(
        "SELECT 1 FROM btc_price_hourly WHERE timestamp = $1",
        [timestamp]
      );

      if (check.rowCount > 0) {
        console.log(`⚠️ Duplicate skipped for ${timestamp.toISOString()}`);
      } else {
        await db.query(
          "INSERT INTO btc_price_hourly (timestamp, close) VALUES ($1, $2)",
          [timestamp, close]
        );
        console.log(`✅ Inserted BTC price at ${timestamp.toISOString()}`);
      }

      break; // exit while loop after success
    } catch (err) {
      if (err.response?.status === 429) {
        console.warn("⏳ Rate limit hit. Retrying in 10s...");
        retries--;
        await delay(10000);
      } else {
        console.error("❌ Error inserting hourly price:", err.response?.data || err.message);
        break;
      }
    }
  }
};


// Run every 10 minutes
const scheduleTenMinuteFetch = () => {
  fetchHourlyPrice(); // Immediately fetch once

  const now = new Date();
  const minutes = now.getMinutes();
  const nextTenMinuteMark = Math.ceil(minutes / 10) * 10;
  const delayToNextRun =
    (nextTenMinuteMark - minutes) * 60 * 1000 -
    now.getSeconds() * 1000 -
    now.getMilliseconds();

  console.log(`⏰ Waiting ${Math.round(delayToNextRun / 1000)} seconds to start 10-minute interval job`);

  setTimeout(() => {
    fetchHourlyPrice(); // Run at the next 10-minute mark

    // Then run every 10 minutes
    setInterval(fetchHourlyPrice, 10 * 60 * 1000);
  }, delayToNextRun);
};

scheduleTenMinuteFetch();



// Health check
app.get('/', (req, res) => {
  res.send('✅ Crypto backend is running!');
});

// Get latest 100 hourly prices
app.get('/price/hourly', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM btc_price_hourly ORDER BY timestamp DESC');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// Serve historical K-line data with optional start/end
app.get('/kline', async (req, res) => {
  const { start, end } = req.query;

  try {
    let result;

    if (start && end) {
      result = await db.query(
        `SELECT * FROM btc_kline
         WHERE timestamp BETWEEN $1::timestamptz AND $2::timestamptz
         ORDER BY timestamp ASC`,
        [start, end]  // 直接用 ISO 字串，Postgres 可自動解析
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
