// File name: server.js
// use terminal by connect_crypto_db
// Author: Sunny

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
          days: 1,
          interval: 'hourly'
        }
      });

      const prices = res.data.prices;
      if (!prices || prices.length === 0) throw new Error("No price data");

      const [timestampMs, close] = prices[prices.length - 1];
      const timestamp = new Date(timestampMs);

      // Round to hour to avoid duplicates
      timestamp.setMinutes(0, 0, 0);

      const check = await db.query("SELECT 1 FROM btc_price_hourly WHERE timestamp = $1", [timestamp]);
      if (check.rowCount > 0) {
        console.log(`⚠️ Duplicate skipped for ${timestamp.toISOString()}`);
        break;
      }

      await db.query(
        "INSERT INTO btc_price_hourly (timestamp, close) VALUES ($1, $2)",
        [timestamp, close]
      );

      console.log(`✅ Inserted BTC price at ${timestamp.toISOString()}`);
      break;

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

// Run every full hour
const scheduleHourlyFetch = () => {
  const now = new Date();
  const delayToNextHour = (60 - now.getMinutes()) * 60 * 1000 - now.getSeconds() * 1000 - now.getMilliseconds();

  console.log(`⏰ Waiting ${Math.round(delayToNextHour / 1000)} seconds to start next full-hour job`);

  setTimeout(() => {
    fetchHourlyPrice(); // Run at the next full hour

    // Then run every hour on the hour
    setInterval(fetchHourlyPrice, 60 * 60 * 1000);
  }, delayToNextHour);
};

scheduleHourlyFetch();


// Health check
app.get('/', (req, res) => {
  res.send('✅ Crypto backend is running!');
});

// Get latest 100 hourly prices
app.get('/price/hourly', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM btc_price_hourly ORDER BY timestamp DESC LIMIT 100');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
