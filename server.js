// File name: server.js
// Author: Sunny

require('dotenv').config();
const express = require("express");
const axios = require("axios");
const { Pool } = require("pg");
const cors = require('cors');
app.use(cors());

const app = express();
const PORT = process.env.PORT || 3001;
const delay = ms => new Promise(res => setTimeout(res, ms));



// PostgreSQL connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Auto-fetch 1-hour BTC/USDT K-line from CoinGecko Pro
const fetchKline = async () => {
  const url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart";

  let retries = 3;

  while (retries > 0) {
    try {
      const res = await axios.get(url, {
        params: {
          vs_currency: 'usd',
          days: 2  // ✅ This gives hourly data now
        }
      });

      const prices = res.data.prices;
      const volumes = res.data.total_volumes;

      if (!prices || prices.length === 0) throw new Error("No price data");

      const latestPrice = prices[prices.length - 1];
      const latestVolume = volumes[volumes.length - 1];

      const timestamp = new Date(latestPrice[0]);
      const close = latestPrice[1];
      const volume = latestVolume[1];
      const open = prices[prices.length - 2]?.[1] || close;
      const high = Math.max(...prices.slice(-6).map(p => p[1]));
      const low = Math.min(...prices.slice(-6).map(p => p[1]));

      const check = await pool.query("SELECT 1 FROM btc_kline WHERE timestamp = $1", [timestamp]);
      if (check.rowCount > 0) {
        console.log(`⚠️ Duplicate skipped for ${timestamp.toISOString()}`);
        break;
      }

      await pool.query(
        "INSERT INTO btc_kline (timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6)",
        [timestamp, open, high, low, close, volume]
      );

      console.log(`✅ Inserted BTC K-line at ${timestamp.toISOString()}`);
      break;

    } catch (err) {
      if (err.response?.status === 429) {
        console.warn("⏳ Rate limit hit. Retrying in 10s...");
        retries--;
        await delay(10000);
      } else {
        console.error("❌ Error inserting kline:", err.response?.data || err.message);
        break;
      }
    }
  }
};

// Run every 1 hour
setInterval(fetchKline, 60 * 60 * 1000); // 60 minutes
fetchKline(); // Run immediately at server start

// Health check
app.get('/', (req, res) => {
  res.send('✅ Crypto backend is running!');
});

// API to get latest 100 K-lines
app.get('/kline', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM btc_kline ORDER BY timestamp DESC LIMIT 100');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
