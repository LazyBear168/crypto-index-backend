// File name: server.js
// Author: Sunny

require('dotenv').config();
const express = require("express");
const axios = require("axios");
const { Pool } = require("pg");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3001;

// Connect to PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// Auto-fetch 1-minute BTC/USDT K-line from Binance
const fetchKline = async () => {
  try {
    const url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&interval=minutely&days=1";

    const res = await axios.get(url, {
      headers: {
        'User-Agent': 'crypto-index-app/1.0'
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
    const high = Math.max(...prices.slice(-5).map(p => p[1]));
    const low = Math.min(...prices.slice(-5).map(p => p[1]));

    await pool.query(
      "INSERT INTO btc_kline (timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6)",
      [timestamp, open, high, low, close, volume]
    );

    console.log(`✅ Inserted CoinGecko BTC/USDT at ${timestamp.toISOString()}`);
  } catch (err) {
    console.error("❌ Error inserting kline:", err.message);
  }
};

// Run every 1 minute
setInterval(fetchKline, 60 * 1000);
fetchKline(); // Also run immediately at server start

// confirm backend is alive 
app.get('/', (req, res) => {
  res.send('✅ Crypto backend is running!');
});

// API route to get all klines
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