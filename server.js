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
    const url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1";
    const res = await axios.get(url);
    const [timestamp, open, high, low, close, volume] = res.data[0];

    await pool.query(
      "INSERT INTO btc_kline (timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6)",
      [new Date(timestamp), open, high, low, close, volume]
    );

    console.log(`✅ Inserted new BTC/USDT K-line at ${new Date(timestamp).toISOString()}`);
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