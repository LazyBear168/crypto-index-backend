// File name: server.js
// use terminal by psql "postgresql://crypto_data_db_user:Ab7BqcyTVswXoiAEERrGKKnUaB57LliQ@dpg-d1bo9e3e5dus73eq88pg-a.oregon-postgres.render.com:5432/crypto_data_db?sslmode=require"
// Author: Sunny1
// Enhanced version with BTC and ETH support

require('dotenv').config();
const express = require("express");
const cors = require("cors"); 
const axios = require("axios");
const db = require("./db");

const app = express(); 
app.use(cors()); 

const PORT = process.env.PORT || 3001;
const delay = ms => new Promise(res => setTimeout(res, ms));

// Cryptocurrency configurations
const CRYPTO_CONFIGS = [
  {
    id: 'bitcoin',
    symbol: 'BTC',
    pair: 'BTC/USDT',
    tableName: 'btc_kline'
  },
  {
    id: 'ethereum',
    symbol: 'ETH',
    pair: 'ETH/USDT',
    tableName: 'eth_kline'
  }
];

// Fetch hourly K-line data from CoinGecko for a specific cryptocurrency
const fetchHourlyKline = async (cryptoConfig) => {
  const { id, symbol, pair, tableName } = cryptoConfig;
  const url = `https://api.coingecko.com/api/v3/coins/${id}/ohlc`;
  let retries = 3;

  while (retries > 0) {
    try {
      // Add delay before each request to avoid rate limiting
      await delay(2000); // 2 second delay
      
      console.log(`🔍 Fetching ${symbol} data...`);
      
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
      if (!ohlcData || ohlcData.length === 0) throw new Error(`No OHLC data for ${symbol}`);

      // Get the latest OHLC entry
      const [timestampMs, open, high, low, close] = ohlcData[ohlcData.length - 1];
      const timestamp = new Date(timestampMs);

      // Check if this timestamp already exists
      const check = await db.query(
        `SELECT 1 FROM ${tableName} WHERE timestamp = $1 AND pair = $2`,
        [timestamp, pair]
      );

      if (check.rowCount > 0) {
        console.log(`⚠️ Duplicate skipped for ${symbol} at ${timestamp.toISOString()}`);
      } else {
        await db.query(
          `INSERT INTO ${tableName} (timestamp, open, high, low, close, volume, pair) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [timestamp, open, high, low, close, 0, pair] // Volume is not available in free CoinGecko API
        );
        console.log(`✅ Inserted ${symbol} K-line at ${timestamp.toISOString()} - O:${open} H:${high} L:${low} C:${close}`);
      }

      break; // exit while loop after success
    } catch (err) {
      if (err.response?.status === 429) {
        console.warn(`⏳ Rate limit hit for ${symbol}. Retrying in 30s... (${retries} retries left)`);
        retries--;
        await delay(30000); // Increased delay to 30 seconds
      } else if (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT') {
        console.warn(`⏳ Request timeout for ${symbol}. Retrying in 15s... (${retries} retries left)`);
        retries--;
        await delay(15000);
      } else {
        console.error(`❌ Error inserting ${symbol} hourly K-line:`, err.response?.data || err.message);
        break;
      }
    }
  }
  
  if (retries === 0) {
    console.error(`❌ Failed to fetch ${symbol} after all retries. Will try again in next cycle.`);
  }
};

// Fetch data for all configured cryptocurrencies
const fetchAllCryptoData = async () => {
  console.log('🚀 Starting crypto data collection cycle...');
  
  for (const config of CRYPTO_CONFIGS) {
    try {
      await fetchHourlyKline(config);
      // Add a small delay between different cryptocurrencies to avoid overwhelming the API
      await delay(3000);
    } catch (error) {
      console.error(`❌ Error processing ${config.symbol}:`, error.message);
    }
  }
  
  console.log('✅ Crypto data collection cycle completed');
};

// Run every 15 minutes instead of 10 to reduce API pressure
const scheduleFifteenMinuteFetch = () => {
  // Don't fetch immediately, wait for first scheduled time
  console.log("🔄 Scheduling first fetch for BTC and ETH...");

  const now = new Date();
  const minutes = now.getMinutes();
  const nextFifteenMinuteMark = Math.ceil(minutes / 15) * 15;
  const delayToNextRun =
    (nextFifteenMinuteMark - minutes) * 60 * 1000 -
    now.getSeconds() * 1000 -
    now.getMilliseconds();

  console.log(`⏰ Waiting ${Math.round(delayToNextRun / 1000)} seconds to start 15-minute interval job`);

  setTimeout(() => {
    fetchAllCryptoData(); // Run at the next 15-minute mark

    // Then run every 15 minutes
    setInterval(fetchAllCryptoData, 15 * 60 * 1000);
  }, delayToNextRun);
};

scheduleFifteenMinuteFetch();

// Health check
app.get('/', (req, res) => {
  res.send('✅ Enhanced Crypto backend (BTC + ETH) is running!');
});

// Get latest hourly K-line data for BTC
app.get('/kline/hourly', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM btc_kline ORDER BY timestamp DESC LIMIT 100');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// Get latest hourly K-line data for ETH
app.get('/kline/hourly/eth', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM eth_kline ORDER BY timestamp DESC LIMIT 100');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// Generic endpoint to get data for any supported cryptocurrency
app.get('/kline/:symbol/hourly', async (req, res) => {
  const { symbol } = req.params;
  const config = CRYPTO_CONFIGS.find(c => c.symbol.toLowerCase() === symbol.toLowerCase());
  
  if (!config) {
    return res.status(404).json({ error: `Cryptocurrency ${symbol} not supported` });
  }

  try {
    const result = await db.query(`SELECT * FROM ${config.tableName} ORDER BY timestamp DESC LIMIT 100`);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// Serve historical K-line data with optional start/end for BTC
app.get('/kline', async (req, res) => {
  const { start, end, pair = 'BTC/USDT' } = req.query;

  try {
    let result;

    if (start && end) {
      result = await db.query(
        `SELECT * FROM btc_kline
         WHERE timestamp BETWEEN $1::timestamptz AND $2::timestamptz AND pair = $3
         ORDER BY timestamp ASC`,
        [start, end, pair]
      );
    } else {
      // fallback: latest 200 entries, still ordered by ASC
      result = await db.query(
        `SELECT * FROM (
        SELECT * FROM btc_kline
        WHERE pair = $1
        ORDER BY timestamp DESC
        LIMIT 200
        ) AS sub
        ORDER BY timestamp ASC`,
        [pair]
      );
    }

    res.json(result.rows);
  } catch (err) {
    console.error("❌ /kline error:", err);
    res.status(500).send('Server error');
  }
});

// Serve historical K-line data with optional start/end for ETH
app.get('/kline/eth', async (req, res) => {
  const { start, end, pair = 'ETH/USDT' } = req.query;

  try {
    let result;

    if (start && end) {
      result = await db.query(
        `SELECT * FROM eth_kline
         WHERE timestamp BETWEEN $1::timestamptz AND $2::timestamptz AND pair = $3
         ORDER BY timestamp ASC`,
        [start, end, pair]
      );
    } else {
      // fallback: latest 200 entries, still ordered by ASC
      result = await db.query(
        `SELECT * FROM (
        SELECT * FROM eth_kline
        WHERE pair = $1
        ORDER BY timestamp DESC
        LIMIT 200
        ) AS sub
        ORDER BY timestamp ASC`,
        [pair]
      );
    }

    res.json(result.rows);
  } catch (err) {
    console.error("❌ /kline/eth error:", err);
    res.status(500).send('Server error');
  }
});

// Generic endpoint for historical data
app.get('/kline/:symbol', async (req, res) => {
  const { symbol } = req.params;
  const { start, end } = req.query;
  
  const config = CRYPTO_CONFIGS.find(c => c.symbol.toLowerCase() === symbol.toLowerCase());
  
  if (!config) {
    return res.status(404).json({ error: `Cryptocurrency ${symbol} not supported` });
  }

  const { tableName, pair } = config;

  try {
    let result;

    if (start && end) {
      result = await db.query(
        `SELECT * FROM ${tableName}
         WHERE timestamp BETWEEN $1::timestamptz AND $2::timestamptz AND pair = $3
         ORDER BY timestamp ASC`,
        [start, end, pair]
      );
    } else {
      // fallback: latest 200 entries, still ordered by ASC
      result = await db.query(
        `SELECT * FROM (
        SELECT * FROM ${tableName}
        WHERE pair = $1
        ORDER BY timestamp DESC
        LIMIT 200
        ) AS sub
        ORDER BY timestamp ASC`,
        [pair]
      );
    }

    res.json(result.rows);
  } catch (err) {
    console.error(`❌ /kline/${symbol} error:`, err);
    res.status(500).send('Server error');
  }
});

// Get list of supported cryptocurrencies
app.get('/supported', (req, res) => {
  const supported = CRYPTO_CONFIGS.map(config => ({
    symbol: config.symbol,
    pair: config.pair,
    id: config.id
  }));
  res.json(supported);
});

// Get combined latest data for all supported cryptocurrencies
app.get('/kline/all/latest', async (req, res) => {
  try {
    const results = {};
    
    for (const config of CRYPTO_CONFIGS) {
      const result = await db.query(
        `SELECT * FROM ${config.tableName} ORDER BY timestamp DESC LIMIT 1`
      );
      results[config.symbol] = result.rows[0] || null;
    }
    
    res.json(results);
  } catch (err) {
    console.error("❌ /kline/all/latest error:", err);
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Enhanced Crypto Server (BTC + ETH) running on port ${PORT}`);
  console.log(`📊 Supported cryptocurrencies: ${CRYPTO_CONFIGS.map(c => c.symbol).join(', ')}`);
});