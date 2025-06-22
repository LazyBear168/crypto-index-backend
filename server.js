// File name: server.js
// Author: Sunny

require('dotenv').config();
const express = require('express');
const app = express();
const db = require('./db');
const axios = require('axios');

app.use(express.json());

app.get('/kline', async (req, res) => {
  const result = await db.query('SELECT * FROM btc_kline ORDER BY timestamp ASC');
  res.json(result.rows);
});

app.listen(process.env.PORT || 3000, () => {
  console.log('Server is running...');
});
