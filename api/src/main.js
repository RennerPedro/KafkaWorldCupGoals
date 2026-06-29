const express = require('express');

const app = express();
const port = process.env.PORT || 3001;

app.get('/health', (_req, res) => {
  res.json({
    service: 'api',
    status: 'ok',
    ts: new Date().toISOString(),
  });
});

app.listen(port, () => {
  console.log(`API listening on ${port}`);
});
