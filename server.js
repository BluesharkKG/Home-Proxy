const express = require('express');

const app = express();

const host = '0.0.0.0';
const port = Number(process.env.PORT) || 8080;

app.disable('x-powered-by');

app.get('/api/ping', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/', (req, res) => {
  res.type('text/plain').send('Backend running on a-Shell. Frontend will be added later.');
});

app.listen(port, host);
