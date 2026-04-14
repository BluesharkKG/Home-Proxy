const express = require('express');
const compression = require('compression');
const { Agent, fetch } = require('undici');
const { PassThrough, pipeline } = require('node:stream');
const { promisify } = require('node:util');

const asyncPipeline = promisify(pipeline);
const app = express();

app.disable('x-powered-by');

const PORT = 3000;
const HOST = '0.0.0.0';
const CACHE_TTL_MS = 60_000;
const FETCH_TIMEOUT_MS = 8_000;
const MAX_CACHE_BODY_BYTES = 2 * 1024 * 1024;

const cache = new Map();

const upstreamAgent = new Agent({
  connect: { timeout: FETCH_TIMEOUT_MS },
  keepAliveTimeout: 10_000,
  keepAliveMaxTimeout: 60_000,
  connections: 100,
  pipelining: 1
});

app.use(compression({ threshold: 256 }));
app.use(express.static('public', { maxAge: '5m', etag: true }));

app.use((req, res, next) => {
  res.removeHeader('X-Powered-By');
  res.setHeader('Server', '');

  if (req.method !== 'GET') {
    next();
    return;
  }

  const cached = cache.get(req.originalUrl);
  if (!cached || cached.expiresAt <= Date.now()) {
    if (cached) cache.delete(req.originalUrl);
    next();
    return;
  }

  for (const [key, value] of Object.entries(cached.headers)) {
    if (value !== undefined) res.setHeader(key, value);
  }

  res.status(cached.status).send(cached.body);
});

app.get('/', (req, res) => {
  res.sendFile('index.html', { root: 'public' });
});

app.get('/search', (req, res) => {
  const query = (req.query.q || '').toString().trim();

  const payload = {
    query,
    results: query
      ? [
          {
            title: `Top result for "${query}"`,
            url: `https://example.com/search?q=${encodeURIComponent(query)}`,
            snippet: `Fast mock result for ${query}. Replace this with a real search backend later.`
          },
          {
            title: `Related: ${query} guide`,
            url: `https://example.com/guide/${encodeURIComponent(query)}`,
            snippet: `Another lightweight result for ${query}.`
          }
        ]
      : []
  };

  const body = JSON.stringify(payload);
  cache.set(req.originalUrl, {
    status: 200,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'public, max-age=60'
    },
    body,
    expiresAt: Date.now() + CACHE_TTL_MS
  });

  res
    .status(200)
    .set('Content-Type', 'application/json; charset=utf-8')
    .set('Cache-Control', 'public, max-age=60')
    .send(body);
});

app.get('/fetch', async (req, res) => {
  const target = (req.query.url || '').toString().trim();

  if (!target) {
    res.status(400).json({ error: 'Missing url query parameter.' });
    return;
  }

  let parsed;
  try {
    parsed = new URL(target);
    if (!['http:', 'https:'].includes(parsed.protocol)) {
      throw new Error('Invalid protocol');
    }
  } catch {
    res.status(400).json({ error: 'Invalid URL. Use http or https.' });
    return;
  }

  const abortController = new AbortController();
  const timeout = setTimeout(() => abortController.abort(), FETCH_TIMEOUT_MS);

  try {
    const upstream = await fetch(parsed, {
      method: 'GET',
      redirect: 'follow',
      dispatcher: upstreamAgent,
      signal: abortController.signal,
      headers: {
        'accept-encoding': 'gzip, deflate, br'
      }
    });

    if (!upstream.body) {
      res.status(502).json({ error: 'Upstream response has no body.' });
      return;
    }

    const headersToForward = {
      'Content-Type': upstream.headers.get('content-type') || 'application/octet-stream',
      'Cache-Control': 'public, max-age=60'
    };

    const contentLength = upstream.headers.get('content-length');
    if (contentLength) headersToForward['Content-Length'] = contentLength;

    const encoding = upstream.headers.get('content-encoding');
    if (encoding) headersToForward['Content-Encoding'] = encoding;

    res.status(upstream.status);
    Object.entries(headersToForward).forEach(([k, v]) => res.setHeader(k, v));

    const tee = new PassThrough();
    const chunks = [];
    let totalBytes = 0;

    tee.on('data', (chunk) => {
      totalBytes += chunk.length;
      if (totalBytes <= MAX_CACHE_BODY_BYTES) chunks.push(chunk);
    });

    await Promise.all([
      asyncPipeline(upstream.body, tee),
      asyncPipeline(tee, res)
    ]);

    if (upstream.status >= 200 && upstream.status < 300 && totalBytes <= MAX_CACHE_BODY_BYTES) {
      cache.set(req.originalUrl, {
        status: upstream.status,
        headers: headersToForward,
        body: Buffer.concat(chunks),
        expiresAt: Date.now() + CACHE_TTL_MS
      });
    }
  } catch {
    if (!res.headersSent) {
      res.status(502).json({ error: 'Failed to fetch upstream URL.' });
    } else {
      res.end();
    }
  } finally {
    clearTimeout(timeout);
  }
});

setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (entry.expiresAt <= now) cache.delete(key);
  }
}, 15_000).unref();

const server = app.listen(PORT, HOST, () => {
  // Intentionally quiet for reduced console noise.
});

server.keepAliveTimeout = 61_000;
server.headersTimeout = 62_000;
