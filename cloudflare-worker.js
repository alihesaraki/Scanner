// OPTIONAL: your own free Cloudflare Worker as a fast, cached AND compact route to Binance.
// Setup: dash.cloudflare.com -> Workers & Pages -> Create -> Worker -> paste -> Deploy.
// Then put the Worker URL in index.html (window.SCANNER_PROXY).
//
//  /lite/prices  -> {"BTC":67000.1,...}  USDT perpetuals only, no timestamps (~85% smaller than Binance's reply)
//  /lite/klines  -> [[closeTime,high,low,close,quoteVolume],...]  5 fields instead of 12 (~60% smaller)
//  other whitelisted Binance paths are passed through unchanged (edge cached)

const TTL = { // edge cache seconds per pass-through endpoint (anything else is rejected)
  '/fapi/v1/exchangeInfo': 3600,
  '/fapi/v1/ticker/24hr': 20,
  '/fapi/v1/ticker/price': 10,
  '/fapi/v1/klines': 15,
};
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,OPTIONS', 'Access-Control-Max-Age': '86400' };
const SYM = /^[A-Z0-9]{1,20}USDT$/;
const INTERVALS = new Set(['15m', '1h', '4h', '1d', '1w', '1M']);
const BINANCE = 'https://fapi.binance.com';

const upstream = (path, ttl) => fetch(BINANCE + path, {
  headers: { Accept: 'application/json' },
  cf: { cacheEverything: true, cacheTtlByStatus: { '200-299': ttl, '400-599': 0 } },
});
const jsonRes = (body, ttl) => new Response(body, {
  headers: { ...CORS, 'Content-Type': 'application/json', 'Cache-Control': `public, max-age=${ttl}` },
});
const fail = (msg, status) => new Response(msg, { status, headers: CORS });

export default {
  async fetch(request) {
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    if (request.method !== 'GET') return fail('Not allowed', 403);
    const url = new URL(request.url);

    if (url.pathname === '/lite/prices') {
      const r = await upstream('/fapi/v1/ticker/price', 5);
      if (!r.ok) return fail('upstream ' + r.status, 502);
      const out = {};
      for (const t of await r.json()) if (SYM.test(t.symbol)) out[t.symbol.slice(0, -4)] = +t.price;
      return jsonRes(JSON.stringify(out), 5);
    }

    if (url.pathname === '/lite/klines') {
      const p = url.searchParams, symbol = p.get('symbol') || '', interval = p.get('interval') || '';
      const limit = Math.min(500, +p.get('limit') || 0), st = p.get('startTime');
      if (!SYM.test(symbol) || !INTERVALS.has(interval) || !limit || (st && !/^\d{1,15}$/.test(st))) return fail('Bad request', 400);
      const r = await upstream(`/fapi/v1/klines?symbol=${symbol}&interval=${interval}&limit=${limit}${st ? `&startTime=${st}` : ''}`, 15);
      if (!r.ok) return fail('upstream ' + r.status, 502);
      const ks = await r.json();
      return jsonRes(JSON.stringify(ks.map(k => [k[6], +k[2], +k[3], +k[4], +k[7]])), 15);
    }

    const ttl = TTL[url.pathname];
    if (ttl === undefined) return fail('Not allowed', 403);
    const r = await upstream(url.pathname + url.search, ttl);
    return new Response(r.body, {
      status: r.status,
      headers: { ...CORS, 'Content-Type': r.headers.get('Content-Type') || 'application/json', 'Cache-Control': `public, max-age=${ttl}` },
    });
  },
};
