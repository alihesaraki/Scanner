/* ============================================================================
 * Network layer
 * Uses Binance Futures directly when possible, then falls back to CORS proxies.
 * Successful connection modes are remembered to improve subsequent requests.
 * ========================================================================== */
const DIRECT_API = 'https://fapi.binance.com';
const CUSTOM_PROXY_BASE = ''; // optional: your own cloudflare-worker.js URL
const CORS_PROXIES = [
  full => `https://corsproxy.io/?url=${encodeURIComponent(full)}`,
  full => `https://api.allorigins.win/raw?url=${encodeURIComponent(full)}`
];
const MODES = (CUSTOM_PROXY_BASE ? [CUSTOM_PROXY_BASE] : []).concat(['DIRECT'], CORS_PROXIES);
const TOTAL_MODES = MODES.length;
function buildUrl(mode, path) {
  const m = MODES[mode];
  if (m === 'DIRECT') return DIRECT_API + path;
  if (typeof m === 'function') return m(DIRECT_API + path);
  return m + path;
}
const readJSON = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch (e) { return d; } };
let activeMode = Number(localStorage.getItem('signalApiMode') || 0);
if (!(Number.isInteger(activeMode) && activeMode >= 0 && activeMode < TOTAL_MODES)) activeMode = 0;
const modeLabel = m => MODES[m] === 'DIRECT' ? 'direct' : (m === 0 && CUSTOM_PROXY_BASE ? 'custom proxy' : `proxy #${m}`);

let pauseUntil = 0;
async function fetchOnce(mode, path) {
  const w = pauseUntil - Date.now();
  if (w > 0) await sleep(w);
  const ctrl = new AbortController(), to = setTimeout(() => ctrl.abort(), 20000);
  try {
    const r = await fetch(buildUrl(mode, path), { cache: 'no-store', signal: ctrl.signal });
    if (!r.ok) {
      if (r.status === 429 || r.status === 418) pauseUntil = Date.now() + (r.status === 418 ? 60e3 : 10e3);
      const err = new Error(`${r.status} ${r.statusText}`); err.status = r.status; throw err;
    }
    return await r.json();
  } finally { clearTimeout(to); }
}
async function getJSON(path, retriesPerMode = 2) {
  let lastErr; const log = [];
  for (let step = 0; step < TOTAL_MODES; step++) {
    const mode = (activeMode + step) % TOTAL_MODES; let modeErr;
    for (let i = 0; i < retriesPerMode; i++) {
      try {
        const data = await fetchOnce(mode, path);
        if (mode !== activeMode) { activeMode = mode; localStorage.setItem('signalApiMode', String(mode)); }
        return data;
      } catch (x) {
        lastErr = modeErr = x;
        if (x.status && x.status !== 429 && x.status < 500) break;
        await sleep(600 * (i + 1));
      }
    }
    log.push(`${modeLabel(mode)}: ${modeErr?.message || 'network error (fetch blocked/failed)'}`);
  }
  const err = lastErr || new Error('Request failed on all connection modes');
  err.summary = log.join(' | '); throw err;
}
const concurrency = () => MODES[activeMode] === 'DIRECT' || typeof MODES[activeMode] === 'string' ? 40 : 8;
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ============================================================================
 * Application configuration and shared state
 * ========================================================================== */
const IV_MS = { '15m': 15 * 60e3, '1h': 36e5, '4h': 4 * 36e5, '1d': 864e5, '1w': 7 * 864e5, '1M': 30 * 864e5 };
const LIMIT = { '15m': 99, '1h': 99, '4h': 99, '1d': 40, '1w': 30, '1M': 30 };
const TRIM  = { '15m': 110, '1h': 110, '4h': 110, '1d': 45, '1w': 35, '1M': 35 };
const TFS = ['15m', '1h', '4h', '1d', '1w', '1M'];
const TF_LABELS = { '15m': '15M', '1h': '1H', '4h': '4H', '1d': '1D', '1w': '1W', '1M': '1M' };
const SMMA_LEN = 7, RSI_LEN = 14, DMI_LEN = 14;
const PRICE_INTERVAL = 5e3;   // live price refresh (tiny request)
const CACHE_SAVE_EVERY = 60e3;
const CACHE_KEY = 'signalScannerCacheV2';

const cache = readJSON(CACHE_KEY, {});
const state = {
  coins: new Map(),
  sortKey: 'volume', sortDir: -1,
  filter: null,      // null | 'LONG' | 'SHORT'
  search: '', timeframe: localStorage.getItem('signalTimeframe') || '1d',
  marketBusy: false, syncing: false, lastPrice: 0
};
const rowsEl = document.getElementById('rows');
const statusText = document.getElementById('statusText');
const statusDot = document.getElementById('statusDot');
const longBtn = document.getElementById('longBtn');
const shortBtn = document.getElementById('shortBtn');
const searchInput = document.getElementById('searchInput');
const timeframeSelect = document.getElementById('timeframeSelect');
if (!TFS.includes(state.timeframe)) state.timeframe = '1d';
if (timeframeSelect) timeframeSelect.value = state.timeframe;

const escapeHtml = s => String(s).replace(/[&<>'"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[ch]));
function setStatus(t, type = '') { statusText.textContent = t; statusDot.className = `status-dot ${type}`; }
const setOk = () => setStatus(`${state.coins.size} Binance USDT perpetuals${activeMode !== 0 ? ' • via ' + modeLabel(activeMode) : ''}`, 'ok');

/* ============================================================================
 * Formatting helpers
 * ========================================================================== */
const fmtPrice = n => {
  if (!Number.isFinite(n)) return '--';
  const a = Math.abs(n), d = a >= 1 ? 2 : a >= .1 ? 4 : a >= .01 ? 6 : 8;
  return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
};
const calcPct = (o, p) => Number.isFinite(o) && o !== 0 && Number.isFinite(p) ? ((p - o) / o) * 100 : null;
const fmtVol = n => {
  if (!Number.isFinite(n)) return '--';
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
};

/* ============================================================================
 * Technical indicator calculations
 * All trend calculations use the selected timeframe's candle data.
 * ========================================================================== */
// SMMA(7) of closed candles vs live price -> 'UP' / 'DOWN' / null
function smmaValue(closes) {
  if (closes.length < SMMA_LEN) return null;
  let v = closes.slice(0, SMMA_LEN).reduce((a, b) => a + b, 0) / SMMA_LEN;
  for (let i = SMMA_LEN; i < closes.length; i++) v = (v * (SMMA_LEN - 1) + closes[i]) / SMMA_LEN;
  return v;
}
// Wilder RSI(14): state after the last CLOSED candle, then the live price is applied as the forming candle.
function rsiState(closes) {
  const len = RSI_LEN;
  if (closes.length < len + 1) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= len; i++) { const d = closes[i] - closes[i - 1]; if (d >= 0) gains += d; else losses -= d; }
  let ag = gains / len, al = losses / len;
  for (let i = len + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    ag = (ag * (len - 1) + (d > 0 ? d : 0)) / len;
    al = (al * (len - 1) + (d < 0 ? -d : 0)) / len;
  }
  return { ag, al, last: closes[closes.length - 1] };
}
function rsiLive(st, price) {
  if (!st || !Number.isFinite(price)) return null;
  const d = price - st.last, len = RSI_LEN;
  const ag = (st.ag * (len - 1) + (d > 0 ? d : 0)) / len, al = (st.al * (len - 1) + (d < 0 ? -d : 0)) / len;
  if (ag === 0 && al === 0) return 50;
  if (al === 0) return 100;
  return 100 - 100 / (1 + ag / al);
}
// Wilder DMI/ADX(14) -> {pDI,mDI,adx} or null
function dmiRaw(h, l, c) {
  const len = DMI_LEN;
  if (h.length < len * 2 + 1) return null;
  let trS = 0, pDMS = 0, mDMS = 0;
  for (let i = 1; i <= len; i++) {
    const up = h[i] - h[i - 1], down = l[i - 1] - l[i];
    const pDM = (up > down && up > 0) ? up : 0, mDM = (down > up && down > 0) ? down : 0;
    const tr = Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]));
    trS += tr; pDMS += pDM; mDMS += mDM;
  }
  let pDI = trS ? 100 * pDMS / trS : 0, mDI = trS ? 100 * mDMS / trS : 0;
  let dx = (pDI + mDI) ? Math.abs(pDI - mDI) / (pDI + mDI) * 100 : 0;
  const dxArr = [dx];
  for (let i = len + 1; i < h.length; i++) {
    const up = h[i] - h[i - 1], down = l[i - 1] - l[i];
    const pDM = (up > down && up > 0) ? up : 0, mDM = (down > up && down > 0) ? down : 0;
    const tr = Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]));
    trS = trS - trS / len + tr; pDMS = pDMS - pDMS / len + pDM; mDMS = mDMS - mDMS / len + mDM;
    pDI = trS ? 100 * pDMS / trS : 0; mDI = trS ? 100 * mDMS / trS : 0;
    dx = (pDI + mDI) ? Math.abs(pDI - mDI) / (pDI + mDI) * 100 : 0;
    dxArr.push(dx);
  }
  if (dxArr.length < len) return null;
  let adx = dxArr.slice(0, len).reduce((a, b) => a + b, 0) / len;
  for (let i = len; i < dxArr.length; i++) adx = (adx * (len - 1) + dxArr[i]) / len;
  return { pDI, mDI, adx };
}
// +DI above ADX above -DI => UP ; -DI above ADX above +DI => DOWN ; else NEUTRAL ; no data => null
function dmiTrend(d) {
  if (!d) return null;
  if (d.pDI > d.adx && d.adx > d.mDI) return 'UP';
  if (d.mDI > d.adx && d.adx > d.pDI) return 'DOWN';
  return 'RANGE';
}
// Light update used on every price tick (candle-based indicators are untouched).
function recomputeLive(c) {
  const data = c[state.timeframe];
  c.smma = c.smmaV == null || !Number.isFinite(c.price) ? null : c.price >= c.smmaV ? 'UP' : 'DOWN';
  c.rsi = rsiLive(c.rsiS, c.price);
  c.pctDay = data && data.c.length ? calcPct(data.c[data.c.length - 1], c.price) : null;
}
// Full recompute: only when candles (or the selected timeframe) change.
function recompute(c) {
  const data = c[state.timeframe] || { c: [], h: [], l: [] };
  c.smmaV = smmaValue(data.c);
  c.rsiS = rsiState(data.c);
  c.dmi = dmiTrend(dmiRaw(data.h || [], data.l || [], data.c || []));
  // Volume = quote volume of the last closed candle of the selected timeframe
  c.volume = Number.isFinite(data.lv) ? data.lv : NaN;
  recomputeLive(c);
}

function newCoin(symbol, price) {
  const c = { symbol, price, volume: NaN };
  for (const tf of TFS) { const cached = cache.k?.[tf]?.[symbol]; if (cached) c[tf] = cached; }
  recompute(c);
  return c;
}

/* ================= candle loading (only new closed candles are ever downloaded) ================= */
function nextClose(tf, n) { // close time of the candle that opens at n
  if (tf === '1M') { const d = new Date(n); return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1) - 1; }
  return n + IV_MS[tf] - 1;
}
function needsUpdate(c, tf, now) {
  if (c._f?.[tf] && now - c._f[tf] < 30e3) return false;       // recent failure: back off
  const st = c[tf];
  if (!st || st.lv === undefined) return true;
  if (st.t && now - st.t < 3e3) return false;                  // just fetched
  return now >= nextClose(tf, st.n);
}
async function loadCandles(c, tf, now) {
  let st = c[tf];
  if (st && (st.lv === undefined || (now - st.n) / IV_MS[tf] > LIMIT[tf])) st = null; // old cache format / far too stale -> full reload
  const cur = st || { n: 0, o: [], h: [], l: [], c: [], lv: null };
  const limit = st ? Math.min(LIMIT[tf], Math.ceil((now - st.n) / IV_MS[tf]) + 2) : LIMIT[tf];
  const ks = await getJSON(`/fapi/v1/klines?symbol=${encodeURIComponent(c.symbol)}&interval=${tf}&limit=${limit}${st ? `&startTime=${st.n}` : ''}`);
  if (!Array.isArray(ks)) return;
  for (const k of ks) {
    const closeTime = Number(k[6]);
    if (closeTime > now) continue; // still-forming candle: excluded
    if (closeTime < cur.n) continue; // already have this one
    cur.o.push(Number(k[1])); cur.h.push(Number(k[2])); cur.l.push(Number(k[3])); cur.c.push(Number(k[4]));
    cur.lv = Number(k[7]); // quote volume of this closed candle
    cur.n = closeTime + 1;
  }
  const trim = TRIM[tf];
  if (cur.c.length > trim) { cur.o = cur.o.slice(-trim); cur.h = cur.h.slice(-trim); cur.l = cur.l.slice(-trim); cur.c = cur.c.slice(-trim); }
  cur.t = cur.c.length ? now : now + 57e3; // empty symbol: retry only after ~1 min
  c[tf] = cur;
}
let syncRequested = false;
async function syncCandles() {
  if (state.syncing) { syncRequested = true; return; }
  state.syncing = true;
  try {
    const now = Date.now();
    const tf = state.timeframe;
    const vv = c => Number.isFinite(c.volume) ? c.volume : -1;
    const todo = [...state.coins.values()].filter(c => needsUpdate(c, tf, now)).sort((a, b) => vv(b) - vv(a));
    const total = todo.length; let p = 0, n = 0;
    async function worker() {
      while (p < total) {
        const c = todo[p++];
        try { await loadCandles(c, tf, now); if (tf === state.timeframe) recompute(c); }
        catch (e) { (c._f ||= {})[tf] = Date.now(); console.warn('klines', c.symbol, e.message || e); }
        if (total > 20 && ++n % 10 === 0) { setStatus(`Loading data… ${n}/${total}`); scheduleRender(); }
      }
    }
    if (total) {
      if (total > 20) setStatus(`Loading data… 0/${total}`);
      await Promise.all(Array.from({ length: Math.min(concurrency(), total) }, worker));
      saveCache();
    }
    scheduleRender(); setOk();
  } finally {
    state.syncing = false;
    // If the user changed timeframe during a sync, immediately fetch the newly selected interval.
    if (syncRequested) { syncRequested = false; queueMicrotask(() => syncCandles()); }
  }
}

/* ================= persistent cache ================= */
let saveTimer = 0, lastSave = 0;
function saveCache() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const k = Object.fromEntries(TFS.map(tf => [tf, {}])), m = {};
    for (const c of state.coins.values()) {
      for (const tf of TFS) if (c[tf]) k[tf][c.symbol] = c[tf];
      m[c.symbol] = c.price;
    }
    lastSave = Date.now();
    try { localStorage.setItem(CACHE_KEY, JSON.stringify({ v: 2, k, m })); } catch (e) {}
  }, 1000);
}

/* ================= market data (one tiny request: symbols + live prices) ================= */
const validSymbol = s => /^[A-Z0-9]{1,20}USDT$/.test(s); // ASCII-only USDT perpetuals (drops dated futures and Chinese-named coins)
function applyTicker(ts) {
  const seen = new Set();
  for (const t of ts) {
    const sym = t.symbol;
    if (!validSymbol(sym)) continue;
    const price = Number(t.price);
    if (!Number.isFinite(price)) continue;
    seen.add(sym);
    const c = state.coins.get(sym);
    if (!c) state.coins.set(sym, newCoin(sym, price));
    else { c.price = price; recomputeLive(c); }
  }
  for (const s of state.coins.keys()) if (!seen.has(s)) state.coins.delete(s);
}
async function refreshMarket() {
  if (state.marketBusy) return;
  state.marketBusy = true;
  try {
    let ts = null;
    if (window.__pre) { try { ts = await window.__pre; } catch (e) {} window.__pre = null; }
    if (!Array.isArray(ts)) ts = await getJSON('/fapi/v1/ticker/price');
    applyTicker(ts);
    state.lastPrice = Date.now();
    scheduleRender();
    if (Date.now() - lastSave > CACHE_SAVE_EVERY) saveCache();
    if (!state.syncing) {
      const now = Date.now(), tf = state.timeframe;
      let need = false;
      for (const c of state.coins.values()) if (needsUpdate(c, tf, now)) { need = true; break; }
      if (need) syncCandles(); else setOk();
    }
  } catch (e) {
    console.error(e);
    if (state.coins.size) setStatus('Update failed • retrying automatically', 'error');
    else {
      const detail = e.summary || e.message || '';
      setStatus(`Could not reach Binance via ${TOTAL_MODES} connection mode(s)${detail ? ' — ' + detail : ''}`, 'error');
      rowsEl.innerHTML = `<tr><td colspan="7" class="loading">${escapeHtml(detail)}</td></tr>`;
    }
  } finally { state.marketBusy = false; }
}

/* ================= rendering (keyed rows: only changed rows touch the DOM) ================= */
function trendCell(v) {
  if (v === 'UP') return '<div class="cell-box up"><span>UP</span></div>';
  if (v === 'DOWN') return '<div class="cell-box down"><span>DOWN</span></div>';
  if (v === 'RANGE') return '<div class="cell-box range"><span>RANGE</span></div>';
  return '<div class="cell-box neutral">--</div>';
}
function rsiCell(v) {
  if (!Number.isFinite(v)) return '<div class="cell-box neutral">--</div>';
  return `<div class="cell-box ${v >= 50 ? 'up' : 'down'}"><span>${v.toFixed(1)}</span></div>`;
}
function candleCell(p) {
  if (!Number.isFinite(p)) return '<div class="cell-plain dim">--</div>';
  return `<div class="cell-plain">${p >= 0 ? '+' : ''}${p.toFixed(2)}%</div>`;
}
const rowHTML = c => `<td class="pair"><span class="pair-name">${escapeHtml(c.symbol.replace(/USDT$/, ''))}</span></td>
  <td class="price">${fmtPrice(c.price)}</td>
  <td>${candleCell(c.pctDay)}</td>
  <td class="volume">${fmtVol(c.volume)}</td>
  <td>${trendCell(c.smma)}</td>
  <td>${rsiCell(c.rsi)}</td>
  <td>${trendCell(c.dmi)}</td>
`;

function sortValue(c, k) {
  switch (k) {
    case 'symbol': return c.symbol;
    case 'price': return c.price;
    case 'volume': return Number.isFinite(c.volume) ? c.volume : -Infinity;
    case 's4': return c.smma || '';
    case 'pctDay': return Number.isFinite(c.pctDay) ? c.pctDay : -Infinity;
    case 'rsi': return Number.isFinite(c.rsi) ? c.rsi : -Infinity;
    case 'dmi': return c.dmi || '';
  }
  return 0;
}
function passesFilter(c) {
  if (state.search && !c.symbol.replace(/USDT$/, '').startsWith(state.search)) return false;
  // LONG/SHORT filters require all three indicators to agree on the selected timeframe.
  if (state.filter === 'LONG') {
    return c.smma === 'UP' && Number.isFinite(c.rsi) && c.rsi >= 50 && c.dmi === 'UP';
  }
  if (state.filter === 'SHORT') {
    return c.smma === 'DOWN' && Number.isFinite(c.rsi) && c.rsi < 50 && c.dmi === 'DOWN';
  }
  return true;
}
function sortedCoins() {
  const a = [...state.coins.values()].filter(passesFilter);
  a.sort((x, y) => {
    // Search results prioritize the shortest base-asset ticker first, then alphabetically.
    if (state.search) {
      const xs = x.symbol.replace(/USDT$/, ''), ys = y.symbol.replace(/USDT$/, '');
      return xs.length - ys.length || xs.localeCompare(ys);
    }
    const av = sortValue(x, state.sortKey), bv = sortValue(y, state.sortKey);
    return typeof av === 'string' ? state.sortDir * av.localeCompare(bv) : state.sortDir * ((av ?? 0) - (bv ?? 0));
  });
  return a;
}
const emptyMsg = () => {
  if (state.search) return `No coins starting with "${state.search}".`;
  if (state.filter === 'LONG') return `No ${TF_LABELS[state.timeframe]} LONG signals where SMMA 7, RSI, and DMI are all positive.`;
  if (state.filter === 'SHORT') return `No ${TF_LABELS[state.timeframe]} SHORT signals where SMMA 7, RSI, and DMI are all negative.`;
  return 'No coins found.';
};

const rowEls = new Map(); let lastOrder = [];
function render() {
  const cs = sortedCoins();
  if (!cs.length) {
    rowsEl.innerHTML = `<tr><td colspan="7" class="loading">${emptyMsg()}</td></tr>`;
    rowEls.clear(); lastOrder = [];
  } else {
    let changed = cs.length !== lastOrder.length;
    for (const c of cs) {
      let tr = rowEls.get(c.symbol);
      if (!tr) { tr = document.createElement('tr'); rowEls.set(c.symbol, tr); changed = true; }
      const h = rowHTML(c);
      if (tr._h !== h) { tr.innerHTML = h; tr._h = h; }
    }
    if (!changed) for (let i = 0; i < cs.length; i++) if (cs[i] !== lastOrder[i]) { changed = true; break; }
    if (changed) {
      for (const s of rowEls.keys()) if (!state.coins.has(s)) rowEls.delete(s);
      const f = document.createDocumentFragment();
      for (const c of cs) f.appendChild(rowEls.get(c.symbol));
      rowsEl.replaceChildren(f);
    }
    lastOrder = cs;
  }
  document.querySelectorAll('th[data-sort]').forEach(th => {
    th.classList.toggle('sorted-asc', th.dataset.sort === state.sortKey && state.sortDir === 1);
    th.classList.toggle('sorted-desc', th.dataset.sort === state.sortKey && state.sortDir === -1);
  });
  longBtn.textContent = state.filter === 'LONG' ? `LONG · ${cs.length}` : 'LONG';
  shortBtn.textContent = state.filter === 'SHORT' ? `SHORT · ${cs.length}` : 'SHORT';
  document.querySelectorAll('[data-tf-label]').forEach(el => { el.textContent = TF_LABELS[state.timeframe]; });
  const L = TF_LABELS[state.timeframe], set = (id, t) => { const el = document.getElementById(id); if (el) el.textContent = t; };
  set('volHead', `VOL (${L})`); set('smmaHead', `SMMA7 (${L})`); set('rsiHead', `RSI (${L})`); set('dmiHead', `DMI (${L})`); set('candleHead', `CANDLE (${L})`);
}
let renderTimer = 0;
function scheduleRender() { if (renderTimer) return; renderTimer = setTimeout(() => { renderTimer = 0; render(); }, 250); }

if (timeframeSelect) timeframeSelect.addEventListener('change', () => {
  const next = timeframeSelect.value; if (!TFS.includes(next) || next === state.timeframe) return;
  state.timeframe = next; localStorage.setItem('signalTimeframe', next);
  document.querySelectorAll('th[data-sort]').forEach(th => { th.classList.remove('sorted-asc', 'sorted-desc'); });
  state.sortKey = 'volume'; state.sortDir = -1;
  for (const c of state.coins.values()) recompute(c);
  render(); setStatus(`Loading ${TF_LABELS[next]} data…`); syncCandles();
});

document.querySelectorAll('th[data-sort]').forEach(th => th.addEventListener('click', () => {
  const k = th.dataset.sort;
  if (state.sortKey === k) state.sortDir *= -1; else { state.sortKey = k; state.sortDir = k === 'symbol' ? 1 : -1; }
  render();
}));
function updateFilterUI() {
  longBtn.classList.toggle('on', state.filter === 'LONG');
  shortBtn.classList.toggle('on', state.filter === 'SHORT');
  longBtn.setAttribute('aria-pressed', state.filter === 'LONG');
  shortBtn.setAttribute('aria-pressed', state.filter === 'SHORT');
}
function setFilter(name) {
  state.filter = state.filter === name ? null : name;
  updateFilterUI();
  document.querySelector('.table-wrap').scrollTop = 0;
  render();
}
longBtn.addEventListener('click', () => setFilter('LONG'));
shortBtn.addEventListener('click', () => setFilter('SHORT'));
updateFilterUI();
// Persian (ISIRI 9147) keyboard -> the English key on the same physical position.
const persianKeyboardMap = {
  'ض':'q','ص':'w','ث':'e','ق':'r','ف':'t','غ':'y','ع':'u','ه':'i','خ':'o','ح':'p',
  'ش':'a','س':'s','ی':'d','ب':'f','ل':'g','ا':'h','ت':'j','ن':'k','م':'l',
  'ظ':'z','ط':'x','ز':'c','ر':'v','ذ':'b','د':'n','پ':'m',
  // Arabic-keyboard variants of the same keys
  'ي':'d','ى':'d','ئ':'d','ك':';','ة':'h','ۀ':'h','أ':'h','إ':'h','آ':'h','ؤ':',',
  // Arabic presentation forms of meem / lam etc. are normalised by NFKC below
  '۰':'0','۱':'1','۲':'2','۳':'3','۴':'4','۵':'5','۶':'6','۷':'7','۸':'8','۹':'9',
  '٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'
};
function normalizeCoinSearch(value) {
  return [...String(value).normalize('NFKC').replace(/[\u200c\u200d\u200e\u200f\u064b-\u065f]/g, '')]
    .map(ch => persianKeyboardMap[ch] || ch)
    .join('')
    .replace(/[^A-Za-z0-9]/g, '')
    .toUpperCase();
}
// Exact physical-key letter for a keydown (works whatever layout is active).
function physicalChar(e) {
  const m = /^(?:Key([A-Z])|Digit([0-9]))$/.exec(e.code || '');
  return m ? (m[1] || m[2]).toLowerCase() : null;
}

searchInput.addEventListener('keydown', e => {
  if (e.ctrlKey || e.metaKey || e.altKey || e.isComposing || e.key.length !== 1 || e.key.charCodeAt(0) < 128) return;
  const ch = physicalChar(e); if (!ch) return;
  e.preventDefault();
  const a = searchInput.selectionStart ?? searchInput.value.length, b = searchInput.selectionEnd ?? a;
  searchInput.setRangeText(ch, a, b, 'end');
  searchInput.dispatchEvent(new Event('input', { bubbles: true }));
});
// Fallback for paste / mobile keyboards / IME: convert characters via the map.
searchInput.addEventListener('input', () => {
  const caretBefore = searchInput.selectionStart ?? searchInput.value.length;
  const normalized = normalizeCoinSearch(searchInput.value);
  if (searchInput.value !== normalized) {
    const caret = Math.min(normalized.length, normalizeCoinSearch(searchInput.value.slice(0, caretBefore)).length);
    searchInput.value = normalized;
    try { searchInput.setSelectionRange(caret, caret); } catch (_) {}
  }
  state.search = normalized;
  render();
});
// Typing anywhere over the scanner table opens the coin search and keeps the existing view intact.
document.addEventListener('keydown', event => {
  if (event.ctrlKey || event.metaKey || event.altKey || event.key.length !== 1) return;
  const target = event.target;
  if (target && (target.matches('input, textarea, select') || target.isContentEditable)) return;
  searchInput.focus();
  const start = searchInput.selectionStart ?? searchInput.value.length;
  const end = searchInput.selectionEnd ?? start;
  searchInput.setRangeText(event.key.charCodeAt(0) >= 128 && physicalChar(event) || event.key, start, end, 'end');
  searchInput.dispatchEvent(new Event('input', { bubbles: true }));
  event.preventDefault();
});

/* ============================================================================
 * Refresh scheduling and application startup
 * ========================================================================== */
async function priceLoop() {
  if (!document.hidden) await refreshMarket();
  setTimeout(priceLoop, PRICE_INTERVAL);
}

function init() {
  if (cache.v === 2 && cache.m) {
    for (const s in cache.m) if (validSymbol(s)) state.coins.set(s, newCoin(s, cache.m[s]));
    if (state.coins.size) { render(); setStatus('Showing saved data • updating…'); }
  } else setStatus('Loading Binance USDT perpetuals…');
  priceLoop();
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshMarket(); });
}
init();
