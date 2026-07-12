// ═══════════════════════════════════════════════════════════════
//  STATE
// ═══════════════════════════════════════════════════════════════
const state = {
  symbol: 'BTCUSDT',
  marketType: 'futures',     // 'futures' or 'spot'
  bids: {},        // price(str) -> qty(float)
  asks: {},
  lastUpdateId: 0,
  trades: [],
  lastPrice: null,
  lastSide: null,
  orders: {},      // id -> order object
  selectedOrderId: null,
  priceDec: 2,
  qtyDec: 4,
  ws: null,
  ws2: null,
  reconnectTimer: null,
  reconnectTimer2: null,
  snapshotLoaded: false,
  bufferedEvents: [],
  tradeCount: 0,
  connectGen: 0,   // incremented on each connect() to cancel stale callbacks
  // Positions: symbol -> { netQty, avgEntry, realizedPnl, fills[] }
  positions: {},
  realizedPnl: 0,
  totalFees: 0,
  makerFeePercent: 0.02,
  takerFeePercent: 0.05,
  closedTrades: 0,
  executions: [],
  savedExecutionIds: new Set(),
  latencyMs: 100,          // simulated order round-trip latency in ms
  // Chart
  chart: null,
  candleSeries: null,
  currentCandle: null,   // { time, open, high, low, close }
  candleTf: 1,           // seconds per candle
};

const QUEUE_EPS = 1e-10;
const QUEUE_CANCEL_BEHIND_BIAS = 1.35; // >1 makes cancels slightly more likely behind us.

// ── Known Spot‑only FX pairs (Binance). Add as needed.
const SPOT_FX_SYMBOLS = new Set([
  'EURUSDT','GBPUSDT','AUDUSDT','NZDUSDT',
  'USDCAD','USDCHF','USDJPY','EURUSD','GBPUSD'  // note: Binance lists EURUSD etc. with USDT? Actually only against USDT.
]);
// ═══════════════════════════════════════════════════════════════
//  FORMATTING
// ═══════════════════════════════════════════════════════════════
function fp(p) {
  return Number(p).toLocaleString('en-US', {
    minimumFractionDigits: state.priceDec,
    maximumFractionDigits: state.priceDec,
  });
}
function fq(q) {
  return Number(q).toFixed(state.qtyDec);
}
function fts(ms) {
  const d = new Date(ms);
  return d.toTimeString().slice(0,8) + '.' + String(d.getMilliseconds()).padStart(3,'0');
}
function nowTs() {
  return fts(Date.now());
}
function genId() {
  return Math.random().toString(36).slice(2,10).toUpperCase();
}

// ═══════════════════════════════════════════════════════════════
//  DECIMAL DETECTION
// ═══════════════════════════════════════════════════════════════
function detectDecimals(sym) {
  sym = sym.toUpperCase();
  // Spot FX pairs typically have 5 price decimals and 2 quantity decimals
  if (SPOT_FX_SYMBOLS.has(sym)) {
    state.priceDec = 5;
    state.qtyDec = 2;
    return;
  }
  if (/BTC|ETH/.test(sym))               { state.priceDec = 2; state.qtyDec = 4; }
  else if (/BNB|SOL|AVAX|NEAR|APT|SUI/.test(sym)) { state.priceDec = 3; state.qtyDec = 3; }
  else if (/DOGE|SHIB|PEPE|FLOKI|BONK/.test(sym)) { state.priceDec = 6; state.qtyDec = 0; }
  else if (/XRP|ADA|MATIC|DOT|LINK/.test(sym))    { state.priceDec = 4; state.qtyDec = 1; }
  else                                             { state.priceDec = 4; state.qtyDec = 3; }
}

// ═══════════════════════════════════════════════════════════════
//  ORDER BOOK LOGIC
// ═══════════════════════════════════════════════════════════════
function bestBid() {
  const keys = Object.keys(state.bids);
  if (!keys.length) return null;
  return Math.max(...keys.map(Number));
}
function bestAsk() {
  const keys = Object.keys(state.asks);
  if (!keys.length) return null;
  return Math.min(...keys.map(Number));
}
function topBids(n = 16) {
  return Object.keys(state.bids).map(parseFloat)
    .sort((a, b) => b - a).slice(0, n)
    .map(p => [p, state.bids[p]]);
}
function topAsks(n = 16) {
  return Object.keys(state.asks).map(parseFloat)
    .sort((a, b) => a - b).slice(0, n)
    .map(p => [p, state.asks[p]]);
}

async function fetchSnapshot() {
  const gen = state.connectGen;
  const sym = state.symbol;
  const isSpot = state.marketType === 'spot';
  const url = isSpot
    ? `https://api.binance.com/api/v3/depth?symbol=${sym}&limit=100`
    : `https://fapi.binance.com/fapi/v1/depth?symbol=${sym}&limit=100`;
  try {
    const res = await fetch(url);
    if (state.connectGen !== gen) return;
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      const msg = err.msg || `HTTP ${res.status}`;
      setConnStatus('disconnected');
      setMsg(`⚠ Invalid symbol "${sym}": ${msg}`, 'var(--ask)');
      document.getElementById('header-sym').textContent = sym + ' ✗';
      return;
    }
    const data = await res.json();
    if (state.connectGen !== gen) return;
    if (!Array.isArray(data.bids) || !Array.isArray(data.asks)) {
      setMsg(`⚠ Unexpected response for "${sym}"`, 'var(--ask)');
      return;
    }
    state.bids = {};
    state.asks = {};
    for (const [p, q] of data.bids) {
      const qty = parseFloat(q);
      if (qty > 0) state.bids[parseFloat(p)] = qty;
    }
    for (const [p, q] of data.asks) {
      const qty = parseFloat(q);
      if (qty > 0) state.asks[parseFloat(p)] = qty;
    }
    state.lastUpdateId = data.lastUpdateId;
    state.snapshotLoaded = true;
    console.log(`[Snapshot] Loaded for ${sym} (${state.marketType}), lastUpdateId=${data.lastUpdateId}`);
    for (const ev of state.bufferedEvents) applyDepthEvent(ev);
    state.bufferedEvents = [];
  } catch(e) {
    if (state.connectGen !== gen) return;
    console.error('Snapshot fetch failed:', e);
    setMsg(`⚠ Snapshot fetch error — retrying…`, 'var(--yellow)');
    setTimeout(fetchSnapshot, 3000);
  }
}

function applyDepthEvent(ev) {
  if (!state.snapshotLoaded) { state.bufferedEvents.push(ev); return; }
  if (ev.u <= state.lastUpdateId) return;
  for (const [p, q] of (ev.b || [])) {
    const px = parseFloat(p), qty = parseFloat(q);
    const prevQty = state.bids[px] || 0;
    if (qty === 0) delete state.bids[px]; else state.bids[px] = qty;
    updateQueueFromDepth('BUY', px, prevQty, qty);
  }
  for (const [p, q] of (ev.a || [])) {
    const px = parseFloat(p), qty = parseFloat(q);
    const prevQty = state.asks[px] || 0;
    if (qty === 0) delete state.asks[px]; else state.asks[px] = qty;
    updateQueueFromDepth('SELL', px, prevQty, qty);
  }
  state.lastUpdateId = ev.u;
}

// ═══════════════════════════════════════════════════════════════
//  FILL ENGINE  (unchanged)
// ═══════════════════════════════════════════════════════════════
