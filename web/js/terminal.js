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
function placeOrder(side, price, qty) {
  const id = genId();
  const px = parseFloat(price);
  const queueAhead = side === 'BUY'
    ? (state.bids[px] || 0)
    : (state.asks[px] || 0);
  state.orders[id] = {
    id, side, price: px, qty: parseFloat(qty),
    symbol: state.symbol,
    filledQty: 0, fills: [],
    status: 'OPEN',
    queueAhead,
    lastLevelQty: queueAhead,
    pendingTradeDepletion: 0,
    pendingTradeDepletionTs: 0,
    recentCancelAdvance: 0,
    recentCancelAdvanceTs: 0,
    placedAt: Date.now(),
  };
  refreshBlotter();
  return id;
}

function cancelOrder(id) {
  const o = state.orders[id];
  if (o && (o.status === 'OPEN' || o.status === 'PARTIAL')) {
    o.status = 'CANCELLED';
    refreshBlotter();
    return true;
  }
  return false;
}

function cancelAll() {
  let n = 0;
  for (const o of Object.values(state.orders)) {
    if (o.status === 'OPEN' || o.status === 'PARTIAL') {
      o.status = 'CANCELLED';
      n++;
    }
  }
  refreshBlotter();
  return n;
}

function activeOrders() {
  return Object.values(state.orders).filter(o => o.status === 'OPEN' || o.status === 'PARTIAL');
}

function marketFillPrice(side, qty) {
  const levels = side === 'BUY' ? topAsks(20) : topBids(20);
  let remaining = qty;
  let notional = 0;
  let filled = 0;
  for (const [price, levelQty] of levels) {
    if (remaining <= QUEUE_EPS) break;
    const take = Math.min(remaining, levelQty);
    notional += price * take;
    filled += take;
    remaining -= take;
  }
  if (filled <= QUEUE_EPS) return null;
  return { price: notional / filled, qty: filled };
}

function placeMarketOrder(side, qty) {
  const id = genId();
  const placedAt = Date.now();
  qty = parseFloat(qty);
  state.orders[id] = {
    id, side, price: 0, qty, symbol: state.symbol,
    filledQty: 0, fills: [], status: 'OPEN',
    queueAhead: 0, lastLevelQty: 0,
    pendingTradeDepletion: 0, pendingTradeDepletionTs: 0,
    recentCancelAdvance: 0, recentCancelAdvanceTs: 0,
    placedAt, isMarketPending: true,
  };
  refreshBlotter();
  setTimeout(() => {
    const order = state.orders[id];
    if (!order || order.status === 'CANCELLED') return;
    order.isMarketPending = false;
    const fill = marketFillPrice(side, qty);
    if (!fill) { order.status = 'CANCELLED'; refreshBlotter(); return; }
    order.price = fill.price;
    executeFill(order, fill.price, fill.qty, 'MARKET');
  }, state.latencyMs);
  return id;
}

function queueCancelAheadProbability(order, nextLevelQty) {
  const front = Math.max(0, order.queueAhead);
  const behind = Math.max(0, nextLevelQty - front);
  if (front <= QUEUE_EPS) return 0;
  if (behind <= QUEUE_EPS) return 1;
  const frontWeight = Math.log1p(front);
  const behindWeight = Math.log1p(behind);
  const uniformProb = frontWeight / (frontWeight + behindWeight);
  return Math.pow(uniformProb, QUEUE_CANCEL_BEHIND_BIAS);
}

function updateQueueFromDepth(side, px, prevLevelQty, nextLevelQty) {
  if (Math.abs(prevLevelQty - nextLevelQty) <= QUEUE_EPS) return;
  let queueChanged = false;
  for (const order of activeOrders()) {
    if (order.side !== side || Math.abs(order.price - px) > 1e-9) continue;
    if (nextLevelQty > prevLevelQty) { order.lastLevelQty = nextLevelQty; continue; }
    const levelDrop = prevLevelQty - nextLevelQty;
    if (Date.now() - (order.pendingTradeDepletionTs || 0) > 1000) order.pendingTradeDepletion = 0;
    const tradeExplained = Math.min(levelDrop, order.pendingTradeDepletion || 0);
    order.pendingTradeDepletion = Math.max(0, (order.pendingTradeDepletion || 0) - tradeExplained);
    const cancelQty = Math.max(0, levelDrop - tradeExplained);
    if (cancelQty > QUEUE_EPS && order.queueAhead > QUEUE_EPS) {
      const aheadProb = queueCancelAheadProbability(order, nextLevelQty);
      const aheadCancelled = Math.min(order.queueAhead, cancelQty * aheadProb);
      if (aheadCancelled > QUEUE_EPS) {
        order.queueAhead -= aheadCancelled;
        order.recentCancelAdvance = (order.recentCancelAdvance || 0) + aheadCancelled;
        order.recentCancelAdvanceTs = Date.now();
        queueChanged = true;
      }
    }
    const clamped = Math.min(order.queueAhead, nextLevelQty);
    if (Math.abs(clamped - order.queueAhead) > QUEUE_EPS) { order.queueAhead = clamped; queueChanged = true; }
    order.lastLevelQty = nextLevelQty;
  }
  if (queueChanged) refreshBlotter();
}

function handleBookUpdate() {
  const bb = bestBid(), ba = bestAsk();
  for (const order of activeOrders()) {
    if (Date.now() - order.placedAt < 200) continue;
    if (order.queueAhead > 0) continue;
    if (order.isMarketPending) continue;
    if (order.side === 'BUY' && ba !== null && ba < order.price - 1e-9) {
      executeFill(order, ba, order.qty - order.filledQty, 'SWEEP');
    } else if (order.side === 'SELL' && bb !== null && bb > order.price + 1e-9) {
      executeFill(order, bb, order.qty - order.filledQty, 'SWEEP');
    }
  }
}

function handleTrade(trade) {
  let needsRender = false;
  for (const order of activeOrders()) {
    if (checkTradeFill(order, trade)) needsRender = true;
  }
  if (needsRender) refreshBlotter();
}

function checkTradeFill(order, trade) {
  if (order.isMarketPending) return false;
  const isBuyFill  = order.side === 'BUY'  && trade.side === 'SELL' && trade.price <= order.price + 1e-9;
  const isSellFill = order.side === 'SELL' && trade.side === 'BUY'  && trade.price >= order.price - 1e-9;
  if (!isBuyFill && !isSellFill) return false;
  const atOurPrice = Math.abs(trade.price - order.price) < 1e-9;
  if (atOurPrice) {
    order.pendingTradeDepletion = (order.pendingTradeDepletion || 0) + trade.qty;
    order.pendingTradeDepletionTs = Date.now();
    let effectiveTradeQty = trade.qty;
    if (Date.now() - (order.recentCancelAdvanceTs || 0) < 250) {
      const alreadyAdvanced = Math.min(effectiveTradeQty, order.recentCancelAdvance || 0);
      effectiveTradeQty -= alreadyAdvanced;
      order.recentCancelAdvance = Math.max(0, (order.recentCancelAdvance || 0) - alreadyAdvanced);
    }
    if (order.queueAhead > 0) {
      const burned = Math.min(order.queueAhead, effectiveTradeQty);
      order.queueAhead -= burned;
      const leftover = effectiveTradeQty - burned;
      if (leftover <= 0) return true;
      const fillQty = Math.min(leftover, order.qty - order.filledQty);
      if (fillQty > 0) executeFill(order, trade.price, fillQty, 'PASSIVE_TRADE');
    } else {
      const fillQty = Math.min(effectiveTradeQty, order.qty - order.filledQty);
      if (fillQty > 0) executeFill(order, trade.price, fillQty, 'PASSIVE_TRADE');
    }
  } else {
    const fillQty = Math.min(trade.qty, order.qty - order.filledQty);
    if (fillQty > 0) executeFill(order, trade.price, fillQty, 'PASSIVE_TRADE');
  }
  return true;
}

function executeFill(order, price, qty, reason) {
  const ts = Date.now();
  order.fills.push({ price, qty, reason, ts });
  order.filledQty += qty;
  if (order.qty - order.filledQty <= 1e-10) {
    order.filledQty = order.qty;
    order.status = 'FILLED';
  } else order.status = 'PARTIAL';
  updatePosition(order.symbol, order.side, price, qty, reason);
  recordExecution(order, price, qty, reason, ts);
  appendFillLog(order, price, qty, reason);
  refreshBlotter();
  refreshPositions();
}

function executionFeeType(reason) {
  return reason === 'MARKET' || reason === 'SWEEP' ? 'TAKER' : 'MAKER';
}

function currentStrategyName() {
  if (window.MeanReversionStrategy?.status?.().running) return 'Mean Reversion';
  if (window.OrderFlowPredictor?.status?.().running) return 'Order-Flow Predictor';
  if (window.AutoMM?.status?.().running) return 'Avellaneda-Stoikov MM';
  return 'Manual';
}

const MAX_EXECUTIONS = 5000;
function recordExecution(order, price, qty, reason, ts) {
  const feeType = executionFeeType(reason);
  const feePercent = feeType === 'TAKER' ? state.takerFeePercent : state.makerFeePercent;
  state.executions.push({
    id: `${order.id}-${order.fills.length}-${ts}`,
    orderId: order.id,
    symbol: order.symbol,
    side: order.side,
    price, qty, reason, feeType,
    feePercentAtFill: feePercent,
    feePaidAtFill: price * qty * feePercent / 100,
    strategy: currentStrategyName(),
    executionMode: document.getElementById('strategy-exec-select')?.value || 'limit',
    ts,
  });
  if (state.executions.length > MAX_EXECUTIONS) {
    const excess = state.executions.length - MAX_EXECUTIONS;
    let dropped = 0;
    const keep = [];
    for (const e of state.executions) {
      if (dropped < excess && state.savedExecutionIds.has(e.id)) dropped++;
      else keep.push(e);
    }
    while (keep.length > MAX_EXECUTIONS) keep.shift();
    state.executions = keep;
  }
}

// ═══════════════════════════════════════════════════════════════
//  POSITION TRACKING
// ═══════════════════════════════════════════════════════════════
function feePercentForReason(reason) {
  return reason === 'MARKET' || reason === 'SWEEP' ? state.takerFeePercent : state.makerFeePercent;
}

function updatePosition(symbol, side, fillPrice, fillQty, reason = 'PASSIVE_TRADE') {
  if (!state.positions[symbol]) {
    state.positions[symbol] = { netQty: 0, avgEntry: 0, realizedPnl: 0, fees: 0 };
  }
  const pos = state.positions[symbol];
  const isBuy = side === 'BUY';
  const fee = fillPrice * fillQty * feePercentForReason(reason) / 100;
  pos.fees = (pos.fees || 0) + fee;
  state.totalFees += fee;
  pos.realizedPnl -= fee;
  state.realizedPnl -= fee;

  if (pos.netQty === 0) {
    pos.avgEntry = fillPrice;
    pos.netQty = isBuy ? fillQty : -fillQty;
  } else if ((isBuy && pos.netQty > 0) || (!isBuy && pos.netQty < 0)) {
    const totalQty = Math.abs(pos.netQty) + fillQty;
    pos.avgEntry = (Math.abs(pos.netQty) * pos.avgEntry + fillQty * fillPrice) / totalQty;
    pos.netQty = isBuy ? totalQty : -totalQty;
  } else {
    const closeQty = Math.min(fillQty, Math.abs(pos.netQty));
    const pnlPerUnit = isBuy ? (pos.avgEntry - fillPrice) : (fillPrice - pos.avgEntry);
    const realized = pnlPerUnit * closeQty;
    pos.realizedPnl += realized;
    state.realizedPnl += realized;
    state.closedTrades++;
    const remaining = fillQty - closeQty;
    pos.netQty = isBuy ? pos.netQty + fillQty : pos.netQty - fillQty;
    if (Math.abs(pos.netQty) < 1e-10) { pos.netQty = 0; pos.avgEntry = 0; }
    else if (remaining > 0) pos.avgEntry = fillPrice;
  }
}

function getUnrealizedPnl(symbol) {
  const pos = state.positions[symbol];
  if (!pos || pos.netQty === 0 || state.lastPrice === null) return 0;
  return pos.netQty * (state.lastPrice - pos.avgEntry);
}

function refreshPositions() {
  const symbols = Object.keys(state.positions).filter(s => {
    const p = state.positions[s];
    return p.netQty !== 0 || p.realizedPnl !== 0;
  });
  let totalUpnl = 0;
  symbols.forEach(s => { totalUpnl += getUnrealizedPnl(s); });
  const totalRpnl = state.realizedPnl;
  const totalPnl = totalUpnl + totalRpnl;

  const pnlClass = (v) => v > 0 ? 'pos-pnl-pos' : v < 0 ? 'pos-pnl-neg' : 'pos-pnl-zero';
  const pnlFmt = (v) => (v >= 0 ? '+' : '') + '$' + v.toFixed(2);

  document.getElementById('pos-upnl-total').textContent = pnlFmt(totalUpnl);
  document.getElementById('pos-upnl-total').className = 'pos-summary-val ' + pnlClass(totalUpnl);
  document.getElementById('pos-rpnl-total').textContent = pnlFmt(totalRpnl);
  document.getElementById('pos-rpnl-total').className = 'pos-stat-val ' + pnlClass(totalRpnl);
  document.getElementById('pos-tpnl-total').textContent = pnlFmt(totalPnl);
  document.getElementById('pos-tpnl-total').className = 'pos-stat-val ' + pnlClass(totalPnl);

  const openPositions = symbols.filter(s => state.positions[s].netQty !== 0);
  document.getElementById('pos-count').textContent = openPositions.length;
  document.getElementById('pos-closed-count').textContent = state.closedTrades + ' trades';

  let html = '';
  if (openPositions.length === 0) {
    html = `<div class="pos-empty"><span class="pos-empty-icon">◈</span><span>No open positions</span></div>`;
  }
  for (const sym of openPositions) {
    const pos = state.positions[sym];
    const upnl = getUnrealizedPnl(sym);
    const isLong = pos.netQty > 0;
    const upnlClass = pnlClass(upnl);
    const rpnlClass = pnlClass(pos.realizedPnl);
    const qty = Math.abs(pos.netQty);
    const lp = state.lastPrice;
    const pnlPct = pos.avgEntry > 0 && lp ? ((lp - pos.avgEntry) / pos.avgEntry * 100 * (isLong ? 1 : -1)) : 0;

    html += `
    <div class="pos-card">
      <div class="pos-card-header">
        <span class="pos-sym">${sym}</span>
        <span class="${isLong ? 'pos-side-long' : 'pos-side-short'}">${isLong ? 'LONG' : 'SHORT'}</span>
      </div>
      <div class="pos-pnl-row">
        <span>
          <span class="pos-pnl-big ${upnlClass}">${pnlFmt(upnl)}</span>
          <span class="pos-pnl-pct ${upnlClass}">(${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(3)}%)</span>
        </span>
        <span style="font-size:9px;color:var(--text3)">UPnL</span>
      </div>
      <div class="pos-detail-grid">
        <div class="pos-detail"><span class="pos-detail-lbl">Size</span><span class="pos-detail-val">${fq(qty)}</span></div>
        <div class="pos-detail"><span class="pos-detail-lbl">Avg Entry</span><span class="pos-detail-val">${fp(pos.avgEntry)}</span></div>
        <div class="pos-detail"><span class="pos-detail-lbl">Mark Price</span><span class="pos-detail-val">${lp ? fp(lp) : '—'}</span></div>
        <div class="pos-detail"><span class="pos-detail-lbl">Realized (net)</span><span class="pos-detail-val ${rpnlClass}">${pnlFmt(pos.realizedPnl)}</span></div>
        <div class="pos-detail"><span class="pos-detail-lbl">Fees</span><span class="pos-detail-val">$${(pos.fees || 0).toFixed(2)}</span></div>
        <div class="pos-detail"><span class="pos-detail-lbl">Notional</span><span class="pos-detail-val">$${lp ? (qty * lp).toLocaleString('en-US',{maximumFractionDigits:0}) : '—'}</span></div>
        <div class="pos-detail"><span class="pos-detail-lbl">Break-even</span><span class="pos-detail-val">${fp(pos.avgEntry)}</span></div>
      </div>
    </div>`;
  }

  const closedWithPnl = symbols.filter(s => state.positions[s].netQty === 0 && state.positions[s].realizedPnl !== 0);
  for (const sym of closedWithPnl) {
    const pos = state.positions[sym];
    html += `
    <div class="pos-card" style="opacity:0.6">
      <div class="pos-card-header">
        <span class="pos-sym">${sym}</span>
        <span style="font-size:9px;color:var(--text3)">CLOSED</span>
      </div>
      <div class="pos-pnl-row">
        <span class="pos-pnl-big ${pnlClass(pos.realizedPnl)}">${pnlFmt(pos.realizedPnl)}</span>
        <span style="font-size:9px;color:var(--text3)">Realized</span>
      </div>
    </div>`;
  }
  document.getElementById('pos-cards').innerHTML = html;
}

// ═══════════════════════════════════════════════════════════════
//  CHART ENGINE
// ═══════════════════════════════════════════════════════════════
function initChart() {
  const container = document.getElementById('chart-container');
  container.innerHTML = '';
  state.chart = LightweightCharts.createChart(container, {
    layout: { background: { color: '#080b10' }, textColor: '#5a7a9a', fontSize: 11, fontFamily: "'JetBrains Mono', monospace" },
    grid: { vertLines: { color: '#1e2d42' }, horzLines: { color: '#1e2d42' } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal, vertLine: { color: '#243448', labelBackgroundColor: '#101520' }, horzLine: { color: '#243448', labelBackgroundColor: '#101520' } },
    rightPriceScale: { borderColor: '#1e2d42', textColor: '#5a7a9a' },
    timeScale: { borderColor: '#1e2d42', textColor: '#5a7a9a', timeVisible: true, secondsVisible: true, tickMarkFormatter: (t) => { const d = new Date(t*1000); return d.toTimeString().slice(0,8); } },
    handleScroll: { mouseWheel: true, pressedMouseMove: true },
    handleScale:  { mouseWheel: true, pinch: true },
    width:  container.clientWidth,
    height: container.clientHeight,
  });
  state.candleSeries = state.chart.addCandlestickSeries({
    upColor: '#00d97e', downColor: '#f03c3c', borderUpColor: '#00d97e', borderDownColor: '#f03c3c', wickUpColor: '#00833c', wickDownColor: '#882222',
  });
  state.currentCandle = null;
  const ro = new ResizeObserver(() => { if (state.chart) state.chart.resize(container.clientWidth, container.clientHeight); });
  ro.observe(container);
}

function updateCandle(price, ts) {
  const tf = state.candleTf * 1000;
  const candleTime = Math.floor(ts / tf) * tf / 1000;
  if (!state.currentCandle || state.currentCandle.time !== candleTime) {
    if (state.currentCandle) state.candleSeries.update(state.currentCandle);
    state.currentCandle = { time: candleTime, open: price, high: price, low: price, close: price };
  } else {
    state.currentCandle.high  = Math.max(state.currentCandle.high,  price);
    state.currentCandle.low   = Math.min(state.currentCandle.low,   price);
    state.currentCandle.close = price;
  }
  state.candleSeries.update(state.currentCandle);
  const c = state.currentCandle;
  const chg = c.close - c.open;
  const chgPct = (chg / c.open * 100).toFixed(3);
  const chgStr = (chg >= 0 ? '+' : '') + fp(chg) + ' (' + (chg >= 0 ? '+' : '') + chgPct + '%)';
  const color = chg >= 0 ? 'var(--bid)' : 'var(--ask)';
  document.getElementById('chart-ohlc').innerHTML =
    `O:<span style="color:var(--text1)">${fp(c.open)}</span>  ` +
    `H:<span style="color:var(--bid)">${fp(c.high)}</span>  ` +
    `L:<span style="color:var(--ask)">${fp(c.low)}</span>  ` +
    `C:<span style="color:var(--text0)">${fp(c.close)}</span>  ` +
    `<span style="color:${color}">${chgStr}</span>`;
}

// ═══════════════════════════════════════════════════════════════
//  WEBSOCKET  (uses state.marketType)
// ═══════════════════════════════════════════════════════════════
function connect(sym) {
  state.connectGen++;
  const gen = state.connectGen;
  setConnStatus('connecting');
  if (state.reconnectTimer)  { clearTimeout(state.reconnectTimer);  state.reconnectTimer  = null; }
  if (state.reconnectTimer2) { clearTimeout(state.reconnectTimer2); state.reconnectTimer2 = null; }
  if (state.ws)  { state.ws.onclose  = null; state.ws.onerror  = null; state.ws.close();  state.ws  = null; }
  if (state.ws2) { state.ws2.onclose = null; state.ws2.onerror = null; state.ws2.close(); state.ws2 = null; }
  state.snapshotLoaded = false;
  state.bufferedEvents = [];
  state.bids = {};
  state.asks = {};
  state.lastUpdateId = 0;

  initChart();
  fetchSnapshot();

  const symLower = sym.toLowerCase();
  const isSpot = state.marketType === 'spot';

  // Depth stream
  const depthUrl = isSpot
    ? `wss://stream.binance.com:9443/ws/${symLower}@depth@100ms`
    : `wss://fstream.binance.com/public/ws/${symLower}@depth@100ms`;
  const ws = new WebSocket(depthUrl);
  state.ws = ws;
  ws.onopen = () => { if (state.connectGen === gen) { setConnStatus('connected'); console.log('[WS depth] Connected'); } };
  ws.onmessage = (e) => {
    if (state.connectGen !== gen) return;
    const data = JSON.parse(e.data);
    applyDepthEvent(data);
    handleBookUpdate();
    renderBook();
    updateHeader();
  };
  ws.onerror = (err) => { console.error('[WS depth] Error:', err); if (state.connectGen === gen) setConnStatus('disconnected'); };
  ws.onclose = (ev) => {
    console.warn('[WS depth] Closed — code:', ev.code, 'clean:', ev.wasClean);
    if (state.connectGen !== gen) return;
    setConnStatus('disconnected');
    state.reconnectTimer = setTimeout(() => { if (state.connectGen === gen) connect(state.symbol); }, 3000);
  };

  // Trade stream
  const tradeUrl = isSpot
    ? `wss://stream.binance.com:9443/ws/${symLower}@aggTrade`
    : `wss://fstream.binance.com/market/ws/${symLower}@aggTrade`;
  const ws2 = new WebSocket(tradeUrl);
  state.ws2 = ws2;
  ws2.onopen  = () => console.log('[WS trade] Connected');
  ws2.onmessage = (e) => {
    if (state.connectGen !== gen) return;
    const data = JSON.parse(e.data);
    const trade = {
      price: parseFloat(data.p),
      qty:   parseFloat(data.q),
      side:  data.m ? 'SELL' : 'BUY',
      ts:    data.T,
    };
    state.lastPrice = trade.price;
    state.lastSide  = trade.side;
    handleTrade(trade);
    appendTape(trade);
    updateCandle(trade.price, trade.ts);
    updateHeader();
    refreshPositions();
  };
  ws2.onerror = (err) => console.error('[WS trade] Error:', err);
  ws2.onclose = (ev) => {
    console.warn('[WS trade] Closed — code:', ev.code, 'clean:', ev.wasClean);
    if (state.connectGen !== gen) return;
    state.reconnectTimer2 = setTimeout(() => {
      if (state.connectGen === gen) {
        const ws2new = new WebSocket(tradeUrl);
        state.ws2 = ws2new;
        ws2new.onopen    = () => console.log('[WS trade] Reconnected');
        ws2new.onmessage = ws2.onmessage;
        ws2new.onerror   = ws2.onerror;
        ws2new.onclose   = ws2.onclose;
      }
    }, 3000);
  };
}

function setConnStatus(s) {
  document.getElementById('conn-dot').className = s;
}

// ═══════════════════════════════════════════════════════════════
//  RENDER: HEADER, BOOK, TAPE, BLOTTER  (unchanged except for
//  possible formatting differences due to decimals)
// ═══════════════════════════════════════════════════════════════
let lastRenderedPrice = null;
function updateHeader() {
  const bb = bestBid(), ba = bestAsk();
  const lp = state.lastPrice;
  if (lp !== null) {
    const el = document.getElementById('hdr-last');
    const dir = lastRenderedPrice !== null ? (lp > lastRenderedPrice ? 'up' : lp < lastRenderedPrice ? 'down' : null) : null;
    el.textContent = fp(lp);
    el.className = 'h-val ' + (state.lastSide === 'BUY' ? 'hdr-bid' : 'hdr-ask');
    if (dir) {
      el.classList.remove('flash-up', 'flash-down');
      void el.offsetWidth;
      el.classList.add(dir === 'up' ? 'flash-up' : 'flash-down');
    }
    lastRenderedPrice = lp;
  }
  if (bb) document.getElementById('hdr-bid').textContent = fp(bb);
  if (ba) document.getElementById('hdr-ask').textContent = fp(ba);
  if (bb && ba) {
    const sp = ba - bb;
    document.getElementById('hdr-spread').textContent = fp(sp);
    document.getElementById('mid-price').textContent  = fp((bb + ba) / 2);
    document.getElementById('spread-val').textContent = fp(sp);
    document.getElementById('spread-pct').textContent = `(${(sp / ba * 100).toFixed(3)}%)`;
  }
  const topN = 5;
  const bidArr = topBids(topN), askArr = topAsks(topN);
  const bv = bidArr.reduce((s,[,q]) => s+q, 0);
  const av = askArr.reduce((s,[,q]) => s+q, 0);
  const tot = bv + av;
  if (tot > 0) {
    const ratio = bv / tot;
    const pct = Math.round(ratio * 100);
    document.getElementById('imb-bar-inner').style.width = pct + '%';
    document.getElementById('imb-pct').textContent = pct + '%';
    const imbBar = document.getElementById('imb-bar-inner');
    imbBar.style.background = pct > 55 ? 'var(--bid)' : pct < 45 ? 'var(--ask)' : 'var(--yellow)';
  }
  const qty = document.getElementById('qty-input').value || '—';
  const bbStr = bb ? fp(bb) : '—';
  const baStr = ba ? fp(ba) : '—';
  document.getElementById('join-bid-btn').textContent = `▲ Join Bid ${bbStr}  [${qty}]`;
  document.getElementById('join-ask-btn').textContent = `▼ Join Ask ${baStr}  [${qty}]`;
}

const DEPTH = 20;
function renderBook() {
  const asks = topAsks(DEPTH);
  const bids = topBids(DEPTH);
  const buyPx  = new Set(activeOrders().filter(o => o.side === 'BUY').map(o => o.price));
  const sellPx = new Set(activeOrders().filter(o => o.side === 'SELL').map(o => o.price));
  const allQty = [...asks, ...bids].map(([,q]) => q);
  const maxQ   = allQty.length ? Math.max(...allQty) : 1;
  let askCum = 0, bidCum = 0;
  const askCums = asks.map(([,q]) => { askCum += q; return askCum; });
  const bidCums = bids.map(([,q]) => { bidCum += q; return bidCum; });
  const maxCum = Math.max(askCum, bidCum, 1);

  let asksHtml = '';
  for (let i = asks.length - 1; i >= 0; i--) {
    const [px, qty] = asks[i];
    const cum = askCums[i];
    const barW = Math.round((cum / maxCum) * 100);
    const isMyOrder = sellPx.has(px);
    asksHtml += `<div class="book-row ask-row${isMyOrder ? ' my-order' : ''}" data-price="${px}" data-side="SELL">
      <span class="r-qty">${fq(qty)}</span><span class="r-price">${fp(px)}</span><span class="r-total">${fq(cum)}</span>
      <div class="depth-bar" style="width:${barW}%"></div></div>`;
  }
  let bidsHtml = '';
  for (let i = 0; i < bids.length; i++) {
    const [px, qty] = bids[i];
    const cum = bidCums[i];
    const barW = Math.round((cum / maxCum) * 100);
    const isMyOrder = buyPx.has(px);
    bidsHtml += `<div class="book-row bid-row${isMyOrder ? ' my-order' : ''}" data-price="${px}" data-side="BUY">
      <span class="r-qty">${fq(qty)}</span><span class="r-price">${fp(px)}</span><span class="r-total">${fq(cum)}</span>
      <div class="depth-bar" style="width:${barW}%"></div></div>`;
  }
  document.getElementById('asks-rows').innerHTML = asksHtml;
  document.getElementById('bids-rows').innerHTML = bidsHtml;
}

const MAX_TAPE = 200;
function appendTape(trade) {
  state.tradeCount++;
  const log = document.getElementById('tape-log');
  const isBuy = trade.side === 'BUY';
  const row = document.createElement('div');
  row.className = `tape-row ${isBuy ? 'buy-trade' : 'sell-trade'}`;
  row.innerHTML = `<span class="t-time">${fts(trade.ts)}</span><span class="t-side">${isBuy ? '▲ BUY' : '▼ SELL'}</span><span class="t-price">${fp(trade.price)}</span><span class="t-qty" style="text-align:right">${fq(trade.qty)}</span>`;
  log.insertBefore(row, log.firstChild);
  while (log.children.length > MAX_TAPE) log.removeChild(log.lastChild);
  document.getElementById('tape-count').textContent = `${state.tradeCount} trades`;
}

function refreshBlotter() {
  const orders = Object.values(state.orders).sort((a, b) => b.placedAt - a.placedAt).slice(0, 50);
  let html = '';
  for (const o of orders) {
    const isBuy = o.side === 'BUY';
    const pct = o.qty > 0 ? (o.filledQty / o.qty * 100) : 0;
    const displayStatus = o.isMarketPending ? 'PENDING' : o.status;
    const statusClass = 'b-status-' + (o.isMarketPending ? 'partial' : o.status.toLowerCase());
    const sideClass   = isBuy ? 'b-side-buy' : 'b-side-sell';
    const priceClass  = isBuy ? 'b-price-buy' : 'b-price-sell';
    const rowClass    = o.status === 'FILLED' ? 'status-filled' : o.status === 'CANCELLED' ? 'status-cancelled' : '';
    const selClass    = state.selectedOrderId === o.id ? 'selected' : '';
    const barColor    = isBuy ? 'var(--bid)' : 'var(--ask)';
    html += `<div class="blotter-row ${rowClass} ${selClass}" data-id="${o.id}">
      <span class="b-id">${o.id}</span><span style="color:var(--text2);font-size:10px">${o.symbol}</span>
      <span class="${sideClass}">${o.side}</span><span class="${priceClass}">${fp(o.price)}</span>
      <span style="color:var(--text1)">${fq(o.qty)}</span>
      <div class="b-filled-wrap" style="position:relative"><div class="b-filled-bar" style="width:${pct}%;background:${barColor}"></div><span class="b-filled-text">${fq(o.filledQty)} <span style="color:var(--text3)">(${pct.toFixed(0)}%)</span></span></div>
      <span class="${statusClass}">${displayStatus}</span><span class="b-queue">${o.queueAhead > 0 ? fq(o.queueAhead) : '—'}</span>
    </div>`;
  }
  document.getElementById('blotter-rows').innerHTML = html;
  document.querySelectorAll('.blotter-row').forEach(row => {
    row.addEventListener('click', () => { state.selectedOrderId = row.dataset.id; refreshBlotter(); });
  });
}

function appendFillLog(order, price, qty, reason) {
  const isBuy = order.side === 'BUY';
  console.log(`%c FILL %c #${order.id} ${order.side} ${fq(qty)} @ ${fp(price)} [${reason}] status=${order.status}`,
    `background:${isBuy ? '#00d97e' : '#f03c3c'};color:#000;font-weight:700;padding:1px 4px;border-radius:2px`, 'color:inherit');
}

// ═══════════════════════════════════════════════════════════════
//  UI EVENTS
// ═══════════════════════════════════════════════════════════════
function setMsg(text, color = 'var(--text2)') {
  const el = document.getElementById('entry-msg');
  el.textContent = text;
  el.style.color = color;
}

function getQty() {
  return parseFloat(document.getElementById('qty-input').value);
}

function syncStrategyQty() { const strategy = activeStrategy(); if (!strategy?.status?.().running) return; const qty = getQty(); if (!Number.isFinite(qty) || qty <= 0) { setMsg('Strategy invalid qty', 'var(--yellow)'); return; } strategy.configure({ qty }); }
function syncFeeConfig() { const makerFee = parseFloat(document.getElementById('maker-fee-input').value); const takerFee = parseFloat(document.getElementById('taker-fee-input').value); if (Number.isFinite(makerFee) && makerFee >= 0) state.makerFeePercent = makerFee; if (Number.isFinite(takerFee) && takerFee >= 0) state.takerFeePercent = takerFee; }
function syncLatencyConfig() { const v = parseFloat(document.getElementById('latency-input').value); if (Number.isFinite(v) && v >= 0) state.latencyMs = v; }
function syncStrategyExecutionMode() { const strategy = activeStrategy(); if (!strategy?.status?.().running) return; strategy.configure({ executionMode: document.getElementById('strategy-exec-select').value }); }
function savedExecutions() { try { return JSON.parse(localStorage.getItem('bfs_saved_executions') || '[]'); } catch { return []; } }
function saveSessionTrades() { const existing = savedExecutions(); const ids = new Set(existing.map(e => e.id)); const fresh = state.executions.filter(e => !ids.has(e.id) && !state.savedExecutionIds.has(e.id)); const next = existing.concat(fresh); localStorage.setItem('bfs_saved_executions', JSON.stringify(next)); fresh.forEach(e => state.savedExecutionIds.add(e.id)); setMsg(`Saved ${fresh.length} trade(s). Total saved: ${next.length}`, 'var(--accent)'); }
function clearSavedTrades() { localStorage.removeItem('bfs_saved_executions'); state.savedExecutionIds = new Set(); setMsg('Cleared saved trades', 'var(--yellow)'); }
function openAnalysisPage() { saveSessionTrades(); window.open('trade_analysis.html', '_blank'); }

function joinBid() { const bb = bestBid(); if (!bb) { setMsg('⚠ No book data', 'var(--ask)'); return; } const qty = getQty(); if (!qty || isNaN(qty) || qty <= 0) { setMsg('⚠ Enter qty first', 'var(--ask)'); return; } const id = placeOrder('BUY', bb, qty); setMsg(`▲ Joined bid @ ${fp(bb)}  qty=${fq(qty)}  #${id}`, 'var(--bid)'); }
function joinAsk() { const ba = bestAsk(); if (!ba) { setMsg('⚠ No book data', 'var(--ask)'); return; } const qty = getQty(); if (!qty || isNaN(qty) || qty <= 0) { setMsg('⚠ Enter qty first', 'var(--ask)'); return; } const id = placeOrder('SELL', ba, qty); setMsg(`▼ Joined ask @ ${fp(ba)}  qty=${fq(qty)}  #${id}`, 'var(--ask)'); }
function submitOrder() { const side = document.getElementById('side-select').value; const price = parseFloat(document.getElementById('price-input').value); const qty = parseFloat(document.getElementById('qty-input').value); if (!price || !qty || isNaN(price) || isNaN(qty)) { setMsg('⚠ Invalid price or qty', 'var(--ask)'); return; } const id = placeOrder(side, price, qty); document.getElementById('price-input').value = ''; const color = side === 'BUY' ? 'var(--bid)' : 'var(--ask)'; setMsg(`${side === 'BUY' ? '▲' : '▼'} ${side} ${fq(qty)} @ ${fp(price)}  #${id}`, color); }

function setStrategyRunning(isRunning) { document.getElementById('strategy-start-btn').disabled = isRunning; document.getElementById('strategy-stop-btn').disabled = !isRunning; }
function activeStrategy() { const selected = document.getElementById('strategy-select').value; if (selected === 'avellaneda') return window.AutoMM; if (selected === 'orderflow') return window.OrderFlowPredictor; if (selected === 'meanreversion') return window.MeanReversionStrategy; return null; }
function stopAllStrategies() { if (window.AutoMM?.status?.().running) window.AutoMM.stop(); if (window.OrderFlowPredictor?.status?.().running) window.OrderFlowPredictor.stop(); if (window.MeanReversionStrategy?.status?.().running) window.MeanReversionStrategy.stop(); setStrategyRunning(false); }
function startSelectedStrategy() { const selected = document.getElementById('strategy-select').value; const strategy = activeStrategy(); if (!strategy) { setMsg('⚠ Strategy file not loaded', 'var(--ask)'); return; } stopAllStrategies(); const qty = getQty(); syncFeeConfig(); const execSelect = document.getElementById('strategy-exec-select'); if (selected === 'meanreversion') execSelect.value = 'limit'; const executionMode = selected === 'meanreversion' ? 'limit' : execSelect.value; const config = Number.isFinite(qty) && qty > 0 ? { qty, executionMode, maxVolBps: 250, volGuardSpreadMultiplier: 8 } : { executionMode, maxVolBps: 250, volGuardSpreadMultiplier: 8 }; strategy.start(config); setStrategyRunning(true); const status = strategy.status(); const label = selected === 'orderflow' ? 'OFP' : selected === 'meanreversion' ? 'MRV' : 'MM'; const color = /predict|quoting|fade|mean exit|take profit/.test(status.lastReason) ? 'var(--bid)' : 'var(--yellow)'; setMsg(`${label} ${status.lastReason}`, color); }
function stopSelectedStrategy() { stopAllStrategies(); setMsg('Strategy stopped', 'var(--yellow)'); }

function changeSymbol(sym) {
  if (window.AutoMM?.status?.().running || window.OrderFlowPredictor?.status?.().running || window.MeanReversionStrategy?.status?.().running) stopSelectedStrategy();
  sym = sym.trim().toUpperCase().replace(/\s+/g, '');
  if (!sym) return;
  const knownQuotes = ['USDT','BUSD','USDC'];
  const knownAltQuotes = ['BTC','ETH','BNB'];
  const hasStableQuote = knownQuotes.some(q => sym.endsWith(q));
  const hasAltQuote    = knownAltQuotes.some(q => sym.endsWith(q) && sym.length > q.length + 1);
  if (!hasStableQuote && !hasAltQuote) sym += 'USDT';
  // Determine market type
  state.marketType = SPOT_FX_SYMBOLS.has(sym) ? 'spot' : 'futures';
  state.symbol = sym;
  detectDecimals(sym);
  document.getElementById('header-sym').textContent = sym + (state.marketType === 'spot' ? ' (SPOT)' : '');
  document.getElementById('sym-input').value = sym;
  document.getElementById('tape-log').innerHTML = '';
  document.getElementById('blotter-rows').innerHTML = '';
  state.orders = {};
  state.selectedOrderId = null;
  state.lastPrice = null;
  lastRenderedPrice = null;
  state.tradeCount = 0;
  state.positions = {};
  state.realizedPnl = 0;
  state.totalFees = 0;
  state.closedTrades = 0;
  document.getElementById('pos-cards').innerHTML = '';
  document.getElementById('hdr-last').textContent = '—';
  document.getElementById('hdr-bid').textContent  = '—';
  document.getElementById('hdr-ask').textContent  = '—';
  document.getElementById('hdr-spread').textContent = '—';
  setMsg('');
  connect(sym);
}

document.getElementById('book-panel').addEventListener('click', (e) => {
  const row = e.target.closest('.book-row');
  if (row) { document.getElementById('price-input').value = row.dataset.price; document.getElementById('side-select').value = row.dataset.side; }
});
document.getElementById('join-bid-btn').addEventListener('click', joinBid);
document.getElementById('join-ask-btn').addEventListener('click', joinAsk);
document.getElementById('place-btn').addEventListener('click', submitOrder);
document.getElementById('strategy-start-btn').addEventListener('click', startSelectedStrategy);
document.getElementById('strategy-stop-btn').addEventListener('click', stopSelectedStrategy);
window.addEventListener('automm-status', (e) => { const detail = e.detail; if (!detail?.running) return; const activeCount = detail.activeOwnedOrders?.length || 0; const color = detail.lastReason === 'quoting' ? 'var(--bid)' : 'var(--yellow)'; setMsg(`MM ${detail.lastReason} (${activeCount} live)`, color); });
window.addEventListener('ofp-status', (e) => { const detail = e.detail; if (!detail?.running) return; const activeCount = detail.activeOwnedOrders?.length || 0; const color = /predict|take profit/.test(detail.lastReason) ? 'var(--bid)' : 'var(--yellow)'; setMsg(`OFP ${detail.lastReason} (${activeCount} live)`, color); });
window.addEventListener('mrv-status', (e) => { const detail = e.detail; if (!detail?.running) return; const activeCount = detail.activeOwnedOrders?.length || 0; const color = /fade|mean exit|take profit/.test(detail.lastReason) ? 'var(--bid)' : 'var(--yellow)'; setMsg(`MRV ${detail.lastReason} (${activeCount} live)`, color); });
document.getElementById('cancel-btn').addEventListener('click', () => { if (!state.selectedOrderId) { setMsg('⚠ Select an order first', 'var(--yellow)'); return; } const ok = cancelOrder(state.selectedOrderId); setMsg(ok ? `Cancelled #${state.selectedOrderId}` : `Not found: ${state.selectedOrderId}`, 'var(--yellow)'); state.selectedOrderId = null; });
document.getElementById('cancel-all-btn').addEventListener('click', () => { const n = cancelAll(); setMsg(`Cancelled ${n} order(s)`, 'var(--yellow)'); });
document.getElementById('clr-pos-btn').addEventListener('click', () => { state.positions = {}; state.realizedPnl = 0; state.totalFees = 0; state.closedTrades = 0; refreshPositions(); });
document.getElementById('sym-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') changeSymbol(e.target.value); });
document.getElementById('price-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') submitOrder(); });
document.getElementById('qty-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') submitOrder(); });
document.getElementById('qty-input').addEventListener('input', syncStrategyQty);
document.getElementById('strategy-exec-select').addEventListener('change', syncStrategyExecutionMode);
document.getElementById('maker-fee-input').addEventListener('input', syncFeeConfig);
document.getElementById('taker-fee-input').addEventListener('input', syncFeeConfig);
document.getElementById('latency-input').addEventListener('input', syncLatencyConfig);
document.getElementById('save-trades-btn').addEventListener('click', saveSessionTrades);
document.getElementById('clear-saved-trades-btn').addEventListener('click', clearSavedTrades);
document.getElementById('analysis-btn').addEventListener('click', openAnalysisPage);
document.addEventListener('keydown', (e) => { if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return; if (e.key === 'b' || e.key === 'B') joinBid(); if (e.key === 'a' || e.key === 'A') joinAsk(); if (e.key === 'c' || e.key === 'C') { const n = cancelAll(); setMsg(`Cancelled ${n} order(s)`, 'var(--yellow)'); } if (e.key === 'Escape') { if (state.selectedOrderId) { cancelOrder(state.selectedOrderId); state.selectedOrderId = null; } } });

// ═══════════════════════════════════════════════════════════════
//  BOOT
// ═══════════════════════════════════════════════════════════════
detectDecimals('BTCUSDT');
connect('BTCUSDT');
refreshPositions();
