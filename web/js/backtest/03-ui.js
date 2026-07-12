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

// chart
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

// websocket (uses state.marketType)
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
    ? `${location.origin.replace(/^http/, 'ws')}/ws/${symLower}@depth@100ms`
    : `${location.origin.replace(/^http/, 'ws')}/public/ws/${symLower}@depth@100ms`;
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
    ? `${location.origin.replace(/^http/, 'ws')}/ws/${symLower}@aggTrade`
    : `${location.origin.replace(/^http/, 'ws')}/market/ws/${symLower}@aggTrade`;
  const ws2 = new WebSocket(tradeUrl);
  state.ws2 = ws2;
  ws2.onopen  = () => console.log('[WS trade] Connected');
  ws2.onmessage = (e) => {
    if (state.connectGen !== gen) return;
    const data = JSON.parse(e.data);
    // in backtest the server sets `T` to wall clock so the strategy's Date.now()
    // math stays valid, and carries the real market time in `mT`. use market time
    // for tape/candle bucketing, else fast replay (>1x) smears everything into a
    // few wall-time candles.
    const marketTs = (typeof data.mT === 'number') ? data.mT : data.T;
    const trade = {
      price: parseFloat(data.p),
      qty:   parseFloat(data.q),
      side:  data.m ? 'SELL' : 'BUY',
      ts:    data.T,       // wall clock, keeps Date.now() math working
      mTs:   marketTs,     // market time, for tape/candle/chart
    };
    state.lastPrice = trade.price;
    state.lastSide  = trade.side;
    handleTrade(trade);
    appendTape({ ...trade, ts: marketTs });
    updateCandle(trade.price, marketTs);
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

// render: header, book, tape, blotter
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

// ui events
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

