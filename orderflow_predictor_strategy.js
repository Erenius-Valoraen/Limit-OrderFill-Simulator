/*
  OrderFlowPredictor: ultra-short-term crypto order-flow strategy.

  Research basis:
    - Cont, Kukanov, Stoikov: short-horizon price changes are strongly related
      to order-flow imbalance at the best bid/ask.
    - Multi-level OFI literature: deeper LOB pressure can add predictive signal.
    - Crypto LOB research: engineered order-flow, imbalance, trade-sign, and
      liquidity features often matter more than adding model complexity.

  Usage:
    1. Load after terminal.html.
    2. Start with:
         OrderFlowPredictor.start({ qty: 0.001 })
    3. Stop and cancel strategy-owned orders:
         OrderFlowPredictor.stop()
*/
(function attachOrderFlowPredictor(global) {
  'use strict';

  const DEFAULTS = {
    qty: null,
    fallbackQty: 0.001,
    maxPosition: 0.006,
    maxPositionMultiplier: 6,
    quoteEveryMs: 350,
    orderTtlMs: 900,
    exitTtlMs: 4500,
    levels: 5,
    signalThreshold: 0.38,
    exitThreshold: 0.12,
    minSpreadBps: 0,
    maxSpreadBps: 120,
    maxVolBps: 350,
    volGuardSpreadMultiplier: 10,
    minTradeSamples: 4,
    historyMax: 120,
    minHoldMs: 700,
    actionCooldownMs: 1000,
    allowPyramiding: false,
    takeProfitBps: 4,
    stopLossBps: 6,
    executionMode: 'limit',
  };

  const local = {
    cfg: { ...DEFAULTS },
    timer: null,
    orderIds: new Set(),
    trades: [],
    mids: [],
    lastBook: null,
    lastReason: 'idle',
    lastSignal: null,
    entryTs: 0,
    lastActionTs: 0,
  };

  function requireTerminal() {
    if (
      typeof state === 'undefined' ||
      typeof bestBid !== 'function' ||
      typeof bestAsk !== 'function' ||
      typeof topBids !== 'function' ||
      typeof topAsks !== 'function' ||
      typeof placeOrder !== 'function' ||
      typeof placeMarketOrder !== 'function' ||
      typeof cancelOrder !== 'function'
    ) {
      throw new Error('OrderFlowPredictor must be loaded after terminal.html has initialized');
    }
  }

  function active(order) {
    return order && (order.status === 'OPEN' || order.status === 'PARTIAL');
  }

  function tickSize() {
    return Math.pow(10, -state.priceDec);
  }

  function getQty() {
    if (Number.isFinite(local.cfg.qty) && local.cfg.qty > 0) return local.cfg.qty;
    const inputQty = parseFloat(document.getElementById('qty-input')?.value);
    if (Number.isFinite(inputQty) && inputQty > 0) return inputQty;
    return local.cfg.fallbackQty;
  }

  function positionQty() {
    return state.positions[state.symbol]?.netQty || 0;
  }

  function captureTrade(trade) {
    local.trades.push({
      price: trade.price,
      qty: trade.qty,
      side: trade.side,
      ts: trade.ts || Date.now(),
    });
    while (local.trades.length > local.cfg.historyMax) local.trades.shift();
  }

  function installTradeCapture() {
    if (global.__ofpTradeCaptureInstalled) return;
    const originalHandleTrade = handleTrade;
    handleTrade = function wrappedHandleTrade(trade) {
      captureTrade(trade);
      return originalHandleTrade.apply(this, arguments);
    };
    global.__ofpTradeCaptureInstalled = true;
  }

  function bookFeatures() {
    const bb = bestBid();
    const ba = bestAsk();
    if (!bb || !ba || ba <= bb) return null;

    const bids = topBids(local.cfg.levels);
    const asks = topAsks(local.cfg.levels);
    const bestBidQty = bids[0]?.[1] || 0;
    const bestAskQty = asks[0]?.[1] || 0;
    const mid = (bb + ba) / 2;
    const spreadBps = (ba - bb) / mid * 10000;
    const bidDepth = bids.reduce((sum, [, q]) => sum + q, 0);
    const askDepth = asks.reduce((sum, [, q]) => sum + q, 0);
    const depthImb = bidDepth + askDepth > 0 ? (bidDepth - askDepth) / (bidDepth + askDepth) : 0;
    const queueImb = bestBidQty + bestAskQty > 0 ? (bestBidQty - bestAskQty) / (bestBidQty + bestAskQty) : 0;
    const micro = bestBidQty + bestAskQty > 0
      ? (ba * bestBidQty + bb * bestAskQty) / (bestBidQty + bestAskQty)
      : mid;
    const microBps = (micro - mid) / mid * 10000;

    return { bb, ba, mid, spreadBps, bids, asks, depthImb, queueImb, microBps };
  }

  function updateVol(mid) {
    const now = Date.now();
    const last = local.mids[local.mids.length - 1];
    if (!last || Math.abs(mid - last.mid) > tickSize() / 2) local.mids.push({ mid, ts: now });
    while (local.mids.length > local.cfg.historyMax) local.mids.shift();
    if (local.mids.length < 8) return 0;

    const returns = [];
    for (let i = 1; i < local.mids.length; i++) {
      returns.push(Math.log(local.mids[i].mid / local.mids[i - 1].mid));
    }
    const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / Math.max(1, returns.length - 1);
    return Math.sqrt(variance) * 10000;
  }

  function orderFlowImbalance(book) {
    if (!local.lastBook) {
      local.lastBook = book;
      return 0;
    }

    let ofi = 0;
    for (let i = 0; i < local.cfg.levels; i++) {
      const prevBid = local.lastBook.bids[i];
      const prevAsk = local.lastBook.asks[i];
      const bid = book.bids[i];
      const ask = book.asks[i];
      const weight = 1 / (i + 1);

      if (prevBid && bid) {
        if (bid[0] > prevBid[0]) ofi += weight * bid[1];
        else if (bid[0] < prevBid[0]) ofi -= weight * prevBid[1];
        else ofi += weight * (bid[1] - prevBid[1]);
      }
      if (prevAsk && ask) {
        if (ask[0] < prevAsk[0]) ofi -= weight * ask[1];
        else if (ask[0] > prevAsk[0]) ofi += weight * prevAsk[1];
        else ofi -= weight * (ask[1] - prevAsk[1]);
      }
    }

    const scale = book.bids.reduce((s, [, q]) => s + q, 0) + book.asks.reduce((s, [, q]) => s + q, 0);
    local.lastBook = book;
    return scale > 0 ? Math.max(-1, Math.min(1, ofi / scale)) : 0;
  }

  function tradePressure() {
    const now = Date.now();
    const recent = local.trades.filter(t => now - t.ts < 2500);
    if (recent.length < local.cfg.minTradeSamples) return 0;
    const signed = recent.reduce((sum, t) => sum + (t.side === 'BUY' ? t.qty : -t.qty), 0);
    const total = recent.reduce((sum, t) => sum + t.qty, 0);
    return total > 0 ? Math.max(-1, Math.min(1, signed / total)) : 0;
  }

  function signal() {
    const book = bookFeatures();
    if (!book) return { skip: true, reason: 'waiting for book' };
    const volBps = updateVol(book.mid);
    const volLimitBps = Math.max(local.cfg.maxVolBps, book.spreadBps * local.cfg.volGuardSpreadMultiplier);
    if (book.spreadBps < local.cfg.minSpreadBps) return { skip: true, reason: 'spread too tight' };
    if (book.spreadBps > local.cfg.maxSpreadBps) return { skip: true, reason: 'spread too wide' };
    if (volBps > volLimitBps) return { skip: true, reason: `volatility guard ${volBps.toFixed(1)}>${volLimitBps.toFixed(1)}bps` };

    const ofi = orderFlowImbalance(book);
    const tape = tradePressure();
    const raw =
      0.34 * book.depthImb +
      0.22 * book.queueImb +
      0.26 * ofi +
      0.18 * tape +
      0.04 * Math.max(-1, Math.min(1, book.microBps / Math.max(1, book.spreadBps)));
    const score = Math.max(-1, Math.min(1, raw));

    return { skip: false, score, side: score > 0 ? 'BUY' : 'SELL', book, volBps, volLimitBps, ofi, tape };
  }

  function ownActiveOrders() {
    const orders = [];
    for (const id of local.orderIds) {
      const order = state.orders[id];
      if (active(order)) orders.push(order);
      else local.orderIds.delete(id);
    }
    return orders;
  }

  function cancelOwned(predicate) {
    for (const order of ownActiveOrders()) {
      if (!predicate || predicate(order)) cancelOrder(order.id);
    }
  }

  function placeOrReplace(side, price, qty) {
    if (local.cfg.executionMode === 'market') {
      cancelOwned();
      const id = placeMarketOrder(side, qty);
      if (id) local.orderIds.add(id);
      local.lastActionTs = Date.now();
      return;
    }

    const existing = ownActiveOrders().filter(o => o.side === side);
    const t = tickSize();
    let hasGood = false;
    for (const order of existing) {
      const stale = Date.now() - order.placedAt > local.cfg.orderTtlMs;
      const moved = Math.abs(order.price - price) > t / 2;
      const wrongQty = Math.abs(order.qty - qty) > 1e-12;
      if (stale || moved || wrongQty) cancelOrder(order.id);
      else hasGood = true;
    }
    for (const order of ownActiveOrders()) {
      if (order.side !== side) cancelOrder(order.id);
    }
    if (!hasGood) {
      local.orderIds.add(placeOrder(side, price, qty));
      local.lastActionTs = Date.now();
    }
  }

  function maybeExit(book, pos, score) {
    if (pos === 0) return false;
    const age = Date.now() - local.entryTs;
    const avgEntry = state.positions[state.symbol]?.avgEntry || book.mid;
    const pnlBps = pos > 0 ? (book.mid - avgEntry) / avgEntry * 10000 : (avgEntry - book.mid) / avgEntry * 10000;
    const signalFlipped = (pos > 0 && score < -local.cfg.exitThreshold) || (pos < 0 && score > local.cfg.exitThreshold);
    const timedOut = age > local.cfg.exitTtlMs;
    const takeProfit = pnlBps >= local.cfg.takeProfitBps;
    const stopLoss = pnlBps <= -local.cfg.stopLossBps;
    if (signalFlipped && age < local.cfg.minHoldMs && !stopLoss) return false;
    if (!signalFlipped && !timedOut && !takeProfit && !stopLoss) return false;

    cancelOwned();
    const side = pos > 0 ? 'SELL' : 'BUY';
    const price = side === 'SELL' ? book.ba : book.bb;
    if (local.cfg.executionMode === 'market') {
      const id = placeMarketOrder(side, Math.abs(pos));
      if (id) local.orderIds.add(id);
    } else {
      local.orderIds.add(placeOrder(side, price, Math.abs(pos)));
    }
    local.lastActionTs = Date.now();
    local.lastReason = takeProfit ? 'take profit' : stopLoss ? 'stop loss' : signalFlipped ? 'signal flip exit' : 'time exit';
    emitStatus();
    return true;
  }

  function emitStatus() {
    global.dispatchEvent(new CustomEvent('ofp-status', { detail: status() }));
  }

  function tick() {
    try {
      requireTerminal();
      installTradeCapture();
      const s = signal();
      local.lastSignal = s;

      if (s.skip) {
        local.lastReason = s.reason;
        cancelOwned();
        emitStatus();
        return s;
      }

      const qty = getQty();
      if (!Number.isFinite(qty) || qty <= 0) {
        local.lastReason = 'invalid qty';
        cancelOwned();
        emitStatus();
        return s;
      }

      const maxPosition = Math.max(local.cfg.maxPosition, qty * local.cfg.maxPositionMultiplier);
      const pos = positionQty();
      if (maybeExit(s.book, pos, s.score)) return s;
      if (Date.now() - local.lastActionTs < local.cfg.actionCooldownMs) {
        local.lastReason = `cooldown ${s.score.toFixed(2)}`;
        emitStatus();
        return s;
      }

      if (Math.abs(s.score) < local.cfg.signalThreshold) {
        local.lastReason = `neutral ${s.score.toFixed(2)}`;
        cancelOwned();
        emitStatus();
        return s;
      }

      const wantsBuy = s.score > 0;
      const sameDirectionPosition = (wantsBuy && pos > 0) || (!wantsBuy && pos < 0);
      const hasEntryOrder = ownActiveOrders().some(o => o.side === (wantsBuy ? 'BUY' : 'SELL'));
      if (!local.cfg.allowPyramiding && (sameDirectionPosition || hasEntryOrder)) {
        local.lastReason = `holding ${s.score.toFixed(2)}`;
      } else if (wantsBuy && pos + qty <= maxPosition) {
        placeOrReplace('BUY', s.book.bb, qty);
        if (pos <= 0) local.entryTs = Date.now();
        local.lastReason = `predict up ${s.score.toFixed(2)}`;
      } else if (!wantsBuy && pos - qty >= -maxPosition) {
        placeOrReplace('SELL', s.book.ba, qty);
        if (pos >= 0) local.entryTs = Date.now();
        local.lastReason = `predict down ${s.score.toFixed(2)}`;
      } else {
        local.lastReason = 'position cap';
        cancelOwned();
      }
      emitStatus();
      return s;
    } catch (err) {
      local.lastReason = err.message;
      console.warn('[OrderFlowPredictor]', err.message);
      stop(false);
      emitStatus();
      return { skip: true, reason: err.message };
    }
  }

  function start(config = {}) {
    requireTerminal();
    installTradeCapture();
    local.cfg = { ...local.cfg, ...config };
    if (local.timer) clearInterval(local.timer);
    tick();
    local.timer = setInterval(tick, local.cfg.quoteEveryMs);
    console.log('[OrderFlowPredictor] started', local.cfg);
    emitStatus();
    return status();
  }

  function stop(cancel = true) {
    if (local.timer) clearInterval(local.timer);
    local.timer = null;
    if (cancel) cancelOwned();
    local.lastReason = 'stopped';
    console.log('[OrderFlowPredictor] stopped');
    emitStatus();
    return status();
  }

  function configure(config = {}) {
    local.cfg = { ...local.cfg, ...config };
    return status();
  }

  function status() {
    return {
      running: !!local.timer,
      symbol: typeof state !== 'undefined' ? state.symbol : null,
      ownedOrderIds: Array.from(local.orderIds),
      activeOwnedOrders: typeof state !== 'undefined' ? ownActiveOrders().map(o => ({
        id: o.id,
        side: o.side,
        price: o.price,
        qty: o.qty,
        filledQty: o.filledQty,
        queueAhead: o.queueAhead,
        status: o.status,
      })) : [],
      position: typeof state !== 'undefined' ? positionQty() : null,
      lastReason: local.lastReason,
      lastSignal: local.lastSignal,
      config: { ...local.cfg },
    };
  }

  global.OrderFlowPredictor = { start, stop, tick, configure, status };
})(window);
