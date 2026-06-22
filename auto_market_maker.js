/*
  AutoMM: Avellaneda-Stoikov-inspired market maker for terminal.html.

  Usage:
    1. Load this file after terminal.html's main script, or paste it into DevTools.
    2. Start with:
         AutoMM.start({ qty: 0.001, maxInventory: 0.01 })
    3. Stop and cancel strategy-owned orders with:
         AutoMM.stop()

  The strategy is research-backed, not guaranteed profitable. It is designed for
  simulated execution and should not be connected to a real exchange without
  exchange adapters, risk checks, fees, latency modelling, and live kill-switches.
*/
(function attachAutoMM(global) {
  'use strict';

  const DEFAULTS = {
    qty: null,                 // null = read #qty-input, fallback to fallbackQty.
    fallbackQty: 0.001,
    maxInventory: 0.01,
    maxInventoryMultiplier: 10,
    quoteEveryMs: 750,
    orderTtlMs: 2500,
    horizonSec: 20,
    minSpreadBps: 0,
    maxSpreadBps: 100,
    maxVolBps: 250,
    volGuardSpreadMultiplier: 8,
    minHalfSpreadBps: 0,
    volSpreadMultiplier: 0.05,
    inventorySpreadBps: 0.4,
    inventorySkewBps: 2.5,
    volSkewMultiplier: 0.15,
    micropriceWeight: 0.65,
    imbalanceCancelThreshold: 0.78,
    historyMax: 160,
  };

  const stateLocal = {
    cfg: { ...DEFAULTS },
    timer: null,
    orderIds: new Set(),
    midHistory: [],
    lastQuote: null,
    lastReason: 'idle',
  };

  function requireTerminal() {
    if (
      typeof state === 'undefined' ||
      typeof bestBid !== 'function' ||
      typeof bestAsk !== 'function' ||
      typeof topBids !== 'function' ||
      typeof topAsks !== 'function' ||
      typeof placeOrder !== 'function' ||
      typeof cancelOrder !== 'function'
    ) {
      throw new Error('AutoMM must be loaded after terminal.html has initialized');
    }
  }

  function active(order) {
    return order && (order.status === 'OPEN' || order.status === 'PARTIAL');
  }

  function tickSize() {
    return Math.pow(10, -state.priceDec);
  }

  function roundBid(px) {
    const t = tickSize();
    return Math.floor(px / t) * t;
  }

  function roundAsk(px) {
    const t = tickSize();
    return Math.ceil(px / t) * t;
  }

  function getQty() {
    if (Number.isFinite(stateLocal.cfg.qty) && stateLocal.cfg.qty > 0) return stateLocal.cfg.qty;
    const inputQty = parseFloat(document.getElementById('qty-input')?.value);
    if (Number.isFinite(inputQty) && inputQty > 0) return inputQty;
    return stateLocal.cfg.fallbackQty;
  }

  function positionQty() {
    return state.positions[state.symbol]?.netQty || 0;
  }

  function bookMetrics() {
    const bb = bestBid();
    const ba = bestAsk();
    if (!bb || !ba || ba <= bb) return null;

    const bids = topBids(5);
    const asks = topAsks(5);
    const bestBidQty = bids[0]?.[1] || 0;
    const bestAskQty = asks[0]?.[1] || 0;
    const mid = (bb + ba) / 2;
    const spread = ba - bb;
    const spreadBps = spread / mid * 10000;
    const micro = bestBidQty + bestAskQty > 0
      ? (ba * bestBidQty + bb * bestAskQty) / (bestBidQty + bestAskQty)
      : mid;
    const depthBid = bids.reduce((sum, [, qty]) => sum + qty, 0);
    const depthAsk = asks.reduce((sum, [, qty]) => sum + qty, 0);
    const imbalance = depthBid + depthAsk > 0 ? depthBid / (depthBid + depthAsk) : 0.5;

    return { bb, ba, mid, spread, spreadBps, micro, imbalance };
  }

  function updateVol(mid) {
    const now = Date.now();
    const hist = stateLocal.midHistory;
    const last = hist[hist.length - 1];
    if (!last || Math.abs(mid - last.mid) > tickSize() / 2) hist.push({ mid, ts: now });
    while (hist.length > stateLocal.cfg.historyMax) hist.shift();
    if (hist.length < 8) return 0;

    const returns = [];
    for (let i = 1; i < hist.length; i++) {
      returns.push(Math.log(hist[i].mid / hist[i - 1].mid));
    }
    const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / Math.max(1, returns.length - 1);
    return Math.sqrt(variance) * 10000;
  }

  function desiredQuote() {
    const m = bookMetrics();
    if (!m) return { skip: true, reason: 'waiting for book' };

    const volBps = updateVol(m.mid);
    if (m.spreadBps < stateLocal.cfg.minSpreadBps) return { skip: true, reason: 'spread too tight' };
    if (m.spreadBps > stateLocal.cfg.maxSpreadBps) return { skip: true, reason: 'spread too wide' };
    const volLimitBps = Math.max(
      stateLocal.cfg.maxVolBps,
      m.spreadBps * stateLocal.cfg.volGuardSpreadMultiplier
    );
    if (volBps > volLimitBps) return { skip: true, reason: `volatility guard ${volBps.toFixed(1)}>${volLimitBps.toFixed(1)}bps` };

    const quoteQty = getQty();
    const maxInventory = Math.max(
      stateLocal.cfg.maxInventory,
      quoteQty * stateLocal.cfg.maxInventoryMultiplier
    );
    const inv = positionQty();
    const invRatio = Math.max(-1, Math.min(1, inv / maxInventory));
    const fair = m.mid * (1 - stateLocal.cfg.micropriceWeight) + m.micro * stateLocal.cfg.micropriceWeight;
    const inventorySkew = invRatio * (stateLocal.cfg.inventorySkewBps + volBps * stateLocal.cfg.volSkewMultiplier) * m.mid / 10000;
    const reservation = fair - inventorySkew;
    const halfSpreadBps = Math.max(
      stateLocal.cfg.minHalfSpreadBps,
      m.spreadBps / 2 + volBps * stateLocal.cfg.volSpreadMultiplier + Math.abs(invRatio) * stateLocal.cfg.inventorySpreadBps
    );
    const halfSpread = halfSpreadBps * m.mid / 10000;

    let bid = Math.min(m.bb, roundBid(reservation - halfSpread));
    let ask = Math.max(m.ba, roundAsk(reservation + halfSpread));
    if (bid >= m.ba) bid = m.bb;
    if (ask <= m.bb) ask = m.ba;

    const allowBid = inv < maxInventory && m.imbalance < stateLocal.cfg.imbalanceCancelThreshold;
    const allowAsk = inv > -maxInventory && m.imbalance > (1 - stateLocal.cfg.imbalanceCancelThreshold);

    return {
      skip: false,
      bid,
      ask,
      allowBid,
      allowAsk,
      qty: quoteQty,
      metrics: { ...m, volBps, volLimitBps, inv, invRatio, maxInventory, fair, reservation, halfSpreadBps },
    };
  }

  function emitStatus() {
    global.dispatchEvent(new CustomEvent('automm-status', { detail: status() }));
  }

  function ownActiveOrders() {
    const orders = [];
    for (const id of stateLocal.orderIds) {
      const order = state.orders[id];
      if (active(order)) orders.push(order);
      else stateLocal.orderIds.delete(id);
    }
    return orders;
  }

  function cancelOwned(predicate) {
    for (const order of ownActiveOrders()) {
      if (!predicate || predicate(order)) cancelOrder(order.id);
    }
  }

  function syncSide(side, price, qty, allowed) {
    const existing = ownActiveOrders().filter(o => o.side === side);
    if (!allowed) {
      for (const order of existing) cancelOrder(order.id);
      return;
    }

    const ttl = stateLocal.cfg.orderTtlMs;
    const t = tickSize();
    let hasGoodOrder = false;
    for (const order of existing) {
      const stale = Date.now() - order.placedAt > ttl;
      const moved = Math.abs(order.price - price) > t / 2;
      const wrongQty = Math.abs(order.qty - qty) > 1e-12;
      if (stale || moved || wrongQty) cancelOrder(order.id);
      else hasGoodOrder = true;
    }

    if (!hasGoodOrder) {
      const id = placeOrder(side, price, qty);
      stateLocal.orderIds.add(id);
    }
  }

  function tick() {
    try {
      requireTerminal();
      const quote = desiredQuote();
      stateLocal.lastQuote = quote;

      if (quote.skip) {
        stateLocal.lastReason = quote.reason;
        cancelOwned();
        emitStatus();
        return quote;
      }

      if (!Number.isFinite(quote.qty) || quote.qty <= 0) {
        stateLocal.lastReason = 'invalid qty';
        cancelOwned();
        emitStatus();
        return quote;
      }

      syncSide('BUY', quote.bid, quote.qty, quote.allowBid);
      syncSide('SELL', quote.ask, quote.qty, quote.allowAsk);
      stateLocal.lastReason = 'quoting';
      emitStatus();
      return quote;
    } catch (err) {
      stateLocal.lastReason = err.message;
      console.warn('[AutoMM]', err.message);
      stop(false);
      emitStatus();
      return { skip: true, reason: err.message };
    }
  }

  function start(config = {}) {
    requireTerminal();
    stateLocal.cfg = { ...stateLocal.cfg, ...config };
    if (stateLocal.timer) clearInterval(stateLocal.timer);
    tick();
    stateLocal.timer = setInterval(tick, stateLocal.cfg.quoteEveryMs);
    console.log('[AutoMM] started', stateLocal.cfg);
    emitStatus();
    return status();
  }

  function stop(cancel = true) {
    if (stateLocal.timer) clearInterval(stateLocal.timer);
    stateLocal.timer = null;
    if (cancel) cancelOwned();
    console.log('[AutoMM] stopped');
    emitStatus();
    return status();
  }

  function configure(config = {}) {
    stateLocal.cfg = { ...stateLocal.cfg, ...config };
    return status();
  }

  function status() {
    return {
      running: !!stateLocal.timer,
      symbol: typeof state !== 'undefined' ? state.symbol : null,
      ownedOrderIds: Array.from(stateLocal.orderIds),
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
      lastReason: stateLocal.lastReason,
      lastQuote: stateLocal.lastQuote,
      config: { ...stateLocal.cfg },
    };
  }

  global.AutoMM = { start, stop, tick, configure, status };
})(window);
