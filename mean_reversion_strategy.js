/*
  MeanReversionStrategy: passive limit-order mean reversion strategy.

  Research basis:
    - Cont, Kukanov, Stoikov: order-flow imbalance explains short-run price
      pressure, so strong one-sided flow is treated as adverse-selection risk.
    - Bechler, Ludkovski: LOB resiliency and deeper shape matter beyond top-book
      imbalance; this strategy waits for stretched prices with replenishing depth.
    - Avellaneda-Stoikov: inventory-aware passive quoting reduces inventory risk.

  The strategy only submits limit orders. It fades short-term dislocations from an
  adaptive EWMA fair value and exits passively when price reverts, risk limits are
  hit, or the setup goes stale.
*/
(function attachMeanReversionStrategy(global) {
  'use strict';

  const DEFAULTS = {
    qty: null,
    fallbackQty: 0.001,
    maxPosition: 0.006,
    maxPositionMultiplier: 6,
    quoteEveryMs: 450,
    orderTtlMs: 1200,
    exitTtlMs: 6500,
    levels: 8,
    ewmaAlpha: 0.08,
    minSamples: 18,
    signalThreshold: 0.52,
    exitThreshold: 0.16,
    maxAdversePressure: 0.72,
    minSpreadBps: 0,
    maxSpreadBps: 140,
    maxVolBps: 300,
    volGuardSpreadMultiplier: 9,
    minHoldMs: 900,
    actionCooldownMs: 900,
    allowPyramiding: false,
    takeProfitBps: 3.5,
    stopLossBps: 7,
    passiveOffsetTicks: 0,
    exitImproveTicks: 0,
  };

  const local = {
    cfg: { ...DEFAULTS },
    timer: null,
    orderIds: new Set(),
    trades: [],
    mids: [],
    ewmaMid: null,
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
      typeof cancelOrder !== 'function'
    ) {
      throw new Error('MeanReversionStrategy must be loaded after terminal.html has initialized');
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

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
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
    while (local.trades.length > 180) local.trades.shift();
  }

  function installTradeCapture() {
    if (global.__mrvTradeCaptureInstalled) return;
    const originalHandleTrade = handleTrade;
    handleTrade = function wrappedHandleTrade(trade) {
      captureTrade(trade);
      return originalHandleTrade.apply(this, arguments);
    };
    global.__mrvTradeCaptureInstalled = true;
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
    const spread = ba - bb;
    const spreadBps = spread / mid * 10000;
    const bidDepth = bids.reduce((sum, [, q]) => sum + q, 0);
    const askDepth = asks.reduce((sum, [, q]) => sum + q, 0);
    const depthImb = bidDepth + askDepth > 0 ? (bidDepth - askDepth) / (bidDepth + askDepth) : 0;
    const queueImb = bestBidQty + bestAskQty > 0 ? (bestBidQty - bestAskQty) / (bestBidQty + bestAskQty) : 0;
    const micro = bestBidQty + bestAskQty > 0
      ? (ba * bestBidQty + bb * bestAskQty) / (bestBidQty + bestAskQty)
      : mid;
    const microBps = (micro - mid) / mid * 10000;

    return { bb, ba, mid, spread, spreadBps, bids, asks, bidDepth, askDepth, depthImb, queueImb, micro, microBps };
  }

  function updateMidStats(mid) {
    const now = Date.now();
    const last = local.mids[local.mids.length - 1];
    if (!last || Math.abs(mid - last.mid) > tickSize() / 2) {
      local.mids.push({ mid, ts: now });
      local.ewmaMid = local.ewmaMid == null
        ? mid
        : local.cfg.ewmaAlpha * mid + (1 - local.cfg.ewmaAlpha) * local.ewmaMid;
    }
    while (local.mids.length > 180) local.mids.shift();

    if (local.mids.length < 8) return { volBps: 0, devBps: 0, z: 0, sigmaDevBps: 0 };
    const returns = [];
    const devs = [];
    for (let i = 1; i < local.mids.length; i++) {
      returns.push(Math.log(local.mids[i].mid / local.mids[i - 1].mid));
      devs.push((local.mids[i].mid - local.ewmaMid) / local.ewmaMid * 10000);
    }
    const retMean = returns.reduce((a, b) => a + b, 0) / returns.length;
    const retVar = returns.reduce((sum, r) => sum + Math.pow(r - retMean, 2), 0) / Math.max(1, returns.length - 1);
    const devMean = devs.reduce((a, b) => a + b, 0) / devs.length;
    const devVar = devs.reduce((sum, d) => sum + Math.pow(d - devMean, 2), 0) / Math.max(1, devs.length - 1);
    const volBps = Math.sqrt(retVar) * 10000;
    const sigmaDevBps = Math.max(Math.sqrt(devVar), 0.25);
    const devBps = (mid - local.ewmaMid) / local.ewmaMid * 10000;
    return { volBps, devBps, z: clamp(devBps / sigmaDevBps, -4, 4), sigmaDevBps };
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

    const scale = book.bidDepth + book.askDepth;
    local.lastBook = book;
    return scale > 0 ? clamp(ofi / scale, -1, 1) : 0;
  }

  function tradePressure() {
    const now = Date.now();
    const recent = local.trades.filter(t => now - t.ts < 3000);
    if (recent.length < 4) return 0;
    const signed = recent.reduce((sum, t) => sum + (t.side === 'BUY' ? t.qty : -t.qty), 0);
    const total = recent.reduce((sum, t) => sum + t.qty, 0);
    return total > 0 ? clamp(signed / total, -1, 1) : 0;
  }

  function signal() {
    const book = bookFeatures();
    if (!book) return { skip: true, reason: 'waiting for book' };

    const stats = updateMidStats(book.mid);
    const volLimitBps = Math.max(local.cfg.maxVolBps, book.spreadBps * local.cfg.volGuardSpreadMultiplier);
    if (local.mids.length < local.cfg.minSamples) return { skip: true, reason: 'warming up' };
    if (book.spreadBps < local.cfg.minSpreadBps) return { skip: true, reason: 'spread too tight' };
    if (book.spreadBps > local.cfg.maxSpreadBps) return { skip: true, reason: 'spread too wide' };
    if (stats.volBps > volLimitBps) return { skip: true, reason: `volatility guard ${stats.volBps.toFixed(1)}>${volLimitBps.toFixed(1)}bps` };

    const ofi = orderFlowImbalance(book);
    const tape = tradePressure();
    const microLean = clamp(book.microBps / Math.max(1, book.spreadBps), -1, 1);
    const stretched = clamp(stats.z / 2.4, -1, 1);
    const flowPressure = clamp(0.46 * ofi + 0.34 * tape + 0.20 * microLean, -1, 1);
    const resiliency = clamp(-0.65 * book.depthImb - 0.35 * book.queueImb, -1, 1);
    const score = clamp(-0.62 * stretched - 0.24 * flowPressure + 0.14 * resiliency, -1, 1);
    const side = score > 0 ? 'BUY' : 'SELL';
    const adversePressure = side === 'BUY' ? -flowPressure : flowPressure;

    if (Math.abs(stretched) < 0.25) {
      return { skip: false, neutral: true, reason: `near mean z=${stats.z.toFixed(2)}`, score, side, book, stats, ofi, tape, flowPressure };
    }
    if (adversePressure > local.cfg.maxAdversePressure) {
      return { skip: false, neutral: true, reason: `one-way flow ${flowPressure.toFixed(2)}`, score, side, book, stats, ofi, tape, flowPressure };
    }

    return { skip: false, score, side, book, stats, ofi, tape, flowPressure };
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

  function passiveEntryPrice(side, book) {
    const offset = local.cfg.passiveOffsetTicks * tickSize();
    return side === 'BUY'
      ? roundBid(book.bb - offset)
      : roundAsk(book.ba + offset);
  }

  function passiveExitPrice(side, book) {
    const improve = local.cfg.exitImproveTicks * tickSize();
    return side === 'SELL'
      ? roundAsk(book.ba - improve)
      : roundBid(book.bb + improve);
  }

  function placeOrReplace(side, price, qty) {
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
      const id = placeOrder(side, price, qty);
      if (id) local.orderIds.add(id);
      local.lastActionTs = Date.now();
    }
  }

  function maybeExit(book, pos, score) {
    if (pos === 0) return false;
    const age = Date.now() - local.entryTs;
    const avgEntry = state.positions[state.symbol]?.avgEntry || book.mid;
    const pnlBps = pos > 0 ? (book.mid - avgEntry) / avgEntry * 10000 : (avgEntry - book.mid) / avgEntry * 10000;
    const meanReached = Math.abs(score) < local.cfg.exitThreshold;
    const signalAgainst = (pos > 0 && score < -local.cfg.exitThreshold) || (pos < 0 && score > local.cfg.exitThreshold);
    const timedOut = age > local.cfg.exitTtlMs;
    const takeProfit = pnlBps >= local.cfg.takeProfitBps;
    const stopLoss = pnlBps <= -local.cfg.stopLossBps;
    if (!meanReached && !signalAgainst && !timedOut && !takeProfit && !stopLoss) return false;
    if (age < local.cfg.minHoldMs && !stopLoss) return false;

    const side = pos > 0 ? 'SELL' : 'BUY';
    const price = passiveExitPrice(side, book);
    placeOrReplace(side, price, Math.abs(pos));
    local.lastReason = takeProfit ? 'take profit' : stopLoss ? 'stop loss' : meanReached ? 'mean exit' : signalAgainst ? 'signal exit' : 'time exit';
    emitStatus();
    return true;
  }

  function emitStatus() {
    global.dispatchEvent(new CustomEvent('mrv-status', { detail: status() }));
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

      const pos = positionQty();
      if (maybeExit(s.book, pos, s.score)) return s;
      if (Date.now() - local.lastActionTs < local.cfg.actionCooldownMs) {
        local.lastReason = `cooldown ${s.score.toFixed(2)}`;
        emitStatus();
        return s;
      }

      if (s.neutral || Math.abs(s.score) < local.cfg.signalThreshold) {
        local.lastReason = s.reason || `neutral ${s.score.toFixed(2)}`;
        cancelOwned();
        emitStatus();
        return s;
      }

      const maxPosition = Math.max(local.cfg.maxPosition, qty * local.cfg.maxPositionMultiplier);
      const wantsBuy = s.score > 0;
      const sameDirectionPosition = (wantsBuy && pos > 0) || (!wantsBuy && pos < 0);
      const entrySide = wantsBuy ? 'BUY' : 'SELL';
      const hasEntryOrder = ownActiveOrders().some(o => o.side === entrySide);

      if (!local.cfg.allowPyramiding && (sameDirectionPosition || hasEntryOrder)) {
        local.lastReason = `holding reversion ${s.score.toFixed(2)}`;
      } else if (wantsBuy && pos + qty <= maxPosition) {
        placeOrReplace('BUY', passiveEntryPrice('BUY', s.book), qty);
        if (pos <= 0) local.entryTs = Date.now();
        local.lastReason = `fade down ${s.score.toFixed(2)}`;
      } else if (!wantsBuy && pos - qty >= -maxPosition) {
        placeOrReplace('SELL', passiveEntryPrice('SELL', s.book), qty);
        if (pos >= 0) local.entryTs = Date.now();
        local.lastReason = `fade up ${s.score.toFixed(2)}`;
      } else {
        local.lastReason = 'position cap';
        cancelOwned();
      }
      emitStatus();
      return s;
    } catch (err) {
      local.lastReason = err.message;
      console.warn('[MeanReversionStrategy]', err.message);
      stop(false);
      emitStatus();
      return { skip: true, reason: err.message };
    }
  }

  function start(config = {}) {
    requireTerminal();
    installTradeCapture();
    local.cfg = { ...local.cfg, ...config, executionMode: 'limit' };
    if (local.timer) clearInterval(local.timer);
    tick();
    local.timer = setInterval(tick, local.cfg.quoteEveryMs);
    console.log('[MeanReversionStrategy] started', local.cfg);
    emitStatus();
    return status();
  }

  function stop(cancel = true) {
    if (local.timer) clearInterval(local.timer);
    local.timer = null;
    if (cancel) cancelOwned();
    local.lastReason = 'stopped';
    console.log('[MeanReversionStrategy] stopped');
    emitStatus();
    return status();
  }

  function configure(config = {}) {
    local.cfg = { ...local.cfg, ...config, executionMode: 'limit' };
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

  global.MeanReversionStrategy = { start, stop, tick, configure, status };
})(window);
