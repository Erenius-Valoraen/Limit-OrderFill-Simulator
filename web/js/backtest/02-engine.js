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

// ── Backtest: per-contract fee model ───────────────────────────────────────
// In live (Binance) mode, `makerFeePercent`/`takerFeePercent` are %
// of notional. In backtest mode against CME futures (NQ etc.), fees are
// a flat dollar amount per contract per side — independent of price.
// The inputs in the entry bar are reinterpreted as $/contract when
// `window.__backtest.contract.is_futures` is true.
function feeForFill(price, qty, reason) {
  const isFutures = !!(window.__backtest && window.__backtest.contract && window.__backtest.contract.is_futures);
  if (isFutures) {
    const perContract = reason === 'MARKET' || reason === 'SWEEP'
      ? state.takerFeePercent     // reused field; now interpreted as $/contract
      : state.makerFeePercent;
    return Math.abs(qty) * (Number.isFinite(perContract) ? perContract : 0);
  }
  const pct = reason === 'MARKET' || reason === 'SWEEP' ? state.takerFeePercent : state.makerFeePercent;
  return price * qty * pct / 100;
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
  const feeRate = feeType === 'TAKER' ? state.takerFeePercent : state.makerFeePercent;
  state.executions.push({
    id: `${order.id}-${order.fills.length}-${ts}`,
    orderId: order.id,
    symbol: order.symbol,
    side: order.side,
    price, qty, reason, feeType,
    feePercentAtFill: feeRate,                  // re-purposed as $/contract for futures
    feePaidAtFill: feeForFill(price, qty, reason),
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
  const fee = feeForFill(fillPrice, fillQty, reason);
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

