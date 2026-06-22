#!/usr/bin/env node
/*
 * ofp_runner.js — Headless, fast, FAITHFUL backtest of the live Binance
 * OrderFlowPredictor on NQ.
 *
 * It loads the LITERAL orderflow_predictor_strategy.js (the exact file the live
 * BTC terminal runs) inside a vm context, and drives it with:
 *   - a MARKET-TIME clock: Date.now() returns the replayed market timestamp, and
 *     the strategy's setInterval(tick, 350ms) fires every 350ms of MARKET time
 *     (not wall time) — so it samples the data exactly as it would live, at full
 *     replay speed.
 *   - a Node port of terminal.html's MARKET-order engine: bestBid/topBids/...,
 *     placeMarketOrder (fills after latency via marketFillPrice VWAP over the
 *     real book), executeFill/updatePosition with CME $/contract fees.
 *   - events streamed from the compact binary cache (build_ofp_cache.py), so no
 *     25 GB JSON re-parse.
 *
 * This is the same strategy CODE as live — no Python re-port — run fast.
 *
 * Usage:
 *   node ofp_runner.js [--qty 1] [--fee 1.25] [--point-value 20] [--latency 100]
 *                      [--exec market] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
 *   (build the cache first:  python build_ofp_cache.py )
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

// ---- args ----
function arg(name, def) {
  const i = process.argv.indexOf('--' + name);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : def;
}
const QTY = parseFloat(arg('qty', '1'));
const FEE = parseFloat(arg('fee', '1.25'));          // $/contract per side
const MULT = parseFloat(arg('point-value', '20'));   // $ per index point
const LATENCY = parseFloat(arg('latency', '100'));   // ms market-order latency
const EXEC = arg('exec', 'market');
const START = arg('start', null);
const END = arg('end', null);
const CACHE = arg('cache', 'ofp_cache');

// OFP strategy-config overrides (so it can be retuned for NQ without touching the .js)
const CFG = { qty: NaN, executionMode: EXEC, maxVolBps: 250, volGuardSpreadMultiplier: 8 };
function cfgOpt(flag, key) { const v = arg(flag, null); if (v !== null) CFG[key] = parseFloat(v); }
cfgOpt('signal-threshold', 'signalThreshold');
cfgOpt('exit-threshold', 'exitThreshold');
cfgOpt('tp-bps', 'takeProfitBps');
cfgOpt('sl-bps', 'stopLossBps');
cfgOpt('exit-ttl', 'exitTtlMs');
cfgOpt('min-hold', 'minHoldMs');
cfgOpt('cooldown', 'actionCooldownMs');
cfgOpt('quote-every', 'quoteEveryMs');
cfgOpt('levels', 'levels');
cfgOpt('max-vol-bps', 'maxVolBps');
cfgOpt('vol-guard', 'volGuardSpreadMultiplier');
// --invert: report the MIRROR strategy (trade the opposite side at the same
// moments, filling at the opposite touch). Decisive test for an anti-predictive signal.
const INVERT = process.argv.includes('--invert');

const meta = JSON.parse(fs.readFileSync(path.join(CACHE, 'meta.json'), 'utf8'));
const TICK = meta.tick || 0.25;
const SYMBOL = meta.symbol || 'NQ';
const OFP_SRC = fs.readFileSync(path.join(__dirname, 'orderflow_predictor_strategy.js'), 'utf8');
const QEPS = 1e-10;

// ---- one engine + strategy instance per day (fresh state) ----
function runDay(dayMeta) {
  const bin = fs.readFileSync(path.join(CACHE, dayMeta.date + '.bin'));
  const anchor = dayMeta.anchor_ms;

  // market clock + scheduling
  let MS = 0;
  const pending = [];          // market-order fills awaiting latency: {dueMs,id,side,qty}
  let interval = null;         // {fn, period, next} from the strategy's setInterval
  let oid = 0;

  // engine state (mirror of terminal.html `state`, market-mode subset)
  const state = {
    bids: {}, asks: {}, orders: {}, positions: {},
    symbol: SYMBOL, priceDec: 2, lastPrice: null, latencyMs: LATENCY,
  };

  // accounting
  let fills = 0, contracts = 0;
  let cyc = null;              // {pts, fees, entryMs}
  const trades = [];          // finalized round-trips {net,pts,fees,holdMs}
  // shadow inverse (only populated when INVERT)
  let invNet = 0, invAvg = 0, invCyc = null;
  const invTrades = [];

  function posObj() {
    return state.positions[state.symbol] ||
      (state.positions[state.symbol] = { netQty: 0, avgEntry: 0, realizedPnl: 0 });
  }

  // ---- book helpers (1:1 with terminal.html) ----
  function bestBid() { const k = Object.keys(state.bids); return k.length ? Math.max.apply(null, k.map(Number)) : null; }
  function bestAsk() { const k = Object.keys(state.asks); return k.length ? Math.min.apply(null, k.map(Number)) : null; }
  function topBids(n) { return Object.keys(state.bids).map(parseFloat).sort((a, b) => b - a).slice(0, n).map(p => [p, state.bids[p]]); }
  function topAsks(n) { return Object.keys(state.asks).map(parseFloat).sort((a, b) => a - b).slice(0, n).map(p => [p, state.asks[p]]); }
  function genId() { return (++oid).toString(36); }

  function marketFillPrice(side, qty) {
    const levels = side === 'BUY' ? topAsks(20) : topBids(20);
    let remaining = qty, notional = 0, filled = 0;
    for (const [price, lq] of levels) {
      if (remaining <= QEPS) break;
      const take = Math.min(remaining, lq);
      notional += price * take; filled += take; remaining -= take;
    }
    if (filled <= QEPS) return null;
    return { price: notional / filled, qty: filled };
  }

  function placeMarketOrder(side, qty) {
    const id = genId(); qty = parseFloat(qty);
    state.orders[id] = { id, side, price: 0, qty, filledQty: 0, status: 'OPEN', isMarketPending: true, placedAt: MS };
    pending.push({ dueMs: MS + state.latencyMs, id, side, qty });
    return id;
  }
  function placeOrder(side, price, qty) {   // limit path (unused in market mode, provided for requireTerminal)
    const id = genId();
    state.orders[id] = { id, side, price: parseFloat(price), qty: parseFloat(qty), filledQty: 0, status: 'OPEN', isMarketPending: false, placedAt: MS };
    return id;
  }
  function cancelOrder(id) {
    const o = state.orders[id];
    if (o && (o.status === 'OPEN' || o.status === 'PARTIAL')) {
      o.status = 'CANCELLED';
      for (let i = pending.length - 1; i >= 0; i--) if (pending[i].id === id) pending.splice(i, 1);
      return true;
    }
    return false;
  }
  function settle(p) {
    const order = state.orders[p.id];
    if (!order || order.status === 'CANCELLED') return;
    order.isMarketPending = false;
    const fill = marketFillPrice(p.side, p.qty);
    if (!fill) { order.status = 'CANCELLED'; return; }
    order.price = fill.price;
    order.filledQty += fill.qty;
    order.status = (order.qty - order.filledQty <= 1e-10) ? 'FILLED' : 'PARTIAL';
    updatePosition(p.side, fill.price, fill.qty);
    if (INVERT) {                              // mirror: opposite side, opposite touch
      const oside = p.side === 'BUY' ? 'SELL' : 'BUY';
      const ifill = marketFillPrice(oside, p.qty);
      if (ifill) updateInv(oside, ifill.price, ifill.qty);
    }
  }
  function updatePosition(side, fillPrice, fillQty) {
    const pos = posObj();
    const isBuy = side === 'BUY';
    const fee = Math.abs(fillQty) * FEE;       // CME $/contract (taker)
    fills++; contracts += Math.abs(fillQty);
    if (!cyc) cyc = { pts: 0, fees: 0, entryMs: MS };
    cyc.fees += fee;
    if (pos.netQty === 0) {
      pos.avgEntry = fillPrice; pos.netQty = isBuy ? fillQty : -fillQty;
    } else if ((isBuy && pos.netQty > 0) || (!isBuy && pos.netQty < 0)) {
      const tq = Math.abs(pos.netQty) + fillQty;
      pos.avgEntry = (Math.abs(pos.netQty) * pos.avgEntry + fillQty * fillPrice) / tq;
      pos.netQty = isBuy ? tq : -tq;
    } else {
      const closeQty = Math.min(fillQty, Math.abs(pos.netQty));
      const pnl = isBuy ? (pos.avgEntry - fillPrice) : (fillPrice - pos.avgEntry);
      cyc.pts += pnl * closeQty;
      const rem = fillQty - closeQty;
      pos.netQty = isBuy ? pos.netQty + fillQty : pos.netQty - fillQty;
      if (Math.abs(pos.netQty) < 1e-10) { pos.netQty = 0; pos.avgEntry = 0; }
      else if (rem > 0) pos.avgEntry = fillPrice;
    }
    if (pos.netQty === 0 && cyc) {
      trades.push({ net: cyc.pts * MULT - cyc.fees, pts: cyc.pts, fees: cyc.fees, holdMs: MS - cyc.entryMs });
      cyc = null;
    }
  }
  function updateInv(side, fillPrice, fillQty) {
    const isBuy = side === 'BUY';
    const fee = Math.abs(fillQty) * FEE;
    if (!invCyc) invCyc = { pts: 0, fees: 0, entryMs: MS };
    invCyc.fees += fee;
    if (invNet === 0) { invAvg = fillPrice; invNet = isBuy ? fillQty : -fillQty; }
    else if ((isBuy && invNet > 0) || (!isBuy && invNet < 0)) {
      const tq = Math.abs(invNet) + fillQty;
      invAvg = (Math.abs(invNet) * invAvg + fillQty * fillPrice) / tq;
      invNet = isBuy ? tq : -tq;
    } else {
      const cq = Math.min(fillQty, Math.abs(invNet));
      const pnl = isBuy ? (invAvg - fillPrice) : (fillPrice - invAvg);
      invCyc.pts += pnl * cq;
      const rem = fillQty - cq;
      invNet = isBuy ? invNet + fillQty : invNet - fillQty;
      if (Math.abs(invNet) < 1e-10) { invNet = 0; invAvg = 0; }
      else if (rem > 0) invAvg = fillPrice;
    }
    if (invNet === 0 && invCyc) {
      invTrades.push({ net: invCyc.pts * MULT - invCyc.fees, pts: invCyc.pts, fees: invCyc.fees, holdMs: MS - invCyc.entryMs });
      invCyc = null;
    }
  }
  function executeFill() { /* folded into settle/updatePosition above */ }
  function handleTrade(trade) { state.lastPrice = trade.price; }  // market mode: no resting-order matching

  // ---- vm sandbox exposing the terminal globals the strategy needs ----
  const sandbox = {
    state, bestBid, bestAsk, topBids, topAsks, placeOrder, placeMarketOrder, cancelOrder,
    handleTrade, console, Math, Number, Object, JSON, Array, isNaN, parseFloat, parseInt,
    document: { getElementById: (id) => id === 'qty-input' ? { value: String(QTY) } : null },
    CustomEvent: function (t, o) { this.type = t; this.detail = o && o.detail; },
    Date: Object.assign(function () { return new Date(); }, { now: () => MS }),
    setInterval: (fn, ms) => { interval = { fn, period: ms, next: MS + ms }; return 1; },
    clearInterval: () => { interval = null; },
    setTimeout: (fn, ms) => { pending.push({ dueMs: MS + ms, _fn: fn }); return 1; },
    clearTimeout: () => {},
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.dispatchEvent = () => {};
  vm.createContext(sandbox);
  vm.runInContext(OFP_SRC, sandbox, { filename: 'orderflow_predictor_strategy.js' });
  const OFP = sandbox.OrderFlowPredictor;

  // advance the market clock to `target`, firing strategy ticks + due fills in order
  function advanceTo(target) {
    for (;;) {
      let t = null, kind = null, idx = -1;
      if (interval && interval.next < target) { t = interval.next; kind = 'tick'; }
      for (let i = 0; i < pending.length; i++) {
        if (pending[i].dueMs < target && (t === null || pending[i].dueMs < t)) { t = pending[i].dueMs; kind = 'fill'; idx = i; }
      }
      if (t === null) break;
      MS = t;
      if (kind === 'tick') { interval.fn(); interval.next += interval.period; }
      else { const p = pending.splice(idx, 1)[0]; if (p._fn) p._fn(); else settle(p); }
    }
  }

  // start the strategy at the RTH open
  MS = anchor;
  OFP.start(Object.assign({}, CFG, { qty: QTY }));

  // ---- drive events from the binary cache ----
  const n = bin.length / 12 | 0;
  let lastTs = anchor;
  for (let r = 0; r < n; r++) {
    const o = r * 12;
    const tag = bin[o];
    const qty = bin.readUInt16LE(o + 2);
    const ts = anchor + bin.readUInt32LE(o + 4);
    const px = bin.readUInt32LE(o + 8) * TICK;
    advanceTo(ts);
    MS = ts;
    if ((tag & 1) === 0) {                 // depth
      const side = (tag >> 1) & 1;          // 0 BID, 1 ASK
      const book = side === 0 ? state.bids : state.asks;
      if (qty === 0) delete book[px]; else book[px] = qty;
    } else {                                // trade
      const side = ((tag >> 1) & 1) === 0 ? 'BUY' : 'SELL';
      state.lastPrice = px;
      sandbox.handleTrade({ price: px, qty, side, ts });   // wrapped -> captureTrade + handleTrade
    }
    lastTs = ts;
  }

  // drain remaining ticks/fills, then flatten residual at last price
  advanceTo(lastTs + 10_000);
  for (const p of pending.slice().sort((a, b) => a.dueMs - b.dueMs)) { MS = Math.max(MS, p.dueMs); if (p._fn) p._fn(); else settle(p); }
  pending.length = 0;
  const pos = posObj();
  if (pos.netQty !== 0 && state.lastPrice !== null) {
    updatePosition(pos.netQty > 0 ? 'SELL' : 'BUY', state.lastPrice, Math.abs(pos.netQty));
  }
  if (INVERT && invNet !== 0 && state.lastPrice !== null) {
    updateInv(invNet > 0 ? 'SELL' : 'BUY', state.lastPrice, Math.abs(invNet));
  }
  try { OFP.stop(false); } catch (e) {}

  return { date: dayMeta.date, trades: INVERT ? invTrades : trades, fills, contracts, events: n };
}

// ---- run all days, report ----
function money(v) { return (v >= 0 ? '+$' : '-$') + Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }

let days = meta.days.slice();
if (START) days = days.filter(d => d.date >= START);
if (END) days = days.filter(d => d.date <= END);

console.log(`[ofp] ${meta.instrument}  tick=${TICK}  exec=${EXEC}  qty=${QTY}  fee=$${FEE}/side  $${MULT}/pt  latency=${LATENCY}ms`);
console.log(`[ofp] config: tp=${CFG.takeProfitBps ?? 4}bps sl=${CFG.stopLossBps ?? 6}bps exitTtl=${CFG.exitTtlMs ?? 4500} sig=${CFG.signalThreshold ?? 0.38} quote=${CFG.quoteEveryMs ?? 350} levels=${CFG.levels ?? 5}`);
console.log(`[ofp] running ${days.length} day(s) of the LITERAL orderflow_predictor_strategy.js on a market-time clock ...`);

const all = [];
const perDay = [];
const t0 = Date.now();
let totEvents = 0;
for (const dm of days) {
  const ds = Date.now();
  const res = runDay(dm);
  totEvents += res.events;
  const net = res.trades.reduce((s, t) => s + t.net, 0);
  perDay.push({ date: dm.date, n: res.trades.length, net });
  all.push(...res.trades);
  console.log(`  ${dm.date}: ${res.trades.length} trades, net ${money(net)}  (${res.events.toLocaleString('en-US')} events, ${((Date.now() - ds) / 1000).toFixed(1)}s)`);
}

const n = all.length;
const wins = all.filter(t => t.net > 0), losses = all.filter(t => t.net < 0);
const net = all.reduce((s, t) => s + t.net, 0);
const gw = wins.reduce((s, t) => s + t.net, 0), gl = -losses.reduce((s, t) => s + t.net, 0);
const fees = all.reduce((s, t) => s + t.fees, 0);
const grossPts = all.reduce((s, t) => s + t.pts, 0);
const holds = all.map(t => t.holdMs);
let eq = 0, peak = 0, dd = 0;
for (const t of all) { eq += t.net; peak = Math.max(peak, eq); dd = Math.max(dd, peak - eq); }

const L = (k, v) => console.log('  ' + k.padEnd(24) + v);
console.log('\n' + '='.repeat(58));
console.log('  ORDER-FLOW PREDICTOR (literal JS) - NQ HEADLESS BACKTEST' + (INVERT ? '  [INVERTED]' : ''));
console.log('='.repeat(58));
L('Days', String(days.length));
L('Events replayed', totEvents.toLocaleString('en-US'));
L('Round-trip trades', n.toLocaleString());
if (n) {
  L('Win rate', `${(wins.length / n * 100).toFixed(1)}%  (${wins.length}W / ${losses.length}L)`);
  L('Gross PnL (points)', `${grossPts >= 0 ? '+' : ''}${grossPts.toFixed(2)} pts`);
  L('Gross PnL', money(grossPts * MULT));
  L('Total fees', money(-fees));
  L('NET PnL', money(net));
  L('Avg trade', money(net / n));
  L('Avg win', wins.length ? money(gw / wins.length) : 'n/a');
  L('Avg loss', losses.length ? money(-gl / losses.length) : 'n/a');
  L('Largest win', money(Math.max(...all.map(t => t.net))));
  L('Largest loss', money(Math.min(...all.map(t => t.net))));
  L('Profit factor', gl > 0 ? (gw / gl).toFixed(2) : 'inf');
  L('Max drawdown', money(-dd));
  L('Avg hold', `${(holds.reduce((a, b) => a + b, 0) / n / 1000).toFixed(1)}s`);
  console.log('-'.repeat(58));
  console.log('  ' + 'Date (ET)'.padEnd(14) + 'Trades'.padStart(8) + 'Net PnL'.padStart(16));
  for (const d of perDay) console.log('  ' + d.date.padEnd(14) + String(d.n).padStart(8) + money(d.net).padStart(16));
} else {
  L('Result', 'no trades');
}
console.log('='.repeat(58));
console.log(`[ofp] done in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
