#!/usr/bin/env python3
"""
mm_sim.py — Passive market-maker simulation on NQ with REAL queue dynamics.

Quotes 1 lot at the best bid and 1 lot at the best ask (joins the touch),
re-quoting when the touch moves, capped by an inventory limit. Fills are
modeled with terminal.html's queue engine, ported faithfully:
  - queue position (queueAhead = size resting ahead of you when you join),
  - cancellation modeling (level-size drops split into trade-depletion vs
    cancels, with a log-weighted cancel-ahead-of-you probability),
  - trade fills (a marketable trade burns your queue then fills you),
  - sweep fills (price trading through your resting order).

This tests the spread-capture edge: a passive round-trip earns 1 tick ($5)
minus 2x fees, but only if a back-of-queue 1-lot actually fills and inventory
doesn't run against you. NQ has NO maker rebate (flat ~$1.25/side).

Reads the L2 event cache (build_ofp_cache.py): ofp_cache/<date>.bin + meta.json.

Usage:
  python3 mm_sim.py [--max-inv 5] [--maker-fee 1.25] [--point-value 20]
                    [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""
import argparse
import json
import math
import sys
import time
from pathlib import Path

import numpy as np

DT = np.dtype([('tag', 'u1'), ('pad', 'u1'), ('qty', '<u2'), ('ts', '<u4'), ('px', '<u4')])
QEPS = 1e-9
CANCEL_BIAS = 1.35
PESSIMISTIC = True      # default: queue advances ONLY via trades (no cancel-ahead credit)


def cancel_ahead_prob(order, next_qty):
    if PESSIMISTIC:
        return 0.0
    front = max(0.0, order['qa'])
    behind = max(0.0, next_qty - front)
    if front <= QEPS:
        return 0.0
    if behind <= QEPS:
        return 1.0
    fw = math.log1p(front); bw = math.log1p(behind)
    return (fw / (fw + bw)) ** CANCEL_BIAS


def update_queue_from_depth(order, prev_qty, next_qty, now):
    if abs(prev_qty - next_qty) <= QEPS:
        return
    if next_qty > prev_qty:
        order['llq'] = next_qty
        return
    drop = prev_qty - next_qty
    if now - order['pdts'] > 1000:
        order['pd'] = 0.0
    explained = min(drop, order['pd'])
    order['pd'] = max(0.0, order['pd'] - explained)
    cancel_qty = max(0.0, drop - explained)
    if cancel_qty > QEPS and order['qa'] > QEPS:
        p = cancel_ahead_prob(order, next_qty)
        ahead_cancelled = min(order['qa'], cancel_qty * p)
        if ahead_cancelled > QEPS:
            order['qa'] -= ahead_cancelled
            order['radv'] += ahead_cancelled
            order['radvts'] = now
    order['qa'] = min(order['qa'], next_qty)
    order['llq'] = next_qty


def check_trade_fill(order, tpx, tqty, tside, now):
    """Return fill qty for this resting order given a trade. Mutates queue."""
    is_buy_fill = order['side'] == 'BUY' and tside == 'SELL' and tpx <= order['px']
    is_sell_fill = order['side'] == 'SELL' and tside == 'BUY' and tpx >= order['px']
    if not is_buy_fill and not is_sell_fill:
        return 0.0, order['px'], None
    rem = order['qty'] - order['filled']
    if tpx == order['px']:                   # trade AT our quoted price
        order['pd'] += tqty
        order['pdts'] = now
        eff = tqty
        if now - order['radvts'] < 250:
            adv = min(eff, order['radv'])
            eff -= adv
            order['radv'] = max(0.0, order['radv'] - adv)
        if order['qa'] > 0:
            burned = min(order['qa'], eff)
            order['qa'] -= burned
            leftover = eff - burned
            if leftover <= 0:
                return 0.0, order['px'], None
            return min(leftover, rem), order['px'], 'queue'   # queue ahead was depleted
        return min(eff, rem), order['px'], 'queue'
    else:                                   # trade printed THROUGH our price
        return min(tqty, rem), order['px'], 'through'         # price ran past us (adverse)


def new_order(side, px_ticks, qty, now, latency):
    # placed now, but only LIVE (fillable, queue captured) at now+latency
    return {'side': side, 'px': px_ticks, 'qty': qty, 'filled': 0.0,
            'qa': 0.0, 'llq': 0.0, 'pd': 0.0, 'pdts': 0,
            'radv': 0.0, 'radvts': 0, 'placed': now,
            'live_at': now + latency, 'is_live': False,
            'dying': False, 'cancel_at': None}


def run_day(date, bin_path, cfg):
    arr = np.fromfile(bin_path, dtype=DT)
    tags = arr['tag']; qtys = arr['qty'].astype(np.int64)
    pxs = arr['px'].astype(np.int64); tss = arr['ts'].astype(np.int64)
    n = arr.shape[0]

    bids = {}; asks = {}
    bid_ord = None; ask_ord = None
    inv = 0.0; avg = 0.0
    realized = 0.0; fees = 0.0
    bid_fills = 0; ask_fills = 0; contracts = 0.0
    max_abs_inv = 0.0
    fee = cfg['maker_fee']; tfee = cfg['taker_fee']
    max_inv = cfg['max_inv']; qsize = cfg['qty']; LAT = cfg['latency_ms']
    QMULT = cfg['queue_mult']
    adverse_fills = 0
    quotes_placed = 0
    # fill breakdown: reason -> [count, markout_ticks_sum]
    by_reason = {'queue': [0, 0.0], 'through': [0, 0.0], 'sweep': [0, 0.0], 'eod': [0, 0.0]}

    def best_bid():
        return max(bids) if bids else None

    def best_ask():
        return min(asks) if asks else None

    def fill(side, px_ticks, fqty, reason, taker=False):
        nonlocal inv, avg, realized, fees, bid_fills, ask_fills, contracts, max_abs_inv
        price = px_ticks  # in ticks
        f = (tfee if taker else fee) * fqty
        fees += f
        contracts += fqty
        if side == 'BUY':
            bid_fills += 1
        else:
            ask_fills += 1
        # immediate markout vs the mid at fill time: + = favorable (we made the
        # spread), - = adverse (price had already moved past us when we filled).
        bbn = best_bid(); ban = best_ask()
        if bbn is not None and ban is not None:
            mid = (bbn + ban) / 2.0
            mk = (mid - price) if side == 'BUY' else (price - mid)
            r = by_reason.setdefault(reason, [0, 0.0])
            r[0] += 1; r[1] += mk
        is_buy = side == 'BUY'
        if inv == 0:
            avg = price; inv = fqty if is_buy else -fqty
        elif (is_buy and inv > 0) or (not is_buy and inv < 0):
            tot = abs(inv) + fqty
            avg = (abs(inv) * avg + fqty * price) / tot
            inv = tot if is_buy else -tot
        else:
            close = min(fqty, abs(inv))
            pnl = (avg - price) if is_buy else (price - avg)
            realized += pnl * close          # in ticks
            rem = fqty - close
            inv = inv + fqty if is_buy else inv - fqty
            if abs(inv) < 1e-9:
                inv = 0.0; avg = 0.0
            elif rem > 0:
                avg = price
        max_abs_inv = max(max_abs_inv, abs(inv))

    def lifecycle(ord, book):
        """Advance an order through latency states. Returns the order or None."""
        if ord is None:
            return None
        if not ord['is_live'] and now >= ord['live_at']:
            ord['is_live'] = True            # order reaches the exchange; capture queue NOW
            # queue ahead = visible aggregate size x inflation (models faster
            # traders / hidden orders ahead of a retail 1-lot that we can't see)
            ord['qa'] = book.get(ord['px'], 0) * QMULT
            ord['llq'] = ord['qa']
            ord['placed'] = ord['live_at']
        if ord['dying'] and now >= ord['cancel_at']:
            return None                      # cancel finally lands (no fill)
        return ord

    for r in range(n):
        tag = tags[r]; now = int(tss[r]); px = int(pxs[r]); q = int(qtys[r])
        bid_ord = lifecycle(bid_ord, bids)
        ask_ord = lifecycle(ask_ord, asks)

        if tag & 1:                          # ---- trade ----
            tside = 'BUY' if ((tag >> 1) & 1) == 0 else 'SELL'
            if bid_ord is not None and bid_ord['is_live']:
                fq, fpx, reason = check_trade_fill(bid_ord, px, q, tside, now)
                if fq > 0:
                    if bid_ord['dying']:
                        adverse_fills += 1   # filled while we were trying to cancel
                    fill('BUY', fpx, fq, reason); bid_ord = None
            if ask_ord is not None and ask_ord['is_live']:
                fq, fpx, reason = check_trade_fill(ask_ord, px, q, tside, now)
                if fq > 0:
                    if ask_ord['dying']:
                        adverse_fills += 1
                    fill('SELL', fpx, fq, reason); ask_ord = None
            continue

        # ---- depth ----
        side = 'BID' if ((tag >> 1) & 1) == 0 else 'ASK'
        book = bids if side == 'BID' else asks
        prev = book.get(px, 0)
        nxt = q                              # 0 == remove
        if bid_ord is not None and bid_ord['is_live'] and side == 'BID' and px == bid_ord['px']:
            update_queue_from_depth(bid_ord, prev, nxt, now)
        if ask_ord is not None and ask_ord['is_live'] and side == 'ASK' and px == ask_ord['px']:
            update_queue_from_depth(ask_ord, prev, nxt, now)
        if nxt == 0:
            book.pop(px, None)
        else:
            book[px] = nxt

        bb = best_bid(); ba = best_ask()
        if bb is None or ba is None or ba <= bb:
            continue

        # sweep fills (price traded through a live resting order) — adverse
        if bid_ord is not None and bid_ord['is_live'] and now - bid_ord['placed'] > 200 and bid_ord['qa'] <= 0:
            if ba < bid_ord['px']:           # price ran through us -> fill at OUR limit (adverse)
                adverse_fills += 1
                fill('BUY', bid_ord['px'], bid_ord['qty'] - bid_ord['filled'], 'sweep'); bid_ord = None
        if ask_ord is not None and ask_ord['is_live'] and now - ask_ord['placed'] > 200 and ask_ord['qa'] <= 0:
            if bb > ask_ord['px']:
                adverse_fills += 1
                fill('SELL', ask_ord['px'], ask_ord['qty'] - ask_ord['filled'], 'sweep'); ask_ord = None

        # (re)quote at the touch with latency: a stale live quote enters a 50ms
        # cancel window (still fillable -> adverse); a new quote is sent but only
        # goes live 50ms later. Pending (not-yet-live) quotes are left as-is.
        if bid_ord is not None and bid_ord['is_live'] and bid_ord['px'] != bb and not bid_ord['dying']:
            bid_ord['dying'] = True; bid_ord['cancel_at'] = now + LAT
        if bid_ord is None and inv < max_inv:
            bid_ord = new_order('BUY', bb, qsize, now, LAT); quotes_placed += 1
        if ask_ord is not None and ask_ord['is_live'] and ask_ord['px'] != ba and not ask_ord['dying']:
            ask_ord['dying'] = True; ask_ord['cancel_at'] = now + LAT
        if ask_ord is None and inv > -max_inv:
            ask_ord = new_order('SELL', ba, qsize, now, LAT); quotes_placed += 1

    # flatten residual inventory at the touch (taker, crosses the spread)
    bb = best_bid(); ba = best_ask()
    if inv > 0 and bb is not None:
        fill('SELL', bb, abs(inv), 'eod', taker=True)
    elif inv < 0 and ba is not None:
        fill('BUY', ba, abs(inv), 'eod', taker=True)

    return {'date': date, 'events': n, 'realized_ticks': realized, 'fees': fees,
            'bid_fills': bid_fills, 'ask_fills': ask_fills, 'contracts': contracts,
            'max_abs_inv': max_abs_inv, 'adverse_fills': adverse_fills,
            'quotes_placed': quotes_placed, 'by_reason': by_reason}


def main():
    ap = argparse.ArgumentParser(description="Passive MM sim with queue dynamics on NQ.")
    ap.add_argument("--cache", default="ofp_cache")
    ap.add_argument("--qty", type=float, default=1.0)
    ap.add_argument("--max-inv", type=float, default=5.0, dest="max_inv")
    ap.add_argument("--maker-fee", type=float, default=1.25, dest="maker_fee")
    ap.add_argument("--taker-fee", type=float, default=1.25, dest="taker_fee")
    ap.add_argument("--point-value", type=float, default=20.0, dest="point_value")
    ap.add_argument("--tick", type=float, default=0.25)
    ap.add_argument("--latency-ms", type=float, default=50.0, dest="latency_ms",
                    help="order placement + cancel latency in ms (default 50)")
    ap.add_argument("--queue-mult", type=float, default=1.0, dest="queue_mult",
                    help="inflate visible queue-ahead by this factor (models faster/hidden orders; default 1)")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--optimistic-queue", action="store_true", dest="optq",
                    help="credit cancel-ahead queue advancement (default OFF = pessimistic: trades only)")
    args = ap.parse_args()
    global PESSIMISTIC
    PESSIMISTIC = not args.optq

    cache = Path(args.cache)
    meta = json.load((cache / "meta.json").open(encoding="utf-8"))
    tick = meta.get("tick", 0.25)
    pv = args.point_value
    # PnL is tracked in ticks; $ per tick = tick * point_value
    dollar_per_tick = tick * pv
    cfg = vars(args)

    days = meta["days"]
    if args.start:
        days = [d for d in days if d["date"] >= args.start]
    if args.end:
        days = [d for d in days if d["date"] <= args.end]

    qmode = "PESSIMISTIC (trades-only queue)" if PESSIMISTIC else "optimistic (cancel-ahead)"
    print(f"[mm] {meta['instrument']} tick={tick} maxInv={args.max_inv} "
          f"makerFee=${args.maker_fee}/side ${pv}/pt  latency={args.latency_ms}ms  [{qmode}]")
    print(f"[mm] quote 1 lot each side at the touch, requote on move; {len(days)} day(s)\n")

    rows = []
    t0 = time.monotonic()
    for dm in days:
        ds = time.monotonic()
        r = run_day(dm["date"], cache / f"{dm['date']}.bin", cfg)
        net = r['realized_ticks'] * dollar_per_tick - r['fees']
        rows.append((dm["date"], r, net))
        print(f"  {dm['date']}: net {'+' if net>=0 else '-'}${abs(net):,.0f}  "
              f"fills {r['bid_fills']+r['ask_fills']:,} (B {r['bid_fills']:,}/A {r['ask_fills']:,})  "
              f"maxInv {r['max_abs_inv']:.0f}  ({time.monotonic()-ds:.0f}s)", flush=True)

    tot_real = sum(r['realized_ticks'] for _, r, _ in rows)
    tot_fees = sum(r['fees'] for _, r, _ in rows)
    tot_fills = sum(r['bid_fills'] + r['ask_fills'] for _, r, _ in rows)
    tot_adv = sum(r['adverse_fills'] for _, r, _ in rows)
    tot_net = sum(net for _, _, net in rows)
    gross = tot_real * dollar_per_tick
    print("\n" + "=" * 58)
    print("  MARKET-MAKER SIM (queue dynamics) - NQ RTH")
    print("=" * 58)
    print(f"  Total fills          {tot_fills:,}  (avg {tot_fills/max(1,len(rows)):.0f}/day)")
    print(f"  Adverse fills        {tot_adv:,}  ({tot_adv/max(1,tot_fills)*100:.0f}% — swept/hit during cancel lag)")
    print(f"  Captured PnL (gross) {'+' if gross>=0 else '-'}${abs(gross):,.2f}  ({tot_real:+,.0f} ticks)")
    print(f"  Fees                 -${tot_fees:,.2f}")
    print(f"  NET PnL              {'+' if tot_net>=0 else '-'}${abs(tot_net):,.2f}")
    if tot_fills:
        print(f"  Net per fill         {'+' if tot_net>=0 else '-'}${abs(tot_net)/tot_fills:.2f}")
    # ---- fill breakdown by reason ----
    tot_quotes = sum(r.get('quotes_placed', 0) for _, r, _ in rows)
    agg = {'queue': [0, 0.0], 'through': [0, 0.0], 'sweep': [0, 0.0], 'eod': [0, 0.0]}
    for _, r, _ in rows:
        for k, v in r.get('by_reason', {}).items():
            a = agg.setdefault(k, [0, 0.0])
            a[0] += v[0]; a[1] += v[1]
    print("-" * 58)
    print("  HOW DID FILLS HAPPEN  (markout = avg ticks vs mid at fill: + good / - adverse)")
    print(f"  {'reason':<28}{'fills':>10}{'% of fills':>12}{'avg markout':>14}")
    labels = {'queue': 'queue depleted (passive)',
              'through': 'price traded THROUGH us',
              'sweep': 'price SWEPT through us',
              'eod': 'end-of-day flatten (taker)'}
    for k in ('queue', 'through', 'sweep', 'eod'):
        c, mk = agg[k]
        if c == 0:
            continue
        print(f"  {labels[k]:<28}{c:>10,}{c/max(1,tot_fills)*100:>11.1f}%{mk/c:>+13.3f}t")
    adverse = agg['through'][0] + agg['sweep'][0]
    print(f"  -> 'at our exact price' (passive): {agg['queue'][0]:,} "
          f"({agg['queue'][0]/max(1,tot_fills)*100:.1f}%)")
    print(f"  -> 'price moved through us':       {adverse:,} "
          f"({adverse/max(1,tot_fills)*100:.1f}%)")
    if tot_quotes:
        print(f"  quotes placed {tot_quotes:,}  ->  fill rate {tot_fills/tot_quotes*100:.1f}% of quotes")
    print("-" * 58)
    print(f"  {'Date (ET)':<14}{'Net':>14}{'Fills':>10}{'MaxInv':>9}")
    for d, r, net in rows:
        print(f"  {d:<14}{('+' if net>=0 else '-')+'$'+format(abs(net),',.0f'):>14}"
              f"{r['bid_fills']+r['ask_fills']:>10,}{r['max_abs_inv']:>9.0f}")
    print("=" * 58)
    print(f"[mm] done in {time.monotonic()-t0:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
