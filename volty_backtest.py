#!/usr/bin/env python3
"""
volty_backtest.py — Faithful translation of TradingView's built-in
"Volty Expan Close Strategy" onto the 1s NQ candles (candles.csv).

Pine source:
    length  = 5
    numATRs = 0.75
    atrs    = ta.sma(ta.tr, length) * numATRs
    strategy.entry("VltClsLE", long,  stop = close + atrs)   # buy-STOP above
    strategy.entry("VltClsSE", short, stop = close - atrs)   # sell-STOP below

What it really is
-----------------
An ALWAYS-IN-MARKET stop-and-reverse VOLATILITY BREAKOUT (momentum):
  - buy-stop at close+atrs  -> goes long when price breaks UP through it
  - sell-stop at close-atrs -> goes short when price breaks DOWN through it
  - orders are re-priced every bar to the latest close +/- atrs
  - because strategy.entry reverses, the opposite stop is the exit. There is
    NO target / time stop — you flip on the opposite breakout.

The flip trap: implementing "long at close+atrs" as a LIMIT (buy below) / sell
above would invert this into mean-reversion — the opposite trades. This file
implements the correct STOP (breakout) version.

Fill model (orders placed on bar i's close fill on bar i+1):
  - buy-stop fills at max(stop, open) (gap-through uses the open)
  - sell-stop fills at min(stop, open)
  - if a bar hits BOTH stops, TradingView's broker emulator assumes the extreme
    nearer the open is reached first — replicated here via open-proximity.
Costs: 0.5 tick slippage per side (market taker pays the half-spread) + $fee/side.

Usage:
  python3 volty_backtest.py candles.csv
  python3 volty_backtest.py candles.csv --length 5 --num-atrs 0.75 --invert
"""
from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


def load_candles(path: Path) -> Dict[str, List[tuple]]:
    days: Dict[str, List[tuple]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as f:
        first = True
        for ln in f:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            p = ln.split(",")
            if first and p and p[0] == "date":
                first = False
                continue
            first = False
            if len(p) < 7:
                continue
            try:
                days[p[0]].append((p[1], float(p[2]), float(p[3]),
                                   float(p[4]), float(p[5]), float(p[6])))
            except ValueError:
                continue
    return days


def resample(rows: List[tuple], bar_seconds: int) -> List[tuple]:
    """Aggregate 1s candles into `bar_seconds` bars (OHLCV nests exactly).
    rows: [(time_et 'HH:MM:SS', o,h,l,c,v), ...] in order."""
    if bar_seconds <= 1:
        return rows
    out = []
    cur_bucket = None
    o = h = l = c = v = None
    t0 = None
    for (te, ro, rh, rl, rc, rv) in rows:
        hh, mm, ss = (int(x) for x in te.split(":"))
        sod = hh * 3600 + mm * 60 + ss
        b = sod // bar_seconds
        if b != cur_bucket:
            if cur_bucket is not None:
                out.append((t0, o, h, l, c, v))
            cur_bucket = b
            t0, o, h, l, c, v = te, ro, rh, rl, rc, rv
        else:
            if rh > h: h = rh
            if rl < l: l = rl
            c = rc
            v += rv
    if cur_bucket is not None:
        out.append((t0, o, h, l, c, v))
    return out


def _money(v: float) -> str:
    return ("+" if v >= 0 else "-") + "$" + f"{abs(v):,.2f}"


def run_day(date, rows, cfg):
    n = len(rows)
    o = [r[1] for r in rows]; h = [r[2] for r in rows]
    l = [r[3] for r in rows]; c = [r[4] for r in rows]
    length, mult = cfg["length"], cfg["num_atrs"]
    tick = cfg["tick"]; slip = cfg["slip_ticks"] * tick
    fee = cfg["fee"]; pv = cfg["point_value"]
    invert = cfg["invert"]

    # true range and atrs = SMA(TR, length) * numATRs
    tr = [0.0] * n
    for i in range(n):
        tr[i] = h[i] - l[i] if i == 0 else max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
    atrs = [0.0] * n
    s = 0.0
    for i in range(n):
        s += tr[i]
        if i >= length:
            s -= tr[i - length]
        atrs[i] = (s / min(i + 1, length)) * mult

    trades = []
    diag = defaultdict(int)
    pos = 0
    entry_px = 0.0
    entry_i = 0

    def open_pos(side, i, px_raw):
        nonlocal pos, entry_px, entry_i
        # apply slippage: buys fill higher, sells lower
        entry_px = px_raw + slip if side > 0 else px_raw - slip
        pos = side
        entry_i = i

    def close_pos(i, px_raw):
        nonlocal pos
        exit_px = px_raw - slip if pos > 0 else px_raw + slip   # closing long=sell, short=buy
        gross = (exit_px - entry_px) if pos > 0 else (entry_px - exit_px)
        net = gross * pv - 2 * fee
        trades.append({"date": date, "side": "LONG" if pos > 0 else "SHORT",
                       "i_in": entry_i, "i_out": i, "net": net})
        pos = 0
        return exit_px

    for j in range(1, n):
        i = j - 1                                  # orders priced on prior bar's close
        if i < length:
            continue
        bs = c[i] + atrs[i]                         # buy-stop (above)
        ss = c[i] - atrs[i]                         # sell-stop (below)
        if invert:                                 # mean-reversion variant (limit-like): swap roles
            bs, ss = ss, bs

        long_dir, short_dir = (1, -1)

        if pos == 0:
            long_hit = h[j] >= bs
            short_hit = l[j] <= ss
            if long_hit and short_hit:
                # whichever extreme is nearer the open is assumed first
                first_long = (h[j] - o[j]) <= (o[j] - l[j])
                if first_long:
                    open_pos(long_dir, j, o[j] if o[j] >= bs else bs)
                else:
                    open_pos(short_dir, j, o[j] if o[j] <= ss else ss)
                diag["entry"] += 1
            elif long_hit:
                open_pos(long_dir, j, o[j] if o[j] >= bs else bs); diag["entry"] += 1
            elif short_hit:
                open_pos(short_dir, j, o[j] if o[j] <= ss else ss); diag["entry"] += 1
        elif pos > 0:
            # long: reverse short if sell-stop breaks
            if l[j] <= ss:
                px = o[j] if o[j] <= ss else ss
                close_pos(j, px)
                open_pos(short_dir, j, px)
                diag["reverse"] += 1
        else:
            if h[j] >= bs:
                px = o[j] if o[j] >= bs else bs
                close_pos(j, px)
                open_pos(long_dir, j, px)
                diag["reverse"] += 1

    if pos != 0:
        close_pos(n - 1, c[n - 1]); diag["eod"] += 1
    return trades, diag


def report(all_trades, diag_tot, cfg, days):
    n = len(all_trades)
    wins = [t for t in all_trades if t["net"] > 0]
    losses = [t for t in all_trades if t["net"] < 0]
    net = sum(t["net"] for t in all_trades)
    gw = sum(t["net"] for t in wins)
    gl = -sum(t["net"] for t in losses)
    holds = [t["i_out"] - t["i_in"] for t in all_trades]
    eq = peak = dd = 0.0
    for t in all_trades:
        eq += t["net"]; peak = max(peak, eq); dd = max(dd, peak - eq)

    def line(k, v): print(f"  {k:<22}{v}")
    print("\n" + "=" * 58)
    bs = cfg.get("bar_seconds", 1)
    print(f"  VOLTY EXPAN CLOSE - NQ {bs}s-BAR BACKTEST"
          + ("  [INVERTED]" if cfg["invert"] else ""))
    print("=" * 58)
    line("Days", str(days))
    line("Params", f"length={cfg['length']}  numATRs={cfg['num_atrs']}")
    line("Costs", f"${cfg['fee']}/side  {cfg['slip_ticks']:g} tick/side  ${cfg['point_value']}/pt")
    print("-" * 58)
    line("Trades", f"{n:,}")
    if not n:
        line("Result", "no trades"); print("=" * 58); return
    line("Win rate", f"{len(wins)/n*100:.1f}%  ({len(wins)}W / {len(losses)}L)")
    line("NET PnL", _money(net))
    line("Avg trade", _money(net / n))
    line("Avg win", _money(gw / len(wins)) if wins else "n/a")
    line("Avg loss", _money(-gl / len(losses)) if losses else "n/a")
    line("Largest win", _money(max(t["net"] for t in all_trades)))
    line("Largest loss", _money(min(t["net"] for t in all_trades)))
    line("Profit factor", f"{gw/gl:.2f}" if gl > 0 else "inf")
    line("Max drawdown", _money(-dd))
    line("Avg hold", f"{sum(holds)/n:.1f} bars ({sum(holds)/n*bs:.0f}s)")
    print("-" * 58)
    byd = defaultdict(lambda: [0, 0.0])
    for t in all_trades:
        byd[t["date"]][0] += 1; byd[t["date"]][1] += t["net"]
    print(f"  {'Date (ET)':<14}{'Trades':>8}{'Net PnL':>16}")
    for d in sorted(byd):
        print(f"  {d:<14}{byd[d][0]:>8}{_money(byd[d][1]):>16}")
    print("=" * 58 + "\n")


def main():
    ap = argparse.ArgumentParser(description="Volty Expan Close (TV built-in) on 1s NQ candles.")
    ap.add_argument("file", nargs="?", default="candles.csv")
    ap.add_argument("--length", type=int, default=5)
    ap.add_argument("--num-atrs", type=float, default=0.75, dest="num_atrs")
    ap.add_argument("--invert", action="store_true",
                    help="run the FLIPPED (mean-reversion) version for comparison")
    ap.add_argument("--fee", type=float, default=1.25)
    ap.add_argument("--slip-ticks", type=float, default=0.5, dest="slip_ticks")
    ap.add_argument("--point-value", type=float, default=20.0, dest="point_value")
    ap.add_argument("--tick", type=float, default=0.25)
    ap.add_argument("--bar-seconds", type=int, default=1, dest="bar_seconds",
                    help="resample 1s candles into N-second bars (e.g. 60 = 1-min)")
    args = ap.parse_args()

    path = Path(args.file).expanduser().resolve()
    if not path.exists():
        print(f"[volty] file not found: {path}")
        return 1
    cfg = vars(args)
    days = load_candles(path)
    if not days:
        print("[volty] no candles parsed.")
        return 1
    if args.bar_seconds > 1:
        days = {d: resample(rows, args.bar_seconds) for d, rows in days.items()}
    bs = args.bar_seconds
    print(f"[volty] {sum(len(v) for v in days.values()):,} bars ({bs}s) over {len(days)} day(s); running ...")
    all_trades = []
    diag_tot = defaultdict(int)
    for d in sorted(days):
        trs, diag = run_day(d, days[d], cfg)
        all_trades.extend(trs)
        for k, v in diag.items():
            diag_tot[k] += v
    report(all_trades, diag_tot, cfg, len(days))
    return 0


if __name__ == "__main__":
    sys.exit(main())
