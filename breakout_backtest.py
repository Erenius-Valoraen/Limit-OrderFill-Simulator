#!/usr/bin/env python3
"""
breakout_backtest.py — Sensitive trend-following / breakout strategy on 1s NQ
candles (the candles.csv produced by make_candles.py).

Strategy (per RTH day, flat by the close)
-----------------------------------------
Entry (long; short is the mirror):
  - close breaks ABOVE the highest high of the prior `lookback` candles, by a
    buffer of max(1 tick, buf_atr * ATR)             [breakout]
  - fast EMA > slow EMA                               [trend alignment]
  - close > close `mom_n` candles ago                 [momentum confirm]
  Fill at the NEXT candle's open + `slip` ticks (market taker, no lookahead).

Exit (whichever hits first):
  - initial hard stop: entry -/+ max(min_stop ticks, stop_atr * ATR_at_entry)
  - trailing stop: lowest low of the last `exit_lookback` candles (Donchian /
    chandelier style) — ratchets with the trend, this is what lets a winner run
    5-30 candles instead of getting scratched on noise
  - time stop: max_hold candles
  Fill at the stop level -/+ `slip` ticks (or the close on a time stop).

Costs (same realism as backtest_cli):
  - 1 tick of slippage per side (you cross to take liquidity)
  - $`fee`/contract per side (CME)
  - PnL dollarized with $`point_value`/pt (E-mini NQ = 20)

Usage:
  python3 breakout_backtest.py candles.csv
  python3 breakout_backtest.py candles.csv --lookback 10 --exit-lookback 6 --max-hold 25
  python3 breakout_backtest.py candles.csv --markers trades.csv
"""
from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def load_candles(path: Path) -> Dict[str, List[tuple]]:
    """date -> [(time_et, o, h, l, c, v), ...] in chronological order."""
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


def ema(values: List[float], n: int) -> List[float]:
    k = 2.0 / (n + 1)
    out = [0.0] * len(values)
    e = values[0] if values else 0.0
    for i, v in enumerate(values):
        e = v * k + e * (1 - k) if i else v
        out[i] = e
    return out


def atr_series(h: List[float], l: List[float], c: List[float], n: int) -> List[float]:
    """Wilder-ish ATR (simple rolling mean of true range)."""
    tr = [0.0] * len(c)
    for i in range(len(c)):
        if i == 0:
            tr[i] = h[i] - l[i]
        else:
            tr[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
    out = [0.0] * len(c)
    s = 0.0
    for i in range(len(c)):
        s += tr[i]
        if i >= n:
            s -= tr[i - n]
        out[i] = s / min(i + 1, n)
    return out


class Trade:
    __slots__ = ("date", "side", "i_in", "i_out", "t_in", "t_out",
                 "px_in", "px_out", "net", "reason")


def run_day(date: str, rows: List[tuple], cfg: dict) -> Tuple[List[Trade], dict]:
    n = len(rows)
    t = [r[0] for r in rows]
    o = [r[1] for r in rows]; h = [r[2] for r in rows]
    l = [r[3] for r in rows]; c = [r[4] for r in rows]
    ef = ema(c, cfg["ema_fast"])
    es = ema(c, cfg["ema_slow"])
    at = atr_series(h, l, c, cfg["atr_n"])
    tick = cfg["tick"]; slip = cfg["slip_ticks"] * tick
    fee = cfg["fee"]; mult = cfg["point_value"]
    lb = cfg["lookback"]; xlb = cfg["exit_lookback"]
    warm = max(cfg["lookback"], cfg["atr_n"], cfg["ema_slow"],
               cfg["exit_lookback"], cfg["mom_n"]) + 1

    trades: List[Trade] = []
    diag = defaultdict(int)
    pos = 0                       # +1 long, -1 short
    pending = None                # ('BUY'/'SELL', atr_at_signal)
    entry_px = init_stop = 0.0
    entry_i = 0
    cooldown_until = -1

    def close_trade(side, i_out, px_out, reason):
        nonlocal pos
        gross = (px_out - entry_px) if side > 0 else (entry_px - px_out)
        net = gross * mult - 2 * fee          # slippage already in px_in/px_out
        tr = Trade()
        tr.date, tr.side = date, "LONG" if side > 0 else "SHORT"
        tr.i_in, tr.i_out = entry_i, i_out
        tr.t_in, tr.t_out = t[entry_i], t[i_out]
        tr.px_in, tr.px_out = entry_px, px_out
        tr.net, tr.reason = net, reason
        trades.append(tr)
        diag[reason] += 1
        pos = 0

    for i in range(n):
        # 1) execute a pending entry at this candle's open
        if pending is not None:
            side = 1 if pending[0] == "BUY" else -1
            entry_atr = pending[1]
            entry_px = o[i] + slip if side > 0 else o[i] - slip
            entry_i = i
            stop_dist = max(cfg["min_stop_ticks"] * tick, cfg["stop_atr"] * entry_atr)
            init_stop = entry_px - stop_dist if side > 0 else entry_px + stop_dist
            pos = side
            pending = None

        if i < warm:
            continue

        # 2) manage an open position (intrabar exits)
        if pos != 0:
            held = i - entry_i
            if pos > 0:
                donch = min(l[i - xlb:i]) if i >= xlb else init_stop
                stop_level = max(init_stop, donch)
                if held >= 1 and l[i] <= stop_level:                      # stop / trail
                    close_trade(pos, i, stop_level - slip,
                                "stop" if stop_level <= init_stop + 1e-9 else "trail")
                elif held >= cfg["max_hold"]:                              # time stop
                    close_trade(pos, i, c[i] - slip, "time")
            else:
                donch = max(h[i - xlb:i]) if i >= xlb else init_stop
                stop_level = min(init_stop, donch)
                if held >= 1 and h[i] >= stop_level:
                    close_trade(pos, i, stop_level + slip,
                                "stop" if stop_level >= init_stop - 1e-9 else "trail")
                elif held >= cfg["max_hold"]:
                    close_trade(pos, i, c[i] + slip, "time")
            if pos != 0:
                continue          # still in a position; no new signal this bar
            cooldown_until = i + cfg["cooldown"]

        # 3) flat -> look for a breakout+trend signal (act next bar)
        if pos == 0 and pending is None and i >= cooldown_until and i + 1 < n:
            prior_hi = max(h[i - lb:i])
            prior_lo = min(l[i - lb:i])
            buf = max(tick, cfg["buf_atr"] * at[i])
            long_sig = (c[i] > prior_hi + buf and ef[i] > es[i]
                        and c[i] > c[i - cfg["mom_n"]])
            short_sig = (c[i] < prior_lo - buf and ef[i] < es[i]
                         and c[i] < c[i - cfg["mom_n"]])
            if long_sig:
                pending = ("BUY", at[i]); diag["signals"] += 1
            elif short_sig:
                pending = ("SELL", at[i]); diag["signals"] += 1

    # force flat at the session close
    if pos != 0:
        px = c[n - 1] - slip if pos > 0 else c[n - 1] + slip
        close_trade(pos, n - 1, px, "eod")
    return trades, diag


def _money(v: float) -> str:
    return ("+" if v >= 0 else "-") + "$" + f"{abs(v):,.2f}"


def report(all_trades: List[Trade], diag_tot: dict, cfg: dict, days: int) -> None:
    n = len(all_trades)
    wins = [t for t in all_trades if t.net > 0]
    losses = [t for t in all_trades if t.net < 0]
    net = sum(t.net for t in all_trades)
    gw = sum(t.net for t in wins)
    gl = -sum(t.net for t in losses)
    holds = [t.i_out - t.i_in for t in all_trades]

    # equity curve / drawdown (trade-sequential)
    eq = peak = dd = 0.0
    for t in all_trades:
        eq += t.net; peak = max(peak, eq); dd = max(dd, peak - eq)

    def line(k, v): print(f"  {k:<24}{v}")
    print("\n" + "=" * 58)
    print("  SENSITIVE BREAKOUT / TREND - NQ 1s BACKTEST")
    print("=" * 58)
    line("Days", f"{days}")
    line("Params", f"lb={cfg['lookback']} xlb={cfg['exit_lookback']} "
                   f"ema={cfg['ema_fast']}/{cfg['ema_slow']} atr={cfg['atr_n']}")
    line("", f"stopATR={cfg['stop_atr']} bufATR={cfg['buf_atr']} "
             f"maxHold={cfg['max_hold']} cooldown={cfg['cooldown']}")
    line("Costs", f"${cfg['fee']}/side  {cfg['slip_ticks']:g} tick slip/side  "
                  f"${cfg['point_value']}/pt")
    print("-" * 58)
    line("Signals", f"{diag_tot.get('signals', 0):,}")
    line("Round-trip trades", f"{n:,}")
    if not n:
        line("Result", "no trades"); print("=" * 58); return
    line("Win rate", f"{len(wins)/n*100:.1f}%  ({len(wins)}W / {len(losses)}L)")
    line("NET PnL", _money(net))
    line("Avg trade", _money(net / n))
    line("Avg win", _money(gw / len(wins)) if wins else "n/a")
    line("Avg loss", _money(-gl / len(losses)) if losses else "n/a")
    line("Largest win", _money(max(t.net for t in all_trades)))
    line("Largest loss", _money(min(t.net for t in all_trades)))
    line("Profit factor", f"{gw/gl:.2f}" if gl > 0 else "inf")
    line("Max drawdown", _money(-dd))
    line("Avg hold", f"{sum(holds)/n:.1f} candles ({sum(holds)/n:.0f}s)")
    line("Hold range", f"{min(holds)}-{max(holds)} candles")
    nets = [t.net for t in all_trades]
    mean = net / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in nets) / n) if n > 1 else 0
    line("Per-trade Sharpe", f"{mean/sd:.3f}" if sd > 0 else "n/a")
    print("-" * 58)
    # per-day
    byd: Dict[str, list] = defaultdict(lambda: [0, 0.0])
    for t in all_trades:
        byd[t.date][0] += 1; byd[t.date][1] += t.net
    print(f"  {'Date (ET)':<14}{'Trades':>8}{'Net PnL':>16}")
    for d in sorted(byd):
        print(f"  {d:<14}{byd[d][0]:>8}{_money(byd[d][1]):>16}")
    print("-" * 58)
    ex = sorted(((k, v) for k, v in diag_tot.items() if k != "signals"),
                key=lambda kv: -kv[1])
    print("  exit reasons: " + ", ".join(f"{k}={v:,}" for k, v in ex))
    print("=" * 58 + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Breakout/trend strategy on 1s NQ candles.")
    ap.add_argument("file", nargs="?", default="candles.csv", help="candles CSV (default %(default)s)")
    ap.add_argument("--lookback", type=int, default=12, help="breakout range candles (default 12)")
    ap.add_argument("--exit-lookback", type=int, default=8, dest="exit_lookback",
                    help="Donchian trailing-exit candles (default 8)")
    ap.add_argument("--ema-fast", type=int, default=8, dest="ema_fast")
    ap.add_argument("--ema-slow", type=int, default=21, dest="ema_slow")
    ap.add_argument("--atr-n", type=int, default=20, dest="atr_n")
    ap.add_argument("--buf-atr", type=float, default=0.10, dest="buf_atr",
                    help="breakout buffer in ATRs (sensitive=small; default 0.10)")
    ap.add_argument("--stop-atr", type=float, default=1.2, dest="stop_atr",
                    help="initial hard stop in ATRs (default 1.2)")
    ap.add_argument("--min-stop-ticks", type=float, default=3.0, dest="min_stop_ticks")
    ap.add_argument("--mom-n", type=int, default=3, dest="mom_n",
                    help="momentum confirm lookback (default 3)")
    ap.add_argument("--max-hold", type=int, default=30, dest="max_hold",
                    help="hard time stop in candles (default 30)")
    ap.add_argument("--cooldown", type=int, default=3, help="candles to wait after an exit (default 3)")
    ap.add_argument("--qty", type=float, default=1.0)
    ap.add_argument("--fee", type=float, default=1.25, help="$/contract per side (default 1.25)")
    ap.add_argument("--slip-ticks", type=float, default=0.5, dest="slip_ticks",
                    help="slippage ticks per SIDE relative to mid (default 0.5; a market "
                         "order pays the half-spread, so round-trip spread = 1 tick)")
    ap.add_argument("--point-value", type=float, default=20.0, dest="point_value")
    ap.add_argument("--tick", type=float, default=0.25)
    ap.add_argument("--markers", default=None, help="optional: write per-trade CSV for the viewer")
    args = ap.parse_args()

    path = Path(args.file).expanduser().resolve()
    if not path.exists():
        print(f"[breakout] file not found: {path}")
        return 1
    cfg = vars(args)

    print(f"[breakout] loading {path.name} ...")
    days = load_candles(path)
    if not days:
        print("[breakout] no candles parsed.")
        return 1
    print(f"[breakout] {sum(len(v) for v in days.values()):,} candles over {len(days)} day(s); running ...")

    all_trades: List[Trade] = []
    diag_tot: dict = defaultdict(int)
    for d in sorted(days):
        trs, diag = run_day(d, days[d], cfg)
        all_trades.extend(trs)
        for k, v in diag.items():
            diag_tot[k] += v

    report(all_trades, diag_tot, cfg, len(days))

    if args.markers:
        mp = Path(args.markers)
        with mp.open("w", encoding="utf-8", newline="") as f:
            f.write("date,side,entry_time,exit_time,entry_px,exit_px,bars_held,net_dollars,reason\n")
            for t in all_trades:
                f.write(f"{t.date},{t.side},{t.t_in},{t.t_out},{t.px_in:.2f},"
                        f"{t.px_out:.2f},{t.i_out - t.i_in},{t.net:.2f},{t.reason}\n")
        print(f"[breakout] wrote {mp} ({len(all_trades):,} trades)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
