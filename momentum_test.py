#!/usr/bin/env python3
"""
momentum_test.py — Directly test the hypothesis "NQ trends, so trade in the
direction of the recent move." No strategy code involved — just the price data.

For each horizon H, sample RTH closes every H seconds, and for each bar trade in
the SIGN of the previous H-second move, capturing the next H-second move:
    pnl_gross(i) = sign(close[i]-close[i-1]) * (close[i+1]-close[i])
Reports the GROSS edge (is momentum continuation even present?) and the NET edge
after a realistic market-order round-trip cost. Resets per day.

Reads candles.csv (1s RTH candles from make_candles.py).
"""
import sys
from collections import defaultdict
from pathlib import Path

TICK = 0.25
MULT = 20.0
# market-order round trip: 1 tick spread ($5) + 2x $1.25 fees = $7.50
COST_USD = 1 * TICK * MULT + 2 * 1.25


def load(path):
    days = defaultdict(list)   # date -> [(sod_seconds, close), ...]
    with open(path, encoding="utf-8") as f:
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
                h, m, s = (int(x) for x in p[1].split(":"))
                days[p[0]].append((h * 3600 + m * 60 + s, float(p[5])))
            except ValueError:
                continue
    return days


def resample_closes(rows, H):
    """Last close in each H-second bucket, in order."""
    out = []
    cur = None
    last_close = None
    for sod, c in rows:
        b = sod // H
        if b != cur:
            if cur is not None:
                out.append(last_close)
            cur = b
        last_close = c
    if cur is not None:
        out.append(last_close)
    return out


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "candles.csv")
    days = load(path)
    horizons = [1, 5, 15, 30, 60, 120, 300]   # seconds

    print(f"NQ momentum-continuation test  (trade WITH the last H-sec move)")
    print(f"round-trip cost = ${COST_USD:.2f}  ({COST_USD/MULT:.3f} pt)\n")
    print(f"{'horizon':>8}{'trades':>9}{'hit%':>7}{'autocorr':>10}"
          f"{'gross/trade':>13}{'net/trade':>11}{'total net':>13}")
    print("-" * 72)

    for H in horizons:
        gross_pts = []
        # for autocorr
        m_prev_all = []
        m_next_all = []
        for d in sorted(days):
            closes = resample_closes(days[d], H)
            if len(closes) < 3:
                continue
            moves = [closes[i + 1] - closes[i] for i in range(len(closes) - 1)]
            for i in range(1, len(moves)):
                mp = moves[i - 1]      # prior move (the signal)
                mn = moves[i]          # next move (what we capture)
                if mp == 0:
                    continue
                g = (1 if mp > 0 else -1) * mn   # trade in direction of prior move
                gross_pts.append(g)
                m_prev_all.append(mp)
                m_next_all.append(mn)
        n = len(gross_pts)
        if n == 0:
            continue
        mean_g = sum(gross_pts) / n
        wins = sum(1 for g in gross_pts if g > 0)
        # lag-1 autocorrelation of H-period moves
        mean_p = sum(m_prev_all) / n
        mean_n = sum(m_next_all) / n
        cov = sum((m_prev_all[i] - mean_p) * (m_next_all[i] - mean_n) for i in range(n)) / n
        var_p = sum((x - mean_p) ** 2 for x in m_prev_all) / n
        var_n = sum((x - mean_n) ** 2 for x in m_next_all) / n
        ac = cov / (var_p ** 0.5 * var_n ** 0.5) if var_p > 0 and var_n > 0 else 0.0
        gross_usd = mean_g * MULT
        net_usd = gross_usd - COST_USD
        total_net = net_usd * n
        print(f"{H:>6}s{n:>9,}{wins/n*100:>6.1f}%{ac:>+10.3f}"
              f"{gross_usd:>+12.2f}${net_usd:>+10.2f}{('+' if total_net>=0 else '-')+'$'+format(abs(total_net),',.0f'):>13}")
    print("-" * 72)
    print("gross/trade = avg $ captured trading WITH the move (before cost).")
    print("If gross/trade <= 0: no momentum edge exists (not a cost problem).")
    print("If gross/trade > 0 but net < 0: edge is real but smaller than cost.")


if __name__ == "__main__":
    main()
