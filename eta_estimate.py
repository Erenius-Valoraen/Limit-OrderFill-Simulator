#!/usr/bin/env python3
"""
eta_estimate.py — Robert-Rosenbaum / Dayri-Rosenbaum "uncertainty-zones"
tick-mean-reversion parameter eta, measured on NQ RTH trade prints.

eta_hat = N_continuations / (2 * N_alternations)
  on the sequence of consecutive DISTINCT traded prices (in ticks):
    - continuation = move in the SAME direction as the previous move
    - alternation  = move in the OPPOSITE direction
  eta = 0.5  -> driftless random walk (no microstructure effect)
  eta < 0.5  -> tick-level MEAN REVERSION (bid-ask bounce / absorption)
  eta > 0.5  -> trending

Reference points (Dayri-Rosenbaum, real futures order books):
  E-mini S&P (ES) 0.035 | ESX 0.087 | Bund 0.138 | DAX 0.275

Reads the trade prints from ofp_cache/*.bin (px stored in ticks).
"""
import json
import struct
from pathlib import Path

CACHE = Path("ofp_cache")
meta = json.load((CACHE / "meta.json").open(encoding="utf-8"))

try:
    import numpy as np
    DT = np.dtype([('tag', 'u1'), ('pad', 'u1'), ('qty', '<u2'), ('ts', '<u4'), ('px', '<u4')])
    HAVE_NP = True
except ImportError:
    HAVE_NP = False


def trade_px_ticks(date):
    """Return the chronological sequence of traded prices (in ticks) for a day."""
    path = CACHE / f"{date}.bin"
    if HAVE_NP:
        arr = np.fromfile(path, dtype=DT)
        tr = arr[(arr['tag'] & 1) == 1]
        return tr['px'].astype(np.int64)
    # pure-python fallback
    data = path.read_bytes()
    out = []
    st = struct.Struct("<BBHII")
    for o in range(0, len(data), 12):
        tag, _pad, _q, _ts, px = st.unpack_from(data, o)
        if tag & 1:
            out.append(px)
    return out


def eta_for_day(date):
    px = trade_px_ticks(date)
    n_tr = len(px)
    if HAVE_NP:
        px = np.asarray(px, dtype=np.int64)
        # collapse to consecutive distinct prices
        keep = np.concatenate(([True], px[1:] != px[:-1]))
        dp = px[keep]
        moves = np.diff(dp)                      # nonzero integer tick moves
        if moves.size < 3:
            return None
        s = np.sign(moves)
        same = s[1:] == s[:-1]
        n_cont = int(np.count_nonzero(same))
        n_alt = int(np.count_nonzero(~same))
        # strict one-tick variant: consecutive moves that are BOTH +/-1 tick
        one = np.abs(moves) == 1
        pair_one = one[1:] & one[:-1]
        c1 = int(np.count_nonzero(same & pair_one))
        a1 = int(np.count_nonzero(~same & pair_one))
    else:
        dp = [px[0]]
        for v in px[1:]:
            if v != dp[-1]:
                dp.append(v)
        moves = [dp[i + 1] - dp[i] for i in range(len(dp) - 1)]
        if len(moves) < 3:
            return None
        s = [1 if m > 0 else -1 for m in moves]
        n_cont = sum(1 for i in range(1, len(s)) if s[i] == s[i - 1])
        n_alt = sum(1 for i in range(1, len(s)) if s[i] != s[i - 1])
        c1 = a1 = 0
        for i in range(1, len(moves)):
            if abs(moves[i]) == 1 and abs(moves[i - 1]) == 1:
                if s[i] == s[i - 1]: c1 += 1
                else: a1 += 1
    eta = n_cont / (2 * n_alt) if n_alt else float('inf')
    eta1 = c1 / (2 * a1) if a1 else float('inf')
    alt_rate = n_alt / (n_cont + n_alt) if (n_cont + n_alt) else 0
    return {"date": date, "n_tr": n_tr, "moves": n_cont + n_alt,
            "n_cont": n_cont, "n_alt": n_alt, "alt_rate": alt_rate,
            "eta": eta, "eta1": eta1, "c1": c1, "a1": a1}


print(f"NQ uncertainty-zones eta (RTH) — eta<0.5 = tick mean reversion; ES reference = 0.035\n")
print(f"{'date':<12}{'trades':>10}{'moves':>10}{'alt%':>8}{'eta':>8}{'eta(1tick)':>12}")
print("-" * 60)
tot_c = tot_a = tot_c1 = tot_a1 = 0
for dm in meta["days"]:
    r = eta_for_day(dm["date"])
    if not r:
        continue
    tot_c += r["n_cont"]; tot_a += r["n_alt"]; tot_c1 += r["c1"]; tot_a1 += r["a1"]
    print(f"{r['date']:<12}{r['n_tr']:>10,}{r['moves']:>10,}{r['alt_rate']*100:>7.1f}%"
          f"{r['eta']:>8.3f}{r['eta1']:>12.3f}")
print("-" * 60)
eta_all = tot_c / (2 * tot_a) if tot_a else float('inf')
eta1_all = tot_c1 / (2 * tot_a1) if tot_a1 else float('inf')
alt_all = tot_a / (tot_c + tot_a) if (tot_c + tot_a) else 0
print(f"{'AGGREGATE':<12}{'':>10}{tot_c+tot_a:>10,}{alt_all*100:>7.1f}%{eta_all:>8.3f}{eta1_all:>12.3f}")
print(f"\ncontinuations={tot_c:,}  alternations={tot_a:,}  (1-tick: c={tot_c1:,} a={tot_a1:,})")
