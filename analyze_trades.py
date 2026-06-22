"""
Quick summariser for paper-trading fills written by paper_trader.py.

Usage:
    python analyze_trades.py                       # summarise every day on disk
    python analyze_trades.py 2026-06-14            # one day
    python analyze_trades.py 2026-06-01 2026-06-14 # range (inclusive)
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent / "paper_data"


def load_day(day: str):
    fills = []
    p = DATA_DIR / f"trades_{day}.jsonl"
    if not p.exists():
        return fills
    with p.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                fills.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return fills


def list_days():
    return sorted(
        p.stem.replace("trades_", "")
        for p in DATA_DIR.glob("trades_*.jsonl")
    )


def replay_pnl(fills):
    """Replay the fills through the simulator's position accounting to derive
    realized PnL, total fees, win/loss counts, etc. — symbol by symbol."""
    by_sym = defaultdict(lambda: {
        "netQty": 0.0, "avgEntry": 0.0, "realizedPnl": 0.0, "fees": 0.0,
        "closes": 0, "wins": 0, "losses": 0, "winPnl": 0.0, "lossPnl": 0.0,
        "fillCount": 0, "buyQty": 0.0, "sellQty": 0.0,
    })
    for f in fills:
        sym = f["symbol"]
        pos = by_sym[sym]
        pos["fillCount"] += 1
        side = f["side"]
        price = float(f["price"])
        qty = float(f["qty"])
        fee = float(f.get("feePaidAtFill", 0.0))
        pos["fees"] += fee
        pos["realizedPnl"] -= fee
        if side == "BUY":
            pos["buyQty"] += qty
        else:
            pos["sellQty"] += qty
        is_buy = side == "BUY"
        if pos["netQty"] == 0:
            pos["avgEntry"] = price
            pos["netQty"] = qty if is_buy else -qty
        elif (is_buy and pos["netQty"] > 0) or (not is_buy and pos["netQty"] < 0):
            tq = abs(pos["netQty"]) + qty
            pos["avgEntry"] = (abs(pos["netQty"]) * pos["avgEntry"] + qty * price) / tq
            pos["netQty"] = tq if is_buy else -tq
        else:
            close_qty = min(qty, abs(pos["netQty"]))
            pnl_unit = (pos["avgEntry"] - price) if is_buy else (price - pos["avgEntry"])
            realised = pnl_unit * close_qty
            pos["realizedPnl"] += realised
            pos["closes"] += 1
            if realised > 0:
                pos["wins"] += 1
                pos["winPnl"] += realised
            elif realised < 0:
                pos["losses"] += 1
                pos["lossPnl"] += realised
            remaining = qty - close_qty
            new_net = pos["netQty"] + qty if is_buy else pos["netQty"] - qty
            if abs(new_net) < 1e-10:
                pos["netQty"] = 0.0
                pos["avgEntry"] = 0.0
            else:
                pos["netQty"] = new_net
                if remaining > 0:
                    pos["avgEntry"] = price
    return by_sym


def fmt_money(v: float) -> str:
    sign = "-" if v < 0 else " "
    return f"{sign}${abs(v):>10.2f}"


def print_summary(label: str, fills: list):
    print(f"\n── {label} ─────────────────────────────────────────────")
    if not fills:
        print("  (no fills)")
        return
    by_sym = replay_pnl(fills)
    for sym, p in by_sym.items():
        win_rate = (p["wins"] / p["closes"] * 100.0) if p["closes"] else 0.0
        avg_win = (p["winPnl"] / p["wins"]) if p["wins"] else 0.0
        avg_loss = (p["lossPnl"] / p["losses"]) if p["losses"] else 0.0
        profit_factor = (p["winPnl"] / -p["lossPnl"]) if p["lossPnl"] < 0 else float("inf")
        gross = p["realizedPnl"] + p["fees"]  # PnL before fees
        print(f"  symbol         {sym}")
        print(f"  fills          {p['fillCount']}  (buy {p['buyQty']:.4f} / sell {p['sellQty']:.4f})")
        print(f"  closed trades  {p['closes']}   wins {p['wins']}  losses {p['losses']}  win%  {win_rate:5.1f}")
        print(f"  realized PnL  {fmt_money(p['realizedPnl'])}   (gross {fmt_money(gross)}, fees {fmt_money(-p['fees'])})")
        print(f"  avg win       {fmt_money(avg_win)}   avg loss {fmt_money(avg_loss)}   profit factor {profit_factor:.2f}")
        print(f"  open position  {p['netQty']:+.6f}  @ avgEntry {p['avgEntry']:.2f}")


def main(args):
    days = list_days()
    if not days:
        print(f"no fill data found in {DATA_DIR}")
        return
    if not args:
        for d in days:
            print_summary(d, load_day(d))
        # aggregate across all days
        all_fills = []
        for d in days:
            all_fills.extend(load_day(d))
        print_summary(f"ALL ({len(days)} day(s))", all_fills)
    elif len(args) == 1:
        d = args[0]
        print_summary(d, load_day(d))
    elif len(args) == 2:
        lo, hi = args
        selected = [d for d in days if lo <= d <= hi]
        all_fills = []
        for d in selected:
            print_summary(d, load_day(d))
            all_fills.extend(load_day(d))
        print_summary(f"RANGE {lo}..{hi} ({len(selected)} day(s))", all_fills)
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
