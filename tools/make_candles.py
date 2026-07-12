#!/usr/bin/env python3
"""
make_candles.py — Build 1-second OHLCV candles (RTH only) from the NT JSONL and
write them into candles.js for candle_viewer.html (TradingView Lightweight Charts).

Candles are built from TRADE prices (open/high/low/close = trade px, volume =
summed contracts) bucketed per UTC second, then the timestamp is shifted to ET
wall-clock so the chart axis reads New-York time (DST handled per day).

Uses the RTH byte-offset index (<file>.btidx.json, written by backtest_server.py)
to read only the RTH windows — fast. Falls back to a full scan if absent.

Usage:
  python3 make_candles.py [bfs_l2_export.jsonl] [--out candles.js]
                          [--start YYYY-MM-DD] [--end YYYY-MM-DD]
Then open candle_viewer.html.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    ET = timezone.utc

WS = b" \t"


def fast_ts(raw: bytes) -> Optional[int]:
    i = raw.find(b'"ts":')
    if i < 0:
        return None
    j = i + 5
    n = len(raw)
    while j < n and raw[j] in WS:
        j += 1
    neg = raw[j:j + 1] == b"-"
    if neg:
        j += 1
    s = j
    while j < n and 48 <= raw[j] <= 57:
        j += 1
    if j == s:
        return None
    v = int(raw[s:j])
    return -v if neg else v


def is_trade(raw: bytes) -> bool:
    i = raw.find(b'"type":')
    if i < 0:
        return False
    j = i + 7
    n = len(raw)
    while j < n and raw[j] in WS:
        j += 1
    if j < n and raw[j:j + 1] == b'"':
        j += 1
    return raw[j:j + 1] == b"t"


def et_offset_sec(date_str: str) -> int:
    y, m, d = map(int, date_str.split("-"))
    off = datetime(y, m, d, 12, 0, tzinfo=ET).utcoffset()
    return int(off.total_seconds()) if off else 0


def rth_end_ms(date_str: str) -> int:
    y, m, d = map(int, date_str.split("-"))
    return int(datetime(y, m, d, 16, 0, 0, tzinfo=ET).timestamp() * 1000)


def build_day(f, off: int, end_ms: int, offset_sec: int) -> List[list]:
    """Read one RTH window from offset to 16:00 ET; return [[t,o,h,l,c,v], ...]
    where t is the candle's ET-shifted unix second."""
    buckets: Dict[int, list] = {}        # utc second -> [o,h,l,c,v]
    f.seek(off)
    for raw in f:
        ts = fast_ts(raw)
        if ts is None:
            continue
        if ts >= end_ms:
            break
        if not is_trade(raw):
            continue
        try:
            ev = json.loads(raw)
            px = float(ev["px"])
            qty = float(ev["qty"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        if px <= 0 or qty <= 0:
            continue
        sec = ts // 1000
        b = buckets.get(sec)
        if b is None:
            buckets[sec] = [px, px, px, px, qty]
        else:
            if px > b[1]:
                b[1] = px
            if px < b[2]:
                b[2] = px
            b[3] = px
            b[4] += qty
    out = []
    for sec in sorted(buckets):
        o, h, l, c, v = buckets[sec]
        out.append([sec + offset_sec, round(o, 4), round(h, 4),
                    round(l, 4), round(c, 4), round(v, 4)])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build 1s RTH candles -> candles.js")
    ap.add_argument("file", nargs="?", default="bfs_l2_export.jsonl")
    ap.add_argument("--out", default="candles.js", help="JS data file for the viewer")
    ap.add_argument("--csv", default="candles.csv", help="CSV output (set '' to skip)")
    ap.add_argument("--start", default=None, help="first RTH date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=None, help="last RTH date YYYY-MM-DD (inclusive)")
    args = ap.parse_args()

    path = Path(args.file).expanduser().resolve()
    if not path.exists():
        print(f"[make_candles] file not found: {path}")
        return 1

    instrument, tick = "NQ", 0.25
    cache = path.with_name(path.name + ".btidx.json")
    rth_index: Dict[str, int] = {}
    if cache.exists():
        try:
            blob = json.load(cache.open(encoding="utf-8"))
            rth_index = {k: int(v) for k, v in blob.get("rth_index", {}).items()}
            instrument = blob.get("instrument", instrument)
            tick = float(blob.get("tick_size", tick))
        except (OSError, json.JSONDecodeError, ValueError):
            rth_index = {}
    if not rth_index:
        print("[make_candles] no RTH index cache found. Start backtest_server.py "
              "once to build <file>.btidx.json (it pre-scans + writes the index), "
              "then re-run this. Aborting.")
        return 1

    dates = sorted(rth_index)
    if args.start:
        dates = [d for d in dates if d >= args.start]
    if args.end:
        dates = [d for d in dates if d <= args.end]
    if not dates:
        print("[make_candles] no RTH dates in range.")
        return 1

    print(f"[make_candles] {instrument} tick={tick}  building 1s RTH candles for "
          f"{len(dates)} day(s): {dates}")
    days = []
    t0 = time.monotonic()
    with path.open("rb") as f:
        for d in dates:
            ts0 = time.monotonic()
            candles = build_day(f, rth_index[d], rth_end_ms(d), et_offset_sec(d))
            days.append({"date": d, "candles": candles})
            print(f"  {d}: {len(candles):,} candles ({time.monotonic()-ts0:.0f}s)", flush=True)

    payload = {"instrument": instrument, "tick": tick, "tz": "ET", "days": days}
    out = Path(args.out)
    with out.open("w", encoding="utf-8") as fo:
        fo.write("window.CANDLE_DATA = ")
        json.dump(payload, fo, separators=(",", ":"))
        fo.write(";\n")
    total = sum(len(x["candles"]) for x in days)
    print(f"[make_candles] wrote {out} ({out.stat().st_size/1e6:.1f} MB, "
          f"{total:,} candles) in {time.monotonic()-t0:.0f}s")

    if args.csv:
        csv_path = Path(args.csv)
        rows = 0
        with csv_path.open("w", encoding="utf-8", newline="") as fc:
            fc.write("date,time_et,open,high,low,close,volume\n")
            for day in days:
                d = day["date"]
                for t, o, h, l, c, v in day["candles"]:
                    # `t` is the ET-shifted unix second, so reading it back as
                    # UTC yields the ET wall-clock H:M:S.
                    hms = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%H:%M:%S")
                    fc.write(f"{d},{hms},{o},{h},{l},{c},{v}\n")
                    rows += 1
        print(f"[make_candles] wrote {csv_path} ({csv_path.stat().st_size/1e6:.1f} MB, {rows:,} rows)")

    print(f"[make_candles] open candle_viewer.html to view.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
