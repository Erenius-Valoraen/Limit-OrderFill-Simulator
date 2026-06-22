#!/usr/bin/env python3
"""
build_ofp_cache.py — Pre-parse the RTH depth+trade events from the NT JSONL into
a compact binary cache so the headless OFP runner (ofp_runner.js) never has to
touch the 25 GB JSON.

For each RTH day it:
  - seeks to the RTH-open byte offset (from <file>.btidx.json),
  - reads to 16:00 ET,
  - drops consecutive byte-identical lines (the NT AddDataSeries double-fire),
  - encodes each event as a fixed 12-byte little-endian record:
        u8  tag      bit0: 0=depth, 1=trade ; bit1: side (depth 0=BID/1=ASK, trade 0=BUY/1=SELL)
        u8  _pad
        u16 qty      (depth: 0 == remove level)
        u32 ts       ms since the day's first event (anchor)
        u32 px_ticks round(price / tick)
  - writes ofp_cache/<date>.bin plus ofp_cache/meta.json.

Usage:
  python3 build_ofp_cache.py [bfs_l2_export.jsonl] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    ET = timezone.utc

WS = b" \t"
REC = struct.Struct("<BBHII")          # tag, pad, qty, ts, px_ticks


def fast_ts(raw: bytes) -> Optional[int]:
    i = raw.find(b'"ts":')
    if i < 0:
        return None
    j = i + 5; n = len(raw)
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


def rth_end_ms(date_str: str) -> int:
    y, m, d = map(int, date_str.split("-"))
    return int(datetime(y, m, d, 16, 0, 0, tzinfo=ET).timestamp() * 1000)


def et_offset_sec(date_str: str) -> int:
    y, m, d = map(int, date_str.split("-"))
    off = datetime(y, m, d, 12, 0, tzinfo=ET).utcoffset()
    return int(off.total_seconds()) if off else 0


def build_day(f, off: int, end_ms: int, tick: float) -> tuple:
    """Return (binary_bytes, anchor_ms, n_records, n_skipped_dups)."""
    f.seek(off)
    buf = bytearray()
    anchor: Optional[int] = None
    prev_line: Optional[bytes] = None
    n = 0; dups = 0
    pack = REC.pack_into
    inv_tick = 1.0 / tick
    for raw in f:
        ts = fast_ts(raw)
        if ts is None:
            continue
        if ts >= end_ms:
            break
        line = raw.rstrip(b"\r\n")
        if line == prev_line:           # NT double-fire: byte-identical consecutive line
            dups += 1
            continue
        prev_line = line
        try:
            ev = json.loads(raw)
        except json.JSONDecodeError:
            continue
        et = ev.get("type")
        if anchor is None:
            anchor = ts
        rts = ts - anchor
        if rts < 0 or rts > 0xFFFFFFFF:
            continue
        if et == "depth":
            try:
                px = float(ev["px"]); qty = float(ev["qty"])
            except (KeyError, TypeError, ValueError):
                continue
            side = 0 if ev.get("side") == "BID" else 1
            op = ev.get("op")
            q = 0 if (op == "REM" or qty <= 0) else min(65535, int(round(qty)))
            tag = (side << 1)            # bit0=0 depth
            pxk = int(round(px * inv_tick))
        elif et == "trade":
            try:
                px = float(ev["px"]); qty = float(ev["qty"])
            except (KeyError, TypeError, ValueError):
                continue
            if px <= 0 or qty <= 0:
                continue
            side = 0 if ev.get("side", "BUY") == "BUY" else 1
            tag = 1 | (side << 1)        # bit0=1 trade
            q = min(65535, int(round(qty)))
            pxk = int(round(px * inv_tick))
        else:
            continue
        if pxk < 0 or pxk > 0xFFFFFFFF:
            continue
        off_w = len(buf)
        buf.extend(b"\x00" * 12)
        pack(buf, off_w, tag, 0, q, rts & 0xFFFFFFFF, pxk & 0xFFFFFFFF)
        n += 1
    return bytes(buf), (anchor or 0), n, dups


def main() -> int:
    ap = argparse.ArgumentParser(description="Build compact OFP event cache from NT JSONL.")
    ap.add_argument("file", nargs="?", default="bfs_l2_export.jsonl")
    ap.add_argument("--outdir", default="ofp_cache")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    path = Path(args.file).expanduser().resolve()
    if not path.exists():
        print(f"[cache] file not found: {path}")
        return 1
    cache = path.with_name(path.name + ".btidx.json")
    if not cache.exists():
        print("[cache] missing index cache; run backtest_server.py once to build "
              f"{cache.name}, then retry.")
        return 1
    blob = json.load(cache.open(encoding="utf-8"))
    rth = {k: int(v) for k, v in blob.get("rth_index", {}).items()}
    instrument = blob.get("instrument", "NQ")
    tick = float(blob.get("tick_size", 0.25))
    symbol = instrument.split()[0] if instrument else "NQ"

    dates = sorted(rth)
    if args.start:
        dates = [d for d in dates if d >= args.start]
    if args.end:
        dates = [d for d in dates if d <= args.end]
    if not dates:
        print("[cache] no RTH dates in range.")
        return 1

    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True)
    meta = {"symbol": symbol, "tick": tick, "instrument": instrument, "days": []}
    print(f"[cache] {instrument} tick={tick} -> {outdir}/  ({len(dates)} day(s))")
    t0 = time.monotonic()
    with path.open("rb") as f:
        for d in dates:
            ts0 = time.monotonic()
            data, anchor, n, dups = build_day(f, rth[d], rth_end_ms(d), tick)
            (outdir / f"{d}.bin").write_bytes(data)
            meta["days"].append({
                "date": d, "anchor_ms": anchor, "et_offset_sec": et_offset_sec(d),
                "n": n, "rth_end_ms": rth_end_ms(d), "bytes": len(data),
            })
            print(f"  {d}: {n:,} events ({dups:,} dup lines dropped), "
                  f"{len(data)/1e6:.0f} MB ({time.monotonic()-ts0:.0f}s)", flush=True)
    (outdir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    tot = sum(x["n"] for x in meta["days"])
    print(f"[cache] done: {tot:,} events across {len(dates)} day(s) in {time.monotonic()-t0:.0f}s")
    print(f"[cache] wrote {outdir}/meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
