#!/usr/bin/env python3
"""
backtest_server.py — Replay an NT BfsL2Exporter JSONL as Binance-shape feeds.

Architecture
------------
  NT JSONL  ──►  this server  ──►  ws://localhost:PORT/ws/<sym>@depth     ──►  backtest.html
                                   ws://localhost:PORT/ws/<sym>@aggTrade  ──►  (same strategy code as live)
                                   GET   /api/v3/depth      (snapshot)
                                   GET   /fapi/v1/depth     (snapshot)
                                   POST  /api/control       (play / pause / speed)
                                   GET   /api/status        (replay position)

Lookahead-bias-free guarantees
------------------------------
  - Events are read from the NT JSONL in file order, which is chronological
    by `ts` (NT writes them as they come off the historical feed).
  - The server emits each event to clients ONLY when its scheduled wall-clock
    time arrives. Clients cannot see future events.
  - The REST snapshot reflects only events that have already been replayed
    up to the current position. A user who hits Play immediately gets the
    pre-Play (empty) snapshot, then starts seeing diffs going forward.
  - Event timestamps in the emitted WebSocket messages are rewritten to the
    current wall clock so the frontend's existing Date.now()-based
    `placedAt` / `trade.ts` comparisons work identically to live mode.
    The original NT timestamp is exposed only through `/api/status` for
    the play-bar's "replaying May 7 09:34 ET" label.
  - Trade matching against placed orders is the simulator's own queue model
    (in terminal.html). It only consumes events that arrive AFTER an order
    is placed, so fills can never come from "future" market data.

Usage
-----
  python3 backtest_server.py /path/to/nq_replay.jsonl
  # then open http://localhost:8080/ in your browser

Defaults to ./paper_data/nq_replay.jsonl if no arg is given.

Tested on Python 3.9+. Requires `aiohttp` (already in your requirements).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from aiohttp import web, WSMsgType

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:                            # Python < 3.9 fallback
    ET = timezone.utc

# ── Config ─────────────────────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 8080
DEFAULT_FILE = "bfs_l2_export.jsonl"
# Group depth events whose original ts is within this many ms of the previous
# emitted event into one Binance depthUpdate message. Reduces WS frame count
# without losing fidelity (frontend just sees a few price levels per message,
# same as Binance's batched diffs).
DEPTH_BATCH_WINDOW_MS = 50

# RTH (US equity index regular trading hours): 9:30am – 4:00pm ET, Mon–Fri.
RTH_START_MIN = 9 * 60 + 30        # 9:30 AM ET in minutes since midnight
RTH_END_MIN   = 16 * 60            # 4:00 PM ET in minutes since midnight

# Sidecar index cache version. Bump if the on-disk index format changes so
# stale caches are rebuilt rather than mis-read.
INDEX_CACHE_VERSION = 2


def is_rth_us(ts_ms: int) -> bool:
    """True if the timestamp falls inside US equity-index regular trading hours
    (9:30 AM – 4:00 PM ET, Monday–Friday). DST handled by zoneinfo."""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=ET)
    except (ValueError, OSError):
        return False
    if dt.weekday() >= 5:          # 5=Sat, 6=Sun
        return False
    minutes = dt.hour * 60 + dt.minute
    return RTH_START_MIN <= minutes < RTH_END_MIN


def derive_price_dec(tick_size: float) -> int:
    """Number of decimals needed to represent the tick size. 0.25 → 2, 0.1 → 1."""
    if tick_size <= 0:
        return 2
    # Strip trailing zeros from "0.25" representation
    s = format(tick_size, ".10f").rstrip("0").rstrip(".")
    if "." in s:
        return len(s.split(".", 1)[1])
    return 0


# ── Mutable state ──────────────────────────────────────────────────────────
class ReplayState:
    """Single owner of all backtest state. Mutated only on the asyncio loop."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.file_size = file_path.stat().st_size if file_path.exists() else 0

        # Playback controls
        self.playing = False
        self.speed = 1.0
        self.shutdown = False
        self.rth_only = False                 # skip non-US-RTH events
        self.seek_to: Optional[int] = None    # byte offset to seek to on next loop iteration
        # When True, the next depth message broadcast is preceded by a full
        # book snapshot ("resync") so the frontend can rebuild its book from a
        # coherent baseline. Set after a seek, an RTH skip gap, or an rth_only
        # toggle — any moment where the server has been mutating its own book
        # without broadcasting and the client's book is therefore stale.
        self.need_full_book = False

        # Cursors
        self.file_position = 0
        self.events_emitted = 0
        self.original_first_ts: Optional[int] = None   # ms — first DATA event ts seen
        self.original_current_ts: Optional[int] = None # ms — most recently emitted event ts

        # Pacing anchors (re-set on each play/resume)
        self.anchor_wall_mono: Optional[float] = None  # monotonic seconds
        self.anchor_event_ts: Optional[int] = None     # event ms

        # Reconstructed book — used only to serve REST snapshot requests
        # to clients that connect mid-replay.
        self.bids: dict = {}
        self.asks: dict = {}

        # Binance-style monotonic sequence numbers
        self.update_seq = 0
        self.agg_seq = 0

        # WebSocket clients
        self.depth_clients: Set[web.WebSocketResponse] = set()
        self.trade_clients: Set[web.WebSocketResponse] = set()

        # Contract metadata — populated by pre_scan()
        self.instrument: str = "UNKNOWN"
        self.tick_size: float = 0.25
        self.is_futures: bool = True           # NQ etc. = futures (integer contracts, $/contract fees)
        self.symbol: str = "NQ"                # short symbol used on the wire
        # Date index: UTC date string → byte offset of first event of that date.
        self.date_index: Dict[str, int] = {}
        # RTH index: UTC date string → byte offset of the first US-RTH event of
        # that date (9:30 AM ET). Lets Seek jump straight to the RTH open in
        # rth_only mode instead of grinding through ~13h of overnight events.
        self.rth_index: Dict[str, int] = {}

    # Repace from "now" — call on play/resume so wall-clock pacing resumes
    # from the current event position without trying to "catch up" the
    # paused gap.
    def reset_pacing(self) -> None:
        self.anchor_wall_mono = time.monotonic()
        self.anchor_event_ts = self.original_current_ts


STATE: Optional[ReplayState] = None  # set in main()


# ── Helpers ────────────────────────────────────────────────────────────────
def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_ts(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return None


def parse_line(raw_bytes: bytes) -> Optional[dict]:
    try:
        return json.loads(raw_bytes.decode("utf-8", errors="replace").strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


# ── Book maintenance ───────────────────────────────────────────────────────
def apply_depth_to_book(state: ReplayState, side: str, op: str, px: float, qty: float) -> None:
    """Apply an NT depth event to the in-memory book. This is the ground-truth
    book the server uses to answer /api/v3/depth snapshot requests."""
    target = state.bids if side == "BID" else state.asks
    if op == "REM" or qty <= 0:
        target.pop(px, None)
    else:
        target[px] = qty


# ── Binance-shape message builders ─────────────────────────────────────────
def _fmt_num(x: float) -> str:
    """Binance-style trimmed decimal string. Empty result → '0'."""
    s = format(x, ".8f").rstrip("0").rstrip(".")
    return s if s else "0"


def build_full_book_msg(state: ReplayState, market_ts: int) -> dict:
    """A depthUpdate carrying the COMPLETE current book plus a reset flag `r`.
    The frontend treats `r:true` as 'clear your book and rebuild from this
    message', which re-syncs clients after a seek or an RTH skip gap where the
    server mutated its book without broadcasting the intervening diffs."""
    state.update_seq += 1
    wall = now_ms()
    bids_out = [[_fmt_num(p), _fmt_num(q)]
                for p, q in sorted(state.bids.items(), reverse=True)]
    asks_out = [[_fmt_num(p), _fmt_num(q)]
                for p, q in sorted(state.asks.items())]
    return {
        "e": "depthUpdate",
        "E": wall,
        "T": wall,
        "mT": market_ts,
        "s": state.symbol,
        "U": state.update_seq,
        "u": state.update_seq,
        "pu": state.update_seq - 1,
        "r": True,
        "b": bids_out,
        "a": asks_out,
    }


def build_depth_msg(state: ReplayState, batch: list, market_ts: int) -> dict:
    """Pack a list of NT depth events at adjacent timestamps into one
    Binance depthUpdate message. Rewrites `T`/`E` (wire timestamps) to wall
    clock so the frontend's Date.now()-based math works, and exposes the
    original market timestamp as `mT` for chart/tape display."""
    bids_out = []
    asks_out = []
    for ev in batch:
        side = ev.get("side")
        op = ev.get("op")
        try:
            px = float(ev["px"])
            qty = float(ev["qty"])
        except (KeyError, TypeError, ValueError):
            continue
        emitted_qty = 0.0 if op == "REM" else qty
        entry = [format(px, ".8f").rstrip("0").rstrip("."),
                 format(emitted_qty, ".8f").rstrip("0").rstrip(".")]
        if entry[0] == "":
            entry[0] = "0"
        if entry[1] == "":
            entry[1] = "0"
        if side == "BID":
            bids_out.append(entry)
        elif side == "ASK":
            asks_out.append(entry)

    state.update_seq += 1
    wall = now_ms()
    return {
        "e": "depthUpdate",
        "E": wall,
        "T": wall,
        "mT": market_ts,
        "s": state.symbol,
        "U": state.update_seq,
        "u": state.update_seq,
        "pu": state.update_seq - 1,
        "b": bids_out,
        "a": asks_out,
    }


def build_trade_msg(state: ReplayState, ev: dict, market_ts: int) -> Optional[dict]:
    try:
        px = float(ev["px"])
        qty = float(ev["qty"])
    except (KeyError, TypeError, ValueError):
        return None
    if px <= 0 or qty <= 0:
        return None
    state.agg_seq += 1
    wall = now_ms()
    # Binance `m` = isBuyerMaker. NT side=BUY → aggressive buyer → buyer is
    # taker → `m`=false. NT side=SELL → aggressive seller → buyer is maker → `m`=true.
    is_buyer_maker = ev.get("side") == "SELL"
    return {
        "e": "aggTrade",
        "E": wall,
        "a": state.agg_seq,
        "s": state.symbol,
        "p": format(px, ".8f").rstrip("0").rstrip("."),
        "q": format(qty, ".8f").rstrip("0").rstrip("."),
        "f": state.agg_seq,
        "l": state.agg_seq,
        "T": wall,
        "mT": market_ts,
        "m": is_buyer_maker,
    }


def derive_wire_symbol(instrument: str) -> str:
    """NT instrument names look like 'NQ 06-26' or 'ES 03-26'. Strip the contract
    month so the frontend gets a stable symbol ('NQ', 'ES'). Falls back to the
    raw value if it doesn't match the pattern."""
    if not instrument or instrument == "UNKNOWN":
        return "NQ"
    m = re.match(r"^([A-Z]{1,4})", instrument.strip().upper())
    return m.group(1) if m else instrument.strip().upper()


_MS_PER_DAY = 86_400_000


_WS = b" \t"


def _fast_ts(raw: bytes) -> Optional[int]:
    """Extract the integer `ts` field from a JSONL line without a full
    json.loads. ~10x faster on the hot pre-scan path over a 23 GB file.
    Tolerant of optional whitespace after the colon."""
    i = raw.find(b'"ts":')
    if i < 0:
        return None
    j = i + 5
    n = len(raw)
    while j < n and raw[j] in _WS:
        j += 1
    neg = False
    if j < n and raw[j:j + 1] == b"-":
        neg = True
        j += 1
    start = j
    while j < n and 48 <= raw[j] <= 57:
        j += 1
    if j == start:
        return None
    try:
        v = int(raw[start:j])
    except ValueError:
        return None
    return -v if neg else v


def _fast_type(raw: bytes) -> Optional[bytes]:
    """First char of the `type` value: b'd' depth, b't' trade, b'm' meta.
    Tolerant of optional whitespace around the colon and opening quote."""
    i = raw.find(b'"type":')
    if i < 0:
        return None
    j = i + 7
    n = len(raw)
    while j < n and raw[j] in _WS:
        j += 1
    if j < n and raw[j:j + 1] == b'"':
        j += 1
    return raw[j:j + 1] if j < n else None


def _rth_window_ms(date_str: str) -> Optional[Tuple[int, int]]:
    """For a UTC date string 'YYYY-MM-DD', return (rth_start_ms, rth_end_ms)
    for that calendar date's 9:30am–4:00pm ET session, or None on weekends.
    DST handled by zoneinfo. Computed once per date during the pre-scan."""
    try:
        y, m, d = (int(x) for x in date_str.split("-"))
        start = datetime(y, m, d, 9, 30, 0, tzinfo=ET)
        end = datetime(y, m, d, 16, 0, 0, tzinfo=ET)
    except (ValueError, OverflowError):
        return None
    if start.weekday() >= 5:               # Sat/Sun ET → no RTH session
        return None
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _index_cache_path(file_path: Path) -> Path:
    return file_path.with_name(file_path.name + ".btidx.json")


def _index_cache_key(file_path: Path) -> str:
    st = file_path.stat()
    return f"{st.st_size}-{int(st.st_mtime)}"


def _try_load_index(state: ReplayState) -> bool:
    """Load instrument metadata + date/RTH indexes from the sidecar cache if it
    matches the current data file. Returns True on a hit."""
    cache = _index_cache_path(state.file_path)
    if not cache.exists():
        return False
    try:
        with cache.open("r", encoding="utf-8") as f:
            blob = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    if blob.get("version") != INDEX_CACHE_VERSION:
        return False
    if blob.get("key") != _index_cache_key(state.file_path):
        return False
    state.instrument = blob.get("instrument", state.instrument)
    state.symbol = blob.get("symbol", state.symbol)
    state.tick_size = float(blob.get("tick_size", state.tick_size))
    state.date_index = {k: int(v) for k, v in blob.get("date_index", {}).items()}
    state.rth_index = {k: int(v) for k, v in blob.get("rth_index", {}).items()}
    return True


def _save_index(state: ReplayState) -> None:
    cache = _index_cache_path(state.file_path)
    try:
        with cache.open("w", encoding="utf-8") as f:
            json.dump({
                "version": INDEX_CACHE_VERSION,
                "key": _index_cache_key(state.file_path),
                "instrument": state.instrument,
                "symbol": state.symbol,
                "tick_size": state.tick_size,
                "date_index": state.date_index,
                "rth_index": state.rth_index,
            }, f)
        print(f"[backtest] wrote index cache -> {cache.name}")
    except OSError as e:
        print(f"[backtest] WARN: could not write index cache: {e}")


def pre_scan(state: ReplayState) -> None:
    """One-time scan of the whole file at startup. Extracts the first meta
    event's instrument/tickSize and builds two byte-offset indexes:
      - date_index: UTC date → first event of that date
      - rth_index:  UTC date → first US-RTH event of that date (9:30 ET)
    Results are cached to a sidecar file so subsequent startups load instantly
    (and rebuild automatically if the data file changes).

    Lookahead safety: this only reads what the user can already see in their
    file; it doesn't pre-load events into memory."""
    if not state.file_path.exists():
        return

    if _try_load_index(state):
        print(f"[backtest] loaded index cache: instrument={state.instrument} "
              f"tick={state.tick_size} dates={len(state.date_index)} "
              f"(rth={len(state.rth_index)})")
        return

    print(f"[backtest] pre-scanning {state.file_path.name} (full file) for "
          f"metadata + date/RTH index ...")
    t0 = time.monotonic()
    pos = 0
    cur_day_no: Optional[int] = None
    cur_date: Optional[str] = None
    rth_start_ms = rth_end_ms = 0
    rth_is_weekday = False
    rth_found_for_date = False

    total = state.file_size or 1
    next_progress = 0.05
    with state.file_path.open("rb") as f:
        first3 = f.read(3)
        if first3 != b"\xef\xbb\xbf":
            f.seek(0)
        pos = f.tell()
        for raw in f:
            line_start = pos
            pos += len(raw)
            # Progress heartbeat every ~5% so a multi-minute first scan of a
            # large file doesn't look hung.
            if pos / total >= next_progress:
                print(f"[backtest] pre-scan {pos / total * 100:.0f}% "
                      f"({pos // (1024 * 1024):,} MB, {len(state.date_index)} dates, "
                      f"{time.monotonic() - t0:.0f}s)")
                next_progress += 0.05
            etype = _fast_type(raw)
            if etype is None:
                continue
            if etype == b"m":                  # meta — rare; full parse is fine
                if state.instrument == "UNKNOWN":
                    ev = parse_line(raw)
                    if ev is not None:
                        instr = ev.get("instrument")
                        tick = ev.get("tickSize")
                        if isinstance(instr, str) and instr.strip():
                            state.instrument = instr.strip()
                            state.symbol = derive_wire_symbol(state.instrument)
                        if isinstance(tick, (int, float)) and tick > 0:
                            state.tick_size = float(tick)
                continue
            if etype not in (b"d", b"t"):
                continue
            ts = _fast_ts(raw)
            if ts is None:
                continue

            day_no = ts // _MS_PER_DAY
            if day_no != cur_day_no:
                cur_day_no = day_no
                try:
                    cur_date = datetime.fromtimestamp(
                        ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    cur_date = None
                if cur_date is not None and cur_date not in state.date_index:
                    state.date_index[cur_date] = line_start
                win = _rth_window_ms(cur_date) if cur_date else None
                if win is not None:
                    rth_start_ms, rth_end_ms = win
                    rth_is_weekday = True
                else:
                    rth_is_weekday = False
                rth_found_for_date = cur_date in state.rth_index if cur_date else True

            if (not rth_found_for_date and rth_is_weekday
                    and rth_start_ms <= ts < rth_end_ms):
                state.rth_index[cur_date] = line_start
                rth_found_for_date = True

    elapsed = time.monotonic() - t0
    print(f"[backtest] pre-scan done in {elapsed:.1f}s: "
          f"instrument={state.instrument} tick={state.tick_size} "
          f"dates={len(state.date_index)} rth={len(state.rth_index)} "
          f"({sorted(state.date_index)[:3]}{'...' if len(state.date_index) > 3 else ''})")
    _save_index(state)


async def broadcast(clients: Set[web.WebSocketResponse], msg: dict) -> None:
    if not clients:
        return
    data = json.dumps(msg, separators=(",", ":"))
    dead: list = []
    for ws in clients:
        if ws.closed:
            dead.append(ws)
            continue
        try:
            await ws.send_str(data)
        except (ConnectionError, RuntimeError):
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# ── Replay loop ────────────────────────────────────────────────────────────
async def replay_task(state: ReplayState) -> None:
    """The single producer. Streams events from the JSONL, paces them by
    their original timestamps scaled by `state.speed`, and emits to all
    connected WebSocket clients."""
    if not state.file_path.exists():
        print(f"[backtest] file not found: {state.file_path}")
        return

    print(f"[backtest] opening {state.file_path} ({state.file_size:,} bytes)")
    with state.file_path.open("rb") as f:
        # Strip UTF-8 BOM if present
        first3 = f.read(3)
        if first3 != b"\xef\xbb\xbf":
            f.seek(0)
        state.file_position = f.tell()

        depth_batch: list = []
        depth_batch_first_ts: Optional[int] = None

        async def flush_depth_batch() -> None:
            nonlocal depth_batch, depth_batch_first_ts
            if not depth_batch:
                return
            ts = depth_batch_first_ts or now_ms()
            msg = build_depth_msg(state, depth_batch, ts)
            await broadcast(state.depth_clients, msg)
            depth_batch = []
            depth_batch_first_ts = None

        while not state.shutdown:
            # Apply pending seek BEFORE reading. Done here (not in handler) so
            # we don't race with replay reads.
            if state.seek_to is not None:
                target = state.seek_to
                state.seek_to = None
                await flush_depth_batch()
                f.seek(target)
                state.file_position = target
                state.bids.clear()
                state.asks.clear()
                state.anchor_wall_mono = None
                state.anchor_event_ts = None
                state.original_current_ts = None
                # Book was cleared and we're reading from a mid-stream offset;
                # force a full-book resync to clients on the next broadcast so
                # they don't layer fresh diffs onto a pre-seek book.
                state.need_full_book = True
                print(f"[backtest] seeked to byte {target}")

            # Wait while paused. Don't read ahead — lookahead-bias impossible:
            # nothing past `state.original_current_ts` is in memory until we
            # actually advance past it here.
            if not state.playing:
                await flush_depth_batch()
                await asyncio.sleep(0.05)
                continue

            line = f.readline()
            if not line:
                # EOF
                await flush_depth_batch()
                print("[backtest] EOF -- pausing")
                state.playing = False
                continue

            state.file_position = f.tell()
            ev = parse_line(line)
            if ev is None:
                continue

            etype = ev.get("type")
            ets = ev.get("ts")
            if etype not in ("depth", "trade") or not isinstance(ets, (int, float)):
                # meta and malformed lines are silently skipped on the wire
                continue
            ets = int(ets)

            # RTH-only filter: if enabled, silently advance through any event
            # whose timestamp is outside US 9:30am–4:00pm ET.
            # We do still update the server-side book on skipped events so that
            # when RTH resumes the snapshot reflects accurate state — i.e., we
            # behave AS IF the strategy is sleeping during non-RTH but the
            # market keeps moving. (For the user's stated goal — "run the
            # strategy only during US regular hours" — the strategy panel will
            # see NO ticks during non-RTH and SEES the correct book at RTH open.)
            if state.rth_only and not is_rth_us(ets):
                if etype == "depth":
                    try:
                        px = float(ev["px"]); qty = float(ev["qty"])
                        apply_depth_to_book(state, ev.get("side"), ev.get("op"), px, qty)
                    except (KeyError, TypeError, ValueError):
                        pass
                state.original_current_ts = ets
                # Reset pacing anchor so the first RTH event after a skip
                # block doesn't get smeared across the wall-clock gap.
                state.anchor_wall_mono = None
                state.anchor_event_ts = None
                # We're mutating the book without broadcasting; the client's
                # book is now diverging. Force a full-book resync when RTH
                # broadcasting resumes.
                state.need_full_book = True
                continue

            # Pacing: figure out when (in wall-clock) this event should fire.
            if state.anchor_wall_mono is None or state.anchor_event_ts is None:
                state.reset_pacing()
                state.anchor_event_ts = ets
            elapsed_event_sec = (ets - state.anchor_event_ts) / 1000.0
            target_wall_mono = state.anchor_wall_mono + elapsed_event_sec / max(state.speed, 1e-9)
            sleep_sec = target_wall_mono - time.monotonic()
            if sleep_sec > 0.001:
                # Flush any pending depth batch before sleeping so the client
                # sees the latest state immediately, even when the next event
                # is far away in market time (e.g. overnight gaps).
                await flush_depth_batch()
                slice_sec = 0.05
                while sleep_sec > 0 and state.playing and not state.shutdown and state.seek_to is None:
                    chunk = min(sleep_sec, slice_sec)
                    await asyncio.sleep(chunk)
                    if not state.playing or state.seek_to is not None:
                        state.reset_pacing()
                        break
                    sleep_sec = target_wall_mono - time.monotonic()
                if not state.playing or state.shutdown or state.seek_to is not None:
                    continue

            # Now actually emit. If a seek / RTH gap left the client's book
            # stale, broadcast a full-book resync first (reflecting the server
            # book as of just BEFORE this event, so the diff below lands on a
            # coherent baseline).
            if state.need_full_book:
                await flush_depth_batch()
                await broadcast(state.depth_clients,
                                build_full_book_msg(state, ets))
                state.need_full_book = False

            if etype == "depth":
                side = ev.get("side")
                op = ev.get("op")
                try:
                    px = float(ev["px"])
                    qty = float(ev["qty"])
                except (KeyError, TypeError, ValueError):
                    continue
                apply_depth_to_book(state, side, op, px, qty)
                if depth_batch_first_ts is None:
                    depth_batch_first_ts = ets
                depth_batch.append(ev)
                if ets - depth_batch_first_ts >= DEPTH_BATCH_WINDOW_MS:
                    await flush_depth_batch()
            elif etype == "trade":
                # Flush any pending depth batch first so book-update ordering
                # vs trade events matches the historical sequence.
                await flush_depth_batch()
                msg = build_trade_msg(state, ev, ets)
                if msg is not None:
                    await broadcast(state.trade_clients, msg)

            if state.original_first_ts is None:
                state.original_first_ts = ets
            state.original_current_ts = ets
            state.events_emitted += 1


# ── HTTP / WebSocket handlers ──────────────────────────────────────────────
async def http_index(_req: web.Request) -> web.Response:
    p = Path(__file__).parent / "backtest.html"
    if not p.exists():
        return web.Response(text="backtest.html not found alongside backtest_server.py",
                             status=404)
    return web.FileResponse(p)


async def http_strategy_file(req: web.Request) -> web.Response:
    """Serve the OFP / MRV / AutoMM JS files from the project root."""
    name = req.match_info["name"]
    if not name.endswith(".js") or "/" in name or ".." in name:
        return web.Response(status=400, text="bad filename")
    p = Path(__file__).parent / name
    if not p.exists():
        return web.Response(status=404, text=f"no such strategy file: {name}")
    return web.FileResponse(p, headers={"Cache-Control": "no-cache"})


def _snapshot_body(state: ReplayState, limit: int) -> dict:
    bids_sorted = sorted(state.bids.items(), reverse=True)[:limit]
    asks_sorted = sorted(state.asks.items())[:limit]
    return {
        "lastUpdateId": state.update_seq,
        "E": now_ms(),
        "T": now_ms(),
        "bids": [[format(p, ".8f").rstrip("0").rstrip("."),
                  format(q, ".8f").rstrip("0").rstrip(".")] for p, q in bids_sorted],
        "asks": [[format(p, ".8f").rstrip("0").rstrip("."),
                  format(q, ".8f").rstrip("0").rstrip(".")] for p, q in asks_sorted],
    }


async def http_snapshot(req: web.Request) -> web.Response:
    limit = max(5, min(1000, int(req.query.get("limit", 100))))
    return web.json_response(_snapshot_body(STATE, limit))


async def http_status(_req: web.Request) -> web.Response:
    s = STATE
    pct = (s.file_position / s.file_size * 100.0) if s.file_size else 0.0
    return web.json_response({
        "playing": s.playing,
        "speed": s.speed,
        "rth_only": s.rth_only,
        "events_emitted": s.events_emitted,
        "file_position": s.file_position,
        "file_size": s.file_size,
        "progress_pct": round(pct, 4),
        "original_first_ts": s.original_first_ts,
        "original_current_ts": s.original_current_ts,
        "original_current_human": fmt_ts(s.original_current_ts),
        "wall_now_ms": now_ms(),
        "depth_clients": len(s.depth_clients),
        "trade_clients": len(s.trade_clients),
        "symbol": s.symbol,
        "instrument": s.instrument,
        "tick_size": s.tick_size,
        "price_dec": derive_price_dec(s.tick_size),
        "is_futures": s.is_futures,
        "update_seq": s.update_seq,
        "available_dates": sorted(s.date_index.keys()),
        "rth_dates": sorted(s.rth_index.keys()),
    })


async def http_control(req: web.Request) -> web.Response:
    try:
        body = await req.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "invalid JSON"}, status=400)
    action = body.get("action")
    s = STATE
    if action == "play":
        if not s.playing:
            s.playing = True
            s.reset_pacing()
            print(f"[backtest] play  speed={s.speed}x")
    elif action == "pause":
        if s.playing:
            s.playing = False
            print(f"[backtest] pause at {fmt_ts(s.original_current_ts)}")
    elif action == "set_speed":
        try:
            new_speed = float(body.get("speed", 1.0))
        except (TypeError, ValueError):
            return web.json_response({"error": "speed must be a number"}, status=400)
        if new_speed <= 0:
            return web.json_response({"error": "speed must be > 0"}, status=400)
        s.speed = new_speed
        if s.playing:
            s.reset_pacing()
        print(f"[backtest] speed = {s.speed}x")
    elif action == "set_rth_only":
        s.rth_only = bool(body.get("rth_only", False))
        # Reset pacing so we don't try to "catch up" the skipped non-RTH gap.
        if s.playing:
            s.reset_pacing()
        # Toggling either direction can desync the client's book (entering
        # rth_only starts suppressing diffs; leaving it resumes mid-stream),
        # so force a resync on the next broadcast.
        s.need_full_book = True
        print(f"[backtest] rth_only = {s.rth_only}")
    elif action == "seek":
        date_str = body.get("date")
        if not isinstance(date_str, str) or date_str not in s.date_index:
            return web.json_response({
                "error": f"date not in index. Available: {sorted(s.date_index.keys())}"
            }, status=400)
        # In rth_only mode, jump straight to the first RTH event of the date
        # (9:30 ET) instead of UTC midnight (~8pm ET prev day) — avoids reading
        # through ~13h of overnight events to reach the RTH open.
        if s.rth_only and date_str in s.rth_index:
            s.seek_to = s.rth_index[date_str]
            print(f"[backtest] seek queued (RTH): {date_str} -> byte {s.seek_to}")
        else:
            s.seek_to = s.date_index[date_str]
            print(f"[backtest] seek queued: {date_str} -> byte {s.seek_to}")
    else:
        return web.json_response({"error": f"unknown action: {action}"}, status=400)
    return web.json_response({
        "playing": s.playing,
        "speed": s.speed,
        "rth_only": s.rth_only,
    })


async def ws_depth(req: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    STATE.depth_clients.add(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
            # We ignore client→server messages; control is via /api/control.
    finally:
        STATE.depth_clients.discard(ws)
    return ws


async def ws_trade(req: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    STATE.trade_clients.add(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        STATE.trade_clients.discard(ws)
    return ws


# ── App wiring ─────────────────────────────────────────────────────────────
def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", http_index)
    app.router.add_get("/backtest.html", http_index)
    # Strategy JS files (OFP / MRV / AutoMM) served from project root
    app.router.add_get("/{name:[\\w_]+\\.js}", http_strategy_file)
    # Binance-compat snapshot endpoints
    app.router.add_get("/api/v3/depth", http_snapshot)
    app.router.add_get("/fapi/v1/depth", http_snapshot)
    # Backtest control
    app.router.add_get("/api/status", http_status)
    app.router.add_post("/api/control", http_control)
    # WebSocket streams — match Binance URL pattern so backtest.html can
    # use the same code path. The `{sym}` is cosmetic; we ignore it and
    # always emit the loaded NT data.
    app.router.add_get(r"/ws/{sym}@depth", ws_depth)
    app.router.add_get(r"/ws/{sym}@depth@100ms", ws_depth)
    app.router.add_get(r"/ws/{sym}@aggTrade", ws_trade)
    app.router.add_get(r"/public/ws/{sym}@depth", ws_depth)
    app.router.add_get(r"/public/ws/{sym}@depth@100ms", ws_depth)
    app.router.add_get(r"/market/ws/{sym}@aggTrade", ws_trade)
    return app


async def main_async(file_path: Path) -> None:
    global STATE
    STATE = ReplayState(file_path)

    # Synchronous pre-scan for instrument metadata + date index.
    # Runs once at startup; the replay loop has nothing else to do yet so
    # blocking briefly here is fine.
    pre_scan(STATE)

    app = build_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    print(f"[backtest] server listening on http://{HOST}:{PORT}")
    print(f"[backtest] open http://{HOST}:{PORT}/ in your browser")
    print(f"[backtest] paused; hit Play in the UI to begin streaming")

    replay = asyncio.create_task(replay_task(STATE))
    try:
        await asyncio.Future()  # run until cancelled
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        STATE.shutdown = True
        STATE.playing = False
        replay.cancel()
        try:
            await replay
        except (asyncio.CancelledError, Exception):
            pass
        await runner.cleanup()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("file", nargs="?", default=DEFAULT_FILE,
                    help="path to NT BfsL2Exporter JSONL file (default: %(default)s)")
    args = ap.parse_args()

    p = Path(args.file).expanduser().resolve()
    if not p.exists():
        print(f"[backtest] FAIL: file not found: {p}")
        return 1
    if not p.is_file():
        print(f"[backtest] FAIL: not a regular file: {p}")
        return 1

    try:
        asyncio.run(main_async(p))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
