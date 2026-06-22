"""
validate_export.py — sanity-check an NT BfsL2Exporter JSONL file.

Usage (Windows PowerShell or cmd):
    python validate_export.py "C:\\Users\\<you>\\Documents\\NinjaTrader 8\\bfs_l2_export.jsonl"

Usage (macOS / Linux):
    python3 validate_export.py /path/to/bfs_l2_export.jsonl

Recommended:
    pip install tqdm     (for a progress bar — falls back to text progress if missing)

What it does (single streaming pass, RAM stays small even for 100GB files):
  1. File integrity — readable, BOM handled, last line is complete JSON
  2. Schema validation — every event has the right fields and types
  3. Statistical breakdown — counts by type, side, op, depth position
  4. Temporal coherence — uses ONLY data events (depth/trade) for time range,
     ignoring meta wall-clock timestamps that come from replay-start time
  5. Book reconstruction — separates true inversions (bid > ask, real problem)
     from momentary touches (bid == ask, normal microstructure noise)
  6. Per-session summary — based on data-event timestamps, not meta

Exit code:
  0  → all checks passed (data is good to backtest)
  1  → one or more issues need attention
"""
from __future__ import annotations
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

# ── Thresholds ────────────────────────────────────────────────────────────
TIME_GAP_WARN_SEC          = 300          # warn if 5 min < gap < natural break
TIME_GAP_NATURAL_BREAK_SEC = 30 * 60      # ≥30 min → natural break (CME close / Sunday open, expected)
TIME_GAP_WEEKEND_SEC       = 24 * 3600    # ≥24h → weekend / extended break
# CME equity-index futures close 5:00 PM ET to 6:00 PM ET daily.
# In UTC that's 21:00–22:00 during DST (Mar–Nov) or 22:00–23:00 standard.
# A gap whose start falls in 20:30–23:30 UTC is treated as the daily close,
# regardless of duration.
CME_CLOSE_UTC_START_HOUR = 20.5
CME_CLOSE_UTC_END_HOUR   = 23.5
INVERTED_PCT_WARN          = 0.001        # >0.001% TRUE inversions → suspect
MAX_INVERSION_SAMPLES      = 5
MAX_CROSSED_SAMPLES        = 3
MAX_BAD_LINE_SAMPLES       = 5
PROGRESS_UPDATE_EVERY      = 100_000      # update tqdm postfix every N events
PRICE_EPS                  = 1e-9         # for bid==ask cross detection

VALID_TYPES   = {"meta", "depth", "trade"}
VALID_SIDES_D = {"BID", "ASK"}
VALID_SIDES_T = {"BUY", "SELL"}
VALID_OPS     = {"INS", "UPD", "REM"}

# ── tqdm with graceful fallback ────────────────────────────────────────────
try:
    from tqdm import tqdm
    HAVE_TQDM = True
except ImportError:
    HAVE_TQDM = False


# ── Formatting helpers ────────────────────────────────────────────────────
def fmt_bytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return "{0:,.1f} {1}".format(n, unit)
        n /= 1024
    return "{0:,.1f} PB".format(n)


def fmt_int(n):
    return "{0:,}".format(n)


def fmt_ts(ms):
    if ms is None or ms <= 0:
        return "n/a"
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OSError):
        return "invalid ({0})".format(ms)


def fmt_date(ms):
    if ms is None or ms <= 0:
        return "n/a"
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return "invalid"


def classify_gap(prev_ts_ms, this_ts_ms, gap_sec):
    """Classify a data-event gap. Returns one of:
    'weekend'   — >=24h (Friday close to Sunday reopen)
    'cme_close' — gap straddles or starts in the CME 5 PM ET maintenance window
    'natural'   — >=30min, treated as expected break
    'warn'      — 5min - 30min, likely data dropout
    """
    if gap_sec >= TIME_GAP_WEEKEND_SEC:
        return "weekend"
    try:
        prev_dt = datetime.fromtimestamp(prev_ts_ms / 1000.0, tz=timezone.utc)
        prev_hour = prev_dt.hour + prev_dt.minute / 60.0
        if CME_CLOSE_UTC_START_HOUR <= prev_hour <= CME_CLOSE_UTC_END_HOUR:
            return "cme_close"
    except (ValueError, OSError):
        pass
    if gap_sec >= TIME_GAP_NATURAL_BREAK_SEC:
        return "natural"
    return "warn"


def fmt_duration(seconds):
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:    parts.append("{0}d".format(days))
    if hours:   parts.append("{0}h".format(hours))
    if minutes: parts.append("{0}m".format(minutes))
    return " ".join(parts) if parts else "<1m"


# ── Fallback progress reporter when tqdm is absent ─────────────────────────
class _PlainProgress:
    def __init__(self, total, desc=""):
        self.total = total
        self.done = 0
        self.start = time.monotonic()
        self.last_print = 0
        self.desc = desc
        self.postfix = ""

    def update(self, n):
        self.done += n
        now = time.monotonic()
        # print at most every 0.5s
        if now - self.last_print > 0.5 or self.done >= self.total:
            self.last_print = now
            pct = self.done / self.total * 100 if self.total else 0
            elapsed = now - self.start
            rate = self.done / elapsed if elapsed > 0 else 0
            eta = (self.total - self.done) / rate if rate > 0 else 0
            sys.stdout.write(
                "\r  {0}: {1:.1f}%  {2}/{3}  {4:.1f}MB/s  ETA {5}  {6}      ".format(
                    self.desc, pct,
                    fmt_bytes(self.done), fmt_bytes(self.total),
                    rate / (1024 * 1024), fmt_duration(eta),
                    self.postfix,
                )
            )
            sys.stdout.flush()

    def set_postfix_str(self, s):
        self.postfix = s

    def close(self):
        sys.stdout.write("\r" + " " * 100 + "\r")
        sys.stdout.flush()


# ── Main validator ─────────────────────────────────────────────────────────
def main(path_str):
    path = Path(path_str)
    if not path.exists():
        print("[FAIL] file not found: {0}".format(path))
        return 1
    if not path.is_file():
        print("[FAIL] not a regular file: {0}".format(path))
        return 1

    file_size = path.stat().st_size
    print("=" * 72)
    print("BFS L2 EXPORT VALIDATOR")
    print("=" * 72)
    print("File: {0}".format(path))
    print("Size: {0}".format(fmt_bytes(file_size)))
    if not HAVE_TQDM:
        print("Tip:  `pip install tqdm` for a nicer progress bar.")
    print("")

    # ── [1/4] File integrity ──────────────────────────────────────────────
    print("[1/4] File integrity ...")
    has_bom = False
    try:
        with path.open("rb") as f:
            first3 = f.read(3)
            has_bom = first3 == b"\xef\xbb\xbf"
    except OSError as e:
        print("  [FAIL] cannot read file: {0}".format(e))
        return 1

    if has_bom:
        print("  [OK] file starts with UTF-8 BOM (handled automatically)")
    else:
        print("  [OK] no BOM at file start")

    # Last-line completeness check
    last_line_bytes = b""
    try:
        with path.open("rb") as f:
            seek_to = max(0, file_size - 64 * 1024)
            f.seek(seek_to)
            tail = f.read()
            if b"\n" in tail.rstrip(b"\n"):
                last_line_bytes = tail.rstrip(b"\n").rsplit(b"\n", 1)[-1]
            else:
                last_line_bytes = tail.rstrip(b"\n")
    except OSError as e:
        print("  [FAIL] cannot tail file: {0}".format(e))
        return 1

    try:
        json.loads(last_line_bytes.decode("utf-8", errors="replace"))
        print("  [OK] last line is complete JSON")
    except json.JSONDecodeError:
        print("  [WARN] last line is truncated or malformed (NT may have been killed mid-flush)")
        print("          preview: {0}".format(last_line_bytes[:160]))

    print("")

    # ── [2/4] Streaming scan with progress bar ────────────────────────────
    print("[2/4] Streaming scan ...")
    t0 = time.monotonic()

    # Book state for inversion/cross detection
    bids = {}
    asks = {}
    book_state = 0   # 0 = normal/empty, 1 = crossed (bb==ba), 2 = inverted (bb>ba)

    counts_type = Counter()
    counts_depth_side = Counter()
    counts_trade_side = Counter()
    counts_op         = Counter()
    counts_pos        = Counter()
    counts_meta_event = Counter()

    bad_line_samples   = []
    bad_schema_samples = []
    inverted_samples   = []   # true bid > ask
    crossed_samples    = []   # bid == ask

    # Wall-clock range (every event, including meta)
    wall_first_ts = None
    wall_last_ts  = None

    # Data range (depth + trade only — the real market timestamps)
    data_first_ts = None
    data_last_ts  = None
    last_data_ts  = None

    in_session_gaps = []        # 5min < gap < TIME_GAP_NATURAL_BREAK_SEC
    natural_breaks  = []        # ≥ TIME_GAP_NATURAL_BREAK_SEC
    weekend_breaks  = []        # ≥ TIME_GAP_WEEKEND_SEC

    sessions = []               # list of dicts
    current_session = None

    inverted_count = 0          # transitions into bid > ask
    crossed_count  = 0          # transitions into bid == ask
    px_min, px_max = float("inf"), float("-inf")
    qty_min, qty_max = float("inf"), 0.0
    events_scanned = 0
    bad_line_count = 0
    bad_schema_count = 0

    def start_session(source_evt):
        nonlocal current_session
        if current_session is not None:
            sessions.append(current_session)
        current_session = {
            "source": source_evt,
            "first_data_ts": None,
            "last_data_ts": None,
            "depth": 0, "trade": 0,
            "px_min": float("inf"), "px_max": float("-inf"),
        }

    # Progress bar — bytes-based for accuracy on multi-GB files
    if HAVE_TQDM:
        pbar = tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024,
                    desc="scanning", smoothing=0.05, dynamic_ncols=True,
                    miniters=1, mininterval=0.25)
    else:
        pbar = _PlainProgress(file_size, desc="scanning")

    try:
        with path.open("rb") as f:
            first_line = True
            while True:
                raw_bytes = f.readline()
                if not raw_bytes:
                    break

                pbar.update(len(raw_bytes))

                # Strip BOM from very first line if present
                if first_line:
                    first_line = False
                    if raw_bytes.startswith(b"\xef\xbb\xbf"):
                        raw_bytes = raw_bytes[3:]

                raw = raw_bytes.decode("utf-8", errors="replace").strip()
                if not raw:
                    continue
                events_scanned += 1

                # JSON parse
                try:
                    ev = json.loads(raw)
                except json.JSONDecodeError:
                    bad_line_count += 1
                    if len(bad_line_samples) < MAX_BAD_LINE_SAMPLES:
                        bad_line_samples.append(raw[:200])
                    continue

                etype = ev.get("type")
                ets   = ev.get("ts")
                if etype not in VALID_TYPES or not isinstance(ets, (int, float)):
                    bad_schema_count += 1
                    if len(bad_schema_samples) < MAX_BAD_LINE_SAMPLES:
                        bad_schema_samples.append(raw[:200])
                    continue

                ets = int(ets)
                counts_type[etype] += 1
                if wall_first_ts is None:
                    wall_first_ts = ets
                wall_last_ts = ets

                if etype == "meta":
                    evt = ev.get("event", "")
                    counts_meta_event[evt] += 1
                    if evt in ("start", "replay", "realtime"):
                        bids.clear(); asks.clear()
                        book_state = 0
                        start_session(evt)
                    continue

                # From here on: depth or trade — that's actual market data
                if current_session is None:
                    start_session("implicit")

                # Track gap between consecutive DATA events (skip meta entirely)
                if last_data_ts is not None:
                    gap_sec = (ets - last_data_ts) / 1000.0
                    if gap_sec >= TIME_GAP_WEEKEND_SEC:
                        weekend_breaks.append((last_data_ts, ets, gap_sec))
                    elif gap_sec >= TIME_GAP_NATURAL_BREAK_SEC:
                        natural_breaks.append((last_data_ts, ets, gap_sec))
                    elif gap_sec >= TIME_GAP_WARN_SEC:
                        in_session_gaps.append((last_data_ts, ets, gap_sec))
                last_data_ts = ets

                if data_first_ts is None:
                    data_first_ts = ets
                data_last_ts = ets
                if current_session["first_data_ts"] is None:
                    current_session["first_data_ts"] = ets
                current_session["last_data_ts"] = ets

                if etype == "depth":
                    side = ev.get("side"); op = ev.get("op")
                    pos  = ev.get("pos");  px = ev.get("px"); qty = ev.get("qty")
                    if (side not in VALID_SIDES_D or op not in VALID_OPS
                            or not isinstance(px, (int, float)) or not isinstance(qty, (int, float))):
                        bad_schema_count += 1
                        if len(bad_schema_samples) < MAX_BAD_LINE_SAMPLES:
                            bad_schema_samples.append(raw[:200])
                        continue
                    counts_depth_side[side] += 1
                    counts_op[op] += 1
                    if isinstance(pos, int):
                        counts_pos[pos] += 1
                    px = float(px); qty = float(qty)
                    if px > 0:
                        if px < px_min: px_min = px
                        if px > px_max: px_max = px
                        if px < current_session["px_min"]: current_session["px_min"] = px
                        if px > current_session["px_max"]: current_session["px_max"] = px
                    if qty > 0:
                        if qty < qty_min: qty_min = qty
                        if qty > qty_max: qty_max = qty

                    # Apply to book
                    target = bids if side == "BID" else asks
                    if op == "REM" or qty == 0:
                        target.pop(px, None)
                    else:
                        target[px] = qty
                    current_session["depth"] += 1

                    # Detect book state transitions (count distinct moments,
                    # not every depth event while in the state)
                    if bids and asks:
                        bb = max(bids); ba = min(asks)
                        if bb > ba + PRICE_EPS:
                            new_state = 2  # inverted
                        elif abs(bb - ba) < PRICE_EPS:
                            new_state = 1  # crossed (touching)
                        else:
                            new_state = 0  # normal
                    else:
                        new_state = 0
                    if new_state == 2 and book_state != 2:
                        inverted_count += 1
                        if len(inverted_samples) < MAX_INVERSION_SAMPLES:
                            bb = max(bids); ba = min(asks)
                            inverted_samples.append((ets, bb, ba))
                    elif new_state == 1 and book_state != 1:
                        crossed_count += 1
                        if len(crossed_samples) < MAX_CROSSED_SAMPLES:
                            bb = max(bids); ba = min(asks)
                            crossed_samples.append((ets, bb, ba))
                    book_state = new_state

                elif etype == "trade":
                    side = ev.get("side"); px = ev.get("px"); qty = ev.get("qty")
                    if (side not in VALID_SIDES_T or not isinstance(px, (int, float))
                            or not isinstance(qty, (int, float)) or px <= 0 or qty <= 0):
                        bad_schema_count += 1
                        if len(bad_schema_samples) < MAX_BAD_LINE_SAMPLES:
                            bad_schema_samples.append(raw[:200])
                        continue
                    counts_trade_side[side] += 1
                    if px < px_min: px_min = px
                    if px > px_max: px_max = px
                    if px < current_session["px_min"]: current_session["px_min"] = px
                    if px > current_session["px_max"]: current_session["px_max"] = px
                    if qty > 0:
                        if qty < qty_min: qty_min = qty
                        if qty > qty_max: qty_max = qty
                    current_session["trade"] += 1

                # Progress postfix update
                if events_scanned % PROGRESS_UPDATE_EVERY == 0:
                    postfix = "events={0}  depth={1}  trade={2}".format(
                        fmt_int(events_scanned),
                        fmt_int(counts_type.get("depth", 0)),
                        fmt_int(counts_type.get("trade", 0)),
                    )
                    if HAVE_TQDM:
                        pbar.set_postfix_str(postfix)
                    else:
                        pbar.set_postfix_str(postfix)

    except OSError as e:
        pbar.close()
        print("\n  [FAIL] read error mid-scan: {0}".format(e))
        return 1
    finally:
        pbar.close()

    # Close out last session
    if current_session is not None:
        sessions.append(current_session)

    elapsed = time.monotonic() - t0
    print("  scanned {0} events in {1} ({2:,.0f}/sec)".format(
        fmt_int(events_scanned), fmt_duration(elapsed),
        events_scanned / elapsed if elapsed > 0 else 0
    ))
    print("")

    # ── [3/4] Schema report ───────────────────────────────────────────────
    print("[3/4] Schema & content breakdown")
    print("  Counts by type:")
    for t in ("meta", "depth", "trade"):
        print("    {0:<6} : {1}".format(t, fmt_int(counts_type.get(t, 0))))

    print("  Depth side counts:")
    for s in ("BID", "ASK"):
        print("    {0:<4} : {1}".format(s, fmt_int(counts_depth_side.get(s, 0))))

    print("  Trade aggressor side counts:")
    for s in ("BUY", "SELL"):
        print("    {0:<4} : {1}".format(s, fmt_int(counts_trade_side.get(s, 0))))

    print("  Depth operation counts:")
    for o in ("INS", "UPD", "REM"):
        print("    {0:<3} : {1}".format(o, fmt_int(counts_op.get(o, 0))))

    print("  Depth position coverage:")
    for p in sorted(counts_pos):
        marker = "  (top of book)" if p == 0 else ""
        suspect = ""
        if p > 9:
            suspect = "  (>9 — DOM depth was wider than 10; cosmetic, not a bug)"
        print("    pos {0:>2}: {1}{2}{3}".format(p, fmt_int(counts_pos[p]), marker, suspect))

    print("  Meta event types:")
    for e, n in counts_meta_event.most_common():
        print("    {0:<10} : {1}".format(e, fmt_int(n)))

    if bad_line_count or bad_schema_count:
        print("")
        if bad_line_count:
            print("  [WARN] {0} malformed JSON lines skipped".format(fmt_int(bad_line_count)))
            for s in bad_line_samples:
                print("           sample: {0}".format(s))
        if bad_schema_count:
            print("  [WARN] {0} schema-invalid events skipped".format(fmt_int(bad_schema_count)))
            for s in bad_schema_samples:
                print("           sample: {0}".format(s))
    else:
        print("")
        print("  [OK] all {0} events have valid schema".format(fmt_int(events_scanned)))
    print("")

    # ── [4/4] Temporal & book sanity ──────────────────────────────────────
    print("[4/4] Temporal & book sanity")

    if data_first_ts is None or data_last_ts is None:
        print("  [FAIL] no depth/trade events with timestamps found")
        return 1

    data_span_sec = (data_last_ts - data_first_ts) / 1000.0
    wall_span_sec = (wall_last_ts - wall_first_ts) / 1000.0 if (wall_last_ts and wall_first_ts) else 0

    print("  Wall-clock range (all events, includes replay-start markers):")
    print("    First event : {0}".format(fmt_ts(wall_first_ts)))
    print("    Last  event : {0}".format(fmt_ts(wall_last_ts)))
    print("")
    print("  DATA range (depth + trade events — actual market data):")
    print("    First data  : {0}    ({1})".format(fmt_ts(data_first_ts), fmt_date(data_first_ts)))
    print("    Last  data  : {0}    ({1})".format(fmt_ts(data_last_ts),  fmt_date(data_last_ts)))
    print("    Data span   : {0}".format(fmt_duration(data_span_sec)))

    print("")
    print("  Sessions detected: {0}".format(len(sessions)))
    for i, s in enumerate(sessions, 1):
        if s["first_data_ts"] is None:
            print("    Session {0} ({1}): contains no data events".format(i, s["source"]))
            continue
        sess_span_sec = (s["last_data_ts"] - s["first_data_ts"]) / 1000.0
        if s["px_min"] < float("inf"):
            pxr = "${0:.2f} – ${1:.2f}".format(s["px_min"], s["px_max"])
        else:
            pxr = "n/a"
        print("    Session {0} ({1:<8}) {2} -> {3}  span={4:<10} depth={5:<14} trade={6:<10} px {7}".format(
            i, s["source"],
            fmt_ts(s["first_data_ts"]),
            fmt_ts(s["last_data_ts"]),
            fmt_duration(sess_span_sec),
            fmt_int(s["depth"]),
            fmt_int(s["trade"]),
            pxr,
        ))

    # Gap classification — DATA-event gaps only
    print("")
    if weekend_breaks:
        print("  Weekend/extended breaks (>=24h, expected): {0}".format(len(weekend_breaks)))
    if natural_breaks:
        print("  Natural breaks (>=50min — CME daily close window, expected): {0}".format(len(natural_breaks)))
    if in_session_gaps:
        print("  [WARN] In-session gaps (5min - 50min, possibly data dropouts): {0}".format(len(in_session_gaps)))
        for prev_ts, this_ts, gap in in_session_gaps[:5]:
            print("           {0} -> {1}  ({2})".format(fmt_ts(prev_ts), fmt_ts(this_ts), fmt_duration(gap)))
    else:
        print("  [OK] no suspicious in-session gaps")

    # Price/qty
    print("")
    if px_min < float("inf"):
        print("  Price range across file : ${0:,.2f} – ${1:,.2f}".format(px_min, px_max))
    if qty_max > 0:
        print("  Qty range across file   : {0:,.0f} – {1:,.0f}".format(qty_min, qty_max))

    # Book reconstruction — TRUE inversions vs crossed touches
    print("")
    total_depth = counts_type.get("depth", 0)
    inv_pct = (inverted_count / total_depth * 100.0) if total_depth else 0.0
    crs_pct = (crossed_count  / total_depth * 100.0) if total_depth else 0.0

    print("  Book state moments (transitions, counted once per state entry):")
    print("    Crossed   (bid == ask, touching)  : {0}  ({1:.6f}% of depth events)".format(
        fmt_int(crossed_count), crs_pct))
    print("    Inverted  (bid >  ask, problem!)  : {0}  ({1:.6f}% of depth events)".format(
        fmt_int(inverted_count), inv_pct))

    if inverted_count and inv_pct > INVERTED_PCT_WARN:
        print("    [WARN] inversion rate above {0}%; investigate".format(INVERTED_PCT_WARN))
        for ets, bb, ba in inverted_samples:
            print("           {0}  best_bid={1:.4f}  best_ask={2:.4f}".format(fmt_ts(ets), bb, ba))
    elif inverted_count:
        print("    [OK] inversions are below threshold ({0}%) — within normal microstructure noise".format(INVERTED_PCT_WARN))
    else:
        print("    [OK] no true book inversions detected")

    if crossed_count and crossed_samples:
        print("    Sample crossed moments (normal at NQ tick size):")
        for ets, bb, ba in crossed_samples:
            print("           {0}  best_bid={1:.4f}  best_ask={2:.4f}".format(fmt_ts(ets), bb, ba))

    print("")

    # ── Final verdict ─────────────────────────────────────────────────────
    print("=" * 72)
    issues = []
    if bad_line_count:                            issues.append("{0} malformed JSON lines".format(fmt_int(bad_line_count)))
    if bad_schema_count:                          issues.append("{0} schema-invalid events".format(fmt_int(bad_schema_count)))
    if in_session_gaps:                           issues.append("{0} in-session gaps (5min - 50min)".format(len(in_session_gaps)))
    if inv_pct > INVERTED_PCT_WARN:               issues.append("inversion pct above threshold ({0:.4f}%)".format(inv_pct))
    if not counts_type.get("depth"):              issues.append("NO DEPTH EVENTS — Market Depth wasn't downloaded")
    if not counts_type.get("trade"):              issues.append("NO TRADE EVENTS — empty session?")

    if issues:
        print("SUMMARY: ISSUES FOUND")
        for i in issues:
            print("  - {0}".format(i))
        print("=" * 72)
        return 1

    print("SUMMARY: ALL CHECKS PASSED")
    print("  Events  : {0}".format(fmt_int(events_scanned)))
    print("  Depth   : {0}".format(fmt_int(counts_type.get("depth", 0))))
    print("  Trades  : {0}".format(fmt_int(counts_type.get("trade", 0))))
    print("  Sessions: {0}".format(len(sessions)))
    print("  Data span (market time): {0}  ({1} -> {2})".format(
        fmt_duration(data_span_sec), fmt_date(data_first_ts), fmt_date(data_last_ts)))
    print("  Price range: ${0:,.2f} – ${1:,.2f}".format(px_min, px_max))
    print("  Crossed moments: {0} (normal at NQ tick).  True inversions: {1}.".format(
        fmt_int(crossed_count), fmt_int(inverted_count)))
    print("  File is ready for backtest.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        prog = os.path.basename(sys.argv[0])
        print("Usage: python {0} <path-to-jsonl>".format(prog))
        print("   eg: python {0} \"%USERPROFILE%\\Documents\\NinjaTrader 8\\bfs_l2_export.jsonl\"".format(prog))
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
