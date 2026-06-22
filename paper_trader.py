"""
Headless paper-trading service that mirrors terminal.html's architecture.

What this is:
  - Connects to Binance WebSocket (depth + aggTrade) for the configured symbol.
  - Maintains a live order book locally (snapshot + diff updates).
  - Runs the OrderFlowPredictor strategy in market-execution mode.
  - Paper-fills market orders against the live top-of-book VWAP.
  - Writes every fill to disk IMMEDIATELY in day-keyed JSONL + CSV files —
    no prompts, no buffering past one fill, no browser dependency.
  - Snapshots session state (positions, realized PnL, counters) after every
    fill so a crash/restart resumes exactly where it left off.

Run it:
    pip install -r requirements.txt
    python paper_trader.py

To keep your Mac from sleeping while it runs:
    caffeinate -di python paper_trader.py

Stop it with Ctrl-C — session state is flushed on shutdown.

Output (under paper_data/):
    trades_YYYY-MM-DD.jsonl   one JSON object per fill, append-only
    trades_YYYY-MM-DD.csv     same fills, CSV with header
    session_state.json        positions / PnL snapshot for restart
    runner.log                console log mirror

To analyze later: read any trades_*.jsonl with one line = one fill,
or open the CSV in Excel / pandas / DuckDB.
"""

import asyncio
import csv
import json
import math
import signal as signal_mod
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web
import websockets

# ────────────────────────────────────────────────────────────────────────────────
#  CONFIG — matches terminal.html / orderflow_predictor_strategy.js defaults.
#  Edit these to change behaviour; no UI required.
# ────────────────────────────────────────────────────────────────────────────────
SYMBOL = "BTCUSDT"
MARKET_TYPE = "futures"          # "futures" (USD-M perp) or "spot"
QTY = 1.0                        # contract size — editable live from dashboard

# ── DATA SOURCE ────────────────────────────────────────────────────────────
# 'live'              → Binance WebSocket (current behavior).
# 'replay:<path>'     → JSONL file produced by BfsL2Exporter.cs in NinjaTrader.
DATA_SOURCE = "live"
REPLAY_SPEED = 5.0               # 5× real-time during replay; raise for faster runs
REPLAY_LOOP  = False             # restart replay from start after EOF

# ── CONTRACT MODEL ─────────────────────────────────────────────────────────
# 'crypto'  → fee = price × qty × fee_pct / 100 (Binance-style % of notional)
# 'futures' → fee = qty × FIXED_FEE_PER_CONTRACT (CME-style flat $ per side)
CONTRACT_MODEL = "crypto"
FIXED_FEE_PER_CONTRACT = 1.29    # used when CONTRACT_MODEL == 'futures' (NQ retail ≈ $1.29/side)
MAKER_FEE_PCT = 0.0006
TAKER_FEE_PCT = 0.0006
LATENCY_MS = 300                 # simulated market-order RTT

# OFP strategy params (mirror of orderflow_predictor_strategy.js DEFAULTS)
LEVELS = 5
SIGNAL_THRESHOLD = 0.38
EXIT_THRESHOLD = 0.12
QUOTE_EVERY_MS = 350
MIN_HOLD_MS = 700
EXIT_TTL_MS = 4500
ACTION_COOLDOWN_MS = 1000
TAKE_PROFIT_BPS = 4.0
STOP_LOSS_BPS = 6.0
MIN_SPREAD_BPS = 0.0
MAX_SPREAD_BPS = 120.0
# IMPORTANT: terminal.html's "Start" button overrides the OFP file's DEFAULTS for
# these two — using stricter guards than what's hard-coded in the strategy file.
# We mirror those overrides exactly so the Python tracks what the JS actually runs.
MAX_VOL_BPS = 250.0              # HTML runtime override (file default: 350)
VOL_GUARD_SPREAD_MULT = 8.0      # HTML runtime override (file default: 10)
MIN_TRADE_SAMPLES = 4
HISTORY_MAX = 120
MAX_POSITION = max(0.006, QTY * 6)
ALLOW_PYRAMIDING = False

DATA_DIR = Path(__file__).parent / "paper_data"
DATA_DIR.mkdir(exist_ok=True)
LOG_PATH = DATA_DIR / "runner.log"
SESSION_PATH = DATA_DIR / "session_state.json"
RUNTIME_CONFIG_PATH = DATA_DIR / "runtime_config.json"

STATUS_INTERVAL_S = 30           # console status print cadence
HTTP_PORT_PRIMARY = 1000         # user-requested port (privileged on macOS)
HTTP_PORT_FALLBACK = 8000        # used if 1000 is not bindable without root

# ────────────────────────────────────────────────────────────────────────────────
#  STATE
# ────────────────────────────────────────────────────────────────────────────────
bids: dict[float, float] = {}
asks: dict[float, float] = {}
last_update_id = 0
snapshot_loaded = False
buffered_events: list[dict] = []

trades_hist: deque = deque(maxlen=HISTORY_MAX)
mids_hist: deque = deque(maxlen=HISTORY_MAX)
tape_hist: deque = deque(maxlen=200)         # for dashboard tape panel
last_book = None
last_price: Optional[float] = None

# Candle aggregation for the chart (matches terminal.html's updateCandle logic).
CANDLE_TF_SEC = 1
candles_hist: deque = deque(maxlen=300)
current_candle: Optional[dict] = None

positions: dict[str, dict] = {}
realized_pnl = 0.0
total_fees = 0.0
closed_trades = 0
trade_count = 0
fill_count = 0

pending_market: list[dict] = []   # {side, qty, due_ts, order_id}
entry_ts = 0
last_action_ts = 0
order_seq = 0

last_signal: dict = {"skip": True, "reason": "starting"}
last_reason: str = "starting"

strategy_enabled = True             # toggled by /api/strategy POST
shutdown_requested = False


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}"
    print(line, flush=True)
    try:
        with LOG_PATH.open("a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def now_ms() -> int:
    return int(time.time() * 1000)


def best_bid():
    return max(bids) if bids else None


def best_ask():
    return min(asks) if asks else None


def top_bids(n=20):
    return sorted(bids.items(), key=lambda kv: -kv[0])[:n]


def top_asks(n=20):
    return sorted(asks.items())[:n]


def position_qty(sym=SYMBOL):
    return positions.get(sym, {}).get("netQty", 0.0)


# ────────────────────────────────────────────────────────────────────────────────
#  ORDER BOOK SYNC (snapshot + diff stream, Binance-style)
# ────────────────────────────────────────────────────────────────────────────────
async def fetch_snapshot():
    global last_update_id, bids, asks, snapshot_loaded
    base = "https://fapi.binance.com/fapi/v1/depth" if MARKET_TYPE == "futures" \
        else "https://api.binance.com/api/v3/depth"
    url = f"{base}?symbol={SYMBOL}&limit=100"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            data = await resp.json()
    bids = {float(p): float(q) for p, q in data["bids"] if float(q) > 0}
    asks = {float(p): float(q) for p, q in data["asks"] if float(q) > 0}
    last_update_id = data["lastUpdateId"]
    snapshot_loaded = True
    # Replay any buffered diffs that came in while we were fetching.
    for ev in buffered_events:
        apply_depth_event(ev)
    buffered_events.clear()
    log(f"snapshot loaded: {len(bids)} bids, {len(asks)} asks, U={last_update_id}")


def apply_depth_event(ev: dict):
    global last_update_id
    if not snapshot_loaded:
        buffered_events.append(ev)
        return
    u = ev.get("u")
    if u is None or u <= last_update_id:
        return
    for p, q in ev.get("b", []):
        px, qty = float(p), float(q)
        if qty == 0:
            bids.pop(px, None)
        else:
            bids[px] = qty
    for p, q in ev.get("a", []):
        px, qty = float(p), float(q)
        if qty == 0:
            asks.pop(px, None)
        else:
            asks[px] = qty
    last_update_id = u


# ────────────────────────────────────────────────────────────────────────────────
#  FILL ACCOUNTING (mirror of terminal.html's updatePosition + executeFill)
# ────────────────────────────────────────────────────────────────────────────────
def fee_pct_for_reason(reason: str) -> float:
    return TAKER_FEE_PCT if reason in ("MARKET", "SWEEP") else MAKER_FEE_PCT


def compute_fee(fill_price: float, fill_qty: float, reason: str) -> float:
    """Two fee models: crypto = % of notional, futures = $/contract flat."""
    if CONTRACT_MODEL == "futures":
        return abs(fill_qty) * FIXED_FEE_PER_CONTRACT
    return fill_price * fill_qty * fee_pct_for_reason(reason) / 100.0


def update_position(side: str, fill_price: float, fill_qty: float, reason: str):
    global realized_pnl, total_fees, closed_trades
    pos = positions.setdefault(SYMBOL, {
        "netQty": 0.0, "avgEntry": 0.0, "realizedPnl": 0.0, "fees": 0.0,
    })
    is_buy = side == "BUY"
    fee = compute_fee(fill_price, fill_qty, reason)
    pos["fees"] += fee
    pos["realizedPnl"] -= fee
    total_fees += fee
    realized_pnl -= fee

    if pos["netQty"] == 0:
        pos["avgEntry"] = fill_price
        pos["netQty"] = fill_qty if is_buy else -fill_qty
    elif (is_buy and pos["netQty"] > 0) or (not is_buy and pos["netQty"] < 0):
        total_qty = abs(pos["netQty"]) + fill_qty
        pos["avgEntry"] = (abs(pos["netQty"]) * pos["avgEntry"] + fill_qty * fill_price) / total_qty
        pos["netQty"] = total_qty if is_buy else -total_qty
    else:
        close_qty = min(fill_qty, abs(pos["netQty"]))
        pnl_per_unit = (pos["avgEntry"] - fill_price) if is_buy else (fill_price - pos["avgEntry"])
        realized = pnl_per_unit * close_qty
        pos["realizedPnl"] += realized
        realized_pnl += realized
        closed_trades += 1
        remaining = fill_qty - close_qty
        new_net = pos["netQty"] + fill_qty if is_buy else pos["netQty"] - fill_qty
        if abs(new_net) < 1e-10:
            pos["netQty"] = 0.0
            pos["avgEntry"] = 0.0
        else:
            pos["netQty"] = new_net
            if remaining > 0:
                pos["avgEntry"] = fill_price

    persist_session()


# ────────────────────────────────────────────────────────────────────────────────
#  PERSISTENCE — append-only JSONL + CSV, written synchronously on every fill.
#  No prompts. No buffering. If the process crashes mid-flush, at most one
#  fill could be lost (and even then only the JSONL tail line — CSV is durable).
# ────────────────────────────────────────────────────────────────────────────────
CSV_COLS = ["ts", "iso", "strategy", "symbol", "side", "reason", "feeType",
            "price", "qty", "feePercentAtFill", "feePaidAtFill",
            "executionMode", "orderId", "id"]


def day_key(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")


def persist_execution(rec: dict) -> None:
    day = day_key(rec["ts"])
    jsonl_path = DATA_DIR / f"trades_{day}.jsonl"
    csv_path = DATA_DIR / f"trades_{day}.csv"
    # JSONL append — one fill per line, no read-modify-write.
    with jsonl_path.open("a") as f:
        f.write(json.dumps(rec) + "\n")
    # CSV append — write header if file is new.
    new_csv = not csv_path.exists()
    with csv_path.open("a", newline="") as f:
        w = csv.writer(f)
        if new_csv:
            w.writerow(CSV_COLS)
        row = []
        for c in CSV_COLS:
            if c == "iso":
                row.append(datetime.fromtimestamp(rec["ts"] / 1000, tz=timezone.utc).isoformat())
            else:
                row.append(rec.get(c, ""))
        w.writerow(row)


def persist_session() -> None:
    snap = {
        "v": 1,
        "ts": now_ms(),
        "symbol": SYMBOL,
        "realizedPnl": realized_pnl,
        "totalFees": total_fees,
        "closedTrades": closed_trades,
        "tradeCount": trade_count,
        "fillCount": fill_count,
        "positions": positions,
    }
    tmp = SESSION_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(snap, indent=2))
    tmp.replace(SESSION_PATH)   # atomic on POSIX


def load_session() -> bool:
    global realized_pnl, total_fees, closed_trades, trade_count, fill_count, positions
    if not SESSION_PATH.exists():
        return False
    try:
        snap = json.loads(SESSION_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return False
    if snap.get("symbol") != SYMBOL:
        log(f"session snapshot symbol {snap.get('symbol')} != current {SYMBOL}, ignoring")
        return False
    realized_pnl = snap.get("realizedPnl", 0.0)
    total_fees = snap.get("totalFees", 0.0)
    closed_trades = snap.get("closedTrades", 0)
    trade_count = snap.get("tradeCount", 0)
    fill_count = snap.get("fillCount", 0)
    positions = snap.get("positions", {})
    age_min = (now_ms() - snap.get("ts", 0)) // 60_000
    log(f"session restored ({age_min}m old): rPnL=${realized_pnl:.2f}, "
        f"positions={len(positions)}, prior fills={fill_count}")
    return True


# ── Runtime-tunable config (persists across restarts) ──────────────────────
def load_runtime_config() -> None:
    """Override module defaults from runtime_config.json if it exists.
    Currently only `qty` is exposed; easy to extend."""
    global QTY, MAX_POSITION
    if not RUNTIME_CONFIG_PATH.exists():
        return
    try:
        cfg = json.loads(RUNTIME_CONFIG_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return
    qty = cfg.get("qty")
    if isinstance(qty, (int, float)) and qty > 0:
        QTY = float(qty)
        MAX_POSITION = max(0.006, QTY * 6)
        log(f"runtime config loaded: QTY={QTY} (MAX_POSITION={MAX_POSITION})")


def save_runtime_config() -> None:
    cfg = {"qty": QTY}
    tmp = RUNTIME_CONFIG_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, indent=2))
    tmp.replace(RUNTIME_CONFIG_PATH)


def update_config(qty=None) -> dict:
    """Apply a live config change. Returns the new effective config."""
    global QTY, MAX_POSITION
    changed = []
    if qty is not None:
        q = float(qty)
        if not (q > 0):
            raise ValueError("qty must be > 0")
        if abs(q - QTY) > 1e-12:
            QTY = q
            MAX_POSITION = max(0.006, QTY * 6)
            changed.append(f"QTY={QTY}")
    if changed:
        save_runtime_config()
        log("config updated: " + ", ".join(changed))
    return {"qty": QTY, "max_position": MAX_POSITION}


# ────────────────────────────────────────────────────────────────────────────────
#  MARKET ORDER SIMULATION (mirror of placeMarketOrder + marketFillPrice)
# ────────────────────────────────────────────────────────────────────────────────
def market_fill_price(side: str, qty: float):
    levels = top_asks(20) if side == "BUY" else top_bids(20)
    remaining, notional, filled = qty, 0.0, 0.0
    for px, level_qty in levels:
        if remaining <= 1e-10:
            break
        take = min(remaining, level_qty)
        notional += px * take
        filled += take
        remaining -= take
    if filled <= 1e-10:
        return None
    return notional / filled, filled


def place_market_order(side: str, qty: float) -> str:
    global order_seq
    order_seq += 1
    order_id = f"M{order_seq:06d}"
    pending_market.append({
        "side": side, "qty": qty,
        "due_ts": now_ms() + LATENCY_MS,
        "order_id": order_id,
    })
    return order_id


def execute_fill(order_id: str, side: str, price: float, qty: float, reason: str):
    global fill_count
    fill_count += 1
    ts = now_ms()
    fee_pct = fee_pct_for_reason(reason)
    rec = {
        "id": f"{order_id}-{fill_count}-{ts}",
        "orderId": order_id,
        "symbol": SYMBOL,
        "side": side,
        "price": price,
        "qty": qty,
        "reason": reason,
        "feeType": "TAKER" if reason in ("MARKET", "SWEEP") else "MAKER",
        "feePercentAtFill": fee_pct,
        "feePaidAtFill": compute_fee(price, qty, reason),
        "strategy": "OrderFlowPredictor-Py",
        "executionMode": "market",
        "ts": ts,
    }
    update_position(side, price, qty, reason)
    persist_execution(rec)
    log(f"FILL {side:4s} {qty:.4f} @ {price:.2f} "
        f"fee=${rec['feePaidAtFill']:.4f} pos={position_qty():+.4f} rPnL=${realized_pnl:+.2f}")


def check_pending_market_fills():
    now = now_ms()
    completed = []
    for order in pending_market:
        if order.get("cancelled"):
            completed.append(order)
            continue
        if now >= order["due_ts"]:
            result = market_fill_price(order["side"], order["qty"])
            if result is not None:
                price, filled = result
                execute_fill(order["order_id"], order["side"], price, filled, "MARKET")
            else:
                log(f"market order {order['order_id']} cancelled — no book depth")
            completed.append(order)
    for c in completed:
        pending_market.remove(c)


def cancel_pending_market_orders():
    """Mirror of JS `cancelOwned()` for market mode — cancel any market orders
    that haven't filled yet. A cancelled pending order will be reaped on the
    next pass of check_pending_market_fills without ever placing a fill."""
    for order in pending_market:
        order["cancelled"] = True


def has_pending_market_on_side(side: str) -> bool:
    """Mirror of JS `hasEntryOrder` — true if an unfilled, uncancelled market
    order on this side already exists."""
    return any(
        not o.get("cancelled") and o["side"] == side
        for o in pending_market
    )


# ────────────────────────────────────────────────────────────────────────────────
#  TRADE HANDLER (also drives market-order fill timing)
# ────────────────────────────────────────────────────────────────────────────────
def update_candle(price: float, ts_ms: int) -> None:
    """Aggregate a trade into the current OHLC candle (mirrors terminal.html)."""
    global current_candle
    bucket = (ts_ms // 1000 // CANDLE_TF_SEC) * CANDLE_TF_SEC
    if current_candle is None or current_candle["time"] != bucket:
        if current_candle is not None:
            candles_hist.append(current_candle)
        current_candle = {"time": bucket, "open": price, "high": price,
                          "low": price, "close": price}
    else:
        current_candle["high"] = max(current_candle["high"], price)
        current_candle["low"] = min(current_candle["low"], price)
        current_candle["close"] = price


def handle_trade(trade: dict):
    """Process a Binance trade. Defensive: Binance occasionally pushes events
    with non-positive prices or qtys (subscription acks, malformed payloads,
    rare gateway noise). Drop those so the chart's price scale doesn't get
    yanked down to 0 by a single bad sample."""
    global trade_count, last_price
    price, qty = trade.get("price", 0.0), trade.get("qty", 0.0)
    if not (price > 0 and qty > 0):
        return
    trade_count += 1
    last_price = price
    trades_hist.append(trade)
    tape_hist.append(trade)
    update_candle(price, trade["ts"])
    check_pending_market_fills()


# ────────────────────────────────────────────────────────────────────────────────
#  OFP STRATEGY (port of orderflow_predictor_strategy.js, market mode)
# ────────────────────────────────────────────────────────────────────────────────
def book_features():
    bb, ba = best_bid(), best_ask()
    if bb is None or ba is None or ba <= bb:
        return None
    bids_top = top_bids(LEVELS)
    asks_top = top_asks(LEVELS)
    best_bid_qty = bids_top[0][1] if bids_top else 0.0
    best_ask_qty = asks_top[0][1] if asks_top else 0.0
    mid = (bb + ba) / 2.0
    spread_bps = (ba - bb) / mid * 10000.0
    bid_depth = sum(q for _, q in bids_top)
    ask_depth = sum(q for _, q in asks_top)
    depth_imb = (bid_depth - ask_depth) / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0.0
    queue_imb = (best_bid_qty - best_ask_qty) / (best_bid_qty + best_ask_qty) if (best_bid_qty + best_ask_qty) > 0 else 0.0
    if (best_bid_qty + best_ask_qty) > 0:
        micro = (ba * best_bid_qty + bb * best_ask_qty) / (best_bid_qty + best_ask_qty)
    else:
        micro = mid
    micro_bps = (micro - mid) / mid * 10000.0
    return {
        "bb": bb, "ba": ba, "mid": mid, "spread_bps": spread_bps,
        "bids": bids_top, "asks": asks_top,
        "depth_imb": depth_imb, "queue_imb": queue_imb, "micro_bps": micro_bps,
    }


def update_vol(mid: float) -> float:
    if not mids_hist or abs(mid - mids_hist[-1][0]) > 1e-12:
        mids_hist.append((mid, now_ms()))
    if len(mids_hist) < 8:
        return 0.0
    rets = []
    prev = mids_hist[0][0]
    for m, _ in list(mids_hist)[1:]:
        rets.append(math.log(m / prev))
        prev = m
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    return math.sqrt(var) * 10000.0


def order_flow_imbalance(book):
    global last_book
    if last_book is None:
        last_book = book
        return 0.0
    ofi = 0.0
    for i in range(LEVELS):
        prev_bid = last_book["bids"][i] if i < len(last_book["bids"]) else None
        prev_ask = last_book["asks"][i] if i < len(last_book["asks"]) else None
        bid = book["bids"][i] if i < len(book["bids"]) else None
        ask = book["asks"][i] if i < len(book["asks"]) else None
        w = 1.0 / (i + 1)
        if prev_bid and bid:
            if bid[0] > prev_bid[0]:
                ofi += w * bid[1]
            elif bid[0] < prev_bid[0]:
                ofi -= w * prev_bid[1]
            else:
                ofi += w * (bid[1] - prev_bid[1])
        if prev_ask and ask:
            if ask[0] < prev_ask[0]:
                ofi -= w * ask[1]
            elif ask[0] > prev_ask[0]:
                ofi += w * prev_ask[1]
            else:
                ofi -= w * (ask[1] - prev_ask[1])
    scale = sum(q for _, q in book["bids"]) + sum(q for _, q in book["asks"])
    last_book = book
    if scale <= 0:
        return 0.0
    return max(-1.0, min(1.0, ofi / scale))


def trade_pressure() -> float:
    now = now_ms()
    recent = [t for t in trades_hist if now - t["ts"] < 2500]
    if len(recent) < MIN_TRADE_SAMPLES:
        return 0.0
    signed = sum(t["qty"] if t["side"] == "BUY" else -t["qty"] for t in recent)
    total = sum(t["qty"] for t in recent)
    if total <= 0:
        return 0.0
    return max(-1.0, min(1.0, signed / total))


def compute_signal():
    book = book_features()
    if book is None:
        return {"skip": True, "reason": "waiting for book"}
    vol_bps = update_vol(book["mid"])
    vol_limit = max(MAX_VOL_BPS, book["spread_bps"] * VOL_GUARD_SPREAD_MULT)
    if book["spread_bps"] < MIN_SPREAD_BPS:
        return {"skip": True, "reason": "spread too tight"}
    if book["spread_bps"] > MAX_SPREAD_BPS:
        return {"skip": True, "reason": "spread too wide"}
    if vol_bps > vol_limit:
        return {"skip": True, "reason": f"vol guard {vol_bps:.1f}>{vol_limit:.1f}bps"}
    ofi = order_flow_imbalance(book)
    tape = trade_pressure()
    micro_term = max(-1.0, min(1.0, book["micro_bps"] / max(1.0, book["spread_bps"])))
    raw = (0.34 * book["depth_imb"] + 0.22 * book["queue_imb"]
           + 0.26 * ofi + 0.18 * tape + 0.04 * micro_term)
    score = max(-1.0, min(1.0, raw))
    return {"skip": False, "score": score, "book": book, "vol_bps": vol_bps}


def maybe_exit(book, pos: float, score: float) -> bool:
    global last_action_ts, last_reason
    if pos == 0:
        return False
    age = now_ms() - entry_ts
    avg_entry = positions.get(SYMBOL, {}).get("avgEntry", book["mid"]) or book["mid"]
    pnl_bps = ((book["mid"] - avg_entry) / avg_entry * 10000.0) if pos > 0 \
        else ((avg_entry - book["mid"]) / avg_entry * 10000.0)
    signal_flipped = (pos > 0 and score < -EXIT_THRESHOLD) or (pos < 0 and score > EXIT_THRESHOLD)
    timed_out = age > EXIT_TTL_MS
    take_profit = pnl_bps >= TAKE_PROFIT_BPS
    stop_loss = pnl_bps <= -STOP_LOSS_BPS
    if signal_flipped and age < MIN_HOLD_MS and not stop_loss:
        return False
    if not signal_flipped and not timed_out and not take_profit and not stop_loss:
        return False
    # placeOrReplace semantics: cancel any in-flight market order before placing
    # the new exit. Prevents pending orders from stacking up across ticks.
    cancel_pending_market_orders()
    side = "SELL" if pos > 0 else "BUY"
    place_market_order(side, abs(pos))
    last_action_ts = now_ms()
    reason = "take profit" if take_profit else "stop loss" if stop_loss \
        else "signal flip" if signal_flipped else "time exit"
    last_reason = f"EXIT {reason} pnl={pnl_bps:+.1f}bps"
    log(f"EXIT  reason={reason} score={score:+.2f} pnl={pnl_bps:+.1f}bps")
    return True


def strategy_tick():
    """Mirrors orderflow_predictor_strategy.js `tick()` for market execution.

    Flow (in order, matching the JS):
      1. compute signal
      2. if skip → cancel owned market orders, set reason, return
      3. read live position
      4. maybe_exit (may cancel + place market exit) — if fired, return
      5. cooldown gate
      6. neutral (|score| < threshold) → cancel owned, set reason, return
      7. pyramiding guard (sameDirectionPosition OR pending same-side order)
      8. entry: cancel any pending market order, then place new one
      9. position-cap branch → cancel owned, set reason
    """
    global entry_ts, last_action_ts, last_signal, last_reason
    s = compute_signal()
    last_signal = s
    if s.get("skip"):
        last_reason = s.get("reason", "skip")
        cancel_pending_market_orders()
        return
    pos = position_qty()
    if maybe_exit(s["book"], pos, s["score"]):
        return
    if now_ms() - last_action_ts < ACTION_COOLDOWN_MS:
        last_reason = f"cooldown score={s['score']:+.2f}"
        return
    if abs(s["score"]) < SIGNAL_THRESHOLD:
        last_reason = f"neutral score={s['score']:+.2f}"
        cancel_pending_market_orders()
        return
    wants_buy = s["score"] > 0
    same_dir = (wants_buy and pos > 0) or (not wants_buy and pos < 0)
    has_entry_order = has_pending_market_on_side("BUY" if wants_buy else "SELL")
    if not ALLOW_PYRAMIDING and (same_dir or has_entry_order):
        last_reason = f"holding score={s['score']:+.2f}"
        return
    if wants_buy and pos + QTY <= MAX_POSITION:
        cancel_pending_market_orders()
        place_market_order("BUY", QTY)
        if pos <= 0:
            entry_ts = now_ms()
        last_action_ts = now_ms()
        last_reason = f"predict up score={s['score']:+.2f}"
        log(f"ENTRY BUY  score={s['score']:+.2f}")
    elif not wants_buy and pos - QTY >= -MAX_POSITION:
        cancel_pending_market_orders()
        place_market_order("SELL", QTY)
        if pos >= 0:
            entry_ts = now_ms()
        last_action_ts = now_ms()
        last_reason = f"predict down score={s['score']:+.2f}"
        log(f"ENTRY SELL score={s['score']:+.2f}")
    else:
        last_reason = "position cap"
        cancel_pending_market_orders()


# ────────────────────────────────────────────────────────────────────────────────
#  ASYNC LOOPS
# ────────────────────────────────────────────────────────────────────────────────
async def depth_ws_loop():
    global snapshot_loaded
    base = "wss://fstream.binance.com/ws" if MARKET_TYPE == "futures" \
        else "wss://stream.binance.com:9443/ws"
    url = f"{base}/{SYMBOL.lower()}@depth"
    while not shutdown_requested:
        try:
            snapshot_loaded = False
            buffered_events.clear()
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                log(f"depth WS connected ({url})")
                # Fire-and-forget snapshot fetch — events buffer until it lands.
                asyncio.create_task(fetch_snapshot())
                async for msg in ws:
                    try:
                        ev = json.loads(msg)
                        apply_depth_event(ev)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            log(f"depth WS error: {type(e).__name__}: {e} — reconnect in 3s")
            await asyncio.sleep(3)


async def trade_ws_loop():
    # NOTE: Binance Futures @aggTrade was silent for us on this network; @trade
    # works reliably. Spot still uses @aggTrade. Both event shapes carry p/q/T/m.
    if MARKET_TYPE == "futures":
        url = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@trade"
    else:
        url = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@aggTrade"
    while not shutdown_requested:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                log(f"trade WS connected ({url})")
                async for msg in ws:
                    try:
                        d = json.loads(msg)
                        handle_trade({
                            "price": float(d["p"]),
                            "qty": float(d["q"]),
                            "side": "SELL" if d["m"] else "BUY",
                            "ts": int(d["T"]),
                        })
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
        except Exception as e:
            log(f"trade WS error: {type(e).__name__}: {e} — reconnect in 3s")
            await asyncio.sleep(3)


async def replay_loop(path: str):
    """Read an NT JSONL export (BfsL2Exporter output) and drive the same
    apply_depth_event / handle_trade pipeline the live WS loops use. Pacing
    is in event-clock terms, scaled by REPLAY_SPEED."""
    global snapshot_loaded, last_update_id, bids, asks
    snapshot_loaded = True             # no separate snapshot for replay; book starts empty
    last_update_id = 0
    bids.clear(); asks.clear()

    p = Path(path)
    if not p.exists():
        log(f"replay: file not found: {path}")
        return

    log(f"replay: starting {path} @ {REPLAY_SPEED}× speed")
    first_event_ms = None
    wall_start_ms = now_ms()
    line_count = 0

    while not shutdown_requested:
        with p.open() as f:
            for line in f:
                if shutdown_requested:
                    return
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue

                ev_ts = ev.get("ts")
                if first_event_ms is None and isinstance(ev_ts, (int, float)):
                    first_event_ms = ev_ts

                # Pace by event time. Sleep until our wall clock catches up.
                if isinstance(ev_ts, (int, float)) and first_event_ms is not None:
                    target_wall = wall_start_ms + (ev_ts - first_event_ms) / REPLAY_SPEED
                    drift = target_wall - now_ms()
                    if drift > 1:
                        await asyncio.sleep(min(drift / 1000.0, 1.0))

                t = ev.get("type")
                if t == "depth":
                    side = ev.get("side"); op = ev.get("op")
                    try:
                        px  = float(ev["px"]);  qty = float(ev["qty"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    target = bids if side == "BID" else asks
                    if op == "REM" or qty == 0:
                        target.pop(px, None)
                    else:
                        target[px] = qty
                    last_update_id += 1
                elif t == "trade":
                    try:
                        handle_trade({
                            "price": float(ev["px"]),
                            "qty":   float(ev["qty"]),
                            "side":  ev.get("side", "BUY"),
                            "ts":    int(ev_ts) if isinstance(ev_ts, (int, float)) else now_ms(),
                        })
                    except (KeyError, ValueError, TypeError):
                        continue
                elif t == "meta":
                    log(f"replay meta: {ev}")
                line_count += 1

        if REPLAY_LOOP:
            log(f"replay: looped after {line_count} events; restarting")
            first_event_ms = None
            wall_start_ms = now_ms()
        else:
            log(f"replay: finished, {line_count} events processed")
            return


async def strategy_loop():
    global last_reason
    while not shutdown_requested:
        try:
            # Drain any market orders whose latency window has elapsed BEFORE
            # the strategy reads position — matches the JS where setTimeout
            # fires fills at the latency deadline, well before the next tick.
            check_pending_market_fills()
            if strategy_enabled:
                strategy_tick()
            else:
                cancel_pending_market_orders()
                last_reason = "stopped"
        except Exception as e:
            log(f"strategy tick error: {type(e).__name__}: {e}")
        await asyncio.sleep(QUOTE_EVERY_MS / 1000.0)


async def status_loop():
    while not shutdown_requested:
        await asyncio.sleep(STATUS_INTERVAL_S)
        bb, ba = best_bid(), best_ask()
        bb_s = f"{bb:.2f}" if bb is not None else "—"
        ba_s = f"{ba:.2f}" if ba is not None else "—"
        log(f"status bid={bb_s} ask={ba_s} pos={position_qty():+.4f} "
            f"rPnL=${realized_pnl:+.2f} fees=${total_fees:.2f} fills={fill_count} "
            f"closed={closed_trades} pending={len(pending_market)}")
        persist_session()


# ────────────────────────────────────────────────────────────────────────────────
#  WEB DASHBOARD (read-only monitor on http://localhost:PORT)
#  Auto-refreshes; no user input needed; safe to leave open or close any time.
# ────────────────────────────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Paper Trader — live</title>
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@300;400;500;600&display=swap');
  :root {
    --bg0:#080b10; --bg1:#0c1018; --bg2:#101520; --bg3:#151c28; --bg4:#1a2230;
    --border:#1e2d42; --border2:#243448;
    --text0:#e8eaf0; --text1:#9aaabb; --text2:#5a7a9a; --text3:#334455;
    --bid:#00d97e; --bid-dim:#00833c; --bid-bg:#051a0e; --bid-bg2:#0a2a18;
    --ask:#f03c3c; --ask-dim:#882222; --ask-bg:#1a0505; --ask-bg2:#2a0a0a;
    --accent:#00aaff; --accent-dim:#006699; --accent-bg:#031422;
    --yellow:#f0b429; --yellow-bg:#1a1000;
    --font:'JetBrains Mono',monospace; --font2:'IBM Plex Mono',monospace;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  html,body { height:100%; background:var(--bg0); color:var(--text0);
              font-family:var(--font); font-size:13px; overflow:hidden; line-height:1.4; }
  #app { display:grid;
         grid-template-rows: auto minmax(240px,1.7fr) minmax(170px,1fr) auto auto minmax(120px,0.75fr);
         height:100vh; width:100vw; }

  /* HEADER */
  #header { display:flex; align-items:center; gap:0; background:var(--bg1);
            border-bottom:1px solid var(--border); padding:8px 14px;
            overflow:hidden; flex-wrap:wrap; row-gap:6px; }
  #header-sym { font-size:16px; font-weight:700; color:var(--accent);
                letter-spacing:0.05em; margin-right:18px; min-width:100px; }
  .h-sep { width:1px; height:28px; background:var(--border); margin:0 12px; flex-shrink:0; }
  .h-field { display:flex; flex-direction:column; margin-right:16px; min-width:88px; }
  .h-field .h-label { font-size:11px; color:var(--text2); text-transform:uppercase;
                      letter-spacing:0.1em; margin-bottom:2px; }
  .h-field .h-val { font-size:15px; font-weight:600; }
  #hdr-last { font-size:18px !important; min-width:120px; }
  .hdr-bid { color:var(--bid); } .hdr-ask { color:var(--ask); }
  .hdr-neutral { color:var(--text0); }
  #imb-wrap { display:flex; align-items:center; gap:8px; margin-left:4px; }
  #imb-label { font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.08em; }
  #imb-bar-outer { width:96px; height:8px; background:var(--ask-dim); border-radius:2px; overflow:hidden; }
  #imb-bar-inner { height:100%; background:var(--bid); border-radius:2px; transition:width 0.15s; }
  #imb-pct { font-size:12px; color:var(--text1); width:38px; }
  #conn-dot { width:7px; height:7px; border-radius:50%; background:var(--text3);
              flex-shrink:0; margin-right:8px; transition:background 0.3s; }
  #conn-dot.connected { background:var(--bid); box-shadow:0 0 5px var(--bid); }
  #conn-dot.disconnected { background:var(--ask); }
  #stamp { margin-left:auto; font-size:12px; color:var(--text3); padding-right:14px; }
  #sym-input-wrap { display:flex; align-items:center; gap:8px; margin-left:14px; }
  #sym-input-wrap label { font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.1em; }
  #sym-input { background:var(--bg3); border:1px solid var(--border2); color:var(--accent);
               font-family:var(--font); font-size:14px; font-weight:600;
               padding:6px 10px; border-radius:3px; width:130px; outline:none;
               text-transform:uppercase; }
  #sym-input:disabled { opacity:0.85; cursor:not-allowed; }

  /* Join Bid/Ask buttons in the order-book panel */
  .join-btn { display:flex; align-items:center; gap:6px;
              padding:5px 12px; border-radius:3px; border:none;
              font-family:var(--font); font-size:12px; font-weight:600;
              cursor:not-allowed; letter-spacing:0.05em; opacity:0.55; }
  #join-bid-btn { background:var(--bid-bg2); color:var(--bid); border:1px solid var(--bid-dim); }
  #join-ask-btn { background:var(--ask-bg2); color:var(--ask); border:1px solid var(--ask-dim); }

  /* RESET button in positions header */
  #clr-pos-btn { font-size:11px; color:var(--ask); background:var(--ask-bg);
                 border:1px solid var(--ask-dim); cursor:pointer; font-family:var(--font);
                 letter-spacing:0.05em; padding:4px 10px; border-radius:3px; font-weight:600; }
  #clr-pos-btn:hover { background:var(--ask); color:#000; }

  /* Action buttons in entry rows */
  #place-btn, #cancel-btn, #cancel-all-btn, #strategy-start-btn, #strategy-stop-btn {
    font-family:var(--font); font-size:13px; font-weight:600;
    padding:7px 14px; border-radius:3px; cursor:pointer;
    letter-spacing:0.05em; transition:all 0.1s; }
  #place-btn { background:var(--accent-bg); color:var(--accent); border:1px solid var(--accent-dim); }
  #cancel-btn { background:var(--yellow-bg); color:var(--yellow); border:1px solid #664400; }
  #cancel-all-btn { background:var(--ask-bg); color:var(--ask); border:1px solid var(--ask-dim); }
  #strategy-start-btn { background:var(--bid-bg2); color:var(--bid); border:1px solid var(--bid-dim); }
  #strategy-start-btn:hover:not(:disabled) { background:var(--bid); color:#000; }
  #strategy-stop-btn  { background:var(--ask-bg2); color:var(--ask); border:1px solid var(--ask-dim); }
  #strategy-stop-btn:hover:not(:disabled)  { background:var(--ask); color:#000; }
  #place-btn:disabled, #cancel-btn:disabled, #cancel-all-btn:disabled,
  #strategy-start-btn:disabled, #strategy-stop-btn:disabled {
    opacity:0.4; cursor:not-allowed; }
  #save-trades-btn, #export-all-btn, #clear-saved-trades-btn, #analysis-btn {
    background:var(--bg3); color:var(--text1); border:1px solid var(--border2);
    font-family:var(--font); font-size:12px; font-weight:600;
    padding:6px 10px; border-radius:3px; cursor:pointer; letter-spacing:0.04em; }
  #save-trades-btn:hover, #export-all-btn:hover, #analysis-btn:hover {
    color:var(--accent); border-color:var(--accent-dim); }
  #clear-saved-trades-btn:hover { color:var(--ask); border-color:var(--ask-dim); }

  /* MAIN ROW */
  #main-row { display:grid;
              grid-template-columns: minmax(340px,400px) minmax(0,1fr) minmax(280px,320px);
              overflow:hidden; border-bottom:1px solid var(--border); min-height:0; }

  /* PANEL HEADER */
  .panel-header { display:flex; align-items:center; justify-content:space-between;
                  padding:0 12px; height:36px; background:var(--bg2);
                  border-bottom:1px solid var(--border); flex-shrink:0; }
  .panel-title { font-size:11px; font-weight:600; text-transform:uppercase;
                 letter-spacing:0.12em; color:var(--text1); }

  /* BOOK */
  #book-panel { display:flex; flex-direction:column; border-right:1px solid var(--border); overflow:hidden; }
  #book-content { flex:1; overflow:hidden; display:flex; flex-direction:column; }
  #book-col-hdr { display:grid; grid-template-columns:1fr 100px 1fr;
                  padding:5px 12px; background:var(--bg2);
                  border-bottom:1px solid var(--border); flex-shrink:0; }
  .book-col-hdr-label { font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.08em; }
  .book-col-hdr-label:nth-child(1) { text-align:left; }
  .book-col-hdr-label:nth-child(2) { text-align:center; }
  .book-col-hdr-label:nth-child(3) { text-align:right; }
  #asks-container, #bids-container { flex:1; overflow:hidden; display:flex; flex-direction:column; }
  #spread-row { display:flex; align-items:center; justify-content:center; gap:14px;
                padding:5px 12px; background:var(--bg3);
                border-top:1px solid var(--border); border-bottom:1px solid var(--border);
                flex-shrink:0; }
  #spread-val { font-size:13px; font-weight:600; color:var(--text1); }
  #spread-pct { font-size:12px; color:var(--text2); }
  #mid-price { font-size:13px; color:var(--text0); font-weight:600; }
  .book-rows { flex:1; overflow:hidden; display:flex; flex-direction:column; }
  #asks-rows { justify-content:flex-end; }
  .book-row { display:grid; grid-template-columns:1fr 100px 1fr; padding:2px 12px;
              position:relative; min-height:22px; align-items:center; }
  .book-row .depth-bar { position:absolute; top:0; height:100%; pointer-events:none;
                         opacity:0.18; transition:width 0.1s; right:0; }
  .ask-row .depth-bar { background:var(--ask); }
  .bid-row .depth-bar { background:var(--bid); }
  .book-row .r-qty { font-size:13px; color:var(--text1); text-align:left; position:relative; z-index:1; }
  .book-row .r-price { font-size:13px; font-weight:600; text-align:center; position:relative; z-index:1; }
  .book-row .r-total { font-size:12px; color:var(--text2); text-align:right; position:relative; z-index:1; }
  .ask-row .r-price { color:var(--ask); }
  .bid-row .r-price { color:var(--bid); }

  /* TAPE */
  #tape-panel { display:flex; flex-direction:column; overflow:hidden; min-height:0; min-width:0; }
  #tape-log { flex:1; overflow-y:auto; overflow-x:hidden; padding:4px 0; min-height:0; }
  #tape-log::-webkit-scrollbar { width:3px; }
  #tape-log::-webkit-scrollbar-thumb { background:var(--border2); }
  .tape-col-hdr { display:grid; grid-template-columns:88px 50px 100px 1fr;
                  padding:5px 12px; background:var(--bg2);
                  border-bottom:1px solid var(--border); flex-shrink:0;
                  font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.08em; }
  .tape-row { display:grid; grid-template-columns:88px 50px 100px 1fr;
              padding:2px 12px; min-height:22px; align-items:center; }
  .tape-row.buy-trade { background:linear-gradient(to right,var(--bid-bg),transparent); }
  .tape-row.sell-trade { background:linear-gradient(to right,var(--ask-bg),transparent); }
  .tape-row .t-time { font-size:12px; color:var(--text2); }
  .tape-row .t-side { font-size:12px; font-weight:700; }
  .tape-row .t-price { font-size:13px; font-weight:600; }
  .tape-row .t-qty { font-size:12px; color:var(--text1); text-align:right; }
  .buy-trade .t-side, .buy-trade .t-price { color:var(--bid); }
  .sell-trade .t-side, .sell-trade .t-price { color:var(--ask); }

  /* POSITIONS */
  #pos-panel { display:flex; flex-direction:column; overflow:hidden;
               border-left:1px solid var(--border); min-height:0; }
  #pos-scroll { flex:1; overflow-y:auto; overflow-x:hidden; min-height:0; }
  .pos-summary { padding:10px 12px 8px; border-bottom:1px solid var(--border);
                 background:var(--bg2); flex-shrink:0; }
  .pos-summary-row { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:4px; }
  .pos-summary-label { font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.1em; }
  .pos-summary-val { font-size:16px; font-weight:700; }
  .pos-pnl-pos { color:var(--bid); } .pos-pnl-neg { color:var(--ask); } .pos-pnl-zero { color:var(--text2); }
  .pos-stats-grid { display:grid; grid-template-columns:1fr 1fr; gap:5px 12px; margin-top:6px; }
  .pos-stat { display:flex; flex-direction:column; }
  .pos-stat-lbl { font-size:10px; color:var(--text2); text-transform:uppercase; letter-spacing:0.08em; }
  .pos-stat-val { font-size:13px; color:var(--text1); font-weight:500; }
  .pos-card { padding:10px 12px; border-bottom:1px solid rgba(30,45,66,0.5); }
  .pos-card-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; }
  .pos-sym { font-size:13px; font-weight:700; color:var(--accent); }
  .pos-side-long { font-size:11px; font-weight:700; color:var(--bid); background:var(--bid-bg2);
                   border:1px solid var(--bid-dim); padding:2px 6px; border-radius:2px; }
  .pos-side-short { font-size:11px; font-weight:700; color:var(--ask); background:var(--ask-bg2);
                    border:1px solid var(--ask-dim); padding:2px 6px; border-radius:2px; }
  .pos-pnl-row { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:4px; }
  .pos-pnl-big { font-size:16px; font-weight:700; }
  .pos-pnl-pct { font-size:12px; color:var(--text2); margin-left:4px; }
  .pos-detail-grid { display:grid; grid-template-columns:1fr 1fr; gap:3px 10px; margin-top:4px; }
  .pos-detail { display:flex; flex-direction:column; }
  .pos-detail-lbl { font-size:10px; color:var(--text2); text-transform:uppercase; letter-spacing:0.07em; }
  .pos-detail-val { font-size:12px; color:var(--text1); }
  .pos-empty { display:flex; flex-direction:column; align-items:center; justify-content:center;
               height:90px; color:var(--text2); font-size:12px; gap:6px; }

  /* CHART */
  #chart-section { display:flex; flex-direction:column; overflow:hidden;
                   border-bottom:1px solid var(--border); background:var(--bg0); min-height:0; }
  #chart-panel-header { display:flex; align-items:center; justify-content:space-between;
                        padding:0 14px; height:32px; background:var(--bg2);
                        border-bottom:1px solid var(--border); flex-shrink:0; }
  #chart-panel-header .panel-title { font-size:12px; }
  #chart-ohlc { font-size:12px; color:var(--text2); font-family:var(--font2); }
  #chart-container { flex:1; min-height:0; position:relative; }

  /* ENTRY BARS (read-only — strategy auto-runs in Python) */
  .entry-bar { display:flex; align-items:center; flex-wrap:wrap; gap:8px 10px;
               padding:8px 12px; background:var(--bg2);
               border-bottom:1px solid var(--border); min-height:52px; }
  #entry-row-2 { background:var(--bg1); }
  .entry-group { display:flex; align-items:center; gap:8px; flex-shrink:0; }
  .entry-label { font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.08em; flex-shrink:0; }
  .entry-input { background:var(--bg3); border:1px solid var(--border2); color:var(--text0);
                 font-family:var(--font); font-size:13px; padding:6px 9px; border-radius:3px;
                 outline:none; min-width:60px; }
  .entry-input[disabled], .entry-input[readonly] { opacity:0.7; cursor:not-allowed; }
  .entry-sep { width:1px; height:28px; background:var(--border); flex-shrink:0; }
  .badge { font-size:11px; padding:4px 10px; border-radius:3px; font-weight:600;
           letter-spacing:0.05em; }
  .badge.running { background:var(--bid-bg2); color:var(--bid); border:1px solid var(--bid-dim); }
  .badge.stopped { background:var(--ask-bg); color:var(--ask); border:1px solid var(--ask-dim); }
  #entry-msg { margin-left:auto; font-size:12px; color:var(--text2); }

  /* BLOTTER */
  #blotter-section { display:flex; flex-direction:column; overflow:hidden; }
  #blotter-wrap { flex:1; overflow-y:auto; overflow-x:hidden; }
  #blotter-wrap::-webkit-scrollbar { width:3px; }
  #blotter-wrap::-webkit-scrollbar-thumb { background:var(--border2); }
  .blotter-hdr { display:grid;
                 grid-template-columns:96px 92px 50px 116px 92px 124px 96px 100px;
                 padding:5px 12px; background:var(--bg2); border-bottom:1px solid var(--border);
                 font-size:11px; color:var(--text2); text-transform:uppercase; letter-spacing:0.08em;
                 flex-shrink:0; position:sticky; top:0; z-index:2; }
  .blotter-row { display:grid;
                 grid-template-columns:96px 92px 50px 116px 92px 124px 96px 100px;
                 padding:4px 12px; border-bottom:1px solid rgba(30,45,66,0.4);
                 font-size:13px; align-items:center; opacity:0.85; }
  .b-id { color:var(--accent); font-weight:600; font-size:12px; }
  .b-side-BUY { color:var(--bid); font-weight:700; }
  .b-side-SELL { color:var(--ask); font-weight:700; }
  .b-price-BUY { color:var(--bid); }
  .b-price-SELL { color:var(--ask); }
  .b-status-FILLED { color:var(--bid); }
  .b-queue { color:var(--text2); font-size:12px; }

  ::-webkit-scrollbar { width:4px; height:4px; }
  ::-webkit-scrollbar-track { background:var(--bg1); }
  ::-webkit-scrollbar-thumb { background:var(--border); border-radius:2px; }
</style>
</head><body>
<div id="app">

  <!-- HEADER -->
  <div id="header">
    <div id="conn-dot"></div>
    <div id="header-sym">—</div>
    <div class="h-sep"></div>
    <div class="h-field"><span class="h-label">Last</span><span class="h-val hdr-neutral" id="hdr-last">—</span></div>
    <div class="h-sep"></div>
    <div class="h-field"><span class="h-label">Bid</span><span class="h-val hdr-bid" id="hdr-bid">—</span></div>
    <div class="h-field"><span class="h-label">Ask</span><span class="h-val hdr-ask" id="hdr-ask">—</span></div>
    <div class="h-field"><span class="h-label">Spread</span><span class="h-val" id="hdr-spread" style="color:var(--text1)">—</span></div>
    <div class="h-sep"></div>
    <div id="imb-wrap">
      <span id="imb-label">Imbalance</span>
      <div id="imb-bar-outer"><div id="imb-bar-inner" style="width:50%"></div></div>
      <span id="imb-pct" style="color:var(--text1)">50%</span>
    </div>
    <span id="stamp">—</span>
    <div id="sym-input-wrap">
      <label>Symbol</label>
      <input id="sym-input" type="text" value="BTCUSDT" disabled>
    </div>
  </div>

  <!-- MAIN ROW -->
  <div id="main-row">
    <div id="book-panel">
      <div class="panel-header">
        <span class="panel-title">Order Book</span>
        <button class="join-btn" id="join-ask-btn" disabled>▼ Join Ask</button>
      </div>
      <div id="book-content">
        <div id="book-col-hdr">
          <span class="book-col-hdr-label">Size</span>
          <span class="book-col-hdr-label" style="text-align:center">Price</span>
          <span class="book-col-hdr-label" style="text-align:right">Total</span>
        </div>
        <div id="asks-container"><div class="book-rows" id="asks-rows"></div></div>
        <div id="spread-row">
          <span id="mid-price">—</span><span id="spread-val">—</span><span id="spread-pct"></span>
        </div>
        <div id="bids-container"><div class="book-rows" id="bids-rows"></div></div>
      </div>
      <div class="panel-header" style="border-top:1px solid var(--border); border-bottom:none; justify-content:flex-end;">
        <button class="join-btn" id="join-bid-btn" disabled>▲ Join Bid</button>
      </div>
    </div>

    <div id="tape-panel">
      <div class="panel-header">
        <span class="panel-title">Trade Tape</span>
        <span id="tape-count" style="font-size:11px; color:var(--text3)"></span>
      </div>
      <div class="tape-col-hdr">
        <span>Time</span><span>Side</span><span>Price</span><span style="text-align:right">Qty</span>
      </div>
      <div id="tape-log"></div>
    </div>

    <div id="pos-panel">
      <div class="panel-header">
        <span class="panel-title">Positions</span>
        <button type="button" id="clr-pos-btn" title="Wipe all positions and PnL (saved trade history is not affected)">RESET</button>
      </div>
      <div class="pos-summary">
        <div class="pos-summary-row">
          <span class="pos-summary-label">Unrealized P&amp;L</span>
          <span class="pos-summary-val pos-pnl-zero" id="pos-upnl-total">$0.00</span>
        </div>
        <div class="pos-stats-grid">
          <div class="pos-stat"><span class="pos-stat-lbl">Realized P&amp;L</span><span class="pos-stat-val" id="pos-rpnl-total">$0.00</span></div>
          <div class="pos-stat"><span class="pos-stat-lbl">Total P&amp;L</span><span class="pos-stat-val" id="pos-tpnl-total">$0.00</span></div>
          <div class="pos-stat"><span class="pos-stat-lbl">Open Positions</span><span class="pos-stat-val" id="pos-count">0</span></div>
          <div class="pos-stat"><span class="pos-stat-lbl">Closed P&amp;L</span><span class="pos-stat-val" id="pos-closed-count">0 trades</span></div>
          <div class="pos-stat"><span class="pos-stat-lbl">Total Fees</span><span class="pos-stat-val" id="pos-fees-total">$0.00</span></div>
          <div class="pos-stat"><span class="pos-stat-lbl">Signal Score</span><span class="pos-stat-val" id="pos-signal">—</span></div>
        </div>
      </div>
      <div id="pos-scroll"><div id="pos-cards"></div></div>
    </div>
  </div>

  <!-- CHART -->
  <div id="chart-section">
    <div id="chart-panel-header">
      <span class="panel-title">Price Chart — <span id="chart-tf-label">1s</span></span>
      <span id="chart-ohlc"></span>
    </div>
    <div id="chart-container"></div>
  </div>

  <!-- ENTRY ROW 1 — Manual order entry (disabled in headless mode; qty IS editable for live strategy) -->
  <div id="entry-row-1" class="entry-bar">
    <div class="entry-group">
      <span class="entry-label">Side</span>
      <select id="side-select" class="entry-input" disabled>
        <option>BUY</option><option>SELL</option>
      </select>
    </div>
    <div class="entry-group">
      <span class="entry-label">Price</span>
      <input id="price-input" class="entry-input" type="number" placeholder="0.00" disabled style="width:120px">
    </div>
    <div class="entry-group">
      <span class="entry-label">Qty</span>
      <input id="cfg-qty" class="entry-input" type="number" step="0.001" min="0.001" style="width:100px">
      <button type="button" id="cfg-qty-save" class="entry-input"
              style="cursor:pointer;background:var(--accent-bg);color:var(--accent);border-color:var(--accent-dim);font-weight:600;">
        Set
      </button>
      <span id="cfg-qty-msg" style="font-size:11px;color:var(--text2);margin-left:6px"></span>
    </div>
    <div class="entry-sep"></div>
    <div class="entry-group">
      <button type="button" id="place-btn" disabled>Place Order</button>
      <button type="button" id="cancel-btn" disabled>Cancel Selected</button>
      <button type="button" id="cancel-all-btn" disabled>Cancel All</button>
    </div>
  </div>

  <!-- ENTRY ROW 2 — Strategy, fees, persistence -->
  <div id="entry-row-2" class="entry-bar">
    <div class="entry-group">
      <span class="entry-label">Strategy</span>
      <select id="strategy-select" class="entry-input" disabled style="width:200px">
        <option>Order-Flow Predictor</option>
      </select>
      <select id="strategy-exec-select" class="entry-input" disabled style="width:92px">
        <option>Market</option>
      </select>
      <button type="button" id="strategy-start-btn">Start</button>
      <button type="button" id="strategy-stop-btn">Stop</button>
    </div>
    <div class="entry-sep"></div>
    <div class="entry-group">
      <span class="entry-label">Maker</span>
      <input id="cfg-maker" class="entry-input" type="text" readonly style="width:80px">
      <span class="entry-label">Taker</span>
      <input id="cfg-taker" class="entry-input" type="text" readonly style="width:80px">
      <span class="entry-label" style="color:var(--text3)">%</span>
    </div>
    <div class="entry-sep"></div>
    <div class="entry-group">
      <span class="entry-label">Latency</span>
      <input id="cfg-latency" class="entry-input" type="text" readonly style="width:70px">
      <span class="entry-label" style="color:var(--text3)">ms</span>
    </div>
    <div class="entry-sep"></div>
    <div class="entry-group">
      <button type="button" id="save-trades-btn" title="Download today's trades as JSON + CSV">Export Today</button>
      <button type="button" id="export-all-btn" title="Download every saved day as JSON + CSV">Export All</button>
      <button type="button" id="clear-saved-trades-btn" title="Wipe day-keyed trade history files">Clear Saved</button>
      <button type="button" id="analysis-btn" title="Open analysis viewer">Analysis</button>
    </div>
    <span id="entry-msg">—</span>
  </div>

  <!-- BLOTTER -->
  <div id="blotter-section">
    <div class="blotter-hdr" id="blotter-hdr-row">
      <span>Order ID</span><span>Symbol</span><span>Side</span>
      <span>Price</span><span>Qty</span><span>Filled</span>
      <span>Status</span><span>Queue Ahead</span>
    </div>
    <div id="blotter-wrap"><div id="blotter-rows"></div></div>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);
const fmtUsd = v => (v >= 0 ? '+$' : '-$') + Math.abs(v).toFixed(2);
const pnlCls = v => v > 0 ? 'pos-pnl-pos' : v < 0 ? 'pos-pnl-neg' : 'pos-pnl-zero';
const fts = ms => { const d = new Date(ms); return d.toTimeString().slice(0,8) + '.' + String(d.getMilliseconds()).padStart(3,'0'); };

let chart = null, candleSeries = null;
function initChart() {
  const c = $('chart-container');
  chart = LightweightCharts.createChart(c, {
    layout: { background: { color: '#080b10' }, textColor: '#9aaabb' },
    grid:   { vertLines: { color: '#1e2d42' }, horzLines: { color: '#1e2d42' } },
    timeScale: { timeVisible: true, secondsVisible: true, borderColor: '#243448' },
    rightPriceScale: { borderColor: '#243448' },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal,
                 vertLine: { color:'#243448', labelBackgroundColor:'#101520' },
                 horzLine: { color:'#243448', labelBackgroundColor:'#101520' } },
    width: c.clientWidth, height: c.clientHeight,
  });
  candleSeries = chart.addCandlestickSeries({
    upColor:'#00d97e', downColor:'#f03c3c',
    borderUpColor:'#00d97e', borderDownColor:'#f03c3c',
    wickUpColor:'#00d97e', wickDownColor:'#f03c3c',
  });
  new ResizeObserver(() => chart.applyOptions({ width: c.clientWidth, height: c.clientHeight })).observe(c);
}

async function refreshStatus() {
  try {
    const s = await fetch('/api/status').then(r => r.json());
    $('conn-dot').className = s.book_ready ? 'connected' : '';
    $('conn-dot').setAttribute('id', s.book_ready ? 'conn-dot' : 'conn-dot');
    $('conn-dot').className = s.book_ready ? 'connected' : 'disconnected';
    $('header-sym').textContent = s.symbol + ' (' + s.market_type.toUpperCase() + ')';
    $('hdr-bid').textContent = s.best_bid != null ? s.best_bid.toFixed(2) : '—';
    $('hdr-ask').textContent = s.best_ask != null ? s.best_ask.toFixed(2) : '—';
    $('hdr-spread').textContent = s.spread_bps != null ? s.spread_bps.toFixed(2) + ' bps' : '—';
    $('stamp').textContent = new Date(s.ts).toLocaleTimeString();
    // Only sync the qty input from the server when the user isn't editing it,
    // so a half-typed number doesn't get clobbered by a refresh.
    const qtyEl = $('cfg-qty');
    if (document.activeElement !== qtyEl) qtyEl.value = s.qty;
    $('cfg-taker').value = s.taker_fee_pct;
    $('cfg-maker').value = s.maker_fee_pct;
    $('cfg-latency').value = s.latency_ms;
    // Position card area
    $('pos-rpnl-total').textContent = fmtUsd(s.realized_pnl);
    $('pos-rpnl-total').className = 'pos-stat-val ' + pnlCls(s.realized_pnl);
    $('pos-upnl-total').textContent = fmtUsd(s.unrealized_pnl);
    $('pos-upnl-total').className = 'pos-summary-val ' + pnlCls(s.unrealized_pnl);
    $('pos-tpnl-total').textContent = fmtUsd(s.total_pnl);
    $('pos-tpnl-total').className = 'pos-stat-val ' + pnlCls(s.total_pnl);
    $('pos-count').textContent = s.position !== 0 ? '1' : '0';
    $('pos-closed-count').textContent = s.closed_trades + ' trades';
    $('pos-fees-total').textContent = '-$' + s.total_fees.toFixed(2);
    $('pos-signal').textContent = s.signal_score != null ? s.signal_score.toFixed(3) : '—';
    $('entry-msg').textContent = s.last_reason || '—';
    $('tape-count').textContent = s.trade_count + ' trades';
    // Toggle start/stop button disabled state based on whether strategy is running
    $('strategy-start-btn').disabled = !!s.strategy_running;
    $('strategy-stop-btn').disabled  = !s.strategy_running;

    // Position card (single symbol)
    const cards = $('pos-cards');
    if (s.position !== 0) {
      const isLong = s.position > 0;
      const upnl = s.unrealized_pnl;
      cards.innerHTML = `<div class="pos-card">
        <div class="pos-card-header">
          <span class="pos-sym">${s.symbol}</span>
          <span class="${isLong ? 'pos-side-long' : 'pos-side-short'}">${isLong ? 'LONG' : 'SHORT'}</span>
        </div>
        <div class="pos-pnl-row">
          <span class="pos-pnl-big ${pnlCls(upnl)}">${fmtUsd(upnl)}</span>
          <span class="pos-pnl-pct">${s.avg_entry > 0 ? ((s.best_bid != null ? ((((isLong ? s.best_bid : s.best_ask) - s.avg_entry) / s.avg_entry) * 100 * (isLong ? 1 : -1)).toFixed(3) : '0.000')) : '0.000'}%</span>
        </div>
        <div class="pos-detail-grid">
          <div class="pos-detail"><span class="pos-detail-lbl">Qty</span><span class="pos-detail-val">${Math.abs(s.position).toFixed(4)}</span></div>
          <div class="pos-detail"><span class="pos-detail-lbl">Avg Entry</span><span class="pos-detail-val">${s.avg_entry.toFixed(2)}</span></div>
        </div>
      </div>`;
    } else {
      cards.innerHTML = '<div class="pos-empty">No open position</div>';
    }
  } catch (e) { $('conn-dot').className = 'disconnected'; }
}

let lastLast = null;
async function refreshBook() {
  try {
    const b = await fetch('/api/book?levels=12').then(r => r.json());
    const asks = b.asks.slice().reverse();   // worst ask first → best ask last (touch)
    const bids = b.bids;                     // best bid first
    const maxQ = Math.max(1e-9, ...asks.map(l => l[1]), ...bids.map(l => l[1]));
    const buildRows = (levels, side) => {
      let cum = 0;
      return levels.map(([px, qty]) => {
        cum += qty;
        const w = (qty / maxQ * 100).toFixed(0);
        return `<div class="book-row ${side}-row">
          <div class="depth-bar" style="width:${w}%"></div>
          <span class="r-qty">${qty.toFixed(3)}</span>
          <span class="r-price">${px.toFixed(2)}</span>
          <span class="r-total">${cum.toFixed(3)}</span></div>`;
      }).join('');
    };
    $('asks-rows').innerHTML = buildRows(asks, 'ask');
    $('bids-rows').innerHTML = buildRows(bids, 'bid');
    if (b.bids[0] && b.asks[0]) {
      const bb = b.bids[0][0], ba = b.asks[0][0];
      const mid = (bb + ba) / 2, sp = ba - bb, spB = sp / mid * 10000;
      $('mid-price').textContent = mid.toFixed(2);
      $('spread-val').textContent = sp.toFixed(2);
      $('spread-pct').textContent = '(' + spB.toFixed(2) + ' bps)';
      // Header last from book mid if no live last yet
      if ($('hdr-last').textContent === '—') $('hdr-last').textContent = mid.toFixed(2);
      // Imbalance from top 10 levels by qty
      const bSum = b.bids.slice(0,10).reduce((s,l) => s+l[1], 0);
      const aSum = b.asks.slice(0,10).reduce((s,l) => s+l[1], 0);
      const ratio = bSum / (bSum + aSum);
      $('imb-bar-inner').style.width = (ratio * 100).toFixed(1) + '%';
      $('imb-pct').textContent = (ratio * 100).toFixed(0) + '%';
    }
  } catch (e) {}
}

async function refreshTape() {
  try {
    const t = await fetch('/api/tape?limit=80').then(r => r.json());
    $('tape-log').innerHTML = t.slice().reverse().map(tr => {
      const isBuy = tr.side === 'BUY';
      return `<div class="tape-row ${isBuy ? 'buy-trade' : 'sell-trade'}">
        <span class="t-time">${fts(tr.ts)}</span>
        <span class="t-side">${isBuy ? '▲ BUY' : '▼ SELL'}</span>
        <span class="t-price">${tr.price.toFixed(2)}</span>
        <span class="t-qty">${tr.qty.toFixed(4)}</span></div>`;
    }).join('');
    if (t.length) {
      const last = t[t.length-1];
      $('hdr-last').textContent = last.price.toFixed(2);
      $('hdr-last').className = 'h-val ' + (last.side === 'BUY' ? 'hdr-bid' : 'hdr-ask');
    }
  } catch (e) {}
}

async function refreshCandles() {
  try {
    const cs = await fetch('/api/candles').then(r => r.json());
    if (candleSeries && cs.length) {
      candleSeries.setData(cs);
      const last = cs[cs.length-1];
      $('chart-ohlc').textContent = `O ${last.open.toFixed(2)}  H ${last.high.toFixed(2)}  L ${last.low.toFixed(2)}  C ${last.close.toFixed(2)}`;
    }
  } catch (e) {}
}

async function refreshFills() {
  try {
    const f = await fetch('/api/fills?limit=80').then(r => r.json());
    $('blotter-rows').innerHTML = f.slice().reverse().map(x => `<div class="blotter-row">
      <span class="b-id">${x.orderId}</span>
      <span>${x.symbol}</span>
      <span class="b-side-${x.side}">${x.side}</span>
      <span class="b-price-${x.side}">${x.price.toFixed(2)}</span>
      <span>${x.qty.toFixed(4)}</span>
      <span>${x.qty.toFixed(4)}</span>
      <span class="b-status-FILLED">FILLED</span>
      <span class="b-queue">0</span></div>`).join('');
  } catch (e) {}
}

async function saveQty() {
  const v = parseFloat($('cfg-qty').value);
  const msgEl = $('cfg-qty-msg');
  if (!Number.isFinite(v) || v <= 0) { msgEl.textContent = '✗ must be > 0'; msgEl.style.color = 'var(--ask)'; return; }
  msgEl.textContent = '…saving'; msgEl.style.color = 'var(--text2)';
  try {
    const r = await fetch('/api/config', { method:'POST', headers:{'Content-Type':'application/json'},
                                            body: JSON.stringify({ qty: v }) });
    if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.error || ('HTTP '+r.status)); }
    const cfg = await r.json();
    msgEl.textContent = '✓ qty = ' + cfg.qty; msgEl.style.color = 'var(--bid)';
    setTimeout(() => { msgEl.textContent = ''; }, 3000);
  } catch (e) { msgEl.textContent = '✗ ' + e.message; msgEl.style.color = 'var(--ask)'; }
}
$('cfg-qty-save').addEventListener('click', saveQty);
$('cfg-qty').addEventListener('keydown', e => { if (e.key === 'Enter') saveQty(); });

// RESET — clears positions and PnL (does NOT touch saved trade files)
$('clr-pos-btn').addEventListener('click', async () => {
  if (!confirm('Reset positions and PnL? (Trade history files are NOT affected.)')) return;
  await fetch('/api/reset', { method: 'POST' });
  refreshStatus();
});

// Strategy start/stop
async function toggleStrategy(action) {
  await fetch('/api/strategy', { method: 'POST', headers:{'Content-Type':'application/json'},
                                  body: JSON.stringify({ action }) });
  refreshStatus();
}
$('strategy-start-btn').addEventListener('click', () => toggleStrategy('start'));
$('strategy-stop-btn').addEventListener('click', () => toggleStrategy('stop'));

// Export helpers — fetch fills, build JSON+CSV, trigger downloads (no prompts).
function downloadFile(name, content, mime) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = name; a.target = '_blank'; a.rel = 'noopener';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function fillsToCsv(fills) {
  const cols = ['ts','iso','strategy','symbol','side','reason','feeType',
                'price','qty','feePercentAtFill','feePaidAtFill','executionMode','orderId','id'];
  const esc = v => { if (v == null) return ''; const s = String(v); return /[",\n]/.test(s) ? '"'+s.replace(/"/g,'""')+'"' : s; };
  const lines = [cols.join(',')];
  for (const f of fills) {
    lines.push(cols.map(c => esc(c === 'iso' ? new Date(f.ts).toISOString() : f[c])).join(','));
  }
  return lines.join('\n');
}
$('save-trades-btn').addEventListener('click', async () => {
  const day = new Date().toISOString().slice(0,10);
  const fills = await fetch(`/api/fills?day=${day}&limit=10000`).then(r => r.json());
  if (!fills.length) { $('entry-msg').textContent = 'No trades today'; return; }
  downloadFile(`bfs_trades_${day}.json`, JSON.stringify(fills, null, 2), 'application/json');
  downloadFile(`bfs_trades_${day}.csv`,  fillsToCsv(fills),               'text/csv');
  $('entry-msg').textContent = `Exported ${fills.length} fills`;
});
$('export-all-btn').addEventListener('click', async () => {
  const days = await fetch('/api/days').then(r => r.json());
  if (!days.length) { $('entry-msg').textContent = 'No saved days'; return; }
  let all = [];
  for (const d of days) all = all.concat(await fetch(`/api/fills?day=${d}&limit=100000`).then(r => r.json()));
  all.sort((a,b) => (a.ts||0) - (b.ts||0));
  const stamp = new Date().toISOString().slice(0,10);
  downloadFile(`bfs_trades_all_${stamp}.json`, JSON.stringify(all, null, 2), 'application/json');
  downloadFile(`bfs_trades_all_${stamp}.csv`,  fillsToCsv(all),               'text/csv');
  $('entry-msg').textContent = `Exported ${all.length} fills across ${days.length} day(s)`;
});
$('clear-saved-trades-btn').addEventListener('click', async () => {
  if (!confirm('Delete ALL saved trade files (JSONL + CSV)?')) return;
  const r = await fetch('/api/clear-saved', { method: 'POST' }).then(r => r.json());
  $('entry-msg').textContent = `Removed ${r.removed} file(s)`;
  refreshFills();
});
$('analysis-btn').addEventListener('click', () => {
  $('entry-msg').textContent = 'Analysis viewer not available in headless mode (use analyze_trades.py)';
});

initChart();
refreshStatus(); refreshBook(); refreshTape(); refreshCandles(); refreshFills();
setInterval(refreshStatus, 1000);
setInterval(refreshBook, 500);
setInterval(refreshTape, 1000);
setInterval(refreshCandles, 1000);
setInterval(refreshFills, 3000);
</script>
</body></html>"""


async def http_dashboard(_req):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def http_status(_req):
    bb, ba = best_bid(), best_ask()
    spread_bps = None
    if bb is not None and ba is not None and ba > bb:
        spread_bps = (ba - bb) / ((ba + bb) / 2.0) * 10000.0
    pos = position_qty()
    avg_entry = positions.get(SYMBOL, {}).get("avgEntry", 0.0)
    mid = (bb + ba) / 2.0 if (bb is not None and ba is not None) else None
    unrealized = 0.0
    if pos != 0 and mid is not None and avg_entry > 0:
        unrealized = pos * (mid - avg_entry)
    score = last_signal.get("score") if isinstance(last_signal, dict) else None
    return web.json_response({
        "ts": now_ms(),
        "symbol": SYMBOL,
        "market_type": MARKET_TYPE,
        "qty": QTY,
        "maker_fee_pct": MAKER_FEE_PCT,
        "taker_fee_pct": TAKER_FEE_PCT,
        "latency_ms": LATENCY_MS,
        "best_bid": bb,
        "best_ask": ba,
        "spread_bps": spread_bps,
        "book_ready": snapshot_loaded and bb is not None,
        "position": pos,
        "avg_entry": avg_entry,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized,
        "total_pnl": realized_pnl + unrealized,
        "total_fees": total_fees,
        "fill_count": fill_count,
        "closed_trades": closed_trades,
        "trade_count": trade_count,
        "pending_market": len(pending_market),
        "signal_score": score,
        "last_reason": last_reason,
        "strategy_running": strategy_enabled,
    })


async def http_fills(req):
    day = req.query.get("day") or datetime.now().strftime("%Y-%m-%d")
    limit = max(1, min(2000, int(req.query.get("limit", 100))))
    p = DATA_DIR / f"trades_{day}.jsonl"
    out = []
    if p.exists():
        with p.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return web.json_response(out[-limit:])


async def http_days(_req):
    return web.json_response(sorted(
        p.stem.replace("trades_", "") for p in DATA_DIR.glob("trades_*.jsonl")
    ))


async def http_book(req):
    n = max(1, min(50, int(req.query.get("levels", 16))))
    return web.json_response({
        "bids": [[p, q] for p, q in top_bids(n)],
        "asks": [[p, q] for p, q in top_asks(n)],
    })


async def http_tape(req):
    limit = max(1, min(500, int(req.query.get("limit", 100))))
    return web.json_response(list(tape_hist)[-limit:])


async def http_candles(_req):
    out = list(candles_hist)
    if current_candle is not None:
        out.append(dict(current_candle))
    # Filter any historical candle whose OHLC wasn't fully populated. Defense
    # against the price-scale getting yanked to 0 by stale/bad data.
    return web.json_response([
        c for c in out
        if c.get("open", 0) > 0 and c.get("high", 0) > 0
           and c.get("low", 0) > 0 and c.get("close", 0) > 0
    ])


async def http_reset(_req):
    """Clear positions and PnL counters (mirror of terminal.html RESET button)."""
    global realized_pnl, total_fees, closed_trades, positions
    realized_pnl = 0.0
    total_fees = 0.0
    closed_trades = 0
    positions = {}
    if SESSION_PATH.exists():
        try: SESSION_PATH.unlink()
        except OSError: pass
    log("RESET: positions and PnL cleared via dashboard")
    return web.json_response({"ok": True})


async def http_strategy(req):
    """GET → current running state. POST {action: 'start'|'stop'} → toggle."""
    global strategy_enabled
    if req.method == "POST":
        try:
            body = await req.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError):
            return web.json_response({"error": "invalid JSON body"}, status=400)
        action = body.get("action")
        if action == "start":
            strategy_enabled = True
            log("strategy STARTED via dashboard")
        elif action == "stop":
            strategy_enabled = False
            cancel_pending_market_orders()
            log("strategy STOPPED via dashboard")
        else:
            return web.json_response({"error": "action must be 'start' or 'stop'"}, status=400)
    return web.json_response({"running": strategy_enabled})


async def http_clear_saved(_req):
    removed = 0
    for p in list(DATA_DIR.glob("trades_*.jsonl")) + list(DATA_DIR.glob("trades_*.csv")):
        try:
            p.unlink()
            removed += 1
        except OSError:
            pass
    log(f"clear-saved: removed {removed} file(s)")
    return web.json_response({"removed": removed})


async def http_config(req):
    """GET → current runtime config. POST → update (JSON body: {"qty": <number>})."""
    if req.method == "POST":
        try:
            body = await req.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError):
            return web.json_response({"error": "invalid JSON body"}, status=400)
        try:
            new = update_config(qty=body.get("qty"))
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        return web.json_response(new)
    return web.json_response({"qty": QTY, "max_position": MAX_POSITION})


async def start_http_server():
    app = web.Application()
    app.router.add_get("/", http_dashboard)
    app.router.add_get("/api/status", http_status)
    app.router.add_get("/api/fills", http_fills)
    app.router.add_get("/api/days", http_days)
    app.router.add_get("/api/book", http_book)
    app.router.add_get("/api/tape", http_tape)
    app.router.add_get("/api/candles", http_candles)
    app.router.add_get("/api/config", http_config)
    app.router.add_post("/api/config", http_config)
    app.router.add_post("/api/reset", http_reset)
    app.router.add_get("/api/strategy", http_strategy)
    app.router.add_post("/api/strategy", http_strategy)
    app.router.add_post("/api/clear-saved", http_clear_saved)
    runner = web.AppRunner(app)
    await runner.setup()
    # Try the user's requested port first; fall back if privileged.
    for port in (HTTP_PORT_PRIMARY, HTTP_PORT_FALLBACK):
        try:
            site = web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            log(f"dashboard listening on http://localhost:{port}")
            return runner, port
        except PermissionError:
            log(f"port {port} requires elevated privileges; trying fallback")
        except OSError as e:
            log(f"port {port} not bindable ({e}); trying fallback")
    log("could not bind any HTTP port; running headless")
    return runner, None


# ────────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ────────────────────────────────────────────────────────────────────────────────
def install_shutdown_handlers(loop):
    def _handle(sig):
        global shutdown_requested
        log(f"signal {sig.name} received — flushing session and exiting")
        shutdown_requested = True
        persist_session()
    for sig in (signal_mod.SIGINT, signal_mod.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle, sig)
        except NotImplementedError:
            pass  # Windows


async def main():
    log("=" * 70)
    log(f"start  symbol={SYMBOL} market={MARKET_TYPE} qty={QTY} "
        f"taker={TAKER_FEE_PCT}% latency={LATENCY_MS}ms data={DATA_DIR}")
    log("=" * 70)
    load_runtime_config()
    load_session()
    install_shutdown_handlers(asyncio.get_running_loop())
    http_runner, _ = await start_http_server()
    try:
        if DATA_SOURCE.startswith("replay:"):
            replay_path = DATA_SOURCE[len("replay:"):]
            log(f"DATA_SOURCE=replay path={replay_path} speed={REPLAY_SPEED}x")
            await asyncio.gather(
                replay_loop(replay_path),
                strategy_loop(),
                status_loop(),
            )
        else:
            log("DATA_SOURCE=live (Binance WebSocket)")
            await asyncio.gather(
                depth_ws_loop(),
                trade_ws_loop(),
                strategy_loop(),
                status_loop(),
            )
    finally:
        if http_runner is not None:
            await http_runner.cleanup()
        persist_session()
        log("session flushed; goodbye")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        persist_session()
        log("KeyboardInterrupt — session flushed")
        sys.exit(0)
