"""
The trading core. Same fill logic as terminal.html, but headless: it reads
Binance directly and writes fills straight to disk, no browser involved.

  - connects to Binance (depth + aggTrade) for one symbol
  - keeps a local order book (snapshot + diffs)
  - runs the OrderFlowPredictor strategy in market-order mode
  - paper-fills market orders at the live top-of-book VWAP
  - appends every fill to disk right away (jsonl + csv) and snapshots session
    state after each one, so a restart resumes where it left off

Output under data/paper_data/:
    trades_YYYY-MM-DD.jsonl   one fill per line
    trades_YYYY-MM-DD.csv     same, with a header
    session_state.json        positions / pnl for restart
    runner.log                log mirror
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

# config, mirrors the terminal.html / OFP defaults. edit here, no UI needed.
SYMBOL = "BTCUSDT"
MARKET_TYPE = "futures"          # "futures" (USD-M perp) or "spot"
QTY = 1.0                        # contract size, editable live from the dashboard

# data source
# 'live'              -> Binance WebSocket (current behavior).
# 'replay:<path>'     -> JSONL file produced by BfsL2Exporter.cs in NinjaTrader.
DATA_SOURCE = "live"
REPLAY_SPEED = 5.0               # 5x real-time during replay; raise for faster runs
REPLAY_LOOP  = False             # restart replay from start after EOF

# contract model
# 'crypto'  -> fee = price x qty x fee_pct / 100 (Binance-style % of notional)
# 'futures' -> fee = qty x FIXED_FEE_PER_CONTRACT (CME-style flat $ per side)
CONTRACT_MODEL = "crypto"
FIXED_FEE_PER_CONTRACT = 1.29    # for CONTRACT_MODEL == 'futures' (NQ retail is ~$1.29/side)
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
# terminal.html's Start button overrides these two with stricter guards than the
# strategy file's defaults. mirror them so Python runs what the JS actually runs.
MAX_VOL_BPS = 250.0              # HTML runtime override (file default: 350)
VOL_GUARD_SPREAD_MULT = 8.0      # HTML runtime override (file default: 10)
MIN_TRADE_SAMPLES = 4
HISTORY_MAX = 120
MAX_POSITION = max(0.006, QTY * 6)
ALLOW_PYRAMIDING = False

# output goes to <repo>/data/paper_data/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = REPO_ROOT / "data" / "paper_data"
DATA_DIR.mkdir(exist_ok=True)
LOG_PATH = DATA_DIR / "runner.log"
SESSION_PATH = DATA_DIR / "session_state.json"
RUNTIME_CONFIG_PATH = DATA_DIR / "runtime_config.json"

STATUS_INTERVAL_S = 30           # console status print cadence
HTTP_PORT_PRIMARY = 1000         # user-requested port (privileged on macOS)
HTTP_PORT_FALLBACK = 8010        # used if 1000 is not bindable without root

# state
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


# order book sync (snapshot + diff stream)
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


# fill accounting (mirrors terminal.html updatePosition + executeFill)
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


# persistence: append-only jsonl + csv, written on every fill.
#  No prompts. No buffering. If the process crashes mid-flush, at most one
#  fill could be lost (and even then only the JSONL tail line, CSV is durable).
CSV_COLS = ["ts", "iso", "strategy", "symbol", "side", "reason", "feeType",
            "price", "qty", "feePercentAtFill", "feePaidAtFill",
            "executionMode", "orderId", "id"]


def day_key(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")


def persist_execution(rec: dict) -> None:
    day = day_key(rec["ts"])
    jsonl_path = DATA_DIR / f"trades_{day}.jsonl"
    csv_path = DATA_DIR / f"trades_{day}.csv"
    # JSONL append, one fill per line, no read-modify-write.
    with jsonl_path.open("a") as f:
        f.write(json.dumps(rec) + "\n")
    # CSV append, write header if file is new.
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


# runtime-tunable config (persists across restarts)
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


# market order sim (mirrors placeMarketOrder + marketFillPrice)
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
    """Mirror of JS `cancelOwned()` for market mode, cancel any market orders
    that haven't filled yet. A cancelled pending order will be reaped on the
    next pass of check_pending_market_fills without ever placing a fill."""
    for order in pending_market:
        order["cancelled"] = True


def has_pending_market_on_side(side: str) -> bool:
    """Mirror of JS `hasEntryOrder`, true if an unfilled, uncancelled market
    order on this side already exists."""
    return any(
        not o.get("cancelled") and o["side"] == side
        for o in pending_market
    )


# trade handler (also drives market-order fill timing)
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


# ofp strategy (port of orderflow_predictor_strategy.js, market mode)
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
      2. if skip -> cancel owned market orders, set reason, return
      3. read live position
      4. maybe_exit (may cancel + place market exit), if fired, return
      5. cooldown gate
      6. neutral (|score| < threshold) -> cancel owned, set reason, return
      7. pyramiding guard (sameDirectionPosition OR pending same-side order)
      8. entry: cancel any pending market order, then place new one
      9. position-cap branch -> cancel owned, set reason
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


# async loops
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
                # Fire-and-forget snapshot fetch, events buffer until it lands.
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
            # the strategy reads position, matches the JS where setTimeout
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


