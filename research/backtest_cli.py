#!/usr/bin/env python3
"""
backtest_cli.py — Headless terminal backtester for OrderFlowPredictor on NQ.

Replays a NinjaTrader BfsL2Exporter JSONL (the same file backtest_server.py
serves) and runs the *exact* OrderFlowPredictor strategy logic against it, in
MARKET-ORDER mode, with CME-futures ($/contract) fees. Prints a comprehensive
performance report.

Fidelity
--------
This is a 1:1 Python port of:
  - orderflow_predictor_strategy.js  (signal, OFI, trade pressure, vol guard,
    maybeExit, cooldown/hold/TTL gating, position-cap + no-pyramiding logic)
  - terminal.html / backtest.html fill engine for MARKET orders:
      placeMarketOrder -> after latencyMs, marketFillPrice() walks up to 20
      levels of the opposite side and fills at the VWAP; updatePosition()
      does the avg-entry / realized-PnL bookkeeping.
  - The production runtime overrides the HTML passes to .start():
      { maxVolBps: 250, volGuardSpreadMultiplier: 8, executionMode: 'market' }

Clock
-----
The live strategy keys everything off Date.now(). A backtest needs a
deterministic clock, so "now" here is the MARKET timestamp of the event stream
(ms epoch). Trade-recency, cooldowns, TTLs and entry-age all measure against
that same market clock, so they behave exactly as live with zero wall-clock
skew. Market-order latency is modeled in market time: an order placed at t
fills against the book as it exists at t + latencyMs.

PnL units
---------
The HTML tracks realized PnL in raw index points but charges fees in dollars.
For NQ that under-counts dollar PnL by the contract multiplier ($20/point).
This tool dollarizes PnL with --point-value (default 20 for E-mini NQ) so net
PnL and fees share units. This does not change any strategy decision (the
strategy reasons in bps), only the reported PnL.

Usage
-----
  python3 backtest_cli.py /path/to/bfs_l2_export.jsonl
  python3 backtest_cli.py bfs_l2_export.jsonl --all-hours
  python3 backtest_cli.py bfs_l2_export.jsonl --qty 2 --latency 150 --fee 1.29
  python3 backtest_cli.py bfs_l2_export.jsonl --start 2026-05-07 --end 2026-05-08

Tested on Python 3.9+. Optional dependency: tqdm (for the progress bar).
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    ET = timezone.utc

try:
    from tqdm import tqdm
    _HAVE_TQDM = True
except ImportError:
    _HAVE_TQDM = False

# RTH (US equity index regular trading hours): 9:30am – 4:00pm ET, Mon–Fri.
RTH_START_MIN = 9 * 60 + 30
RTH_END_MIN = 16 * 60

QUEUE_EPS = 1e-10


def is_rth_us(ts_ms: int) -> bool:
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=ET)
    except (ValueError, OSError):
        return False
    if dt.weekday() >= 5:
        return False
    minutes = dt.hour * 60 + dt.minute
    return RTH_START_MIN <= minutes < RTH_END_MIN


def derive_price_dec(tick_size: float) -> int:
    if tick_size <= 0:
        return 2
    s = format(tick_size, ".10f").rstrip("0").rstrip(".")
    return len(s.split(".", 1)[1]) if "." in s else 0


# ── Fast field extraction (avoid full json.loads on skipped non-RTH lines) ───
_WS = b" \t"


def _fast_ts(raw: bytes) -> Optional[int]:
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
    v = int(raw[start:j])
    return -v if neg else v


def _fast_type(raw: bytes) -> Optional[bytes]:
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


# ── Strategy config — DEFAULTS from the .js, with production .start() overrides ──
def make_config(qty: float) -> dict:
    return {
        "qty": qty,
        "fallbackQty": qty,
        "maxPosition": 0.006,
        "maxPositionMultiplier": 6,
        "quoteEveryMs": 350,
        "orderTtlMs": 900,
        "exitTtlMs": 4500,
        "levels": 5,
        "signalThreshold": 0.38,
        "exitThreshold": 0.12,
        "minSpreadBps": 0,
        "maxSpreadBps": 120,
        "maxVolBps": 250,                 # production override (file default 350)
        "volGuardSpreadMultiplier": 8,    # production override (file default 10)
        "minTradeSamples": 4,
        "historyMax": 120,
        "minHoldMs": 700,
        "actionCooldownMs": 1000,
        "allowPyramiding": False,
        "takeProfitBps": 4,
        "stopLossBps": 6,
        "executionMode": "market",
    }


class Backtester:
    def __init__(self, cfg: dict, price_dec: int, latency_ms: float,
                 fee_per_contract: float, point_value: float, symbol: str,
                 tick: float, max_slip_ticks: float,
                 sl_ticks: float, tp_ticks: float) -> None:
        self.cfg = cfg
        self.price_dec = price_dec
        self.latency_ms = latency_ms
        self.fee = fee_per_contract
        self.mult = point_value
        self.symbol = symbol
        self.tick_sz = tick                   # instrument tick size (0.25 for NQ)
        self.max_slip_ticks = max_slip_ticks  # protection band on market fills
        self.sl_ticks = sl_ticks              # tick-based hard stop (0 = off)
        self.tp_ticks = tp_ticks              # tick-based take profit (0 = off)

        # Market state
        self.now = 0                       # current market clock (ms)
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_price: Optional[float] = None

        # Strategy-local state (mirror of orderflow_predictor_strategy.js `local`)
        self.trades: List[dict] = []
        self.mids: List[dict] = []
        self.last_book: Optional[dict] = None
        self.entry_ts = 0
        self.last_action_ts = 0

        # Position (mirror of state.positions[symbol])
        self.net_qty = 0.0
        self.avg_entry = 0.0

        # Pending market orders awaiting latency: list of dicts
        self.pending: List[dict] = []
        self._oid = 0

        # ── Accounting (kept in separate units, dollarized for reporting) ──
        self.realized_points = 0.0         # sum of pnl points across all closes
        self.fees_dollars = 0.0
        self.fills = 0
        self.contracts_traded = 0.0
        # Per-round-trip cycle accumulators (flat -> flat)
        self._cyc_points = 0.0
        self._cyc_fees = 0.0
        self._cyc_entry_ts = 0
        self._cyc_open = False
        self.trades_log: List[dict] = []   # finalized round-trips
        # Equity curve (cumulative net $ after each finalized round-trip)
        self.equity = 0.0
        self.equity_peak = 0.0
        self.max_drawdown = 0.0
        # Tick diagnostics
        self.tick_count = 0
        self.skip_reasons: Dict[str, int] = defaultdict(int)
        self.exit_reasons: Dict[str, int] = defaultdict(int)
        self.entries = 0
        # Fill realism diagnostics
        self.slip_dollars = 0.0            # total $ paid to slippage beyond touch
        self.band_capped = 0               # fills that hit the protection band
        self.unfilled_qty = 0.0            # qty dropped because book was too thin

    # ── Book helpers (mirror terminal.html) ──────────────────────────────
    def best_bid(self) -> Optional[float]:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return min(self.asks) if self.asks else None

    def top_bids(self, n: int) -> List[List[float]]:
        ks = sorted(self.bids, reverse=True)[:n]
        return [[p, self.bids[p]] for p in ks]

    def top_asks(self, n: int) -> List[List[float]]:
        ks = sorted(self.asks)[:n]
        return [[p, self.asks[p]] for p in ks]

    def tick_size(self) -> float:
        return math.pow(10, -self.price_dec)

    def position_qty(self) -> float:
        return self.net_qty

    # ── Trade tape ───────────────────────────────────────────────────────
    def capture_trade(self, price: float, qty: float, side: str, ts: int) -> None:
        self.trades.append({"price": price, "qty": qty, "side": side, "ts": ts})
        while len(self.trades) > self.cfg["historyMax"]:
            self.trades.pop(0)

    # ── Signal pipeline (1:1 with the .js) ───────────────────────────────
    def book_features(self) -> Optional[dict]:
        bb = self.best_bid()
        ba = self.best_ask()
        if not bb or not ba or ba <= bb:
            return None
        levels = self.cfg["levels"]
        bids = self.top_bids(levels)
        asks = self.top_asks(levels)
        best_bid_qty = bids[0][1] if bids else 0
        best_ask_qty = asks[0][1] if asks else 0
        mid = (bb + ba) / 2
        spread_bps = (ba - bb) / mid * 10000
        bid_depth = sum(q for _, q in bids)
        ask_depth = sum(q for _, q in asks)
        depth_imb = (bid_depth - ask_depth) / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0
        queue_imb = (best_bid_qty - best_ask_qty) / (best_bid_qty + best_ask_qty) if (best_bid_qty + best_ask_qty) > 0 else 0
        micro = ((ba * best_bid_qty + bb * best_ask_qty) / (best_bid_qty + best_ask_qty)
                 if (best_bid_qty + best_ask_qty) > 0 else mid)
        micro_bps = (micro - mid) / mid * 10000
        return {"bb": bb, "ba": ba, "mid": mid, "spreadBps": spread_bps,
                "bids": bids, "asks": asks, "depthImb": depth_imb,
                "queueImb": queue_imb, "microBps": micro_bps}

    def update_vol(self, mid: float) -> float:
        now = self.now
        last = self.mids[-1] if self.mids else None
        if not last or abs(mid - last["mid"]) > self.tick_size() / 2:
            self.mids.append({"mid": mid, "ts": now})
        while len(self.mids) > self.cfg["historyMax"]:
            self.mids.pop(0)
        if len(self.mids) < 8:
            return 0.0
        returns = [math.log(self.mids[i]["mid"] / self.mids[i - 1]["mid"])
                   for i in range(1, len(self.mids))]
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
        return math.sqrt(variance) * 10000

    def order_flow_imbalance(self, book: dict) -> float:
        if not self.last_book:
            self.last_book = book
            return 0.0
        ofi = 0.0
        for i in range(self.cfg["levels"]):
            prev_bid = self.last_book["bids"][i] if i < len(self.last_book["bids"]) else None
            prev_ask = self.last_book["asks"][i] if i < len(self.last_book["asks"]) else None
            bid = book["bids"][i] if i < len(book["bids"]) else None
            ask = book["asks"][i] if i < len(book["asks"]) else None
            weight = 1.0 / (i + 1)
            if prev_bid and bid:
                if bid[0] > prev_bid[0]:
                    ofi += weight * bid[1]
                elif bid[0] < prev_bid[0]:
                    ofi -= weight * prev_bid[1]
                else:
                    ofi += weight * (bid[1] - prev_bid[1])
            if prev_ask and ask:
                if ask[0] < prev_ask[0]:
                    ofi -= weight * ask[1]
                elif ask[0] > prev_ask[0]:
                    ofi += weight * prev_ask[1]
                else:
                    ofi -= weight * (ask[1] - prev_ask[1])
        scale = sum(q for _, q in book["bids"]) + sum(q for _, q in book["asks"])
        self.last_book = book
        return max(-1.0, min(1.0, ofi / scale)) if scale > 0 else 0.0

    def trade_pressure(self) -> float:
        now = self.now
        recent = [t for t in self.trades if now - t["ts"] < 2500]
        if len(recent) < self.cfg["minTradeSamples"]:
            return 0.0
        signed = sum((t["qty"] if t["side"] == "BUY" else -t["qty"]) for t in recent)
        total = sum(t["qty"] for t in recent)
        return max(-1.0, min(1.0, signed / total)) if total > 0 else 0.0

    def signal(self) -> dict:
        book = self.book_features()
        if not book:
            return {"skip": True, "reason": "waiting for book"}
        vol_bps = self.update_vol(book["mid"])
        vol_limit = max(self.cfg["maxVolBps"], book["spreadBps"] * self.cfg["volGuardSpreadMultiplier"])
        if book["spreadBps"] < self.cfg["minSpreadBps"]:
            return {"skip": True, "reason": "spread too tight"}
        if book["spreadBps"] > self.cfg["maxSpreadBps"]:
            return {"skip": True, "reason": "spread too wide"}
        if vol_bps > vol_limit:
            return {"skip": True, "reason": "volatility guard"}
        ofi = self.order_flow_imbalance(book)
        tape = self.trade_pressure()
        raw = (0.34 * book["depthImb"] +
               0.22 * book["queueImb"] +
               0.26 * ofi +
               0.18 * tape +
               0.04 * max(-1.0, min(1.0, book["microBps"] / max(1.0, book["spreadBps"]))))
        score = max(-1.0, min(1.0, raw))
        return {"skip": False, "score": score, "side": "BUY" if score > 0 else "SELL", "book": book}

    # ── Order placement / fills (market mode only) ───────────────────────
    def _new_oid(self) -> int:
        self._oid += 1
        return self._oid

    def cancel_owned(self) -> None:
        # Mirrors cancelOwned() in market mode: strategy-owned OPEN orders are
        # cancelled, so a pending market order whose latency hasn't elapsed is
        # dropped before it can fill. (With latency < quote interval this is
        # rarely non-empty, but kept for 1:1 fidelity.)
        self.pending = []

    def place_market_order(self, side: str, qty: float) -> None:
        self.pending.append({"id": self._new_oid(), "side": side,
                             "qty": float(qty), "fill_ts": self.now + self.latency_ms})

    def market_fill_price(self, side: str, qty: float) -> Optional[Tuple[float, float, float, bool]]:
        """Walk the opposite book for a market order, but bound how far it can
        sweep with a protection band (max_slip_ticks from the touch). This is
        the realistic 1-lot model: you take the touch, and a thin/gapped book
        cannot drag a fill arbitrarily deep (mirrors a CME protection band).
        Returns (vwap, filled_qty, touch_price, hit_band) or None if no book."""
        levels = self.top_asks(20) if side == "BUY" else self.top_bids(20)
        if not levels:
            return None
        touch = levels[0][0]
        band = self.max_slip_ticks * self.tick_sz
        worst = touch + band if side == "BUY" else touch - band
        remaining = qty
        notional = 0.0
        filled = 0.0
        hit_band = False
        for price, level_qty in levels:
            if remaining <= QUEUE_EPS:
                break
            if (side == "BUY" and price > worst + 1e-9) or (side == "SELL" and price < worst - 1e-9):
                hit_band = True       # next level is beyond the band — stop sweeping
                break
            take = min(remaining, level_qty)
            notional += price * take
            filled += take
            remaining -= take
        if filled <= QUEUE_EPS:
            return None
        if remaining > QUEUE_EPS:
            hit_band = True
        return notional / filled, filled, touch, hit_band

    def execute_pending_fill(self, order: dict) -> None:
        fill = self.market_fill_price(order["side"], order["qty"])
        if fill is None:
            return
        price, qty, touch, hit_band = fill
        # Slippage = how far the VWAP landed past the touch (always >= 0).
        slip = (price - touch) if order["side"] == "BUY" else (touch - price)
        if slip > 1e-9:
            self.slip_dollars += slip * qty * self.mult
        if hit_band:
            self.band_capped += 1
            self.unfilled_qty += max(0.0, order["qty"] - qty)
        self.update_position(order["side"], price, qty)

    def update_position(self, side: str, fill_price: float, fill_qty: float) -> None:
        """Mirror of updatePosition() in backtest.html, but PnL is accumulated
        in points (dollarized at report time) and fees are tracked in $."""
        is_buy = side == "BUY"
        fee = abs(fill_qty) * self.fee                      # $/contract (taker = MARKET)
        self.fees_dollars += fee
        self.fills += 1
        self.contracts_traded += abs(fill_qty)
        if not self._cyc_open:
            self._cyc_open = True
            self._cyc_points = 0.0
            self._cyc_fees = 0.0
            self._cyc_entry_ts = self.now
        self._cyc_fees += fee

        if self.net_qty == 0:
            self.avg_entry = fill_price
            self.net_qty = fill_qty if is_buy else -fill_qty
        elif (is_buy and self.net_qty > 0) or (not is_buy and self.net_qty < 0):
            total_qty = abs(self.net_qty) + fill_qty
            self.avg_entry = (abs(self.net_qty) * self.avg_entry + fill_qty * fill_price) / total_qty
            self.net_qty = total_qty if is_buy else -total_qty
        else:
            close_qty = min(fill_qty, abs(self.net_qty))
            pnl_per_unit = (self.avg_entry - fill_price) if is_buy else (fill_price - self.avg_entry)
            realized = pnl_per_unit * close_qty
            self.realized_points += realized
            self._cyc_points += realized
            remaining = fill_qty - close_qty
            self.net_qty = self.net_qty + fill_qty if is_buy else self.net_qty - fill_qty
            if abs(self.net_qty) < 1e-10:
                self.net_qty = 0.0
                self.avg_entry = 0.0
            elif remaining > 0:
                self.avg_entry = fill_price

        if self.net_qty == 0 and self._cyc_open:
            self._finalize_cycle()

    def _finalize_cycle(self) -> None:
        net_dollars = self._cyc_points * self.mult - self._cyc_fees
        self.trades_log.append({
            "net": net_dollars,
            "points": self._cyc_points,
            "fees": self._cyc_fees,
            "hold_ms": self.now - self._cyc_entry_ts,
            "ts": self.now,
        })
        self.equity += net_dollars
        self.equity_peak = max(self.equity_peak, self.equity)
        self.max_drawdown = max(self.max_drawdown, self.equity_peak - self.equity)
        self._cyc_open = False

    # ── maybeExit (1:1 with the .js) ─────────────────────────────────────
    def maybe_exit(self, book: dict, pos: float, score: float) -> bool:
        if pos == 0:
            return False
        age = self.now - self.entry_ts
        avg_entry = self.avg_entry if self.avg_entry else book["mid"]
        pnl_bps = ((book["mid"] - avg_entry) / avg_entry * 10000 if pos > 0
                   else (avg_entry - book["mid"]) / avg_entry * 10000)
        # NQ adaptation: tick-based TP/SL that actually fire. The .js uses bps
        # (takeProfitBps=4, stopLossBps=6) which on NQ at ~28k translate to
        # ~11/17 points and never trigger, so every trade exits on the 4.5s
        # timeout and adverse moves run unbounded. A hard tick stop caps the
        # per-trade loss. Set --sl-ticks/--tp-ticks 0 to fall back to bps only.
        pnl_ticks = ((book["mid"] - avg_entry) if pos > 0 else (avg_entry - book["mid"])) / self.tick_sz
        signal_flipped = ((pos > 0 and score < -self.cfg["exitThreshold"]) or
                          (pos < 0 and score > self.cfg["exitThreshold"]))
        timed_out = age > self.cfg["exitTtlMs"]
        take_profit = pnl_bps >= self.cfg["takeProfitBps"] or (self.tp_ticks > 0 and pnl_ticks >= self.tp_ticks)
        stop_loss = pnl_bps <= -self.cfg["stopLossBps"] or (self.sl_ticks > 0 and pnl_ticks <= -self.sl_ticks)
        if signal_flipped and age < self.cfg["minHoldMs"] and not stop_loss:
            return False
        if not signal_flipped and not timed_out and not take_profit and not stop_loss:
            return False
        self.exit_reasons["stop_loss" if stop_loss else "take_profit" if take_profit
                          else "signal_flip" if signal_flipped else "timeout"] += 1
        self.cancel_owned()
        side = "SELL" if pos > 0 else "BUY"
        self.place_market_order(side, abs(pos))
        self.last_action_ts = self.now
        return True

    # ── tick (1:1 with the .js) ──────────────────────────────────────────
    def tick(self) -> None:
        self.tick_count += 1
        s = self.signal()
        if s["skip"]:
            self.skip_reasons[s["reason"]] += 1
            self.cancel_owned()
            return
        qty = self.cfg["qty"]
        if not (qty and qty > 0):
            self.cancel_owned()
            return
        max_position = max(self.cfg["maxPosition"], qty * self.cfg["maxPositionMultiplier"])
        pos = self.position_qty()
        if self.maybe_exit(s["book"], pos, s["score"]):
            return
        if self.now - self.last_action_ts < self.cfg["actionCooldownMs"]:
            self.skip_reasons["cooldown"] += 1
            return
        if abs(s["score"]) < self.cfg["signalThreshold"]:
            self.skip_reasons["neutral"] += 1
            self.cancel_owned()
            return
        wants_buy = s["score"] > 0
        same_dir = (wants_buy and pos > 0) or (not wants_buy and pos < 0)
        has_entry_order = any(p["side"] == ("BUY" if wants_buy else "SELL") for p in self.pending)
        if not self.cfg["allowPyramiding"] and (same_dir or has_entry_order):
            self.skip_reasons["holding"] += 1
        elif wants_buy and pos + qty <= max_position:
            self.cancel_owned()
            self.place_market_order("BUY", qty)
            self.last_action_ts = self.now
            if pos <= 0:
                self.entry_ts = self.now
            self.entries += 1
        elif not wants_buy and pos - qty >= -max_position:
            self.cancel_owned()
            self.place_market_order("SELL", qty)
            self.last_action_ts = self.now
            if pos >= 0:
                self.entry_ts = self.now
            self.entries += 1
        else:
            self.skip_reasons["position cap"] += 1
            self.cancel_owned()

    # ── time advancement: run due fills + ticks up to `until` (exclusive) ──
    def advance_to(self, until: int, next_tick: int) -> int:
        """Execute pending market fills and 350ms strategy ticks scheduled
        strictly before `until`, in time order, against the current (constant
        between events) book. Returns the updated next_tick."""
        while True:
            # earliest pending fill due before `until`
            fill_due = None
            for p in self.pending:
                if p["fill_ts"] < until and (fill_due is None or p["fill_ts"] < fill_due["fill_ts"]):
                    fill_due = p
            tick_due = next_tick if next_tick < until else None
            if fill_due is None and tick_due is None:
                return next_tick
            # whichever comes first in market time
            if tick_due is not None and (fill_due is None or tick_due <= fill_due["fill_ts"]):
                self.now = tick_due
                self.tick()
                next_tick = tick_due + self.cfg["quoteEveryMs"]
            else:
                self.now = fill_due["fill_ts"]
                self.pending.remove(fill_due)
                self.execute_pending_fill(fill_due)

    def force_flatten(self) -> None:
        """Close any residual position at the last available price (taker fee)."""
        if self.net_qty == 0:
            return
        side = "SELL" if self.net_qty > 0 else "BUY"
        fill = self.market_fill_price(side, abs(self.net_qty))
        if fill is not None:
            price, qty, _, _ = fill
        elif self.last_price is not None:
            price, qty = self.last_price, abs(self.net_qty)
        else:
            return
        self.update_position(side, price, qty)

    def end_session_reset(self) -> None:
        """Called at each new RTH day. Flatten any residual position at the
        last price (you don't hold NQ overnight), then clear book + per-session
        strategy state so the next session starts fresh (the live strategy is
        effectively restarted each RTH day)."""
        if self.net_qty != 0 and self.last_price is not None:
            side = "SELL" if self.net_qty > 0 else "BUY"
            self.update_position(side, self.last_price, abs(self.net_qty))
        self.bids.clear()
        self.asks.clear()
        self.last_book = None
        self.mids = []
        self.trades = []
        self.pending = []


def run(args) -> int:
    path = Path(args.file).expanduser().resolve()
    if not path.exists():
        print(f"[backtest_cli] file not found: {path}")
        return 1
    file_size = path.stat().st_size

    # Pull instrument/tick from the first meta line (lightweight).
    instrument, tick_size = "NQ", 0.25
    with path.open("rb") as f:
        head = f.read(4096).lstrip(b"\xef\xbb\xbf")
        for line in head.split(b"\n"):
            if b'"type":"meta"' in line or b'"type": "meta"' in line:
                try:
                    m = json.loads(line)
                    instrument = m.get("instrument", instrument)
                    if isinstance(m.get("tickSize"), (int, float)) and m["tickSize"] > 0:
                        tick_size = float(m["tickSize"])
                    break
                except json.JSONDecodeError:
                    pass
    price_dec = derive_price_dec(tick_size)
    symbol = instrument.split()[0] if instrument else "NQ"

    cfg = make_config(args.qty)
    bt = Backtester(cfg, price_dec, args.latency, args.fee, args.point_value, symbol,
                    tick_size, args.max_slippage_ticks, args.sl_ticks, args.tp_ticks)

    rth_only = not args.all_hours
    # Numeric UTC ms bounds for fast date filtering (avoid a datetime per line).
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None        # exclusive
    if args.start:
        y, m, d = (int(x) for x in args.start.split("-"))
        start_ms = int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)
    if args.end:
        y, m, d = (int(x) for x in args.end.split("-"))
        end_ms = int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000) + 86_400_000

    mode = "RTH only (9:30-16:00 ET)" if rth_only else "all hours (24h Globex)"
    print(f"[backtest_cli] {instrument}  tick={tick_size}  pointValue=${args.point_value}/pt")
    print(f"[backtest_cli] mode={mode}  qty={args.qty}  latency={args.latency}ms  "
          f"fee=${args.fee}/contract/side  exec=market")
    print(f"[backtest_cli] strategy overrides: maxVolBps=250 volGuardSpreadMultiplier=8")
    sl_txt = f"{args.sl_ticks:g} ticks" if args.sl_ticks > 0 else "off (bps only)"
    tp_txt = f"{args.tp_ticks:g} ticks" if args.tp_ticks > 0 else "off (bps only)"
    print(f"[backtest_cli] stop-loss={sl_txt}  take-profit={tp_txt}  "
          f"slippage band={args.max_slippage_ticks:g} ticks")
    if rth_only:
        print(f"[backtest_cli] book warmup: {args.warmup_min:g} min before 9:30 ET open")
    print(f"[backtest_cli] streaming {file_size / 1e9:.1f} GB ...")

    next_tick = None
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    depth_events = 0
    trade_events = 0
    GAP_RESYNC = cfg["quoteEveryMs"] * 8   # collapse dead gaps (overnight, halts)
    warmup_ms = int(args.warmup_min * 60_000)

    # Current ET-day session bounds (recomputed once per ET day; numeric ms).
    cur_day_start = cur_day_end = None
    cur_rs = cur_re = cur_warm = 0
    cur_weekend = False

    def session_for(ts_ms):
        d = datetime.fromtimestamp(ts_ms / 1000.0, tz=ET)
        midnight = datetime(d.year, d.month, d.day, tzinfo=ET)
        rs = int(datetime(d.year, d.month, d.day, 9, 30, tzinfo=ET).timestamp() * 1000)
        re = int(datetime(d.year, d.month, d.day, 16, 0, tzinfo=ET).timestamp() * 1000)
        return (int(midnight.timestamp() * 1000),
                int((midnight + timedelta(days=1)).timestamp() * 1000),
                rs, re, midnight.weekday() >= 5)

    bar = None
    if _HAVE_TQDM:
        bar = tqdm(total=file_size, unit="B", unit_scale=True, desc="replay",
                   smoothing=0.05)
    t0 = time.monotonic()
    bytes_since = 0
    lines_since = 0

    with path.open("rb") as f:
        first3 = f.read(3)
        if first3 != b"\xef\xbb\xbf":
            f.seek(0)
        for raw in f:
            nbytes = len(raw)
            bytes_since += nbytes
            lines_since += 1
            if (bar is not None) and lines_since >= 50000:
                bar.update(bytes_since)
                bytes_since = 0
                lines_since = 0

            etype = _fast_type(raw)
            if etype not in (b"d", b"t"):
                continue
            ts = _fast_ts(raw)
            if ts is None:
                continue
            if start_ms is not None and ts < start_ms:
                continue
            if end_ms is not None and ts >= end_ms:
                break

            # Decide whether to MAINTAIN the book and whether to TICK (trade).
            # In RTH mode we warm the book for `warmup_min` before 9:30 so the
            # open starts on a fully populated book (no gapped/empty-book fills),
            # but only let the strategy trade inside 9:30-16:00 ET.
            if rth_only:
                if cur_day_end is None or ts >= cur_day_end or ts < cur_day_start:
                    cur_day_start, cur_day_end, cur_rs, cur_re, cur_weekend = session_for(ts)
                    cur_warm = cur_rs - warmup_ms
                    bt.end_session_reset()      # flatten + clear at each new ET day
                    next_tick = None
                if cur_weekend or ts < cur_warm or ts >= cur_re:
                    continue
                tick_on = ts >= cur_rs
            else:
                tick_on = True

            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # Strategy clock: run due fills + ticks (against the pre-event book)
            # only while trading is on. During warmup we just build the book.
            if tick_on:
                if first_ts is None:
                    first_ts = ts
                if next_tick is None:
                    next_tick = ts
                elif ts - next_tick > GAP_RESYNC:
                    next_tick = ts
                next_tick = bt.advance_to(ts, next_tick)
                last_ts = ts

            # Apply the event to the book / tape.
            if etype == b"d":
                depth_events += 1
                side = ev.get("side")
                op = ev.get("op")
                try:
                    px = float(ev["px"])
                    qty = float(ev["qty"])
                except (KeyError, TypeError, ValueError):
                    continue
                target = bt.bids if side == "BID" else bt.asks
                if op == "REM" or qty <= 0:
                    target.pop(px, None)
                else:
                    target[px] = qty
            else:
                trade_events += 1
                try:
                    px = float(ev["px"])
                    qty = float(ev["qty"])
                except (KeyError, TypeError, ValueError):
                    continue
                if px > 0 and qty > 0:
                    bt.last_price = px
                    bt.capture_trade(px, qty, ev.get("side", "BUY"), ts)

    if bar is not None:
        bar.update(bytes_since)
        bar.close()

    # Flush any remaining due fills/ticks, then flatten residual position.
    if first_ts is not None and last_ts is not None:
        bt.now = last_ts
        # drain pending fills
        for p in sorted(bt.pending, key=lambda x: x["fill_ts"]):
            bt.now = max(bt.now, p["fill_ts"])
            bt.execute_pending_fill(p)
        bt.pending = []
        bt.force_flatten()

    elapsed = time.monotonic() - t0
    print(f"[backtest_cli] done in {elapsed:.1f}s  "
          f"({depth_events:,} depth + {trade_events:,} trade events)")
    report(bt, first_ts, last_ts, mode, args)
    return 0


def _fmt_money(v: float) -> str:
    return ("+" if v >= 0 else "-") + "$" + f"{abs(v):,.2f}"


def report(bt: Backtester, first_ts: Optional[int], last_ts: Optional[int],
           mode: str, args) -> None:
    trades = bt.trades_log
    n = len(trades)
    wins = [t for t in trades if t["net"] > 0]
    losses = [t for t in trades if t["net"] < 0]
    gross_win = sum(t["net"] for t in wins)
    gross_loss = -sum(t["net"] for t in losses)
    net = sum(t["net"] for t in trades)
    gross_points = sum(t["points"] for t in trades)
    avg_hold = (sum(t["hold_ms"] for t in trades) / n) if n else 0

    def line(label, val):
        print(f"  {label:<26}{val}")

    print()
    print("=" * 58)
    print("  ORDER-FLOW PREDICTOR - NQ BACKTEST REPORT")
    print("=" * 58)
    if first_ts is not None:
        fs = datetime.fromtimestamp(first_ts / 1000.0, tz=ET).strftime("%Y-%m-%d %H:%M ET")
        ls = datetime.fromtimestamp(last_ts / 1000.0, tz=ET).strftime("%Y-%m-%d %H:%M ET")
        line("Period", f"{fs}  ->  {ls}")
    line("Session filter", mode)
    line("Contract / qty", f"{bt.symbol}  x{args.qty}  (${args.point_value}/pt)")
    print("-" * 58)
    line("Strategy ticks", f"{bt.tick_count:,}")
    line("Entries taken", f"{bt.entries:,}")
    line("Fills executed", f"{bt.fills:,}")
    line("Contracts traded", f"{bt.contracts_traded:,.0f}")
    line("Round-trip trades", f"{n:,}")
    print("-" * 58)
    if n:
        line("Win rate", f"{len(wins)/n*100:.1f}%  ({len(wins)}W / {len(losses)}L)")
        line("Gross PnL (points)", f"{gross_points:+,.2f} pts")
        line("Gross PnL", _fmt_money(gross_points * bt.mult))
        line("Total fees", _fmt_money(-bt.fees_dollars))
        line("NET PnL", _fmt_money(net))
        print("-" * 58)
        line("Avg trade", _fmt_money(net / n))
        line("Avg win", _fmt_money(gross_win / len(wins)) if wins else "n/a")
        line("Avg loss", _fmt_money(-gross_loss / len(losses)) if losses else "n/a")
        line("Largest win", _fmt_money(max(t["net"] for t in trades)))
        line("Largest loss", _fmt_money(min(t["net"] for t in trades)))
        pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
        line("Profit factor", f"{pf:.2f}" if gross_loss > 0 else "inf (no losses)")
        line("Expectancy/trade", _fmt_money(net / n))
        line("Max drawdown", _fmt_money(-bt.max_drawdown))
        line("Avg hold time", f"{avg_hold/1000:.1f}s")
        # Per-trade Sharpe (not annualized) — sanity stat on consistency.
        nets = [t["net"] for t in trades]
        mean = net / n
        sd = math.sqrt(sum((x - mean) ** 2 for x in nets) / n) if n > 1 else 0
        line("Per-trade Sharpe", f"{mean/sd:.3f}" if sd > 0 else "n/a")
    else:
        line("Result", "no round-trip trades taken")

    # Per-day breakdown
    if n:
        by_day: Dict[str, dict] = defaultdict(lambda: {"n": 0, "net": 0.0, "fees": 0.0})
        for t in trades:
            d = datetime.fromtimestamp(t["ts"] / 1000.0, tz=ET).strftime("%Y-%m-%d")
            by_day[d]["n"] += 1
            by_day[d]["net"] += t["net"]
            by_day[d]["fees"] += t["fees"]
        print("-" * 58)
        print(f"  {'Date (ET)':<14}{'Trades':>8}{'Net PnL':>16}{'Fees':>14}")
        for d in sorted(by_day):
            r = by_day[d]
            print(f"  {d:<14}{r['n']:>8}{_fmt_money(r['net']):>16}{_fmt_money(-r['fees']):>14}")
    print("=" * 58)
    # Fill realism: how much went to slippage and how often the protection band
    # bound the fill (the "sweep into a thin book" cases the band now prevents).
    line("Slippage paid (total)", _fmt_money(-bt.slip_dollars))
    if bt.fills:
        line("Avg slippage / fill", _fmt_money(-bt.slip_dollars / bt.fills))
    line("Band-capped fills", f"{bt.band_capped:,}"
         + (f"  ({bt.unfilled_qty:,.0f} contracts left unfilled)" if bt.unfilled_qty else ""))
    if bt.exit_reasons:
        ex = sorted(bt.exit_reasons.items(), key=lambda kv: -kv[1])
        print("  exit reasons:      " + ", ".join(f"{k}={v:,}" for k, v in ex))
    if bt.skip_reasons:
        top = sorted(bt.skip_reasons.items(), key=lambda kv: -kv[1])[:6]
        print("  tick skip reasons: " + ", ".join(f"{k}={v:,}" for k, v in top))
    print()


def main() -> int:
    ap = argparse.ArgumentParser(description="Headless OrderFlowPredictor NQ backtester (market orders, CME fees).")
    ap.add_argument("file", nargs="?", default="bfs_l2_export.jsonl",
                    help="NT BfsL2Exporter JSONL (default: %(default)s)")
    ap.add_argument("--qty", type=float, default=1.0, help="contracts per entry (default 1)")
    ap.add_argument("--latency", type=float, default=100.0, help="market-order latency ms (default 100)")
    ap.add_argument("--fee", type=float, default=1.29, help="$/contract per side (default 1.29)")
    ap.add_argument("--point-value", type=float, default=20.0, dest="point_value",
                    help="$ per index point per contract (NQ=20, MNQ=2; default 20)")
    ap.add_argument("--all-hours", action="store_true",
                    help="trade 24h Globex instead of RTH only")
    ap.add_argument("--sl-ticks", type=float, default=8.0, dest="sl_ticks",
                    help="hard stop-loss in ticks (0 = off, use bps only; default 8)")
    ap.add_argument("--tp-ticks", type=float, default=4.0, dest="tp_ticks",
                    help="take-profit in ticks (0 = off, use bps only; default 4)")
    ap.add_argument("--max-slippage-ticks", type=float, default=8.0, dest="max_slippage_ticks",
                    help="protection band: market fills cannot sweep more than this "
                         "many ticks past the touch (default 8)")
    ap.add_argument("--warmup-min", type=float, default=30.0, dest="warmup_min",
                    help="minutes of pre-open depth to warm the book before 9:30 ET (default 30)")
    ap.add_argument("--start", type=str, default=None, help="UTC start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", type=str, default=None, help="UTC end date YYYY-MM-DD (inclusive)")
    args = ap.parse_args()
    if args.qty <= 0:
        print("[backtest_cli] --qty must be > 0")
        return 1
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
