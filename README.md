# Binance Fill Simulator

A limit-order **fill-simulation and strategy-research platform** for high-frequency / market-making experiments, built on live Level-2 market data. It reconstructs an exchange order book from a live feed, simulates **queue-aware** execution of resting limit orders, runs research-grounded strategies against that fill engine, and supports lookahead-bias-free replay of historical data — without placing a single real trade.

> ⚠️ **Paper trading only.** Every fill is simulated. Nothing here executes real orders, and none of it is financial advice or a profitable trading system. The strategies are research-backed experiments, not guarantees.

## The core problem: would my limit order actually have filled?

For an aggressive (market) order, simulated execution is easy — walk the book and consume liquidity. For a **passive (resting limit) order**, it's the hard problem in execution simulation, because a fill depends on your **queue position**, which the exchange never tells you. Binance (like most venues) broadcasts **market-by-price (MBP)** data: the *aggregate* size at each price level, not the individual orders or their order IDs. So you can see "there are 4.2 BTC bid at 67,000" but not whether your 0.1 BTC sits 1st or 400th in that queue.

The entire fill engine is built around **estimating queue position from MBP data** and updating that estimate as the book and trade streams evolve. This is the technically interesting part of the project, so it's documented in full below.

## Queue & fill model

Each resting order tracks, beyond the obvious fields:

| Field | Meaning |
|---|---|
| `queueAhead` | estimated aggregate quantity resting **ahead** of us at our price |
| `lastLevelQty` | last observed aggregate size at our price level (to detect deltas) |
| `pendingTradeDepletion` | trade volume seen at our price, waiting to be reconciled against a depth decrease |
| `pendingTradeDepletionTs` | timestamp for expiring stale pending-trade volume (1 s TTL) |
| `recentCancelAdvance` | queue advancement we *attributed to cancels*, held briefly in case a delayed trade explains it instead |
| `recentCancelAdvanceTs` | timestamp for the 250 ms delayed-trade correction window |

Tunable constants:

```js
QUEUE_EPS = 1e-10;                 // float-noise threshold
QUEUE_CANCEL_BEHIND_BIAS = 1.35;   // >1 ⇒ cancels biased to occur behind us
state.latencyMs = 100;             // simulated order round-trip latency
```

### 1. Queue initialisation (FIFO back-of-queue assumption)

When an order is placed at price `P`, `queueAhead` is initialised to the **current aggregate size at `P`**:

```
queueAhead = state.bids[P]   (for a BUY)   or   state.asks[P]   (for a SELL)
```

This encodes the assumption that we join at the **back** of the existing FIFO queue — the most pessimistic and most realistic default, since we cannot have priority over orders already resting there.

### 2. Passive trade fills

On every `aggTrade`, an order at price `P` is eligible to fill if the trade is an **opposing aggressor that reaches our price**:

- BUY limit at `P` fills against a **SELL** aggressor trading at `price ≤ P`
- SELL limit at `P` fills against a **BUY** aggressor trading at `price ≥ P`

Two cases:

- **Trade price strictly better than our limit** (e.g. a sell prints at 66,999 while our BUY rests at 67,000): the market has traded *through* us, so we fill immediately for `min(tradeQty, remainingQty)` with **no queue cost** — anything trading at a better price than ours must clear our level first.
- **Trade at exactly our price**: the trade first **burns down `queueAhead`** (orders ahead of us fill before we do). Only the **leftover** after the queue is exhausted fills our order:

```
burned   = min(queueAhead, tradeQty)
queueAhead -= burned
leftover = tradeQty - burned
fillQty  = min(leftover, remainingQty)   // only if leftover > 0
```

This is the FIFO priority rule made explicit: volume at your price serves the front of the queue first.

### 3. Queue advancement from cancellations (the probabilistic core)

The subtle part. When the aggregate size at our level **decreases**, that drop has two possible causes:

1. **Trades** at our price (already handled above), or
2. **Cancellations / size reductions** by other resting orders.

Only cancellations *ahead* of us advance our queue position; cancellations *behind* us don't help. But MBP data doesn't say which. The engine separates the two and then estimates the split:

**Step A — subtract trade-explained volume.** Trade volume seen at our price is accumulated in `pendingTradeDepletion`. When a depth decrease arrives, that portion is attributed to trades first (and not double-counted), with a 1 s TTL so stale trade volume expires:

```
levelDrop     = prevLevelQty - nextLevelQty
tradeExplained = min(levelDrop, pendingTradeDepletion)
cancelQty      = levelDrop - tradeExplained
```

**Step B — estimate how much of the cancelled size was ahead of us.** The remaining `cancelQty` is split using a size-weighted probability. With `front = queueAhead` and `behind = nextLevelQty − front`:

```
uniformProb = log1p(front) / ( log1p(front) + log1p(behind) )
aheadProb   = uniformProb ^ QUEUE_CANCEL_BEHIND_BIAS      // bias exponent = 1.35
aheadCancelled = min(queueAhead, cancelQty * aheadProb)
queueAhead -= aheadCancelled
```

The reasoning behind each piece:

- **`log1p` weighting, not linear.** A purely proportional split (`front / total`) overstates how often the cancel lands ahead of us when the queue is deep. Using `log1p(size)` compresses large sizes so the probability responds to *relative* depth without being dominated by one huge level. It also handles the `front = 0` and `behind = 0` edge cases cleanly (returns 0 and 1 respectively).
- **The behind-bias exponent (1.35 > 1).** Raising a probability in (0,1) to a power > 1 pushes it **down**, so cancels are made slightly *more* likely to occur behind us than a neutral model would predict. This is deliberately conservative: it prevents the simulator from unrealistically yanking our order to the front of the queue every time someone cancels, which would inflate fill rates and flatter the strategy.
- **Clamp.** `queueAhead` is finally clamped to `≤ nextLevelQty` — our position ahead can never exceed the total size now resting at the level.

### 4. Delayed-trade / depth-ordering correction

Depth and trade messages arrive on the same combined socket but **not in guaranteed order** — a depth decrease can land *before* the trade message that caused it. Without a guard, the engine would (a) advance the queue via the cancel model when the depth drop arrived, then (b) burn the queue *again* when the trade finally arrived — double-counting the same volume.

The fix is a **250 ms reconciliation window**:

- Whenever the cancel model advances the queue, that amount is recorded in `recentCancelAdvance` with a timestamp.
- When a trade at our price arrives within 250 ms, its quantity is first netted against `recentCancelAdvance` (`effectiveTradeQty -= alreadyAdvanced`) before being used to burn the queue.

So volume that already advanced us as a "cancel" can't also fill us as a "trade." This is the kind of cross-stream consistency bug that quietly inflates simulated fills, and it's handled explicitly.

### 5. Book-sweep fills

Separately from trade-driven fills, `handleBookUpdate()` fills a resting order if the **book itself moves through its price** and the order is already at the front (`queueAhead == 0`):

- BUY at `P` sweeps if `bestAsk < P`
- SELL at `P` sweeps if `bestBid > P`

A **100 ms grace period** (`Date.now() - placedAt < 200` guard, plus the latency model) excludes freshly-placed orders, so an order joined at the current best isn't swept before any real book movement occurs.

### 6. Market orders & latency

Market orders are filled by walking up to 20 levels of the opposite side and computing a **size-weighted VWAP**, after a simulated `latencyMs` round-trip delay — modelling the reality that your order reaches the matching engine some milliseconds after you press the button.

## Order book maintenance

The book is kept correct via the standard Binance snapshot+diff protocol, with two correctness details worth noting:

- **Float-keyed levels.** `state.bids` / `state.asks` are keyed by `parseFloat(price)`, never the raw exchange string, because `"67000.10"` and `Number("67000.10").toString() → "67000.1"` would otherwise miss each other and silently corrupt the book.
- **Snapshot/diff sequencing.** Depth diffs that arrive before the REST snapshot are buffered and replayed in order; diffs older than the snapshot's `lastUpdateId` are discarded.
- **Connection generations.** Every `connect()` increments a generation counter; stale async callbacks (snapshot fetches, socket handlers, reconnect timers from a previous symbol) check their captured generation and no-op if superseded — preventing race conditions when switching symbols rapidly.

## System overview

```
                       ┌─────────────────────────────┐
   Binance WS feed ───►│  terminal.html              │  Browser terminal:
   (depth + trades)    │  (single-file fill engine)  │  live book, tape, manual
                       └──────────────┬──────────────┘  order entry + JS strategies
                                      │
   ┌──────────────────────────────────┼──────────────────────────────┐
   ▼                                  ▼                              ▼
┌─────────────────┐        ┌────────────────────┐        ┌────────────────────┐
│ JS strategies   │        │ paper_trader.py    │        │ backtest_server.py │
│ (3 research-    │        │ headless service:  │◄───────│ replays NT JSONL   │
│  based algos)   │        │ live fills → disk  │        │ as Binance-shape   │
└─────────────────┘        └────────────────────┘        │ feeds, no lookahead│
                                      ▲                   └─────────┬──────────┘
                           ┌──────────┴───────────┐      ┌──────────┴──────────┐
                           │ analyze_trades.py    │      │ BfsL2Exporter.cs    │
                           │ session P&L summary  │      │ NinjaTrader → JSONL │
                           └──────────────────────┘      │ (real CME L2 data)  │
                                                         └─────────────────────┘
```

### Components

- **`terminal.html`** — single-file, zero-dependency browser terminal: live order book + trade tape, manual order entry, and the queue-aware fill engine described above.
- **JavaScript strategies**, each grounded in market-microstructure research:
  - `orderflow_predictor_strategy.js` — short-horizon **order-flow imbalance (OFI)** at and beyond top-of-book (Cont–Kukanov–Stoikov; multi-level OFI).
  - `mean_reversion_strategy.js` — passive limit-order fading of dislocations from an adaptive EWMA fair value; inventory-aware (Avellaneda–Stoikov), treats one-sided flow as adverse-selection risk.
  - `auto_market_maker.js` — Avellaneda–Stoikov-inspired market maker with inventory limits and reservation-price quoting.
- **`paper_trader.py`** — async headless service mirroring the browser engine; runs a strategy, paper-fills against live top-of-book VWAP, and writes every fill to disk immediately (day-keyed JSONL + CSV) with session-state snapshots for crash-safe restart. Supports live or replay data sources.
- **`backtest_server.py`** — replays historical L2 JSONL as Binance-shaped WS+REST feeds so the **same strategy code** runs unchanged on history, with explicit lookahead-bias guarantees (events emitted only at their scheduled wall-clock time; snapshots reflect only already-replayed events; fills only consume post-placement events).
- **`BfsL2Exporter.cs`** — NinjaScript indicator that streams real CME futures L2 depth + trades to JSONL, feeding the replay server (see `NT_REPLAY_SETUP.md`).
- **`analyze_trades.py`** / **`validate_export.py`** / **`trade_analysis.html`** / **`backtest.html`** — P&L summarisation, export validation, and browser-based analysis.

## Tech stack

- **JavaScript** (vanilla) — terminal, fill engine, strategies
- **Python** (asyncio, websockets, aiohttp) — headless trader, replay server
- **C#** (NinjaScript) — NinjaTrader L2 exporter
- **Binance Futures WebSocket + REST** — live market data
- **HTML/CSS** — single-file terminal and dashboards

## Getting started

```bash
pip install -r requirements.txt
```

```bash
# Browser terminal — no setup, public data only:
open terminal.html

# Headless paper trader (fills → paper_data/):
python paper_trader.py

# Backtest replay:
python backtest_server.py /path/to/replay.jsonl   # then open http://localhost:8080/

# Analyse a day's fills:
python analyze_trades.py 2026-06-14
```

## Known limitations (read before trusting any P&L)

- **Queue position is estimated, not known** — it's the whole point of the model, but it's still an estimate from MBP data. The `1.35` behind-bias and `log1p` weighting are reasonable heuristics, not ground truth.
- **No fees, funding, or latency-of-data modelling** in the P&L — only order round-trip latency is simulated.
- **Aggregated trades** collapse same-price/same-ms prints, so very small fills may attach to one larger trade.
- **Browser state is in-memory** (the Python service persists; the browser doesn't).
- **Binance USD-M futures only** — other venues need new adapters.

## Possible extensions

- Calibrate the queue model's bias parameter against a venue that *does* publish MBO (market-by-order) data, to measure how well the MBP estimate tracks true queue position.
- Add fees, funding, and slippage to make simulated P&L economically meaningful.
- Proper out-of-sample train/test splitting in the replay server for honest strategy evaluation.