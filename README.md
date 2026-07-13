# Binance Fill Simulator

A sandbox built around one stubborn question: *if I'd actually placed this limit order, would it have filled?*

It rebuilds a live order book from Binance's feed, simulates resting limit orders with a queue-aware fill model, runs a handful of research-y strategies against that engine, and can replay recorded historical data through the exact same code path. Nothing here ever sends a real order.

> **Paper trading only.** Every fill is invented by the simulator. It never touches a real exchange, none of it is financial advice, and none of the strategies are money printers, they're experiments. Treat the P&L as a research signal, not a promise.

## The part that's actually hard

Simulating a *market* order is easy: walk the book, eat liquidity, add up what you paid. A resting *limit* order is where it gets interesting, because whether you fill depends on where you sit in the queue at your price, and the exchange never tells you that.

Binance, like most venues, only broadcasts market-by-price data: the total size at each level, not the individual orders stacked behind it. You can see "4.2 BTC bid at 67,000" but not whether your 0.1 BTC is first in line or four-hundredth. So the whole thing comes down to one problem: **guess your queue position from aggregate data, and keep that guess honest as trades and cancels come through.** That's the interesting part, so it gets the most detail below.

## How the engine guesses your place in line

Every resting order carries a few extra fields beyond the obvious price/side/qty:

| field | what it's for |
|---|---|
| `queueAhead` | current best guess at how much size rests *ahead* of you at your price |
| `lastLevelQty` | the last size we saw at your level, so we can tell when it changes |
| `pendingTradeDepletion` | trade volume at your price we haven't yet reconciled against a size drop |
| `pendingTradeDepletionTs` | expiry clock for that (goes stale after 1s) |
| `recentCancelAdvance` | queue progress we chalked up to a cancel, held in case a late trade explains it instead |
| `recentCancelAdvanceTs` | the 250ms window for that second guess |

And a few knobs at the top:

```js
QUEUE_EPS = 1e-10;                 // ignore floating-point dust
QUEUE_CANCEL_BEHIND_BIAS = 1.35;   // >1 means "lean toward cancels happening behind you"
state.latencyMs = 100;             // pretend round-trip to the matching engine
```

**You join at the back.** Place an order at price `P` and `queueAhead` starts at whatever size is already sitting at `P`. You don't get to cut the line ahead of orders that were already there, so this is both the pessimistic and the realistic default.

**Trades that reach you.** On every `aggTrade`, your order is a fill candidate if an opposing aggressor trades at (or through) your price: a BUY at `P` fills against a SELL printing at `price ≤ P`, a SELL at `P` against a BUY at `price ≥ P`. Two cases:

- *Someone traded through you.* A sell prints at 66,999 while your BUY rests at 67,000. The market just traded at a better price than yours, which means your level had to clear first, so you fill immediately for `min(tradeQty, remaining)` with no queue cost.
- *Someone traded at your exact price.* The trade burns down the queue in front of you first; you only get whatever's left over:

  ```
  burned     = min(queueAhead, tradeQty)
  queueAhead -= burned
  leftover   = tradeQty - burned
  fillQty    = min(leftover, remaining)   // only if there's anything left
  ```

  That's just FIFO priority spelled out: volume at your price serves the front of the line before it gets to you.

**Cancels are the tricky one.** When the size at your level drops, there are only two explanations: a trade (handled above), or someone ahead of or behind you pulling their order. Cancels *ahead* of you move you up; cancels *behind* you do nothing for you. The aggregate feed won't say which it was, so the engine estimates.

First it subtracts anything a recent trade already accounts for (that's what `pendingTradeDepletion` is for, with a 1s expiry so stale trade volume doesn't linger):

```
levelDrop      = prevLevelQty - nextLevelQty
tradeExplained = min(levelDrop, pendingTradeDepletion)
cancelQty      = levelDrop - tradeExplained
```

Then it splits the leftover cancel between "ahead of me" and "behind me" with a size-weighted probability. With `front = queueAhead` and `behind = nextLevelQty − front`:

```
uniformProb    = log1p(front) / (log1p(front) + log1p(behind))
aheadProb      = uniformProb ^ QUEUE_CANCEL_BEHIND_BIAS   // exponent 1.35
aheadCancelled = min(queueAhead, cancelQty * aheadProb)
queueAhead    -= aheadCancelled
```

Two deliberate choices in there:

- **`log1p`, not a flat proportion.** A plain `front / total` split overstates how often a cancel lands ahead of you when the queue is deep. Taking `log1p` of the sizes compresses the big numbers so the probability tracks *relative* depth instead of getting dominated by one huge level. It also falls out cleanly at the edges (0 when nothing's ahead, 1 when nothing's behind).
- **That 1.35 exponent.** Raising a probability between 0 and 1 to a power above 1 pushes it down, so cancels get nudged toward happening *behind* you. This is on purpose. Without it the simulator would happily yank you to the front every time someone cancels, fill rates would balloon, and the strategy would look far better than it is.

Finally `queueAhead` is clamped so it can never exceed the total size now at the level.

**When messages arrive out of order.** Depth and trade updates share one socket and don't always arrive in causal order, so a size drop can land *before* the trade that caused it. Left alone, the engine would advance you once via the cancel model when the drop shows up, then burn your queue *again* when the trade finally arrives, double-counting the same volume. The fix is a 250ms reconciliation window: whenever the cancel model advances you, it stashes that amount in `recentCancelAdvance`, and any trade at your price within 250ms nets against it first before burning the queue. Volume that already moved you as a "cancel" can't also fill you as a "trade." It's a quiet little consistency bug that would otherwise inflate every result, so it's handled on purpose.

**Book-sweep fills.** Separately, if the book itself moves through your price while you're already at the front (`queueAhead == 0`), you fill: a BUY at `P` sweeps when `bestAsk < P`, a SELL when `bestBid > P`. There's a short grace period after placement (plus the latency model) so an order joined at the current best doesn't get instantly swept before the book has actually moved.

**Market orders.** Filled by walking up to 20 levels of the far side and taking a size-weighted VWAP, after a simulated `latencyMs` delay, because your order reaches the matching engine a few milliseconds after you hit the button, not the instant you do.

## Keeping the book honest

The book is maintained with the standard Binance snapshot + diff dance. Three details that matter more than they look:

- **Levels are keyed by number, not string.** `state.bids` / `state.asks` use `parseFloat(price)`, never the raw exchange string, because `"67000.10"` and `Number("67000.10").toString()` (`"67000.1"`) would otherwise be two different keys and silently rot the book.
- **Diffs are sequenced.** Anything that arrives before the REST snapshot is buffered and replayed in order; anything older than the snapshot's `lastUpdateId` is thrown away.
- **Stale callbacks get fenced off.** Every `connect()` bumps a generation counter, and old async callbacks (snapshot fetches, socket handlers, reconnect timers from a symbol you already switched away from) check their captured generation and quietly no-op if they've been superseded. Otherwise flipping symbols quickly races the book into garbage.

## How the pieces fit together

The browser terminal (`terminal.html`) is the center of gravity: it holds the fill engine and talks straight to Binance. Everything else is a variation on it.

- The **three JS strategies** plug into that same engine, live in the browser.
- **`paper_trader.py`** is the headless version: same fill logic, no browser, runs a strategy against live Binance and writes every fill to disk.
- **`backtest_server.py`** feeds recorded history to `backtest.html` disguised as a live Binance feed, so the identical strategy + engine code runs on the past.
- **`BfsL2Exporter.cs`** is where that recorded history comes from: a NinjaTrader script dumping real CME futures depth + trades to a JSONL file.
- **`analyze_trades.py`** reads the paper trader's fills back and totals up the P&L.

### The main components

- **`web/terminal.html`** — the live browser terminal: order book, trade tape, manual order entry, and the queue-aware fill engine above. The HTML is thin; the styling is in `web/css/terminal.css` and the logic in ordered modules under `web/js/terminal/` (`01-core` → `02-engine` → `03-ui` → `04-main`).
- **`web/strategies/`** — three strategies, each based on real market-microstructure research:
  - `orderflow_predictor_strategy.js` — trades short-horizon order-flow imbalance at and beyond the top of book (Cont–Kukanov–Stoikov style, multi-level).
  - `mean_reversion_strategy.js` — passively fades price away from an adaptive EWMA fair value, inventory-aware (Avellaneda–Stoikov), and treats persistent one-sided flow as adverse selection to back off from.
  - `auto_market_maker.js` — an Avellaneda–Stoikov market maker with inventory limits and reservation-price quoting.
- **`server/paper_trader.py`** — the headless trader. Runs a strategy, paper-fills market orders at the live top-of-book VWAP, and writes each fill to disk immediately (day-keyed JSONL + CSV) with session snapshots so a crash-restart picks up mid-session. Its monitor page is `server/templates/paper_dashboard.html`.
- **`server/backtest_server.py`** — replays a recorded L2 JSONL as Binance-shaped WS + REST feeds, so the same strategy code runs unchanged on history. It's careful about lookahead: events only go out when their scheduled wall-clock time arrives, the snapshot only reflects what's already been replayed, and fills only ever see events from after an order was placed. Serves `web/backtest.html`.
- **`ninjatrader/BfsL2Exporter.cs`** — NinjaScript that streams real CME futures L2 depth + trades to JSONL for the replay server (setup in `docs/NT_REPLAY_SETUP.md`).
- **`tools/analyze_trades.py`**, **`tools/validate_export.py`**, **`web/trade_analysis.html`** — P&L summaries, export sanity-checks, and browser-based trade analysis.

## What it's built on

- **JavaScript** (plain, no framework) for the terminal, fill engine, and strategies
- **Python** (asyncio, websockets, aiohttp) for the headless trader and replay server
- **C#** (NinjaScript) for the CME data exporter
- **Binance USD-M Futures** WebSocket + REST for live data

## Repository layout

```
web/          Browser front-ends. terminal.html (live) and backtest.html (replay),
              plus css/, the js/<page>/ modules, strategies/ (the 3 algos both
              pages load), and vendor/ (the charting lib).
server/       The long-running Python services: paper_trader.py (live paper
              trading + its monitor dashboard) and backtest_server.py (the replay
              dashboard on :8080). templates/ holds the paper-trader page.
research/     One-off backtests and experiments: backtest_cli, mm_sim, the volty /
              breakout / momentum tests, eta_estimate, ofp_runner.js.
tools/        The data pipeline: make_candles, build_ofp_cache, validate_export,
              analyze_trades.
ninjatrader/  C# scripts that run inside NinjaTrader (data export + a couple of
              strategies). Separate world, no ties to the Python/JS.
data/         Everything that's data rather than code: the L2 export, candles,
              caches, paper-trading fills, exports. Mostly gitignored.
docs/         HANDOFF.md and the NinjaTrader replay setup guide.
```

Scripts find `data/` relative to the repo root, so you can run them from anywhere, and every path is still overridable with a CLI flag.

## Getting started

```bash
pip install -r requirements.txt
```

```bash
# Live browser terminal. No setup, public data only:
open web/terminal.html

# Headless paper trader. Fills go to data/paper_data/, dashboard on :1000 (or :8000):
python server/paper_trader.py

# Replay dashboard. Defaults to data/bfs_l2_export.jsonl:
python server/backtest_server.py          # then open http://localhost:8080/
python server/backtest_server.py path/to/replay.jsonl   # or point it somewhere else

# Total up a day of paper fills:
python tools/analyze_trades.py 2026-06-14
```

## Before you trust any P&L

Things the model does *not* do, worth knowing before you read too much into a number:

- **Queue position is a guess, not fact.** That's the whole exercise, but it's still an estimate from aggregate data. The `1.35` bias and the `log1p` weighting are reasonable heuristics, not ground truth.
- **The P&L skips fees, funding, and data latency.** Only the order round-trip is simulated.
- **Aggregated trades lump same-price, same-millisecond prints together**, so a tiny fill can end up attached to one bigger trade.
- **The browser keeps state in memory only.** The Python service persists; the browser tab doesn't.
- **Binance USD-M futures only.** Any other venue needs a new adapter.

## Where it could go next

- Calibrate that queue-bias parameter against a venue that *does* publish market-by-order data, to actually measure how close the guess is to real queue position.
- Add fees, funding, and slippage so the P&L means something in dollars.
- Real out-of-sample train/test splitting in the replay server, for honest evaluation instead of fitting to the same days you test on.
