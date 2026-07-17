<div align="center">

# Limit Fill Simulator

**Would my limit order have filled?** A queue-aware fill simulator for live Binance order books.

</div>

Exchanges tell you the total size at each price, never where your order sits in line. That one gap is what makes simulating a limit-order fill hard, and it's the whole point of this project: reconstruct the book from a live feed, estimate your queue position from aggregated data, and fill (or don't fill) your resting orders the way the real matching engine would have. Then run strategies against it, live or replayed over recorded history, without ever sending an order.

> Paper trading only. Every fill is simulated. It never touches a real exchange, it isn't financial advice, and the strategies are experiments, not a system that makes money. Read the P&L as a research signal.


## Demo

> **▶ 60-second demo:** VIDEO_URL &nbsp;•&nbsp; **Live paper trader:** `python server/paper_trader.py`, then http://localhost:1000/

<!-- To make the video play inline: drag docs/media/demo.mp4 into a GitHub issue or
     PR comment, then paste the user-attachments URL it returns on the line below
     (and into VIDEO_URL above). A plain repo path will not render as a player. -->

VIDEO_URL

## Why this exists

A market order is trivial to simulate: walk the book, take liquidity, tally the price. A limit order isn't, because whether you fill depends on your place in the queue at your price, and no exchange publishes that.

Binance sends market-by-price data. You get the total size at 67,000, not the individual orders behind it, and definitely not which one is yours. See "4.2 BTC bid at 67,000" and your 0.1 BTC could be first in line or four-hundredth. The rest of the fill either happens or doesn't based entirely on that hidden number, so the sim spends most of its effort estimating it and correcting the estimate as trades and cancels come in.

## The fill model

Each resting order carries some bookkeeping beyond price/side/qty:

| field | meaning |
|---|---|
| `queueAhead` | estimated size resting ahead of you at your price |
| `lastLevelQty` | last size seen at your level, to detect changes |
| `pendingTradeDepletion` | trade volume at your price not yet matched to a size drop |
| `pendingTradeDepletionTs` | expiry for the above (1s) |
| `recentCancelAdvance` | queue progress credited to a cancel, held in case a late trade explains it |
| `recentCancelAdvanceTs` | window for that correction (250ms) |

```js
QUEUE_EPS = 1e-10;                 // ignore float noise
QUEUE_CANCEL_BEHIND_BIAS = 1.35;   // >1 leans cancels toward being behind you
state.latencyMs = 100;             // simulated round-trip to the matching engine
```

**Joining.** A new order at price `P` starts with `queueAhead` set to the size already resting at `P`. You're at the back of the line; you can't jump orders that were there first. Pessimistic on purpose, and correct.

**Filling on a trade.** On each `aggTrade`, your order is eligible if an opposing aggressor reaches your price (a BUY at `P` fills against a SELL at `≤ P`, a SELL against a BUY at `≥ P`). If the trade printed *through* you (a sell at 66,999 against your BUY at 67,000), the market already traded at a better price than yours, so your level cleared and you fill right away. If it printed *at* your price, it burns the queue in front of you first and you only take the remainder:

```
burned     = min(queueAhead, tradeQty)
queueAhead -= burned
fillQty     = min(tradeQty - burned, remaining)   // whatever's left after the front
```

**Cancels.** When your level shrinks and it wasn't a trade, someone pulled an order. Only cancels ahead of you help, and the feed won't say where they happened. So the sim subtracts anything a recent trade already explains, then splits the rest by a size-weighted probability:

```
cancelQty      = levelDrop - min(levelDrop, pendingTradeDepletion)
uniformProb    = log1p(front) / (log1p(front) + log1p(behind))   // front=queueAhead
aheadProb      = uniformProb ** 1.35
queueAhead    -= min(queueAhead, cancelQty * aheadProb)
```

Two choices worth calling out. The `log1p` weighting keeps a deep level from dominating the split the way a raw `front / total` ratio would, and it degenerates cleanly when one side is empty. The `1.35` exponent bends the probability toward cancels landing *behind* you; without it the sim would drag you to the front on every cancel, fills would balloon, and every strategy would look like a winner.

**Out-of-order messages.** Depth and trades share one socket and don't always arrive in causal order, so a size drop can show up before the trade that caused it. Handled naively, the sim advances you once on the drop and again on the trade, double-counting. A 250ms window fixes it: queue progress credited to a cancel is remembered, and a following trade nets against it before burning anything. Volume can't move you as a cancel *and* fill you as a trade.

**Sweeps and market orders.** If the book moves through your price while you're already at the front, you fill (with a short post-placement grace period so a fresh order at the touch isn't swept instantly). Market orders walk up to 20 levels of the far side for a size-weighted VWAP, after a `latencyMs` delay, because your order reaches the engine a few milliseconds late.

## Keeping the book sane

Standard Binance snapshot + diff, with three details that bite if you skip them:

- **Levels keyed by number, not string.** `"67000.10"` and `"67000.1"` are the same price but different strings, so keying on `parseFloat(price)` avoids silently forking a level in two.
- **Diffs are sequenced.** Anything arriving before the REST snapshot is buffered and replayed; anything older than its `lastUpdateId` is dropped.
- **Stale callbacks are fenced.** Each `connect()` bumps a generation counter, and leftover async callbacks from a symbol you already switched away from check it and bail. Otherwise fast symbol-switching races the book into garbage.

## How it's wired

`terminal.html` is the core: the fill engine plus a live Binance connection, running in the browser. Everything else reuses it.

- The three JS strategies plug into that engine in the browser.
- `paper_trader.py` is the headless version, same fill logic, no browser, writing every fill to disk.
- `backtest_server.py` feeds recorded history to `backtest.html` dressed up as a live Binance feed, so identical code runs on the past.
- `BfsL2Exporter.cs` is where that history comes from: a NinjaTrader script dumping real CME futures depth + trades to JSONL.
- `analyze_trades.py` reads the paper fills back and totals the P&L.

**The main pieces**

- **`web/terminal.html`** — live terminal: order book, tape, manual order entry, and the fill engine above. Thin HTML; styling in `web/css/terminal.css`, logic in ordered modules under `web/js/terminal/` (`01-core` → `02-engine` → `03-ui` → `04-main`).
- **`web/strategies/`** — three research-backed strategies:
  - `orderflow_predictor_strategy.js` — short-horizon order-flow imbalance across the top levels (Cont–Kukanov–Stoikov).
  - `mean_reversion_strategy.js` — passively fades price off an adaptive EWMA fair value, inventory-aware (Avellaneda–Stoikov), backing off when flow is one-sided.
  - `auto_market_maker.js` — Avellaneda–Stoikov market maker with inventory limits and reservation-price quoting.
- **`server/paper_trader.py`** — runs a strategy live, fills at the top-of-book VWAP, writes day-keyed JSONL + CSV plus session snapshots for crash-safe restart. Monitor page: `server/templates/paper_dashboard.html`.
- **`server/backtest_server.py`** — replays an L2 JSONL as Binance-shaped WS + REST feeds, lookahead-free (events fire on schedule, snapshots reflect only what's replayed, fills only see post-placement events). Serves `web/backtest.html`.
- **`ninjatrader/BfsL2Exporter.cs`** — NinjaScript that records CME futures L2 to JSONL (`docs/NT_REPLAY_SETUP.md`).
- **`tools/`, `web/trade_analysis.html`** — P&L summaries, export validation, browser trade analysis.

## Layout

```
web/          front-ends: terminal.html (live), backtest.html (replay),
              css/, the js/<page>/ modules, strategies/, vendor/ (charts)
server/       paper_trader.py (live + dashboard) and backtest_server.py (:8080)
research/     backtests and experiments: backtest_cli, mm_sim, volty/breakout/
              momentum, eta_estimate, ofp_runner.js
tools/        data pipeline: make_candles, build_ofp_cache, validate_export, analyze_trades
ninjatrader/  C# scripts that run inside NinjaTrader (separate runtime)
data/         L2 export, candles, caches, paper fills, exports (mostly gitignored)
docs/         handoff notes + NinjaTrader setup
```

Scripts resolve `data/` from the repo root, so they run from anywhere, and every path takes a CLI override.

## Run it

```bash
pip install -r requirements.txt
```

```bash
open web/terminal.html                       # live terminal, no setup, public data
python server/paper_trader.py                # headless trader, dashboard on :1000 (or :8000)
python server/backtest_server.py             # replay dashboard on http://localhost:8080/
python tools/analyze_trades.py 2026-06-14    # P&L for one day of paper fills
```

## What it doesn't do

- Queue position is estimated, not known. That's the exercise, but the `1.35` bias and `log1p` weighting are heuristics, not truth.
- P&L ignores fees, funding, and data latency. Only the order round-trip is modelled.
- Aggregated trades merge same-price, same-ms prints, so a tiny fill can ride along on a bigger trade.
- Browser state lives in memory. The Python service persists; the tab doesn't.
- Binance USD-M futures only. Anything else needs an adapter.

## If I kept going

- Calibrate the queue-bias against a venue that publishes market-by-order data, to measure how close the estimate really is.
- Put fees, funding, and slippage into the P&L so it means dollars.
- Real out-of-sample splits in the replay server, instead of testing on the days you tuned on.

## Built with

Vanilla JavaScript (terminal, fill engine, strategies) · Python with asyncio/aiohttp/websockets (headless trader, replay server) · C# NinjaScript (CME exporter) · Binance USD-M Futures WebSocket + REST.
