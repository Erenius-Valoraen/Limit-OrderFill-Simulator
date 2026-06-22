# Binance Fill Simulator

A limit-order **fill-simulation and strategy-research platform** for high-frequency / market-making experiments, built on live Level-2 market data. It reconstructs an exchange order book from a live feed, simulates realistic queue-based execution of resting limit orders, runs research-grounded trading strategies against that simulated fill engine, and supports lookahead-bias-free replay of historical data — all without placing a single real trade.

> ⚠️ **Paper trading only.** Every fill in this project is simulated. Nothing here connects to a live exchange for real order execution, and none of it is financial advice or a profitable trading system. The strategies are research-backed experiments, not guarantees.

## Why it exists

The hard part of testing a passive (limit-order) strategy isn't the signal — it's knowing *whether your order would actually have filled*. A resting limit order's fill depends on queue position, cancellations ahead of you, and whether the book trades through your price, none of which a naive backtest captures. This project's core is a **queue-aware fill engine** that models those mechanics against real market-by-price data, so strategies can be evaluated under realistic execution assumptions.

## System overview

The platform has five parts that share one fill model:

```
                       ┌─────────────────────────────┐
   Binance WS feed ───►│  terminal.html              │  Browser terminal:
   (depth + trades)    │  (single-file fill engine)  │  live book, tape, manual
                       └──────────────┬──────────────┘  order entry + JS strategies
                                      │
   ┌──────────────────────────────────┼──────────────────────────────┐
   │                                  │                              │
   ▼                                  ▼                              ▼
┌─────────────────┐        ┌────────────────────┐        ┌────────────────────┐
│ JS strategies   │        │ paper_trader.py    │        │ backtest_server.py │
│ (3 research-    │        │ headless service:  │◄───────│ replays NT JSONL   │
│  based algos)   │        │ live fills → disk  │        │ as Binance-shape   │
└─────────────────┘        └────────────────────┘        │ feeds (no lookahead)│
                                      ▲                   └─────────┬──────────┘
                                      │                             │
                           ┌──────────┴───────────┐      ┌──────────┴──────────┐
                           │ analyze_trades.py    │      │ BfsL2Exporter.cs    │
                           │ session P&L summary  │      │ NinjaTrader → JSONL │
                           └──────────────────────┘      │ (real CME L2 data)  │
                                                         └─────────────────────┘
```

### 1. `terminal.html` — browser fill engine

A single-file, zero-dependency browser app that connects directly to Binance Futures public WebSocket streams, renders a live order book and trade tape, and runs a **client-side queue-based fill engine**. You place resting bids/offers and the engine decides whether they fill by watching the live book and trades. It models three fill paths — passive trade fills (with a queue-depletion model), book-sweep fills, and probabilistic queue advancement when depth decreases that aren't explained by same-price trades. The fill mechanics are documented in detail in [`terminal.html`'s own README section](#fill-engine-details). No server, no build step — open the file and it runs.

### 2. JavaScript strategies

Three automated strategies that drive the fill engine, each grounded in published market-microstructure research:

- **`orderflow_predictor_strategy.js`** — an ultra-short-horizon strategy based on **order-flow imbalance (OFI)** at the best bid/ask and across deeper book levels (Cont–Kukanov–Stoikov and multi-level OFI literature).
- **`mean_reversion_strategy.js`** — a passive limit-order strategy that fades short-term dislocations from an adaptive EWMA fair value, using inventory-aware quoting (Avellaneda–Stoikov) and treating one-sided flow as adverse-selection risk.
- **`auto_market_maker.js`** — an Avellaneda–Stoikov-inspired market maker with inventory limits and reservation-price quoting.

### 3. `paper_trader.py` — headless paper-trading service

An async Python service that mirrors the browser engine's logic without a browser. It maintains the order book locally (snapshot + diff), runs the OrderFlowPredictor strategy, paper-fills against live top-of-book VWAP, and **writes every fill to disk immediately** in day-keyed JSONL + CSV, snapshotting session state after each fill so a crash or restart resumes exactly where it left off. It can read either a live Binance feed or a replay file.

### 4. `backtest_server.py` — lookahead-bias-free replay

Replays a historical L2 JSONL as Binance-shaped WebSocket + REST feeds, so the **exact same strategy code** runs against historical data unchanged. It is explicit about avoiding lookahead bias: events are emitted only when their scheduled wall-clock time arrives, REST snapshots reflect only already-replayed events, and the fill engine only consumes events that arrive after an order is placed — so a fill can never come from future data.

### 5. `BfsL2Exporter.cs` — NinjaTrader bridge

A NinjaScript indicator that streams Level-2 depth and trades from NinjaTrader to a JSONL file, enabling the replay server to test strategies on **real CME futures L2 data** (e.g. NQ) via NinjaTrader's free Market Replay. The end-to-end pipeline is documented in [`NT_REPLAY_SETUP.md`](./NT_REPLAY_SETUP.md).

Supporting tools: **`analyze_trades.py`** summarizes per-day fills and P&L; **`validate_export.py`** checks exported data integrity; **`trade_analysis.html`** / **`backtest.html`** provide browser-based analysis views.

## Tech stack

- **JavaScript** — browser terminal, fill engine, and strategies (vanilla, no framework)
- **Python (asyncio, websockets, aiohttp)** — headless paper trader and replay server
- **C# (NinjaScript)** — NinjaTrader L2 exporter
- **Binance Futures WebSocket + REST** — live market data
- **HTML/CSS** — single-file terminal and analysis dashboards

## Getting started

### ⚠️ Before anything: credentials

This project reads credentials from a `.env` file (broker API key, client code, PIN, TOTP secret). **Never commit `.env` to version control.** Add it to `.gitignore`, keep only a `.env.example` with placeholder values in the repo, and if real credentials have ever been shared or committed, rotate them with your broker (regenerate the API key, change the PIN, and re-issue the TOTP secret).

```bash
pip install -r requirements.txt
```

**Browser terminal (no setup):** open `terminal.html` in a browser. It uses public Binance data only.

**Headless paper trader:**

```bash
python paper_trader.py
# fills are written under paper_data/ as day-keyed JSONL + CSV
```

**Backtest replay:**

```bash
python backtest_server.py /path/to/replay.jsonl
# then open http://localhost:8080/
```

**Analyze results:**

```bash
python analyze_trades.py 2026-06-14
```

## Fill engine details

The queue and fill mechanics — passive trade fills, the market-by-price queue-advancement model, book-sweep fills, snapshot/diff sequencing, and the known approximations — are documented in depth in the dedicated section of `terminal.html`'s README. In short: because the exchange provides market-by-price (not market-by-order) data, queue position is *estimated* rather than known, using a probabilistic model of how much cancelled size was ahead of you, with safeguards against double-counting trades and depth decreases.

## Known limitations

- **Queue position is approximate** — orders are assumed to join the back of the queue, and queue advancement from cancellations is modeled probabilistically, not observed.
- **Sweep fills can be slightly optimistic** despite a grace period to prevent instant fills.
- **Aggregated trades** collapse multiple same-price/same-ms prints, so very small fills may be attributed to one larger trade.
- **No persistence in the browser version** — refreshing clears state. (The Python service *does* persist.)
- **Venue-specific** — Binance USD-M futures endpoints; other venues need different adapters.

## Possible extensions

- Add fees, latency modeling, and slippage to make simulated P&L more realistic.
- Extend the replay server with proper out-of-sample train/test splits for strategy evaluation.
- Add exchange adapters and hard risk limits if ever moving beyond simulation (with extreme caution).