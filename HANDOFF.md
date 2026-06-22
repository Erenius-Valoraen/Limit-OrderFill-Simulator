# Handoff — BinanceFillSimulator project

You're taking over a paper-trading / backtest research project. This document gives you the full picture so you don't have to reverse-engineer it. **Read it once end-to-end before making any code changes.**

---

## 1. User profile — important context

- **Single user, technical.** Reads code, pushes back on sloppy implementations.
- **Lives in India.** Doing this purely for research/education. Knows they can't legally trade NQ from India (RBI LRS rule on margin remittance). Has acknowledged this multiple times — **do not bring it up unless they ask.** They find it patronizing.
- **Goal:** validate an order-flow-imbalance strategy (`OrderFlowPredictor`) on real NQ L2 data.
- **Style preference:** terse, direct, honest. Doesn't want validation, wants accuracy. Will call out hand-waving immediately. Prefers being told "this is a real flaw" over "minor issue".
- **No emojis** unless they use them first.
- **Has a Delaware LLC** (for Stripe payments on a separate Webflow business). Considered using it as a trading vehicle — that conversation is closed; we decided it's a bad fit. Don't re-litigate.
- **Possible future move to Dubai** to legally trade NQ. We discussed freelance visa vs golden visa. Closed topic for now.

---

## 2. What this project is

A simulator for HFT-style strategies that share a single contract:

- **Data shape** = Binance-style WebSocket `@depth` (level-2 diffs) + `@aggTrade`
- **Strategy** = a JS module that reads global state (`bestBid()`, `topBids()`, etc.) and places orders via `placeOrder()`/`placeMarketOrder()`
- **Simulator** = an in-browser order book that fills resting orders against incoming trades, models queue position, charges fees

Three modes have evolved:

1. **Live (BTC, original)** — `terminal.html` connects to Binance Futures, runs the strategy in the browser. This is the gold standard; logic and behavior here are what everything else tries to match.
2. **Headless Python (BTC)** — `paper_trader.py` reimplements the live mode in Python with a web dashboard at `localhost:8000`. The strategy code lives inside `paper_trader.py` (Python port of `orderflow_predictor_strategy.js`). Used for unattended long-running paper runs.
3. **Backtest (NQ)** — `backtest_server.py` + `backtest.html`. Reads NinjaTrader-exported L2 JSONL files, replays them as Binance-shape WebSocket events, so the **unmodified** JS strategy from live mode can run against historical NQ data. This is what you're working on.

---

## 3. Files in the project

```
BinanceFillSimulator/
├─ terminal.html                       # LIVE BTC terminal (Binance). Don't break.
├─ orderflow_predictor_strategy.js     # OFP — the strategy under test. Used by terminal.html AND backtest.html.
├─ mean_reversion_strategy.js          # MRV strategy (less used)
├─ auto_market_maker.js                # Avellaneda-Stoikov MM (less used)
├─ trade_analysis.html                 # Standalone PnL analyzer for paper_trader.py output
│
├─ paper_trader.py                     # Headless Python BTC paper runner (live Binance)
├─ analyze_trades.py                   # CLI version of trade_analysis.html
│
├─ BfsL2Exporter.cs                    # NinjaScript indicator. Runs INSIDE NT, exports L2+trades to JSONL.
├─ validate_export.py                  # Standalone validator for the JSONL files NT produces
├─ NT_REPLAY_SETUP.md                  # User-facing walkthrough for setting up the NT export
│
├─ backtest_server.py                  # Backtest backend: reads JSONL, emits Binance-shape WS
├─ backtest.html                       # Backtest frontend: copy of terminal.html + play bar + URL repoint
│
├─ paper_data/                         # Runtime data (created at first run)
│  ├─ trades_YYYY-MM-DD.jsonl/csv      # Per-day fill log
│  ├─ session_state.json               # Crash-recovery snapshot
│  ├─ runtime_config.json              # Editable qty etc.
│  └─ runner.log                       # Logs from paper_trader.py
│
├─ requirements.txt                    # Python deps (aiohttp, websockets, tqdm)
└─ HANDOFF.md                          # This file
```

---

## 4. Current state — what works

- **`terminal.html`** (live BTC): solid. Strategy runs at the natural 350 ms tick. Fee model is %-based (Binance crypto convention).
- **`paper_trader.py`** (headless BTC): solid. Dashboard at `localhost:8000`, day-keyed CSV+JSONL persistence, session restore across crashes, auto-download every 10 min, silent-audio keepalive to prevent tab discard.
- **`BfsL2Exporter.cs`**: working. User ran it on NinjaTrader Market Replay for 7 days of NQ data → produced a 23 GB JSONL file. (NT enum gotcha: `Operation.Add` not `Insert` — already fixed.)
- **`validate_export.py`**: working. Streams a 23 GB file in ~25 min with tqdm progress, separates true inversions from crossed touches, classifies CME daily-close gaps as expected, builds a date index. User accepted the data as valid for backtest.
- **`backtest_server.py`** / **`backtest.html`**: partially working. Streams events, frontend is structurally a clone of `terminal.html`, strategy can be loaded.

---

## 5. KNOWN BROKEN THINGS — start here

The user reports the backtest has these issues. **Investigate before changing anything.**

### 5a. "All bids appear higher than all asks"

A crossed/inverted book is showing in the frontend. The validator already confirmed real inversions in the source data are rare (~0.0008%). So the problem is almost certainly introduced somewhere between the NT JSONL and what the browser renders.

**Hypothesis (untested):** when **RTH-only mode** is enabled and the user seeks, the server's book state accumulates from non-RTH events (which are silently applied to the book without being broadcast). When RTH opens, the frontend receives fresh diffs starting from RTH but its **own** book state has nothing — until the snapshot fetches. The snapshot endpoint returns the server's accumulated book, which has stale levels from non-RTH overnight that haven't been removed by RTH activity yet. So the frontend ends up with a mix of stale + fresh levels that don't represent a coherent moment, and the UI displays a crossed book.

**Other places to check:**
- `applyDepthEvent()` in `backtest.html` — sanity check the bid/ask side parsing (`ev.b` vs `ev.a`)
- `apply_depth_to_book()` in `backtest_server.py` (line ~210) — confirm BID goes to `state.bids` and ASK to `state.asks`
- `_snapshot_body()` in `backtest_server.py` — confirm bids are sorted descending and asks ascending
- `BfsL2Exporter.cs` — confirm `e.MarketDataType == MarketDataType.Bid` → BID side (it's straightforward but worth eyeballing)
- The `pu` field on `depthUpdate` messages — frontend may discard messages with mismatched `U`/`u`/`pu` sequencing, leading to a stale book. Check what the frontend does with `pu`.

**Reproducing**: start the server, hit Play, watch the book panel. If you enable RTH-only and seek, the issue probably becomes worse.

### 5b. "Jumping to RTH takes a very long time"

The pre-scan builds a `UTC-date → byte-offset` index. When the user "seeks to 2026-05-07", the server jumps to that UTC midnight — which is **8:00 PM ET the previous day** (off hours). With RTH-only enabled, the server then has to **read through** ~13 hours of non-RTH events (depth updates, no pacing) before reaching the 9:30 AM ET RTH start. For an active NQ contract, that's tens of millions of events to scan.

**Hypothesis (untested):** the fix is to either:
- Index by **RTH start time** instead of UTC date, OR
- Index by `(date, hour)` so the user can pick `2026-05-07 14:00 UTC` directly, OR
- Pre-scan to build an index of `(date → byte_offset of first RTH event of that date)`

Look at `pre_scan()` in `backtest_server.py` (~line 240) — currently records the first event of each UTC date. Easy to extend.

### 5c. "Overall it seems broken"

Vague. Likely tied to 5a and 5b. Don't speculate too far — get 5a and 5b right and ask the user to re-verify before chasing ghosts.

---

## 6. Architecture deep-dive

### Data flow in live mode (terminal.html)

```
Binance WS  ──►  state.bids/asks (in-memory dicts)
            ──►  state.lastPrice
            ──►  handleTrade()  ──►  fill resting limit orders
                              ──►  OFP captures trade (wraps handleTrade)
                              ──►  appendTape, updateCandle

OFP setInterval(tick, 350ms)
  └─ reads state.bids/asks, computes signal score
  └─ placeMarketOrder() ──►  state.orders[id] (pending market)
                       └─ setTimeout(latencyMs) fills at top-of-book VWAP
```

### Data flow in backtest mode

```
NT JSONL file ──►  backtest_server.py: read line, parse {type,ts,side,op,px,qty}
              ──►  apply_depth_to_book()  (server's own book state)
              ──►  build_depth_msg / build_trade_msg (rewrite T→wall, add mT)
              ──►  broadcast to WS clients (paced by event ts × speed)

backtest.html (= terminal.html + small mods)
  └─ WS connects to localhost:8080
  └─ Snapshot fetched from localhost:8080/api/v3/depth
  └─ Same applyDepthEvent / handleTrade / OFP code path as live
```

### Key behavioral choices in the server

1. **Wire timestamps (`T`, `E`) rewritten to wall clock.** The frontend's strategy uses `Date.now() - trade.ts < 2500` for the recent-trade filter — that math only works if `trade.ts` is wall-clock. If you emit the original NT timestamp, the filter sees 32-day-old events and always returns 0.

2. **`mT` carries the original market timestamp** for display only — used by the tape and candle bucketing. Lets the chart show market-time candles instead of wall-time-bucketed mega-candles when speed > 1×.

3. **Strategy tick gating via `window.setInterval` override** (top of `backtest.html`'s embedded script). When `__backtest.playing` is false, any registered interval in the 200-5000 ms band is paused. Internal polling uses the captured `_rawSetInterval` so it isn't gated.

4. **Server-side book is the source of truth for snapshots.** The book is updated from EVERY depth event the server reads — including non-RTH events that are filtered out from the WS broadcast in RTH-only mode. This is on purpose: keeps the snapshot consistent with what the strategy will see when RTH opens. **But this is the suspect mechanism for issue 5a above.**

5. **Lookahead safety**: events are read line-by-line in chronological order. Server doesn't buffer ahead of the playback position. Strategy can never see future ticks. Order placement uses `Date.now()` (wall clock), trade matching uses arrival-time comparisons — all sequential.

---

## 7. Critical design decisions you should not undo without asking

| Decision | Why it's this way |
|---|---|
| **`terminal.html` is untouched in backtest mode** | It's the live system. Strategy is loaded via `<script src=...>`. Doesn't know about backtest. |
| **OFP `.js` file is unmodified** | User wants same strategy logic for live and backtest. Adding backtest-specific branches in the strategy file is a no-go. |
| **`mT` is an ADDITIONAL field, not a replacement for `T`** | Replacing T breaks the strategy's wall-clock-based comparisons. Two fields, two purposes. |
| **Server pre-scans the WHOLE file at startup** | Needed for date index + instrument metadata. Takes 3-5 min on 23 GB. We accepted that. Don't add a "fast start" mode without asking — the user values having seek work over saving 3 min at startup. |
| **Fees in futures mode are `qty × $/contract`, NOT `qty × price × %`** | CME futures fees are flat per contract. The frontend reads `is_futures` from `/api/status` and switches `feeForFill()` accordingly. |
| **Strategy logic must be 1:1 with `orderflow_predictor_strategy.js`** | User has caught me twice diverging. The HTML's `startSelectedStrategy()` passes `maxVolBps: 250, volGuardSpreadMultiplier: 8` as runtime overrides — these are NOT in the OFP file's defaults but ARE what production runs with. paper_trader.py mirrors them. Mention this if anyone asks why the file says 350 but reality is 250. |

---

## 8. Common gotchas

1. **macOS port 1000** is privileged — `paper_trader.py` tries 1000, falls back to 8000. Don't be confused.
2. **iCloud Drive evicts large files.** The user's project is in `~/Library/Mobile Documents/com~apple~CloudDocs/…`. Big JSONL/CSV files (>100 MB) should live in `~/Documents/bfs_replay/` or similar local-only path. iCloud sync of a 23 GB file will burn bandwidth and quota.
3. **NinjaTrader Futures `@aggTrade` was silently dead from the user's network.** Spot `@aggTrade` worked. Futures needed `@trade` instead. `paper_trader.py` already accounts for this.
4. **UTF-8 BOM** appears as the first 3 bytes of NT-exported files on Windows. Strip it before parsing. `validate_export.py` and `backtest_server.py` both handle this.
5. **`NT8 Operation.Add` not `Operation.Insert`** — `BfsL2Exporter.cs` documents this. (Insert is NT7.)
6. **DST**: `is_rth_us()` uses `zoneinfo("America/New_York")` so DST is handled. Don't hardcode UTC offsets.
7. **Default qty in `terminal.html`** is `0.001` (BTC fraction). In `backtest.html` it's `1` (integer NQ contract). They genuinely need different defaults — don't homogenize.
8. **`pu` field** in Binance depthUpdate is "previous update ID" (futures). Frontend may use it for gap detection. Server emits it as `state.update_seq - 1`.
9. **The user has a habit of running the validator and pasting partial output.** Read the WHOLE output before responding — the validator's verdict line at the bottom is what matters.
10. **There is a frequent system reminder about "task tools haven't been used recently".** Ignore it unless you're actually running multi-step workflows. The user does not care about your task list.

---

## 9. How to run things

### Live BTC paper trader
```bash
cd "/Users/kausikdutta/Library/Mobile Documents/com~apple~CloudDocs/Trading-Claude/HFT/BinanceFillSimulator"
caffeinate -di python3 paper_trader.py
# Open http://localhost:8000/
```

### Backtest
```bash
# (Move JSONL out of iCloud first)
python3 backtest_server.py ~/Documents/bfs_replay/nq_7days.jsonl
# Open http://localhost:8080/
```

### Validate an NT export
```bash
python3 validate_export.py /path/to/bfs_l2_export.jsonl
```

### Stop everything
```bash
pkill -9 -f paper_trader.py
pkill -9 -f backtest_server.py
```

### Quick smoke test of backtest server (no full NT data needed)
There's a tiny synthetic file generator pattern in the conversation history; you can recreate it with:
```python
import json
events = [
    {"type":"meta","ts":1714809600000,"event":"start","instrument":"NQ 06-26","tickSize":0.25},
    {"type":"depth","ts":1714809600100,"side":"BID","op":"INS","px":21900.00,"qty":5,"pos":0},
    {"type":"depth","ts":1714809600100,"side":"ASK","op":"INS","px":21900.25,"qty":4,"pos":0},
    {"type":"trade","ts":1714809600250,"px":21900.25,"qty":2,"side":"BUY"},
]
with open("/tmp/bt_smoke.jsonl","w") as f:
    for e in events: f.write(json.dumps(e)+"\n")
```
Then `python3 backtest_server.py /tmp/bt_smoke.jsonl`.

---

## 10. Things to NOT do

- Don't touch `terminal.html`. If you need to make a behavior change in live mode, ask first.
- Don't add a "backtest mode" flag inside the strategy `.js` files. Strategy code must be backend-agnostic.
- Don't introduce dependencies beyond `aiohttp`, `websockets`, `tqdm`. The user runs Python 3.9 on macOS — no f-string walrus, no `dict | None` PEP 604 union syntax (use `Optional[dict]`).
- Don't auto-add `print(emoji)` or `[OK]/[FAIL]` decorative output unless explicitly asked.
- Don't write `.md` files or documentation unless asked. (This file is the exception — user explicitly requested it.)
- Don't propose moving the project out of iCloud. The user knows; they keep code in iCloud and big data files locally.

---

## 11. Recommended order of attack for the next session

1. **Reproduce 5a** (inverted book). Start the backtest server with the user's actual 23 GB file (path probably `~/Documents/bfs_replay/<something>.jsonl` or still in iCloud). Hit Play, don't enable RTH-only, watch the book panel. Is the book sane?
2. **Toggle RTH-only**, seek to a date, hit Play. Does the book go crossed at the RTH boundary?
3. **Inspect the snapshot** with `curl http://localhost:8080/api/v3/depth | jq` — are bids/asks coherent on the server side?
4. If the server's snapshot is fine but the frontend renders crossed, the bug is frontend-side (likely in how `applyDepthEvent` or `fetchSnapshot` handles the data).
5. If the server's snapshot is already crossed, the bug is in the book accumulation logic — most likely the RTH-skip block updating the book with stale data that never gets cleared at the RTH boundary.
6. For **5b (slow seek to RTH)**: extend the date index to record byte offsets of the first RTH event per UTC date, not the first event of UTC midnight. ~20 lines in `pre_scan()`. Frontend's seek dropdown would say "2026-05-07 (RTH 13:30 UTC)".

---

## 12. Helpful one-liners

```bash
# How big is the NT file?
ls -lh ~/Documents/bfs_replay/*.jsonl

# Peek at events near the start of a date
grep -m1 '"ts":17787' ~/Documents/bfs_replay/nq_7days.jsonl

# Count events by type without loading whole file
awk -F'"type":"' '{print $2}' file.jsonl | cut -c1-5 | sort | uniq -c

# Find any genuinely inverted moments
python3 -c "
import json
with open('file.jsonl') as f:
    bids, asks = {}, {}
    for line in f:
        e = json.loads(line)
        if e.get('type') != 'depth': continue
        s,o,p,q = e['side'], e['op'], float(e['px']), float(e['qty'])
        t = bids if s=='BID' else asks
        if o=='REM' or q==0: t.pop(p, None)
        else: t[p] = q
        if bids and asks:
            bb, ba = max(bids), min(asks)
            if bb > ba + 1e-9:
                print(e['ts'], 'INV bb=%.2f ba=%.2f' % (bb, ba))
"
```

---

## 13. Conversation history TL;DR

Recent threads, newest first:

- Built `backtest_server.py` + `backtest.html` to replay NT data. Added `mT` field for market-time chart bucketing, `setInterval` gating for strategy timers, $/contract fee mode, integer qty for NQ, RTH-only checkbox, seek-by-date dropdown, contract auto-detect via meta event.
- Built `validate_export.py`. User ran it on their 23 GB file. Output confirmed data is good (BOM handled, dates May 6–14, inversions below threshold). Initial false-positive "issues" were all explained as benign: CME daily-close gaps, bid==ask touches at NQ tick size.
- Built `BfsL2Exporter.cs` NinjaScript indicator. Hit `Operation.Insert` doesn't-exist error; fixed to `Operation.Add` (NT8 enum). User confirmed it compiles and exports.
- Discussed Dubai relocation / Delaware LLC for trading. Both topics closed.
- Discussed data sources for NQ from India (Databento, Polygon, NinjaTrader). User went with NT Market Replay (free, has L2). NinjaTrader Brokerage doesn't onboard Indian residents — settled.
- Built `paper_trader.py`. Multiple iterations to match `orderflow_predictor_strategy.js` logic exactly. Found and discussed the HTML's `maxVolBps: 250 / volGuardSpreadMultiplier: 8` runtime overrides — paper_trader uses those, not the OFP file's 350/10 defaults.
- Code-reviewed `orderflow_predictor_strategy.js` for major flaws. Found: queue priority destruction in `maybeExit`, partial-market-becomes-passive-limit bug, mid-based TP/SL, position abandonment during skip conditions. User acknowledged.
- Original `terminal.html` and OFP/MRV/AutoMM strategy files were inherited at the start of the project. They're the spec.

---

## 14. If the user asks you to do something that contradicts this document

Trust the user. This is a snapshot — they may have changed their mind. But surface the contradiction so they're aware:

> "Earlier we decided X because Y — are you sure you want to do Z now?"

The user appreciates being asked once, hates being asked twice.

---

Good luck.