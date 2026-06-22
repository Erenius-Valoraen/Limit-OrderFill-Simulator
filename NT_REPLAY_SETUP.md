# NinjaTrader → paper_trader.py — L2 replay setup

End-to-end pipeline for testing your OFP strategy on real CME NQ L2 data,
using free NinjaTrader Market Replay as the data source.

```
NT Market Replay  ──►  BfsL2Exporter.cs  ──►  bfs_l2_export.jsonl
                                                     │
                                                     ▼
                              paper_trader.py (DATA_SOURCE="replay:...")
                                                     │
                                                     ▼
                              live dashboard + day-keyed CSV/JSONL fills
```

---

## One-time prerequisites

You need NinjaTrader on a Windows machine (or Mac via Parallels / UTM with
Windows ARM). NT does not run natively on macOS; CrossOver works for the
platform but L2 streams are flaky there.

1. Download **NinjaTrader 8** (free) from https://ninjatrader.com/PlatformDirect
2. Install. The free version is fully sufficient for Market Replay — you do
   NOT need the $1,099 Lifetime License for this workflow.
3. Open NT. The Control Center will appear.

---

## Step 1 — Install the exporter indicator

1. Copy **`BfsL2Exporter.cs`** (in this project folder) to:
   ```
   <Windows>\Documents\NinjaTrader 8\bin\Custom\Indicators\BfsL2Exporter.cs
   ```
   If you're on Mac with the project in iCloud, the file is at
   `BinanceFillSimulator/BfsL2Exporter.cs` — copy across to the Windows VM.

2. In NT, open the **NinjaScript Editor**:
   `Control Center → New → NinjaScript Editor` (or press F11)

3. In the editor sidebar, expand **Indicators** → you'll see `BfsL2Exporter`.
   Open it.

4. Press **F5** to compile. The Output window at the bottom should say
   "0 errors, 0 warnings". If it errors out, paste the error in chat.

---

## Step 2 — Download NQ Market Replay data

1. Control Center → `Connections → Market Replay Connection`. Click **Connect**.

2. Control Center → `Tools → Historical Data`.

3. In the Historical Data window:
   - **Instrument**: type `NQ 03-26` (front-month NQ futures). Use whichever
     contract month is current; NT auto-suggests.
   - **Type**: tick `Last`, `Bid`, `Ask`, **AND `Market Depth`**. Market Depth
     is the L2 channel — without it the exporter has no depth events.
   - **Date range**: pick a recent session (e.g. yesterday, 24 hours).
   - Click **Download**.

   This may take a few minutes; depth data is bulky. Progress shows in the
   bottom status bar.

4. Once download completes, the data is on disk at
   `Documents\NinjaTrader 8\db\replay\<symbol>\`.

---

## Step 3 — Run the replay with the exporter attached

1. Control Center → `New → Chart`. Symbol: `NQ 03-26`, type **Tick**, 1 tick.

2. On the chart, right-click → `Indicators` → find `BfsL2Exporter` in the
   list → click **Add**. Configure:
   - **Output File**: leave default (`Documents\NinjaTrader 8\bfs_l2_export.jsonl`),
     or set to your own path on the iCloud shared folder so the Mac side
     sees it immediately.
   - **Flush Every (ms)**: 500 (fine)
   - **Verbose Log**: tick if you want event counts in the NinjaScript Output
     window.
   Click **OK**.

3. Switch the chart's connection to **Market Replay Connection**:
   chart toolbar → connection dropdown → Market Replay.

4. Open the **Playback Control** panel:
   `Control Center → New → Playback Control`.
   - **Start date**: morning of the session you downloaded
   - **End date**: end of session
   - **Speed**: 1× for realistic timing, 5×–10× to compress hours into minutes
   - Click **Play**.

5. The exporter starts writing immediately. Open the output file in a tail
   tool to confirm events are landing:
   ```
   # Windows PowerShell
   Get-Content "$env:USERPROFILE\Documents\NinjaTrader 8\bfs_l2_export.jsonl" -Wait -Tail 5
   ```

   Each line is one event:
   ```json
   {"type":"depth","ts":1781407812345,"side":"BID","op":"UPD","px":21900.25,"qty":12,"pos":0}
   {"type":"trade","ts":1781407812350,"px":21900.50,"qty":1,"side":"BUY"}
   ```

6. Let it run as long as you want data for. Stop the playback when done; the
   file is closed cleanly.

---

## Step 4 — Move the JSONL to Mac & configure paper_trader.py

1. Copy `bfs_l2_export.jsonl` from the Windows machine to
   `BinanceFillSimulator/paper_data/nq_replay.jsonl` on the Mac (drag-drop
   via shared folder, USB, scp, whatever).

2. Edit the top of **`paper_trader.py`**:

   ```python
   SYMBOL = "NQH6"                                  # whatever contract you downloaded
   DATA_SOURCE = "replay:paper_data/nq_replay.jsonl"
   REPLAY_SPEED = 10.0                              # 10× = compress 1 hr of data into 6 min
   REPLAY_LOOP  = False                             # set True to loop forever

   CONTRACT_MODEL = "futures"                       # use $/contract fee model
   FIXED_FEE_PER_CONTRACT = 1.29                    # NT Lifetime tier; adjust for your broker
   QTY = 1.0                                        # 1 NQ contract per trade
   ```

3. Important: positions and PnL from the prior BTC run will leak across
   unless you wipe them. Either:
   ```bash
   rm paper_data/session_state.json
   ```
   …or click **RESET** in the dashboard after start.

4. Run as usual:
   ```bash
   pip3 install -r requirements.txt   # if not already
   caffeinate -di python3 paper_trader.py
   ```

5. Open http://localhost:8000 — the dashboard will show NQ L2 instead of BTC,
   strategy will trade against the replayed book, fills land in
   `paper_data/trades_<today>.jsonl` and `.csv` exactly as before.

---

## What you get

- **Free CME NQ L2 data** — NT's Market Replay covers roughly 6 months back
  for major futures. Re-download anytime.
- **Same OFP strategy logic** that the BTC run uses — no code duplication.
- **NQ-correct fee model** — $1.29/contract per side instead of % of notional.
- **Reproducible research** — same JSONL replayed at any speed gives identical
  fills, so you can A/B test strategy parameter changes.
- **Same dashboard** — book, tape, chart, blotter, all work identically.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Exporter compiles but file stays empty | Chart connection isn't Market Replay, or Market Depth wasn't checked at download time |
| File grows but only `trade` events, no `depth` | Same — Market Depth missing from the download |
| File grows but Python sees no events | Path mismatch; check `DATA_SOURCE` in `paper_trader.py` |
| Python errors on parsing JSON | Open the file, find the bad line, post here |
| Strategy doesn't trade | Score never crosses threshold; NQ behaves differently than BTC. Try lowering `SIGNAL_THRESHOLD` to e.g. 0.3 for initial validation |
| Fills look wrong magnitude | Forgot to set `CONTRACT_MODEL = "futures"`; PnL is being computed as % of notional |
| Want to replay faster | Raise `REPLAY_SPEED` to 50 or 100 — the strategy still runs at its 350ms tick cadence but events fly by faster |

---

## Notes on data format

The exporter writes JSON Lines (one event per line). Three event types:

```jsonc
{"type":"meta",  "ts":<ms>, "event":"start|replay|realtime", "instrument":"NQ 03-26", "tickSize":0.25}
{"type":"depth", "ts":<ms>, "side":"BID|ASK", "op":"INS|UPD|REM", "px":<float>, "qty":<int>, "pos":<level>}
{"type":"trade", "ts":<ms>, "px":<float>, "qty":<int>, "side":"BUY|SELL"}
```

- `op=INS` adds a new price level
- `op=UPD` changes qty at an existing level
- `op=REM` removes a level
- `pos` is the depth rank (0 = best, 1 = second best, …)
- Trade `side` is the *aggressor* — `BUY` means a market buy hit the offer.
  Inferred from comparison with last seen best bid/ask.

If you ever need to re-implement a different consumer, the format is stable.
