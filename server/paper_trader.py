#!/usr/bin/env python3
"""
Headless paper-trading service — entry point.

Run it:
    pip install -r requirements.txt
    python server/paper_trader.py

The implementation lives in the `paper_trader_app` package next to this file:
    engine.py   order book, fill accounting, persistence, strategy, feed loops
    web.py      read-only monitor dashboard (http://localhost:1000 or :8000)
    app.py      orchestration + entry point (run)

Output (under data/paper_data/): day-keyed JSONL + CSV fills, session_state.json
snapshot for crash-safe restart, and runner.log. Stop with Ctrl-C — session
state is flushed on shutdown.
"""
from paper_trader_app.app import run

if __name__ == "__main__":
    run()
