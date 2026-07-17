#!/usr/bin/env python3
"""
Headless paper trader. Entry point only; the real code is in paper_trader_app/.

    pip install -r requirements.txt
    python server/paper_trader.py

    engine.py   book, fills, persistence, strategy, feed loops
    web.py      monitor dashboard (localhost:1000, or :8000 if that's taken)
    app.py      startup + main loop

Fills land in data/paper_data/ (jsonl + csv), with a session snapshot so a
restart picks up where it left off. Ctrl-C flushes and exits.
"""
from paper_trader_app.app import run

if __name__ == "__main__":
    run()
