"""
Read-only monitor dashboard for the paper trader (http://localhost:PORT).

Auto-refreshes; no user input needed; safe to leave open or close any time.
Every handler reads live state from the `engine` module via attribute access
(engine.<name>) so the values are always current, and the few handlers that
mutate state write straight back onto the engine module.
"""
import json
from datetime import datetime
from pathlib import Path

import aiohttp
from aiohttp import web

from paper_trader_app import engine

# The dashboard page is a static HTML template served verbatim. It lives in
# server/templates/paper_dashboard.html so this module stays focused on the
# request handlers rather than a 670-line embedded string.
_DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "templates" / "paper_dashboard.html"
DASHBOARD_HTML = _DASHBOARD_PATH.read_text(encoding="utf-8")


async def http_dashboard(_req):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def http_status(_req):
    bb, ba = engine.best_bid(), engine.best_ask()
    spread_bps = None
    if bb is not None and ba is not None and ba > bb:
        spread_bps = (ba - bb) / ((ba + bb) / 2.0) * 10000.0
    pos = engine.position_qty()
    avg_entry = engine.positions.get(engine.SYMBOL, {}).get("avgEntry", 0.0)
    mid = (bb + ba) / 2.0 if (bb is not None and ba is not None) else None
    unrealized = 0.0
    if pos != 0 and mid is not None and avg_entry > 0:
        unrealized = pos * (mid - avg_entry)
    score = engine.last_signal.get("score") if isinstance(engine.last_signal, dict) else None
    return web.json_response({
        "ts": engine.now_ms(),
        "symbol": engine.SYMBOL,
        "market_type": engine.MARKET_TYPE,
        "qty": engine.QTY,
        "maker_fee_pct": engine.MAKER_FEE_PCT,
        "taker_fee_pct": engine.TAKER_FEE_PCT,
        "latency_ms": engine.LATENCY_MS,
        "best_bid": bb,
        "best_ask": ba,
        "spread_bps": spread_bps,
        "book_ready": engine.snapshot_loaded and bb is not None,
        "position": pos,
        "avg_entry": avg_entry,
        "realized_pnl": engine.realized_pnl,
        "unrealized_pnl": unrealized,
        "total_pnl": engine.realized_pnl + unrealized,
        "total_fees": engine.total_fees,
        "fill_count": engine.fill_count,
        "closed_trades": engine.closed_trades,
        "trade_count": engine.trade_count,
        "pending_market": len(engine.pending_market),
        "signal_score": score,
        "last_reason": engine.last_reason,
        "strategy_running": engine.strategy_enabled,
    })


async def http_fills(req):
    day = req.query.get("day") or datetime.now().strftime("%Y-%m-%d")
    limit = max(1, min(2000, int(req.query.get("limit", 100))))
    p = engine.DATA_DIR / f"trades_{day}.jsonl"
    out = []
    if p.exists():
        with p.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return web.json_response(out[-limit:])


async def http_days(_req):
    return web.json_response(sorted(
        p.stem.replace("trades_", "") for p in engine.DATA_DIR.glob("trades_*.jsonl")
    ))


async def http_book(req):
    n = max(1, min(50, int(req.query.get("levels", 16))))
    return web.json_response({
        "bids": [[p, q] for p, q in engine.top_bids(n)],
        "asks": [[p, q] for p, q in engine.top_asks(n)],
    })


async def http_tape(req):
    limit = max(1, min(500, int(req.query.get("limit", 100))))
    return web.json_response(list(engine.tape_hist)[-limit:])


async def http_candles(_req):
    out = list(engine.candles_hist)
    if engine.current_candle is not None:
        out.append(dict(engine.current_candle))
    # Filter any historical candle whose OHLC wasn't fully populated. Defense
    # against the price-scale getting yanked to 0 by stale/bad data.
    return web.json_response([
        c for c in out
        if c.get("open", 0) > 0 and c.get("high", 0) > 0
           and c.get("low", 0) > 0 and c.get("close", 0) > 0
    ])


async def http_reset(_req):
    """Clear positions and PnL counters (mirror of terminal.html RESET button)."""
    engine.realized_pnl = 0.0
    engine.total_fees = 0.0
    engine.closed_trades = 0
    engine.positions = {}
    if engine.SESSION_PATH.exists():
        try: engine.SESSION_PATH.unlink()
        except OSError: pass
    engine.log("RESET: positions and PnL cleared via dashboard")
    return web.json_response({"ok": True})


async def http_strategy(req):
    """GET → current running state. POST {action: 'start'|'stop'} → toggle."""
    if req.method == "POST":
        try:
            body = await req.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError):
            return web.json_response({"error": "invalid JSON body"}, status=400)
        action = body.get("action")
        if action == "start":
            engine.strategy_enabled = True
            engine.log("strategy STARTED via dashboard")
        elif action == "stop":
            engine.strategy_enabled = False
            engine.cancel_pending_market_orders()
            engine.log("strategy STOPPED via dashboard")
        else:
            return web.json_response({"error": "action must be 'start' or 'stop'"}, status=400)
    return web.json_response({"running": engine.strategy_enabled})


async def http_clear_saved(_req):
    removed = 0
    for p in list(engine.DATA_DIR.glob("trades_*.jsonl")) + list(engine.DATA_DIR.glob("trades_*.csv")):
        try:
            p.unlink()
            removed += 1
        except OSError:
            pass
    engine.log(f"clear-saved: removed {removed} file(s)")
    return web.json_response({"removed": removed})


async def http_config(req):
    """GET → current runtime config. POST → update (JSON body: {"qty": <number>})."""
    if req.method == "POST":
        try:
            body = await req.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError):
            return web.json_response({"error": "invalid JSON body"}, status=400)
        try:
            new = engine.update_config(qty=body.get("qty"))
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        return web.json_response(new)
    return web.json_response({"qty": engine.QTY, "max_position": engine.MAX_POSITION})


async def start_http_server():
    app = web.Application()
    app.router.add_get("/", http_dashboard)
    app.router.add_get("/api/status", http_status)
    app.router.add_get("/api/fills", http_fills)
    app.router.add_get("/api/days", http_days)
    app.router.add_get("/api/book", http_book)
    app.router.add_get("/api/tape", http_tape)
    app.router.add_get("/api/candles", http_candles)
    app.router.add_get("/api/config", http_config)
    app.router.add_post("/api/config", http_config)
    app.router.add_post("/api/reset", http_reset)
    app.router.add_get("/api/strategy", http_strategy)
    app.router.add_post("/api/strategy", http_strategy)
    app.router.add_post("/api/clear-saved", http_clear_saved)
    runner = web.AppRunner(app)
    await runner.setup()
    # Try the user's requested port first; fall back if privileged.
    for port in (engine.HTTP_PORT_PRIMARY, engine.HTTP_PORT_FALLBACK):
        try:
            site = web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            engine.log(f"dashboard listening on http://localhost:{port}")
            return runner, port
        except PermissionError:
            engine.log(f"port {port} requires elevated privileges; trying fallback")
        except OSError as e:
            engine.log(f"port {port} not bindable ({e}); trying fallback")
    engine.log("could not bind any HTTP port; running headless")
    return runner, None
