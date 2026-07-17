"""
Startup and main loop. Wires the engine (feeds + strategy) to the web
dashboard and runs everything on one asyncio loop.
"""
import asyncio
import signal as signal_mod
import sys

from paper_trader_app import engine
from paper_trader_app import web as webui


def install_shutdown_handlers(loop):
    def _handle(sig):
        engine.log(f"signal {sig.name} received — flushing session and exiting")
        engine.shutdown_requested = True
        engine.persist_session()
    for sig in (signal_mod.SIGINT, signal_mod.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle, sig)
        except NotImplementedError:
            pass  # Windows


async def main():
    engine.log("=" * 70)
    engine.log(f"start  symbol={engine.SYMBOL} market={engine.MARKET_TYPE} qty={engine.QTY} "
               f"taker={engine.TAKER_FEE_PCT}% latency={engine.LATENCY_MS}ms data={engine.DATA_DIR}")
    engine.log("=" * 70)
    engine.load_runtime_config()
    engine.load_session()
    install_shutdown_handlers(asyncio.get_running_loop())
    http_runner, _ = await webui.start_http_server()
    try:
        if engine.DATA_SOURCE.startswith("replay:"):
            replay_path = engine.DATA_SOURCE[len("replay:"):]
            engine.log(f"DATA_SOURCE=replay path={replay_path} speed={engine.REPLAY_SPEED}x")
            await asyncio.gather(
                engine.replay_loop(replay_path),
                engine.strategy_loop(),
                engine.status_loop(),
            )
        else:
            engine.log("DATA_SOURCE=live (Binance WebSocket)")
            await asyncio.gather(
                engine.depth_ws_loop(),
                engine.trade_ws_loop(),
                engine.strategy_loop(),
                engine.status_loop(),
            )
    finally:
        if http_runner is not None:
            await http_runner.cleanup()
        engine.persist_session()
        engine.log("session flushed; goodbye")


def run():
    """Run the paper trader until interrupted, flushing session state on exit."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        engine.persist_session()
        engine.log("KeyboardInterrupt — session flushed")
        sys.exit(0)
