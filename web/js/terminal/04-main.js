document.getElementById('book-panel').addEventListener('click', (e) => {
  const row = e.target.closest('.book-row');
  if (row) { document.getElementById('price-input').value = row.dataset.price; document.getElementById('side-select').value = row.dataset.side; }
});
document.getElementById('join-bid-btn').addEventListener('click', joinBid);
document.getElementById('join-ask-btn').addEventListener('click', joinAsk);
document.getElementById('place-btn').addEventListener('click', submitOrder);
document.getElementById('strategy-start-btn').addEventListener('click', startSelectedStrategy);
document.getElementById('strategy-stop-btn').addEventListener('click', stopSelectedStrategy);
window.addEventListener('automm-status', (e) => { const detail = e.detail; if (!detail?.running) return; const activeCount = detail.activeOwnedOrders?.length || 0; const color = detail.lastReason === 'quoting' ? 'var(--bid)' : 'var(--yellow)'; setMsg(`MM ${detail.lastReason} (${activeCount} live)`, color); });
window.addEventListener('ofp-status', (e) => { const detail = e.detail; if (!detail?.running) return; const activeCount = detail.activeOwnedOrders?.length || 0; const color = /predict|take profit/.test(detail.lastReason) ? 'var(--bid)' : 'var(--yellow)'; setMsg(`OFP ${detail.lastReason} (${activeCount} live)`, color); });
window.addEventListener('mrv-status', (e) => { const detail = e.detail; if (!detail?.running) return; const activeCount = detail.activeOwnedOrders?.length || 0; const color = /fade|mean exit|take profit/.test(detail.lastReason) ? 'var(--bid)' : 'var(--yellow)'; setMsg(`MRV ${detail.lastReason} (${activeCount} live)`, color); });
document.getElementById('cancel-btn').addEventListener('click', () => { if (!state.selectedOrderId) { setMsg('⚠ Select an order first', 'var(--yellow)'); return; } const ok = cancelOrder(state.selectedOrderId); setMsg(ok ? `Cancelled #${state.selectedOrderId}` : `Not found: ${state.selectedOrderId}`, 'var(--yellow)'); state.selectedOrderId = null; });
document.getElementById('cancel-all-btn').addEventListener('click', () => { const n = cancelAll(); setMsg(`Cancelled ${n} order(s)`, 'var(--yellow)'); });
document.getElementById('clr-pos-btn').addEventListener('click', () => { state.positions = {}; state.realizedPnl = 0; state.totalFees = 0; state.closedTrades = 0; refreshPositions(); });
document.getElementById('sym-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') changeSymbol(e.target.value); });
document.getElementById('price-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') submitOrder(); });
document.getElementById('qty-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') submitOrder(); });
document.getElementById('qty-input').addEventListener('input', syncStrategyQty);
document.getElementById('strategy-exec-select').addEventListener('change', syncStrategyExecutionMode);
document.getElementById('maker-fee-input').addEventListener('input', syncFeeConfig);
document.getElementById('taker-fee-input').addEventListener('input', syncFeeConfig);
document.getElementById('latency-input').addEventListener('input', syncLatencyConfig);
document.getElementById('save-trades-btn').addEventListener('click', saveSessionTrades);
document.getElementById('clear-saved-trades-btn').addEventListener('click', clearSavedTrades);
document.getElementById('analysis-btn').addEventListener('click', openAnalysisPage);
document.addEventListener('keydown', (e) => { if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return; if (e.key === 'b' || e.key === 'B') joinBid(); if (e.key === 'a' || e.key === 'A') joinAsk(); if (e.key === 'c' || e.key === 'C') { const n = cancelAll(); setMsg(`Cancelled ${n} order(s)`, 'var(--yellow)'); } if (e.key === 'Escape') { if (state.selectedOrderId) { cancelOrder(state.selectedOrderId); state.selectedOrderId = null; } } });

// boot
detectDecimals('BTCUSDT');
connect('BTCUSDT');
refreshPositions();
