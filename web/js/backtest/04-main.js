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

// ═══════════════════════════════════════════════════════════════
//  BOOT — backtest-aware
//  Pulls contract metadata from the backtest server, configures
//  symbol/price-dec/qty-dec/fee-model accordingly, then starts WS.
// ═══════════════════════════════════════════════════════════════
async function bootBacktest() {
  let s;
  try {
    s = await fetch('/api/status').then(r => r.json());
  } catch (e) {
    s = { symbol: 'NQ', instrument: 'UNKNOWN', tick_size: 0.25, price_dec: 2, is_futures: true };
  }

  // Cache contract info globally so feeForFill() can switch model.
  window.__backtest.contract = {
    symbol: s.symbol || 'NQ',
    instrument: s.instrument || 'UNKNOWN',
    tick_size: s.tick_size || 0.25,
    price_dec: (typeof s.price_dec === 'number') ? s.price_dec : 2,
    is_futures: s.is_futures !== false,
  };

  // Apply to state
  state.symbol   = window.__backtest.contract.symbol;
  state.priceDec = window.__backtest.contract.price_dec;
  state.qtyDec   = window.__backtest.contract.is_futures ? 0 : 4;

  // Header label
  const hdrSym = document.getElementById('header-sym');
  if (hdrSym) hdrSym.textContent =
    `${state.symbol} (BACKTEST · ${window.__backtest.contract.instrument})`;
  const symInput = document.getElementById('sym-input');
  if (symInput) {
    symInput.value = state.symbol;
    symInput.disabled = true;
    symInput.title = 'Symbol is fixed by the replayed file';
  }

  // Qty input: integer-only when futures, with sensible default of 1 contract
  const qtyIn = document.getElementById('qty-input');
  if (qtyIn && window.__backtest.contract.is_futures) {
    qtyIn.step = '1';
    qtyIn.min  = '1';
    qtyIn.value = qtyIn.value && parseFloat(qtyIn.value) >= 1
      ? String(Math.max(1, Math.floor(parseFloat(qtyIn.value))))
      : '1';
    qtyIn.placeholder = '1';
    // Round any non-integer typing back to int on blur
    qtyIn.addEventListener('blur', () => {
      const v = parseFloat(qtyIn.value);
      if (Number.isFinite(v)) qtyIn.value = String(Math.max(1, Math.round(v)));
      // Re-sync running strategy
      if (typeof syncStrategyQty === 'function') syncStrategyQty();
    });
  }

  // Fee inputs: re-label and default to $/contract for futures
  const makerIn = document.getElementById('maker-fee-input');
  const takerIn = document.getElementById('taker-fee-input');
  if (window.__backtest.contract.is_futures) {
    if (makerIn) {
      makerIn.title = 'Maker fee in $/contract per side (CME has no maker/taker split — use the same value)';
      if (!makerIn.dataset.touched) makerIn.value = '1.29';
    }
    if (takerIn) {
      takerIn.title = 'Taker fee in $/contract per side (NQ retail ≈ $1.29 all-in on NT Lifetime)';
      if (!takerIn.dataset.touched) takerIn.value = '1.29';
    }
    if (typeof syncFeeConfig === 'function') syncFeeConfig();
  }
  // Mark inputs as touched once user modifies them so we don't overwrite later
  for (const el of [makerIn, takerIn]) {
    if (el) el.addEventListener('input', () => { el.dataset.touched = '1'; });
  }

  // Start WS — backtest server will simply not send events until Play.
  connect(state.symbol);
  refreshPositions();
}
bootBacktest();

// ═══════════════════════════════════════════════════════════════
//  BACKTEST PLAY BAR — wired to /api/control + /api/status
// ═══════════════════════════════════════════════════════════════
(function () {
  const playBtn   = document.getElementById('bt-play-btn');
  const pauseBtn  = document.getElementById('bt-pause-btn');
  const speedSel  = document.getElementById('bt-speed-select');
  const rthChk    = document.getElementById('bt-rth');
  const seekSel   = document.getElementById('bt-seek-select');
  const dateEl    = document.getElementById('bt-date');
  const fillEl    = document.getElementById('bt-progress-fill');
  const pctEl     = document.getElementById('bt-progress-pct');
  const noteEl    = document.getElementById('bt-fidelity-note');
  const stratStartBtn = document.getElementById('strategy-start-btn');

  async function ctrl(action, extra) {
    const body = Object.assign({ action }, extra || {});
    try {
      const r = await fetch('/api/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) console.warn('[backtest] control failed:', await r.text());
      return await r.json().catch(() => ({}));
    } catch (e) {
      console.warn('[backtest] control error:', e);
    }
  }

  playBtn.addEventListener('click', () => ctrl('play'));
  pauseBtn.addEventListener('click', () => ctrl('pause'));
  speedSel.addEventListener('change', () => {
    const speed = parseFloat(speedSel.value) || 1;
    ctrl('set_speed', { speed });
    noteEl.classList.toggle('show', speed > 1);
  });
  rthChk.addEventListener('change', () => {
    ctrl('set_rth_only', { rth_only: !!rthChk.checked });
  });
  seekSel.addEventListener('change', async () => {
    const date = seekSel.value;
    if (!date) return;
    await ctrl('seek', { date });
    seekSel.value = '';                   // reset to placeholder
  });

  // Backtest-aware strategy gating: prevent starting a strategy with the
  // backtest paused (the strategy would just spin on stale state).
  function refreshStrategyGate(playing) {
    const anyRunning =
      (window.OrderFlowPredictor?.status?.().running) ||
      (window.MeanReversionStrategy?.status?.().running) ||
      (window.AutoMM?.status?.().running);
    // Don't fight the existing strategy enable/disable logic — only forbid
    // STARTING from a paused state. Once running, the timer is auto-gated
    // by the setInterval override at the top of this file.
    if (!playing && !anyRunning) {
      if (stratStartBtn) {
        stratStartBtn.disabled = true;
        stratStartBtn.title = 'Press Play to start the backtest before running a strategy';
      }
    } else {
      if (stratStartBtn) {
        stratStartBtn.disabled = anyRunning;
        stratStartBtn.title = '';
      }
    }
  }

  let lastDatesKey = '';
  async function pollStatus() {
    try {
      const s = await fetch('/api/status').then(r => r.json());
      window.__backtest.playing = !!s.playing;
      playBtn.disabled  = !!s.playing;
      pauseBtn.disabled = !s.playing;
      refreshStrategyGate(s.playing);

      if (Math.abs(parseFloat(speedSel.value) - s.speed) > 1e-9) {
        speedSel.value = String(s.speed);
        noteEl.classList.toggle('show', s.speed > 1);
      }
      if (rthChk.checked !== !!s.rth_only) rthChk.checked = !!s.rth_only;

      // Refresh seek list when available_dates changes
      const rthSet = new Set(s.rth_dates || []);
      const datesKey = (s.available_dates || []).join(',') + '|' + (s.rth_dates || []).join(',');
      if (datesKey !== lastDatesKey) {
        lastDatesKey = datesKey;
        const opts = ['<option value="">— pick a date —</option>'];
        for (const d of (s.available_dates || [])) {
          const tag = rthSet.has(d) ? ' (RTH 9:30 ET)' : '';
          opts.push(`<option value="${d}">${d}${tag}</option>`);
        }
        seekSel.innerHTML = opts.join('');
      }

      dateEl.textContent = s.original_current_human
        ? `${s.original_current_human} UTC`
        : (s.playing ? 'starting…' : '— paused —');
      const pct = s.progress_pct || 0;
      fillEl.style.width = pct.toFixed(3) + '%';
      pctEl.textContent  = pct.toFixed(3) + '%';
    } catch (e) {
      dateEl.textContent = '⚠ server unreachable';
      window.__backtest.playing = false;
    }
  }
  pollStatus();
  _rawSetInterval(pollStatus, 500);     // use raw — don't gate ourselves
})();
