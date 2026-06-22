// VoltyExpanClose.cs — NinjaTrader 8 Strategy
// Faithful port of the TradingView "Volty Expan Close" stop-and-reverse
// volatility breakout (the momentum strategy that was correctly-signed on NQ).
//
//   atrs = SMA(TrueRange, Length) * NumATRs
//   buy-STOP at Close + atrs   (go long on an upside break)
//   sell-STOP at Close - atrs  (go short on a downside break)
//   always-in-market, reverses on the opposite stop.
//
// Run it on a 1-MINUTE chart (Calculate.OnBarClose => 1-min bars).
// In our offline tests 1-min was break-even-to-marginally-positive (the one
// profitable run was driven by a single trend day) — treat this as "right
// direction, needs many more days to prove," NOT a sure thing.
//
// Install: copy to  Documents\NinjaTrader 8\bin\Custom\Strategies\
// then compile in the NinjaScript Editor (F5).
#region Using declarations
using System;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
    public class VoltyExpanClose : Strategy
    {
        [NinjaScriptProperty]
        public int Length { get; set; }

        [NinjaScriptProperty]
        public double NumATRs { get; set; }

        [NinjaScriptProperty]
        public int Qty { get; set; }

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name        = "VoltyExpanClose";
                Description = "Volatility-expansion close stop-and-reverse breakout (NQ).";
                Calculate   = Calculate.OnBarClose;        // act on completed bars
                EntriesPerDirection = 1;
                EntryHandling       = EntryHandling.AllEntries;
                IsExitOnSessionCloseStrategy = true;       // flat by the session close
                ExitOnSessionCloseSeconds    = 30;
                BarsRequiredToTrade = 10;

                Length  = 5;          // TradingView default
                NumATRs = 0.75;       // TradingView default (try 1.5 for fewer/longer holds)
                Qty     = 1;
            }
        }

        // True range that matches Pine's ta.tr (uses prior close once available)
        private double Tr(int barsAgo)
        {
            if (CurrentBar - barsAgo < 1)
                return High[barsAgo] - Low[barsAgo];
            double pc = Close[barsAgo + 1];
            double a = High[barsAgo] - Low[barsAgo];
            double b = Math.Abs(High[barsAgo] - pc);
            double c = Math.Abs(Low[barsAgo] - pc);
            return Math.Max(a, Math.Max(b, c));
        }

        protected override void OnBarUpdate()
        {
            if (CurrentBar < Length) return;

            double sumTr = 0;
            for (int i = 0; i < Length; i++) sumTr += Tr(i);
            double atrs = (sumTr / Length) * NumATRs;

            // Re-posted every bar at the new straddle prices (managed orders
            // update in place by signal name). Opposite fill reverses the
            // position => stop-and-reverse.
            EnterLongStopMarket(Qty, Close[0] + atrs, "LE");
            EnterShortStopMarket(Qty, Close[0] - atrs, "SE");
        }
    }
}
