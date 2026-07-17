// BfsMarketMaker.cs — NinjaTrader 8 Strategy  (UNMANAGED order handling)
//
// Passive market maker: quotes 1 lot at the best bid and 1 lot at the best ask,
// re-quotes as the touch moves, capped by an inventory limit. Captures the
// 1-tick spread when both sides fill; bleeds via inventory when price trends.
//
// *** THIS IS THE STRATEGY TO VALIDATE — NOT A PROVEN WINNER. ***
// In our offline sim it looked very profitable, but that result depended
// entirely on fill assumptions our data could not verify. The whole point of
// running it here is that NinjaTrader places these orders in the reconstructed
// book and fills them with its own engine. Expect to debug it in Sim, and
// remember NT's historical limit-fill model is itself OPTIMISTIC — the
// trustworthy test is Market Replay PLAYBACK and real-time Sim (see steps),
// not the Strategy Analyzer.
//
// KNOWN THING TO HARDEN: when the net position crosses zero the correct
// OrderAction flips (Buy<->BuyToCover, Sell<->SellShort). This skeleton cancels
// and re-submits with the right action, but verify the behaviour in Sim before
// trusting any PnL.
//
// Install: Documents\NinjaTrader 8\bin\Custom\Strategies\  then compile (F5).
#region Using declarations
using System;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
    public class BfsMarketMaker : Strategy
    {
        private Order buyOrder    = null;
        private Order sellOrder   = null;
        private Order flatOrder   = null;          // market order used to cut adverse inventory
        private DateTime cooldownUntil = DateTime.MinValue;

        [NinjaScriptProperty] public int Qty          { get; set; }
        [NinjaScriptProperty] public int MaxInventory { get; set; }
        [NinjaScriptProperty] public int StopLossTicks { get; set; }   // flatten if inventory runs this far against us
        [NinjaScriptProperty] public int CooldownSeconds { get; set; } // pause quoting after a stop, so we don't reload into the trend

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name        = "BfsMarketMaker";
                Description = "Passive two-sided market maker (validate in Sim/Replay).";
                Calculate   = Calculate.OnEachTick;
                IsUnmanaged = true;                        // we manage every order
                IsExitOnSessionCloseStrategy = true;
                ExitOnSessionCloseSeconds    = 30;
                Qty = 1; MaxInventory = 5;
                StopLossTicks = 10; CooldownSeconds = 30;
                DefaultQuantity = 1;
                // Don't let a single order rejection halt the strategy (the usual
                // reason a hand-rolled unmanaged MM "does a few trades then stops").
                RealtimeErrorHandling = RealtimeErrorHandling.IgnoreAllErrors;
            }
            else if (State == State.Realtime)
            {
                // If you never see this line in the NinjaScript Output window, the
                // strategy never reached real-time => it's not getting a live/Playback
                // feed, which is why no orders appear.
                Print(Name + ": entered REAL-TIME — quoting is now active.");
            }
        }

        private int NetPos()
        {
            if (Position.MarketPosition == MarketPosition.Long)  return  Position.Quantity;
            if (Position.MarketPosition == MarketPosition.Short) return -Position.Quantity;
            return 0;
        }

        private bool Working(Order o)
        {
            return o != null && (o.OrderState == OrderState.Working
                || o.OrderState == OrderState.Accepted
                || o.OrderState == OrderState.Submitted
                || o.OrderState == OrderState.ChangePending);
        }

        protected override void OnBarUpdate()
        {
            if (CurrentBars[0] < 1) return;

            double bid = GetCurrentBid();
            double ask = GetCurrentAsk();

            // Debug heartbeat: one line per bar so you can confirm it's alive and
            // that bid/ask are real. If bid/ask are 0 here, there's no Level-1 data
            // (not connected / not playing) — that's why nothing quotes.
            if (IsFirstTickOfBar)
                Print(String.Format("{0}  bid={1} ask={2} pos={3}", Time[0], bid, ask, NetPos()));

            if (bid <= 0 || ask <= 0 || ask <= bid) return;

            int pos = NetPos();

            // ---- RISK CONTROL: hard inventory stop ----
            // A passive MM can't cut a losing position with passive orders (in a
            // trend the reducing side never fills). So if inventory runs more than
            // StopLossTicks against our average entry, cross the spread and flatten
            // at market, then pause (cooldown) so we don't immediately reload into
            // the same trend.
            if (pos != 0 && StopLossTicks > 0)
            {
                double avg = Position.AveragePrice;
                // mark at the price we'd actually exit into (cross the spread)
                double exitPx = pos > 0 ? bid : ask;
                double adverseTicks = (pos > 0 ? (avg - exitPx) : (exitPx - avg)) / TickSize;
                if (adverseTicks >= StopLossTicks)
                {
                    if (Working(buyOrder))  CancelOrder(buyOrder);
                    if (Working(sellOrder)) CancelOrder(sellOrder);
                    if (flatOrder == null)
                    {
                        OrderAction fa = pos > 0 ? OrderAction.Sell : OrderAction.BuyToCover;
                        flatOrder = SubmitOrderUnmanaged(0, fa, OrderType.Market,
                                        Math.Abs(pos), 0, 0, "", "MMflat");
                        Print(String.Format("{0}  STOP: flattening {1} @ ~{2} (adverse {3:0.0} ticks)",
                                             Time[0], pos, exitPx, adverseTicks));
                    }
                    cooldownUntil = Time[0].AddSeconds(CooldownSeconds);
                    return;
                }
            }

            // ---- COOLDOWN: after a stop, stop quoting briefly ----
            if (Time[0] < cooldownUntil)
            {
                if (Working(buyOrder))  CancelOrder(buyOrder);
                if (Working(sellOrder)) CancelOrder(sellOrder);
                return;
            }

            // Recover any leaked reference: if an order object is non-null but is
            // in a dead/unknown state (not working, not pending), drop it so we
            // re-quote next tick instead of getting permanently stuck.
            if (buyOrder  != null && !Working(buyOrder))  buyOrder  = null;
            if (sellOrder != null && !Working(sellOrder)) sellOrder = null;

            // ---- bid side: buy 1 lot at the touch, unless we're already max long ----
            if (pos < MaxInventory)
            {
                OrderAction want = pos < 0 ? OrderAction.BuyToCover : OrderAction.Buy;
                if (buyOrder == null)
                    buyOrder = SubmitOrderUnmanaged(0, want, OrderType.Limit, Qty, bid, 0, "", "MMbid");
                else if (buyOrder.OrderState == OrderState.Working)
                {
                    if (buyOrder.OrderAction != want)        CancelOrder(buyOrder);  // action flipped across flat
                    else if (buyOrder.LimitPrice != bid)     ChangeOrder(buyOrder, Qty, bid, 0);
                }
                // else: still Submitted/Accepted/ChangePending -> wait (avoid churn rejects)
            }
            else if (buyOrder != null && buyOrder.OrderState == OrderState.Working)
                CancelOrder(buyOrder);

            // ---- ask side: sell 1 lot at the touch, unless we're already max short ----
            if (pos > -MaxInventory)
            {
                OrderAction want = pos > 0 ? OrderAction.Sell : OrderAction.SellShort;
                if (sellOrder == null)
                    sellOrder = SubmitOrderUnmanaged(0, want, OrderType.Limit, Qty, ask, 0, "", "MMask");
                else if (sellOrder.OrderState == OrderState.Working)
                {
                    if (sellOrder.OrderAction != want)       CancelOrder(sellOrder);
                    else if (sellOrder.LimitPrice != ask)    ChangeOrder(sellOrder, Qty, ask, 0);
                }
            }
            else if (Working(sellOrder))
                CancelOrder(sellOrder);
        }

        protected override void OnOrderUpdate(Order order, double limitPrice, double stopPrice,
            int quantity, int filled, double averageFillPrice, OrderState orderState, DateTime time,
            ErrorCode error, string nativeError)
        {
            if (orderState == OrderState.Rejected)
                Print(String.Format("{0}  ORDER REJECTED: {1} {2} {3}@{4} -> {5} / {6}",
                    time, order.Name, order.OrderAction, quantity, limitPrice, error, nativeError));

            // Release the reference when an order dies so we re-quote next tick.
            if (order == buyOrder && (orderState == OrderState.Cancelled
                || orderState == OrderState.Filled || orderState == OrderState.Rejected))
                buyOrder = null;
            if (order == sellOrder && (orderState == OrderState.Cancelled
                || orderState == OrderState.Filled || orderState == OrderState.Rejected))
                sellOrder = null;
            if (order == flatOrder && (orderState == OrderState.Cancelled
                || orderState == OrderState.Filled || orderState == OrderState.Rejected))
                flatOrder = null;
        }
    }
}
