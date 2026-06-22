// BfsL2Exporter.cs — NinjaScript indicator that streams L2 depth + trades
// to a JSONL file on disk. Drop this file into:
//   Documents\NinjaTrader 8\bin\Custom\Indicators\BfsL2Exporter.cs
// Then compile inside NT (F5 in the NinjaScript Editor) and add the
// indicator to a chart that has Market Depth enabled.
//
// Each event is one JSON object per line; format is intentionally simple so
// the Python replay bridge (paper_trader.py REPLAY_FILE mode) can stream it.
//
// Event shapes:
//   {"type":"depth","ts":<ms_epoch>,"side":"BID"|"ASK","op":"INS"|"UPD"|"REM","px":<price>,"qty":<size>,"pos":<level>}
//   {"type":"trade","ts":<ms_epoch>,"px":<price>,"qty":<size>,"side":"BUY"|"SELL"}
//   {"type":"meta","ts":<ms_epoch>,"event":"start"|"realtime"|"replay","instrument":"NQ 03-26","tickSize":0.25}
//
// Notes:
//   - Operates from State.DataLoaded onwards so historical Market Replay data
//     also flushes through the same handlers.
//   - Buffered StreamWriter; flushes every FlushIntervalMs (default 500ms)
//     and on every trade so the tail isn't lost if NT crashes.
//   - Output path defaults to Documents\NinjaTrader 8\bfs_l2_export.jsonl.
//     Override via the indicator's "Output File" property.

#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
#endregion

namespace NinjaTrader.NinjaScript.Indicators
{
    public class BfsL2Exporter : Indicator
    {
        private StreamWriter writer;
        private readonly object writerLock = new object();
        private System.Threading.Timer flushTimer;
        private double lastBestBid;
        private double lastBestAsk;
        private long eventCount;
        private long startTicks;
        private static readonly DateTime EpochUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        [NinjaScriptProperty]
        [Display(Name = "Output File", Order = 1, GroupName = "Parameters",
                 Description = "Absolute path to the JSONL file. Will be appended.")]
        public string OutputFile { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Flush Every (ms)", Order = 2, GroupName = "Parameters",
                 Description = "How often the buffered writer flushes to disk.")]
        public int FlushIntervalMs { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Verbose Log", Order = 3, GroupName = "Parameters",
                 Description = "Print event counts to NinjaScript Output window.")]
        public bool VerboseLog { get; set; }

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description = "Export L2 depth + trades to JSONL for the BFS paper trader.";
                Name = "BfsL2Exporter";
                Calculate = Calculate.OnEachTick;
                IsOverlay = true;
                DisplayInDataBox = false;
                DrawOnPricePanel = false;
                IsSuspendedWhileInactive = false;     // keep running when chart isn't focused

                OutputFile = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
                    "NinjaTrader 8", "bfs_l2_export.jsonl");
                FlushIntervalMs = 500;
                VerboseLog = false;
            }
            else if (State == State.Configure)
            {
                // Ensure we get full L2, not just L1.
                AddDataSeries(Data.BarsPeriodType.Tick, 1);
            }
            else if (State == State.DataLoaded)
            {
                try
                {
                    var dir = Path.GetDirectoryName(OutputFile);
                    if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                        Directory.CreateDirectory(dir);

                    writer = new StreamWriter(OutputFile, append: true, encoding: Encoding.UTF8)
                    {
                        AutoFlush = false,
                        NewLine = "\n"
                    };
                    Print($"BfsL2Exporter: writing to {OutputFile}");
                    WriteMeta("start");

                    flushTimer = new System.Threading.Timer(_ => FlushSafe(),
                        null, FlushIntervalMs, FlushIntervalMs);

                    startTicks = DateTime.UtcNow.Ticks;
                    eventCount = 0;
                }
                catch (Exception e)
                {
                    Print($"BfsL2Exporter: failed to open file: {e.Message}");
                    writer = null;
                }
            }
            else if (State == State.Historical)
            {
                WriteMeta("replay");
            }
            else if (State == State.Realtime)
            {
                WriteMeta("realtime");
            }
            else if (State == State.Terminated)
            {
                try { if (flushTimer != null) flushTimer.Dispose(); } catch { }
                lock (writerLock)
                {
                    if (writer != null)
                    {
                        try { writer.Flush(); writer.Dispose(); } catch { }
                        writer = null;
                    }
                }
                Print($"BfsL2Exporter: terminated. Wrote {eventCount} events.");
            }
        }

        protected override void OnBarUpdate()
        {
            // Not used. We're event-driven via OnMarketDepth + OnMarketData.
        }

        // ── L2 depth updates ────────────────────────────────────────────────
        protected override void OnMarketDepth(MarketDepthEventArgs e)
        {
            if (writer == null) return;
            string side = e.MarketDataType == MarketDataType.Bid ? "BID" : "ASK";
            // NT8 enum is Operation.Add / Update / Remove (NOT 'Insert' — that's NT7).
            string op =
                e.Operation == Operation.Add    ? "INS" :
                e.Operation == Operation.Update ? "UPD" : "REM";
            long ts = ToEpochMs(e.Time);
            var sb = new StringBuilder(96);
            sb.Append("{\"type\":\"depth\",\"ts\":").Append(ts)
              .Append(",\"side\":\"").Append(side).Append("\"")
              .Append(",\"op\":\"").Append(op).Append("\"")
              .Append(",\"px\":").Append(e.Price.ToString("R", CultureInfo.InvariantCulture))
              .Append(",\"qty\":").Append(e.Volume.ToString(CultureInfo.InvariantCulture))
              .Append(",\"pos\":").Append(e.Position)
              .Append("}");
            WriteLine(sb.ToString());
        }

        // ── L1 trades (Last) + best bid/ask tracking ────────────────────────
        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (writer == null) return;

            if (e.MarketDataType == MarketDataType.Bid) { lastBestBid = e.Price; return; }
            if (e.MarketDataType == MarketDataType.Ask) { lastBestAsk = e.Price; return; }
            if (e.MarketDataType != MarketDataType.Last) return;

            // Aggressor inference: print at/above prevailing ask = buy; at/below bid = sell.
            string aggressor;
            if (lastBestAsk > 0 && e.Price >= lastBestAsk)      aggressor = "BUY";
            else if (lastBestBid > 0 && e.Price <= lastBestBid) aggressor = "SELL";
            else                                                 aggressor = "BUY";   // fallback

            long ts = ToEpochMs(e.Time);
            var sb = new StringBuilder(96);
            sb.Append("{\"type\":\"trade\",\"ts\":").Append(ts)
              .Append(",\"px\":").Append(e.Price.ToString("R", CultureInfo.InvariantCulture))
              .Append(",\"qty\":").Append(e.Volume.ToString(CultureInfo.InvariantCulture))
              .Append(",\"side\":\"").Append(aggressor).Append("\"")
              .Append("}");
            WriteLine(sb.ToString());
            // Trades are usually the moment you care most about — flush right away.
            FlushSafe();
        }

        // ── helpers ─────────────────────────────────────────────────────────
        private void WriteMeta(string evt)
        {
            try
            {
                string instr = Instrument != null && Instrument.FullName != null
                    ? Instrument.FullName.Replace("\"", "")
                    : "(unknown)";
                double tick = Instrument != null && Instrument.MasterInstrument != null
                    ? Instrument.MasterInstrument.TickSize
                    : 0.0;
                var sb = new StringBuilder(160);
                sb.Append("{\"type\":\"meta\",\"ts\":").Append(ToEpochMs(DateTime.UtcNow))
                  .Append(",\"event\":\"").Append(evt).Append("\"")
                  .Append(",\"instrument\":\"").Append(instr).Append("\"")
                  .Append(",\"tickSize\":").Append(tick.ToString("R", CultureInfo.InvariantCulture))
                  .Append("}");
                WriteLine(sb.ToString());
                FlushSafe();
            }
            catch { /* swallow — meta is best-effort */ }
        }

        private void WriteLine(string line)
        {
            lock (writerLock)
            {
                if (writer == null) return;
                try
                {
                    writer.WriteLine(line);
                    eventCount++;
                    if (VerboseLog && eventCount % 10000 == 0)
                        Print($"BfsL2Exporter: {eventCount} events written");
                }
                catch (Exception ex)
                {
                    Print($"BfsL2Exporter: write failed: {ex.Message}");
                }
            }
        }

        private void FlushSafe()
        {
            lock (writerLock)
            {
                if (writer == null) return;
                try { writer.Flush(); } catch { }
            }
        }

        private static long ToEpochMs(DateTime t)
        {
            DateTime utc = t.Kind == DateTimeKind.Utc ? t : t.ToUniversalTime();
            return (long)((utc - EpochUtc).TotalMilliseconds);
        }
    }
}
