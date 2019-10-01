using System;
using System.Runtime.InteropServices;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Tests.Helpers;
using NLog;
using NLog.Config;
using NLog.Targets;

namespace EventStore.Core.Tests {
	internal class Program {
		static Program() {
			AppDomain.CurrentDomain.DomainUnload += (_, e) => TearDown();  
			System.Net.ServicePointManager.DefaultConnectionLimit = 1000;
			Console.WriteLine("Initializing tests (setting console loggers)...");

			var originalFormatter = ConfigurationItemFactory.Default.ValueFormatter;
			ConfigurationItemFactory.Default.ValueFormatter = new NLogValueFormatter(originalFormatter, false);
			ConsoleTarget consoleTarget = new ConsoleTarget("testconsole");
			var config = new LoggingConfiguration();
			config.AddRule(LogLevel.Trace, LogLevel.Fatal, consoleTarget);
			consoleTarget.Layout =
				"[${processid:padCharacter=0:padding=5},${threadid:padCharacter=0:padding=2},${date:universalTime=true:format=HH\\:mm\\:ss\\.fff},${level:padding=-5:uppercase=true}] ${message}${onexception:${newline}${literal:text=EXCEPTION OCCURRED}${newline}${exception:format=message}}";
			NLog.LogManager.Configuration = config;
			EventStore.Common.Log.LogManager.SetLogFactory(x => new NLogger(x));

			Application.AddDefines(new[] {Application.AdditionalCommitChecks});
			LogEnvironmentInfo();
		}

		private static void LogEnvironmentInfo() {
			var log = EventStore.Common.Log.LogManager.GetLoggerFor<Program>();

			log.Info("\n{0,-25} {1} ({2}/{3}, {4})\n"
			         + "{5,-25} {6} ({7})\n"
			         + "{8,-25} {9} ({10}-bit)\n"
			         + "{11,-25} {12}\n\n",
				"ES VERSION:", VersionInfo.Version, VersionInfo.Branch, VersionInfo.Hashtag, VersionInfo.Timestamp,
				"OS:", OS.OsFlavor, Environment.OSVersion,
				"RUNTIME:", OS.GetRuntimeVersion(), Marshal.SizeOf(typeof(IntPtr)) * 8,
				"GC:",
				GC.MaxGeneration == 0
					? "NON-GENERATION (PROBABLY BOEHM)"
					: string.Format("{0} GENERATIONS", GC.MaxGeneration + 1));
		}

		public static void TearDown() {
			var runCount = Math.Max(1, MiniNode.RunCount);
			var msg =
				$"Total running time of MiniNode: {MiniNode.RunningTime.Elapsed} (mean {TimeSpan.FromTicks(MiniNode.RunningTime.Elapsed.Ticks / runCount)})\n" +
				$"Total starting time of MiniNode: {MiniNode.StartingTime.Elapsed} (mean {TimeSpan.FromTicks(MiniNode.StartingTime.Elapsed.Ticks / runCount)})\n" +
				$"Total stopping time of MiniNode: {MiniNode.StoppingTime.Elapsed} (mean {TimeSpan.FromTicks(MiniNode.StoppingTime.Elapsed.Ticks / runCount)})\n" +
				$"Total run count: {MiniNode.RunCount}";

			Console.WriteLine(msg);
			EventStore.Common.Log.LogManager.Finish();
		}
	}
}
