using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Services.Monitoring.Stats;
using EventStore.Transport.Tcp;
using Microsoft.Diagnostics.Tools.RuntimeClient;
using Microsoft.Diagnostics.Tracing;

namespace EventStore.Core.Services.Monitoring.Utils {
	[EventSource(Guid = Id, Name = "EventStore")]
	public class EventStoreEventSource : EventSource {
		private const string Id = "21FC861F-86DF-49A3-AFDB-D1E20D69F294";
		private static EventStoreEventSource Instance;
		private static readonly ILogger Logger = LogManager.GetLoggerFor(typeof(EventStoreEventSource));
		private readonly Timer _timer;

		private DiskIo _diskIo;
		private TcpStats _tcpStats;
		private EsDriveInfo _esDriveInfo;
		private QueueStats[] _queueStats;
		private PollingCounter _diskIoReadBytes;
		private PollingCounter _diskIoWrittenBytes;
		private PollingCounter _diskIoReadOps;
		private PollingCounter _diskIoWriteOps;
		private PollingCounter _tcpConnections;
		private PollingCounter _tcpReceivingSpeed;
		private PollingCounter _tcpSendingSpeed;
		private PollingCounter _tcpInSend;
		private PollingCounter _diskAvailableBytes;

		public static void Initialize(string dbPath, TimeSpan interval) =>
			Instance = new EventStoreEventSource(dbPath, interval);

		private readonly ConcurrentDictionary<string, DiagnosticCounter> _counters;

		private EventStoreEventSource(string dbPath, TimeSpan interval) : base("EventStore",
			EventSourceSettings.EtwSelfDescribingEventFormat) {
			_counters = new ConcurrentDictionary<string, DiagnosticCounter>();
			_timer = new Timer(GetStats, null, interval, interval);
			GetStats();

			void GetStats(object _ = default) {
				var process = Process.GetCurrentProcess();
				Volatile.Write(ref _diskIo, DiskIo.GetDiskIo(process.Id, Logger));
				Volatile.Write(ref _tcpStats, TcpConnectionMonitor.Default.GetTcpStats());
				Volatile.Write(ref _esDriveInfo, EsDriveInfo.FromDirectory(dbPath, Logger));
				Volatile.Write(ref _queueStats, QueueMonitor.Default.GetStats());
			}
		}

		protected override void Dispose(bool disposing) {
			base.Dispose(disposing);

			if (!disposing) return;
			_timer.Dispose();
		}

		protected override void OnEventCommand(EventCommandEventArgs command) {
			if (command.Command != EventCommand.Enable) {
				return;
			}

			AddOrReplace(new PollingCounter("proc-diskIo-readBytes", this, () => _diskIo?.ReadBytes ?? 0) {
				DisplayUnits = "bytes",
				DisplayName = "Read Bytes"
			});
			AddOrReplace(new PollingCounter("proc-diskIo-writtenBytes", this, () => _diskIo?.WrittenBytes ?? 0) {
				DisplayUnits = "bytes",
				DisplayName = "Written Bytes"
			});
			AddOrReplace(new PollingCounter("proc-diskIo-readOps", this, () => _diskIo?.ReadOps ?? 0) {
				DisplayName = "Read Ops"
			});
			AddOrReplace(new PollingCounter("proc-diskIo-writeOps", this, () => _diskIo?.WriteOps ?? 0) {
				DisplayName = "Write Ops"
			});

			AddOrReplace(new PollingCounter("proc-tcp-connections", this, () => _tcpStats?.Connections ?? 0) {
				DisplayName = "TCP Connections"
			});
			AddOrReplace(new PollingCounter("proc-tcp-receivingSpeed", this, () => _tcpStats?.ReceivingSpeed ?? 0) {
				DisplayName = "TCP Receiving Speed",
				DisplayUnits = "bytes / sec"
			});
			AddOrReplace(new PollingCounter("proc-tcp-sendingSpeed", this, () => _tcpStats?.SendingSpeed ?? 0) {
				DisplayName = "TCP Sending Speed",
				DisplayUnits = "bytes / sec"
			});
			AddOrReplace(new PollingCounter("proc-tcp-inSend", this, () => _tcpStats?.InSend ?? 0) {
				DisplayName = "TCP In Send",
				DisplayUnits = "bytes"
			});

			var esDriveInfo = Volatile.Read(ref _esDriveInfo);

			if (esDriveInfo != null) {
				AddOrReplace(new PollingCounter(DriveStat("availableBytes"), this,
					() => (double)(_esDriveInfo?.AvailableBytes ?? 0)) {
					DisplayName = "Available Bytes on Disk",
					DisplayUnits = "bytes"
				});
			}

			var queueStats = Volatile.Read(ref _queueStats);

			if (queueStats != null) {
				foreach (var queue in queueStats) {
					AddOrReplace(new PollingCounter(QueueStat(queue, "avgItemsPerSecond"), this,
						() => queue.AvgItemsPerSecond) {
						DisplayUnits = "items / sec",
						DisplayName = $"{queue.Name} Items Per Second"
					});
					AddOrReplace(new PollingCounter(QueueStat(queue, "avgProcessingTime"), this,
						() => queue.AvgProcessingTime) {
						DisplayUnits = "sec",
						DisplayName = $"{queue.Name} Average Processing Time"
					});
				}
			}

			string DriveStat(string stat) =>
				$"sys-drive-{esDriveInfo.DiskName.Replace("\\", "").Replace(":", "")}-{stat}";

			string QueueStat(QueueStats queue, string stat)
				=> $"es-queue-{queue.Name}-{stat}";

			void AddOrReplace(DiagnosticCounter counter) =>
				_counters.AddOrUpdate(counter.Name, counter, (_, counter) => counter);
		}
	}

	internal class EventCounterHelper : ICounters {
		private readonly int _processId;
		private readonly Counters _counters;

		public EventCounterHelper() {
			_processId = Process.GetCurrentProcess().Id;
			_counters = new Counters();
			Task.Run(() => {
				var source = GetEventPipeEventSource("System.Runtime");
				source.Dynamic.All += DynamicOnAll;
				source.Process();
				source.Dispose();
			});
		}

		private void DynamicOnAll(TraceEvent traceEvent) {
			if (traceEvent.EventName != "EventCounters") {
				return;
			}

			var payloadVal = (IDictionary<string, object>)traceEvent.PayloadValue(0);
			var fields = (IDictionary<string, object>)payloadVal["Payload"];

			_counters[fields["Name"].ToString()] = fields["CounterType"].Equals("Sum")
				? fields["Mean"]
				: fields["Increment"];
		}

		private EventPipeEventSource GetEventPipeEventSource(string name) {
			var providerName = $"{name}:0xffffffff:5:EventCounterIntervalSec=5";
			try {
				return RequestTracingV2(providerName);
			} catch (EventPipeUnknownCommandException) {
				return RequestTracingV1(providerName);
			}
		}

		// Use EventPipe CollectTracing2 command to start monitoring. This may throw.
		private EventPipeEventSource RequestTracingV2(string providerString) {
			var configuration = new SessionConfigurationV2(
				circularBufferSizeMB: 1000,
				format: EventPipeSerializationFormat.NetTrace,
				requestRundown: false,
				providers: ToProviders(providerString));
			var binaryReader = EventPipeClient.CollectTracing2(_processId, configuration, out _);
			return new EventPipeEventSource(binaryReader);
		}

		// Use EventPipe CollectTracing command to start monitoring. This may throw.
		private EventPipeEventSource RequestTracingV1(string providerString) {
			var configuration = new SessionConfiguration(
				circularBufferSizeMB: 1000,
				format: EventPipeSerializationFormat.NetTrace,
				providers: ToProviders(providerString));
			var binaryReader = EventPipeClient.CollectTracing(_processId, configuration, out _);
			return new EventPipeEventSource(binaryReader);
		}

		private static List<Provider> ToProviders(string providers) {
			if (providers == null)
				throw new ArgumentNullException(nameof(providers));
			return string.IsNullOrWhiteSpace(providers)
				? new List<Provider>()
				: providers.Split(',').Select(ToProvider).ToList();
		}

		private static Provider ToProvider(string provider) {
			if (string.IsNullOrWhiteSpace(provider))
				throw new ArgumentNullException(nameof(provider));

			var tokens = provider.Split(new[] {':'}, 4, StringSplitOptions.None); // Keep empty tokens;

			// Provider name
			string providerName = tokens.Length > 0 ? tokens[0] : null;

			// Check if the supplied provider is a GUID and not a name.
			if (Guid.TryParse(providerName, out _)) {
				Console.WriteLine(
					$"Warning: --provider argument {providerName} appears to be a GUID which is not supported by dotnet-trace. Providers need to be referenced by their textual name.");
			}

			if (string.IsNullOrWhiteSpace(providerName))
				throw new ArgumentException("Provider name was not specified.");

			// Keywords
			ulong keywords = tokens.Length > 1 && !string.IsNullOrWhiteSpace(tokens[1])
				? Convert.ToUInt64(tokens[1], 16)
				: ulong.MaxValue;

			// Level
			EventLevel eventLevel = tokens.Length > 2 && !string.IsNullOrWhiteSpace(tokens[2])
				? GetEventLevel(tokens[2])
				: EventLevel.Verbose;

			// Event counters
			string filterData = tokens.Length > 3 ? tokens[3] : null;
			filterData = string.IsNullOrWhiteSpace(filterData) ? null : filterData;

			return new Provider(providerName, keywords, eventLevel, filterData);

			static EventLevel GetEventLevel(string token) =>
				int.TryParse(token, out int level) && level >= 0
					? level > (int)EventLevel.Verbose ? EventLevel.Verbose : (EventLevel)level
					: token.ToLower() switch {
						"critical" => EventLevel.Critical,
						"error" => EventLevel.Error,
						"informational" => EventLevel.Informational,
						"logalways" => EventLevel.LogAlways,
						"verbose" => EventLevel.Verbose,
						"warning" => EventLevel.Warning,
						_ => throw new ArgumentException($"Unknown EventLevel: {token}")
					};
		}

		public void RefreshInstanceName() {
		}

		public float GetTotalCpuUsage() => Convert.ToSingle(_counters["cpu-usage"]);

		public long GetFreeMemory() {
			return 0L;
		}

		public float GetProcCpuUsage() => Convert.ToSingle(_counters["cpu-usage"]);

		public int GetProcThreadsCount() => Convert.ToInt32(_counters["threadpool-thread-count"]);

		public float GetThrownExceptionsRate() => Convert.ToSingle(_counters["exception-count"]);

		public float GetContentionsRateCount() => Convert.ToSingle(_counters["monitor-lock-contention-count"]);

		public GcStats GetGcStats() => new GcStats(
			Convert.ToInt64(_counters["gen-0-gc-count"]),
			Convert.ToInt64(_counters["gen-1-gc-count"]),
			Convert.ToInt64(_counters["gen-2-gc-count"]),
			Convert.ToInt64(_counters["gen-0-size"]),
			Convert.ToInt64(_counters["gen-1-size"]),
			Convert.ToInt64(_counters["gen-2-size"]),
			Convert.ToInt64(_counters["loh-size"]),
			Convert.ToSingle(_counters["alloc-rate"]),
			Convert.ToSingle(_counters["time-in-gc"]),
			Convert.ToInt64(_counters["gc-heap-size"]));

		public void Dispose() {
		}

		private class Counters {
			private readonly IDictionary<string, object> _inner;

			public object this[string key] {
				get => _inner[key];
				set => _inner[key] = value;
			}

			public Counters() {
				_inner = new Dictionary<string, object> {
					["cpu-usage"] = 0,
					["working-set"] = 0,
					["gc-heap-size"] = 0,
					["gen-0-gc-count"] = 0,
					["gen-1-gc-count"] = 0,
					["gen-2-gc-count"] = 0,
					["time-in-gc"] = 0,
					["gen-0-size"] = 0,
					["gen-1-size"] = 0,
					["gen-2-size"] = 0,
					["loh-size"] = 0,
					["alloc-rate"] = 0,
					["assembly-count"] = 0,
					["exception-count"] = 0,
					["threadpool-thread-count"] = 0,
					["monitor-lock-contention-count"] = 0,
					["threadpool-queue-length"] = 0,
					["threadpool-completed-items-count"] = 0,
					["active-timer-count"] = 0,
				};
			}
		}
	}

	internal interface ICounters : IDisposable {
		/// <summary>
		/// Re-examines the performance counter instances for the correct instance name for this process.
		/// </summary>
		/// <remarks>
		/// The performance counter instance on .NET Framework can change at any time
		/// due to creation or destruction of processes with the same image name. This method should be called before using the Get methods.
		///
		/// The correct instance name must be found by dereferencing via a look up counter, e.g. .Net CLR Memory/Process Id
		/// </remarks>
		void RefreshInstanceName();

		///<summary>
		///Total CPU usage in percentage
		///</summary>
		float GetTotalCpuUsage();

		///<summary>
		///Free memory in bytes
		///</summary>
		long GetFreeMemory();

		///<summary>
		///Total process CPU usage
		///</summary>
		float GetProcCpuUsage();

		///<summary>
		///Current thread count
		///</summary>
		int GetProcThreadsCount();

		///<summary>
		///Number of exceptions thrown per second
		///</summary>
		float GetThrownExceptionsRate();

		///<summary>
		///The rate at which threads in the runtime attempt to acquire a managed lock unsuccessfully
		///</summary>
		float GetContentionsRateCount();

		GcStats GetGcStats();
	}

	internal class PerfCounterHelper : ICounters, IDisposable {
		private const string DotNetProcessIdCounterName = "Process Id";
		private const string ProcessIdCounterName = "ID Process";
		private const string DotNetMemoryCategory = ".NET CLR Memory";
		private const string ProcessCategory = "Process";
		private const int InvalidCounterResult = -1;

		private readonly ILogger _log;

		private readonly PerformanceCounter _totalCpuCounter;
		private readonly PerformanceCounter _totalMemCounter; //doesn't work on mono
		private readonly PerformanceCounter _procCpuCounter;
		private readonly PerformanceCounter _procThreadsCounter;

		private readonly PerformanceCounter _thrownExceptionsRateCounter;
		private readonly PerformanceCounter _contentionsRateCounter;

		private readonly PerformanceCounter _gcGen0ItemsCounter;
		private readonly PerformanceCounter _gcGen1ItemsCounter;
		private readonly PerformanceCounter _gcGen2ItemsCounter;
		private readonly PerformanceCounter _gcGen0SizeCounter;
		private readonly PerformanceCounter _gcGen1SizeCounter;
		private readonly PerformanceCounter _gcGen2SizeCounter;
		private readonly PerformanceCounter _gcLargeHeapSizeCounter;
		private readonly PerformanceCounter _gcAllocationSpeedCounter;
		private readonly PerformanceCounter _gcTimeInGcCounter;
		private readonly PerformanceCounter _gcTotalBytesInHeapsCounter;
		private readonly int _pid;

		public PerfCounterHelper(ILogger log) {
			_log = log;

			var currentProcess = Process.GetCurrentProcess();
			_pid = currentProcess.Id;

			_totalCpuCounter = CreatePerfCounter("Processor", "% Processor Time", "_Total");
			_totalMemCounter = CreatePerfCounter("Memory", "Available Bytes");

			var processInstanceName = GetProcessInstanceName(ProcessCategory, ProcessIdCounterName);

			if (processInstanceName != null) {
				_procCpuCounter = CreatePerfCounter(ProcessCategory, "% Processor Time", processInstanceName);
				_procThreadsCounter = CreatePerfCounter(ProcessCategory, "Thread Count", processInstanceName);
			}

			var netInstanceName = GetProcessInstanceName(DotNetMemoryCategory, DotNetProcessIdCounterName);

			if (netInstanceName != null) {
				_thrownExceptionsRateCounter =
					CreatePerfCounter(".NET CLR Exceptions", "# of Exceps Thrown / sec", netInstanceName);
				_contentionsRateCounter =
					CreatePerfCounter(".NET CLR LocksAndThreads", "Contention Rate / sec", netInstanceName);
				_gcGen0ItemsCounter = CreatePerfCounter(DotNetMemoryCategory, "# Gen 0 Collections", netInstanceName);
				_gcGen1ItemsCounter = CreatePerfCounter(DotNetMemoryCategory, "# Gen 1 Collections", netInstanceName);
				_gcGen2ItemsCounter = CreatePerfCounter(DotNetMemoryCategory, "# Gen 2 Collections", netInstanceName);
				_gcGen0SizeCounter = CreatePerfCounter(DotNetMemoryCategory, "Gen 0 heap size", netInstanceName);
				_gcGen1SizeCounter = CreatePerfCounter(DotNetMemoryCategory, "Gen 1 heap size", netInstanceName);
				_gcGen2SizeCounter = CreatePerfCounter(DotNetMemoryCategory, "Gen 2 heap size", netInstanceName);
				_gcLargeHeapSizeCounter = CreatePerfCounter(DotNetMemoryCategory, "Large Object Heap size",
					netInstanceName);
				_gcAllocationSpeedCounter =
					CreatePerfCounter(DotNetMemoryCategory, "Allocated Bytes/sec", netInstanceName);
				_gcTimeInGcCounter = CreatePerfCounter(DotNetMemoryCategory, "% Time in GC", netInstanceName);
				_gcTotalBytesInHeapsCounter =
					CreatePerfCounter(DotNetMemoryCategory, "# Bytes in all Heaps", netInstanceName);
			}
		}

		private PerformanceCounter CreatePerfCounter(string category, string counter, string instance = null) {
			try {
				return string.IsNullOrEmpty(instance)
					? new PerformanceCounter(category, counter)
					: new PerformanceCounter(category, counter, instance);
			} catch (Exception ex) {
				_log.Trace(
					"Could not create performance counter: category='{category}', counter='{counter}', instance='{instance}'. Error: {e}",
					category, counter, instance ?? string.Empty, ex.Message);
				return null;
			}
		}

		private string GetProcessInstanceName(string categoryName, string counterName) {
			// On Unix or MacOS, use the PID as the instance name
			if (Runtime.IsUnixOrMac) {
				return _pid.ToString();
			}

			// On Windows use the Performance Counter to get the name
			try {
				if (PerformanceCounterCategory.Exists(categoryName)) {
					var category = new PerformanceCounterCategory(categoryName).ReadCategory();

					if (category.Contains(counterName)) {
						var instanceDataCollection = category[counterName];

						if (instanceDataCollection.Values != null) {
							foreach (InstanceData item in instanceDataCollection.Values) {
								var instancePid = (int)item.RawValue;
								if (_pid.Equals(instancePid)) {
									return item.InstanceName;
								}
							}
						}
					}
				}
			} catch (InvalidOperationException) {
				_log.Trace("Unable to get performance counter category '{category}' instances.", categoryName);
			}

			return null;
		}


		/// <summary>
		/// Re-examines the performance counter instances for the correct instance name for this process.
		/// </summary>
		/// <remarks>
		/// The performance counter instance on .NET Framework can change at any time
		/// due to creation or destruction of processes with the same image name. This method should be called before using the Get methods.
		///
		/// The correct instance name must be found by dereferencing via a look up counter, e.g. .Net CLR Memory/Process Id
		/// </remarks>
		public void RefreshInstanceName() {
			if (!Runtime.IsWindows) {
				return;
			}

			if (_procCpuCounter != null) {
				var processInstanceName = GetProcessInstanceName(ProcessCategory, ProcessIdCounterName);

				if (processInstanceName != null) {
					if (_procCpuCounter != null) _procCpuCounter.InstanceName = processInstanceName;
					if (_procThreadsCounter != null) _procThreadsCounter.InstanceName = processInstanceName;
				}
			}

			if (_gcLargeHeapSizeCounter != null) {
				var netInstanceName = GetProcessInstanceName(DotNetMemoryCategory, DotNetProcessIdCounterName);

				if (netInstanceName != null) {
					if (_thrownExceptionsRateCounter != null)
						_thrownExceptionsRateCounter.InstanceName = netInstanceName;
					if (_contentionsRateCounter != null) _contentionsRateCounter.InstanceName = netInstanceName;
					if (_gcGen0ItemsCounter != null) _gcGen0ItemsCounter.InstanceName = netInstanceName;
					if (_gcGen1ItemsCounter != null) _gcGen1ItemsCounter.InstanceName = netInstanceName;
					if (_gcGen2ItemsCounter != null) _gcGen2ItemsCounter.InstanceName = netInstanceName;
					if (_gcGen0SizeCounter != null) _gcGen0SizeCounter.InstanceName = netInstanceName;
					if (_gcGen1SizeCounter != null) _gcGen1SizeCounter.InstanceName = netInstanceName;
					if (_gcGen2SizeCounter != null) _gcGen2SizeCounter.InstanceName = netInstanceName;
					if (_gcLargeHeapSizeCounter != null) _gcLargeHeapSizeCounter.InstanceName = netInstanceName;
					if (_gcAllocationSpeedCounter != null) _gcAllocationSpeedCounter.InstanceName = netInstanceName;
					if (_gcTimeInGcCounter != null) _gcTimeInGcCounter.InstanceName = netInstanceName;
					if (_gcTotalBytesInHeapsCounter != null) _gcTotalBytesInHeapsCounter.InstanceName = netInstanceName;
				}
			}
		}

		///<summary>
		///Total CPU usage in percentage
		///</summary>
		public float GetTotalCpuUsage() {
			return _totalCpuCounter?.NextValue() ?? InvalidCounterResult;
		}

		///<summary>
		///Free memory in bytes
		///</summary>
		public long GetFreeMemory() {
			return _totalMemCounter?.NextSample().RawValue ?? InvalidCounterResult;
		}

		///<summary>
		///Total process CPU usage
		///</summary>
		public float GetProcCpuUsage() {
			return _procCpuCounter?.NextValue() ?? InvalidCounterResult;
		}

		///<summary>
		///Current thread count
		///</summary>
		public int GetProcThreadsCount() {
			return (int)(_procThreadsCounter?.NextValue() ?? InvalidCounterResult);
		}

		///<summary>
		///Number of exceptions thrown per second
		///</summary>
		public float GetThrownExceptionsRate() {
			return _thrownExceptionsRateCounter?.NextValue() ?? InvalidCounterResult;
		}

		///<summary>
		///The rate at which threads in the runtime attempt to acquire a managed lock unsuccessfully
		///</summary>
		public float GetContentionsRateCount() {
			return _contentionsRateCounter?.NextValue() ?? InvalidCounterResult;
		}

		public GcStats GetGcStats() {
			return new GcStats(
				gcGen0Items: _gcGen0ItemsCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcGen1Items: _gcGen1ItemsCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcGen2Items: _gcGen2ItemsCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcGen0Size: _gcGen0SizeCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcGen1Size: _gcGen1SizeCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcGen2Size: _gcGen2SizeCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcLargeHeapSize: _gcLargeHeapSizeCounter?.NextSample().RawValue ?? InvalidCounterResult,
				gcAllocationSpeed: _gcAllocationSpeedCounter?.NextValue() ?? InvalidCounterResult,
				gcTimeInGc: _gcTimeInGcCounter?.NextValue() ?? InvalidCounterResult,
				gcTotalBytesInHeaps: _gcTotalBytesInHeapsCounter?.NextSample().RawValue ?? InvalidCounterResult);
		}

		public void Dispose() {
			_totalCpuCounter?.Dispose();
			_totalMemCounter?.Dispose();
			_procCpuCounter?.Dispose();
			_procThreadsCounter?.Dispose();

			_thrownExceptionsRateCounter?.Dispose();
			_contentionsRateCounter?.Dispose();

			_gcGen0ItemsCounter?.Dispose();
			_gcGen1ItemsCounter?.Dispose();
			_gcGen2ItemsCounter?.Dispose();
			_gcGen0SizeCounter?.Dispose();
			_gcGen1SizeCounter?.Dispose();
			_gcGen2SizeCounter?.Dispose();
			_gcLargeHeapSizeCounter?.Dispose();
			_gcAllocationSpeedCounter?.Dispose();
			_gcTimeInGcCounter?.Dispose();
			_gcTotalBytesInHeapsCounter?.Dispose();
		}
	}
}
