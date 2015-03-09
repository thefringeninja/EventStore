using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EvHttpSharp;

namespace EventStore.Transport.Http.Server
{
    public sealed class HttpAsyncServer
    {
        private static readonly ILogger Logger = LogManager.GetLoggerFor<HttpAsyncServer>();

#pragma warning disable 67
        public event Action<HttpAsyncServer, EventStoreHttpContext> RequestReceived;
#pragma warning restore 67
        
        public bool IsListening { get { return false; } }
        public readonly string[] _listenPrefixes;

        public HttpAsyncServer(string[] prefixes)
        {
            Ensure.NotNull(prefixes, "prefixes");

            _listenPrefixes = prefixes;
        }

        public bool TryStart()
        {
            throw new NotImplementedException();
        }

        private void TryAddAcl(string address)
        {
            if (Runtime.IsMono)
                return;

            var args = string.Format("http add urlacl url={0} user=\"{1}\\{2}\"", address, Environment.UserDomainName, Environment.UserName);
            Logger.Info("Attempting to add permissions for " + address + " using netsh " + args);
            var startInfo = new ProcessStartInfo("netsh", args)
                          {
                              Verb = "runas",
                              CreateNoWindow = true,
                              WindowStyle = ProcessWindowStyle.Hidden,
                              UseShellExecute = true
                          };

            var aclProcess = Process.Start(startInfo);

            if (aclProcess != null) 
                aclProcess.WaitForExit();
        }
    
        public void Shutdown()
        {
            try
            {
                
            }
            catch (ObjectDisposedException)
            {
                // that's ok
            }
            catch (Exception e)
            {
                Logger.ErrorException(e, "Error while shutting down http server");
            }
        }


#if __MonoCS__
       private static Func<HttpListenerRequest, HttpListenerContext> CreateGetContext()
        {
            var r = System.Linq.Expressions.Expression.Parameter(typeof (HttpListenerRequest), "r");
            var piHttpListenerContext = typeof (HttpListenerRequest).GetProperty("HttpListenerContext",
                                                                                 System.Reflection.BindingFlags.GetProperty
                                                                                 | System.Reflection.BindingFlags.NonPublic
                                                                                 | System.Reflection.BindingFlags.FlattenHierarchy
                                                                                 | System.Reflection.BindingFlags.Instance);
            var fiContext = typeof (HttpListenerRequest).GetField("context",
                                                                  System.Reflection.BindingFlags.GetProperty
                                                                  | System.Reflection.BindingFlags.NonPublic
                                                                  | System.Reflection.BindingFlags.FlattenHierarchy
                                                                  | System.Reflection.BindingFlags.Instance);
            var body = piHttpListenerContext != null
                           ? System.Linq.Expressions.Expression.Property(r, piHttpListenerContext)
                           : System.Linq.Expressions.Expression.Field(r, fiContext);
            var debugExpression = System.Linq.Expressions.Expression.Lambda<Func<HttpListenerRequest, HttpListenerContext>>(body, r);
            return debugExpression.Compile();
        }
#endif

    }
}