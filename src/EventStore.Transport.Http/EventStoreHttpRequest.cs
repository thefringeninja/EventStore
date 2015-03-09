using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Security.Principal;

namespace EventStore.Transport.Http
{
    public class EventStoreHttpContext
    {
        public readonly EventStoreHttpRequest Request;
        public readonly EventStoreHttpResponse Response;
        public readonly IPrincipal User;

        public EventStoreHttpContext(IDictionary<string, object> environment)
        {
            Request = new EventStoreHttpRequest(environment);
            Response = new EventStoreHttpResponse(environment);

            User = null;
        }

    }

    public class EventStoreHttpRequest
    {
        public EventStoreHttpRequest(IDictionary<string, object> environment)
        {
            var headers = environment["owin.RequestHeaders"] as IDictionary<string, string[]>;

            Headers = new NameValueCollection();
            foreach (var header in headers)
            {
                foreach (var value in header.Value)
                {
                    Headers.Add(header.Key, value);
                }
            }

            InputStream = (Stream) environment["owin.RequestBody"];
            HttpMethod = (string) environment["owin.RequestStream"];
            //QueryString = 
        }

        public Uri Url { get; private set; }
        public NameValueCollection Headers { get; private set; }
        public Stream InputStream { get; private set; }

        public string HttpMethod { get; private set; }

        public string ContentType
        {
            get { return Headers["Content-Type"]; }
        }

        public NameValueCollection QueryString { get; private set; }

        public string[] AcceptTypes
        {
            get { return Headers.GetValues("Accept"); }
        }

        public long ContentLength64
        {
            get
            {
                long length;
                return Int64.TryParse(Headers.Get("Content.Length"), out length)
                    ? length
                    : -1;
            }
        }

        public string RawUrl { get; private set; }
    }

    public class EventStoreHttpResponse
    {
        public EventStoreHttpResponse(IDictionary<string, object> environment)
        {
            Headers = new NameValueCollection();
        }

        public int StatusCode { get; set; }
        public string StatusDescription { get; set; }
        public string ContentType { get; set; }
        public long ContentLength64 { get; set; }
        public NameValueCollection Headers { get; private set; }
        public Stream OutputStream { get; set; }

        public void AddHeader(string key, string value)
        {
            Headers.Add(key, value);
        }

        public void Close()
        {
            
        }
    }
}