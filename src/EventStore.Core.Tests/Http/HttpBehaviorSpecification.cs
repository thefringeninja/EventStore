﻿using System;
using System.ComponentModel;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using EventStore.ClientAPI;
using EventStore.Common.Utils;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Http.Streams;
using EventStore.Core.Tests.Http.Users;
using NUnit.Framework;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using EventStore.ClientAPI.Transport.Http;

namespace EventStore.Core.Tests.Http {
	public abstract class HttpBehaviorSpecification : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;
		protected IEventStoreConnection _connection;
		protected HttpResponseMessage _lastResponse;
		protected string _lastResponseBody;
		protected byte[] _lastResponseBytes;
		protected JsonException _lastJsonException;

		private readonly System.Collections.Generic.List<HttpResponseMessage> _allResponses =
			new System.Collections.Generic.List<HttpResponseMessage>();

		private Func<HttpResponseMessage, byte[]> _dumpResponse;
		private Func<HttpResponseMessage, int> _dumpResponse2;
		private Func<HttpRequestMessage, byte[]> _dumpRequest;
		private Func<HttpRequestMessage, byte[]> _dumpRequest2;
		private string _tag;
		private bool _createdMiniNode;
		private NetworkCredential _defaultCredentials = null;
		protected HttpClient _client;

		public override async Task TestFixtureSetUp() {
			Helper.EatException(() => _dumpResponse = CreateDumpResponse());
			Helper.EatException(() => _dumpResponse2 = CreateDumpResponse2());
			Helper.EatException(() => _dumpRequest = CreateDumpRequest());
			Helper.EatException(() => _dumpRequest2 = CreateDumpRequest2());

			await base.TestFixtureSetUp();

			_createdMiniNode = false;
			if (SetUpFixture._connection != null && SetUpFixture._node != null) {
				_tag = "_" + (++SetUpFixture._counter);
				_node = SetUpFixture._node;
				_connection = SetUpFixture._connection;
			} else {
				_createdMiniNode = true;
				_tag = "_1";
				_node = CreateMiniNode();
				_node.Start();

				_connection = TestConnection.Create(_node.TcpEndPoint);
				await _connection.ConnectAsync();
			}

			_lastResponse = null;
			_lastResponseBody = null;
			_lastResponseBytes = null;
			_lastJsonException = null;
			_client = new HttpClient(new HttpClientHandler {
				AllowAutoRedirect = false
			}) {
				BaseAddress = new UriBuilder {
					Host = _node.ExtHttpEndPoint.Address.ToString(),
					Port = _node.ExtHttpEndPoint.Port
				}.Uri
			};
			try {
				Given();
				When();
			} catch {
				if (_createdMiniNode) {
					if (_connection != null)
						try {
							_connection.Close();
						} catch {
						}

					if (_node != null)
						try {
							_node.Shutdown();
						} catch {
						}
				}

				throw;
			}
		}

		public string TestStream {
			get { return "/streams/test" + Tag; }
		}

		public string TestStreamName {
			get { return "test" + Tag; }
		}

		public string TestMetadataStream {
			get { return "/streams/$$test" + Tag; }
		}

		public string Tag {
			get { return _tag; }
		}

		protected virtual MiniNode CreateMiniNode() {
			return new MiniNode(PathName, skipInitializeStandardUsersCheck: GivenSkipInitializeStandardUsersCheck());
		}

		protected virtual bool GivenSkipInitializeStandardUsersCheck() {
			return false;
		}

		public override async Task TestFixtureTearDown() {
			if (_createdMiniNode) {
				_connection.Close();
				_node.Shutdown();
			}

			await base.TestFixtureTearDown();
			foreach (var response in _allResponses) {
				response?.Dispose();
			}
			_client?.Dispose();
		}

		protected HttpRequestMessage CreateRequest(
			string path, string extra, string method, string contentType, NetworkCredential credentials = null,
			NameValueCollection headers = null) {
			credentials = credentials??_defaultCredentials;
			var uri = MakeUrl(path, extra);
			var httpWebRequest = new HttpRequestMessage(new System.Net.Http.HttpMethod(method), uri);
			if (headers != null) {
				foreach (var key in headers.AllKeys) {
					httpWebRequest.Headers.Add(key, headers.GetValues(key));
				}
			}
			if (credentials != null) {
				httpWebRequest.Headers.Add("authorization", $"Basic {GetAuthorizationHeader(credentials)}");
			}

			return httpWebRequest;
		}

		protected HttpRequestMessage CreateRequest(string path, string method, NetworkCredential credentials = null,
			string extra = null)
			=> CreateRequest(path, extra, method, null, credentials);

		protected static string GetAuthorizationHeader(NetworkCredential credentials)
			=> Convert.ToBase64String(Encoding.ASCII.GetBytes($"{credentials.UserName}:{credentials.Password}"));

		protected Uri MakeUrl(string path, string extra = "") {
			var supplied = string.IsNullOrWhiteSpace(extra)
				? new Uri(path, UriKind.RelativeOrAbsolute)
				: new Uri(path + "?" + extra, UriKind.RelativeOrAbsolute);

			if (supplied.IsAbsoluteUri && !supplied.IsFile) // NOTE: is file imporant for mono
				return supplied;

			var httpEndPoint = _node.ExtHttpEndPoint;
			var x = new UriBuilder("http", httpEndPoint.Address.ToString(), httpEndPoint.Port, path);
			x.Query = extra;
			Console.WriteLine(new string('*', 50) + Environment.NewLine + path + Environment.NewLine + extra +
			                  Environment.NewLine + x.Uri + Environment.NewLine + new string('*', 50));

			return x.Uri;
		}

		protected HttpResponseMessage MakeJsonPut<T>(string path, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRawJsonPostRequest(path, "PUT", body, credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}


		protected HttpResponseMessage MakeJsonPost<T>(string path, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRawJsonPostRequest(path, "POST", body, credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}

		protected HttpResponseMessage MakeArrayEventsPost<T>(string path, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateEventsJsonPostRequest(path, "POST", body, credentials, extra);
			var response = GetRequestResponse(request);
			return response;
		}

		protected HttpResponseMessage MakeRawJsonPost<T>(string path, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRawJsonPostRequest(path, "POST", body, credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}

		protected JObject MakeJsonPostWithJsonResponse<T>(string path, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRawJsonPostRequest(path, "POST", body, credentials, extra);
			_lastResponse = GetRequestResponse(request);
			var memoryStream = new MemoryStream();
			_lastResponse.Content.CopyToAsync(memoryStream).Wait();
			var bytes = memoryStream.ToArray();
			_lastResponseBody = Helper.UTF8NoBom.GetString(bytes);
			try {
				return _lastResponseBody.ParseJson<JObject>();
			} catch (JsonException ex) {
				_lastJsonException = ex;
				return default(JObject);
			}
		}

		protected JObject MakeJsonEventsPostWithJsonResponse<T>(string path, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateEventsJsonPostRequest(path, "POST", body, credentials, extra);
			_lastResponse = GetRequestResponse(request);
			var memoryStream = new MemoryStream();
			_lastResponse.Content.CopyToAsync(memoryStream).Wait();
			var bytes = memoryStream.ToArray();
			_lastResponseBody = Helper.UTF8NoBom.GetString(bytes);
			try {
				return _lastResponseBody.ParseJson<JObject>();
			} catch (JsonException ex) {
				_lastJsonException = ex;
				return default(JObject);
			}
		}


		protected HttpResponseMessage MakeEventsJsonPut<T>(string path, T body, NetworkCredential credentials, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateEventsJsonPostRequest(path, "PUT", body, credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}

		protected HttpResponseMessage MakeRawJsonPut<T>(string path, T body, NetworkCredential credentials, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRawJsonPostRequest(path, "PUT", body, credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}

		protected HttpResponseMessage MakeDelete(string path, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRequest(path, "DELETE", credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}

		protected HttpResponseMessage MakePost(string path, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateJsonPostRequest(path, credentials, extra);
			var httpWebResponse = GetRequestResponse(request);
			return httpWebResponse;
		}

		protected XDocument GetAtomXml(Uri uri, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			Get(uri.ToString(), extra, ContentType.Atom, credentials);
			return XDocument.Parse(_lastResponseBody);
		}

		protected XDocument GetXml(Uri uri, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			Get(uri.ToString(), null, ContentType.Xml, credentials);
			return XDocument.Parse(_lastResponseBody);
		}

		protected T GetJson<T>(string path, string accept = null, NetworkCredential credentials = null,
			NameValueCollection headers = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			Get(path, extra, accept, credentials, headers: headers);
			try {
				return _lastResponseBody.ParseJson<T>();
			} catch (JsonException ex) {
				_lastJsonException = ex;
				return default(T);
			}
		}

		protected T GetJson2<T>(string path, string extra, string accept = null, NetworkCredential credentials = null) {
			credentials = credentials??_defaultCredentials;
			Get(path, extra, accept, credentials);
			try {
				return _lastResponseBody.ParseJson<T>();
			} catch (JsonException ex) {
				_lastJsonException = ex;
				return default(T);
			}
		}

		protected T GetJsonWithoutAcceptHeader<T>(string path) {
			var request = CreateRequest(path, "", "GET", null);
			_lastResponse = GetRequestResponse(request);
			var memoryStream = new MemoryStream();
			_lastResponse.Content.CopyToAsync(memoryStream).Wait();
			var bytes = memoryStream.ToArray();
			_lastResponseBody = Helper.UTF8NoBom.GetString(bytes);
			try {
				return _lastResponseBody.ParseJson<T>();
			} catch (JsonException ex) {
				_lastJsonException = ex;
				return default(T);
			}
		}

		protected void Get(string path, string extra, string accept = null, NetworkCredential credentials = null,
			bool setAcceptHeader = true, NameValueCollection headers = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRequest(path, extra, "GET", null, credentials, headers);
			if (setAcceptHeader) {
				request.Headers.Add("accept", accept ?? "application/json");
			}

			_lastResponse = GetRequestResponse(request);
			var memoryStream = new MemoryStream();
			_lastResponse.Content.CopyToAsync(memoryStream).Wait();
			var bytes = memoryStream.ToArray();
			_lastResponseBytes = bytes;
			_lastResponseBody = Helper.UTF8NoBom.GetString(bytes);
		}

		protected HttpResponseMessage GetRequestResponse(HttpRequestMessage request) {
			var response = _client.SendAsync(request).Result;
			_allResponses.Add(response);

			if (_dumpRequest != null) {
				var bytes = _dumpRequest(request);
				if (bytes != null)
					Console.WriteLine(Encoding.ASCII.GetString(bytes, 0, GetBytesLength(bytes)).TrimEnd('\0'));
			}

			if (_dumpRequest2 != null) {
				var bytes = _dumpRequest2(request);
				if (bytes != null)
					Console.WriteLine(Encoding.ASCII.GetString(bytes, 0, GetBytesLength(bytes)).TrimEnd('\0'));
			}

			Console.WriteLine();
			if (_dumpResponse != null) {
				var bytes = _dumpResponse(response);
				var len = _dumpResponse2(response);
				if (bytes != null)
					Console.WriteLine(Encoding.ASCII.GetString(bytes, 0, len).TrimEnd('\0'));
			}

			return response;
		}

		private int GetBytesLength(byte[] bytes) {
			var index = Array.IndexOf(bytes, 0);
			return index < 0 ? bytes.Length : index;
		}

		protected HttpRequestMessage CreateEventsJsonPostRequest<T>(
			string path, string method, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRequest(path, extra, method, "application/vnd.eventstore.events+json", credentials);
			request.Content = new ByteArrayContent(body.ToJsonBytes()) {
				Headers = { ContentType = new MediaTypeHeaderValue("application/vnd.eventstore.events+json")}
			};
			return request;
		}

		protected HttpRequestMessage CreateRawJsonPostRequest<T>(
			string path, string method, T body, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRequest(path, extra, method, "application/json", credentials);
			request.Content = new ByteArrayContent(body.ToJsonBytes()) {
				Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
			};
			return request;
		}
		private HttpRequestMessage CreateJsonPostRequest(string path, NetworkCredential credentials = null, string extra = null) {
			credentials = credentials??_defaultCredentials;
			var request = CreateRequest(path, "POST", credentials, extra);
			
			return request;
		}
		protected void SetDefaultCredentials(NetworkCredential credentials){
			_defaultCredentials = credentials;
		}

		protected abstract void Given();
		protected abstract void When();

		private static Func<HttpResponseMessage, byte[]> CreateDumpResponse() {
			var r = Expression.Parameter(typeof(HttpResponseMessage), "r");
			var piCoreResponseData = typeof(HttpResponseMessage).GetProperty(
				"CoreResponseData",
				BindingFlags.GetProperty | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy |
				BindingFlags.Instance);
			var fim_ConnectStream = piCoreResponseData.PropertyType.GetField("m_ConnectStream",
				BindingFlags.GetField | BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
			var connectStreamType = AppDomain.CurrentDomain.GetAssemblies()
				.Select(a => a.GetType("System.Net.ConnectStream")).Where(t => t != null).FirstOrDefault();
			var fim_ReadBuffer = connectStreamType.GetField("m_ReadBuffer",
				BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
			var body = Expression.Field(
				Expression.Convert(Expression.Field(Expression.Property(r, piCoreResponseData), fim_ConnectStream),
					connectStreamType), fim_ReadBuffer);
			var debugExpression = Expression.Lambda<Func<HttpResponseMessage, byte[]>>(body, r);
			return debugExpression.Compile();
		}

		private static Func<HttpResponseMessage, int> CreateDumpResponse2() {
			var r = Expression.Parameter(typeof(HttpResponseMessage), "r");
			var piCoreResponseData = typeof(HttpResponseMessage).GetProperty(
				"CoreResponseData",
				BindingFlags.GetProperty | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy |
				BindingFlags.Instance);
			var fim_ConnectStream = piCoreResponseData.PropertyType.GetField("m_ConnectStream",
				BindingFlags.GetField | BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
			var connectStreamType = AppDomain.CurrentDomain.GetAssemblies()
				.Select(a => a.GetType("System.Net.ConnectStream")).Where(t => t != null).FirstOrDefault();
			var fim_ReadOffset = connectStreamType.GetField("m_ReadOffset",
				BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
			var fim_ReadBufferSize = connectStreamType.GetField("m_ReadBufferSize",
				BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
			var stream =
				Expression.Convert(Expression.Field(Expression.Property(r, piCoreResponseData), fim_ConnectStream),
					connectStreamType);
			var body = Expression.Add(Expression.Field(stream, fim_ReadOffset),
				Expression.Field(stream, fim_ReadBufferSize));
			var debugExpression = Expression.Lambda<Func<HttpResponseMessage, int>>(body, r);
			return debugExpression.Compile();
		}

		private static Func<HttpRequestMessage, byte[]> CreateDumpRequest() {
			var r = Expression.Parameter(typeof(HttpRequestMessage), "r");
			var fi_WriteBuffer = typeof(HttpRequestMessage).GetField("_WriteBuffer",
				BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.FlattenHierarchy |
				BindingFlags.Instance);
			var body = Expression.Field(r, fi_WriteBuffer);
			var debugExpression = Expression.Lambda<Func<HttpRequestMessage, byte[]>>(body, r);
			return debugExpression.Compile();
		}

		private static Func<HttpRequestMessage, byte[]> CreateDumpRequest2() {
			var r = Expression.Parameter(typeof(HttpRequestMessage), "r");
			var fi_SubmitWriteStream = typeof(HttpRequestMessage).GetField(
				"_SubmitWriteStream",
				BindingFlags.GetField | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy
				| BindingFlags.Instance);
			var connectStreamType = AppDomain.CurrentDomain.GetAssemblies()
				.Select(a => a.GetType("System.Net.ConnectStream")).Where(t => t != null).FirstOrDefault();
			var piBufferedData = connectStreamType.GetProperty(
				"BufferedData",
				BindingFlags.GetProperty | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.FlattenHierarchy
				| BindingFlags.Instance);
			var fiheadChunk = piBufferedData.PropertyType.GetField(
				"headChunk",
				BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.FlattenHierarchy
				| BindingFlags.Instance);
			var piBuffer = fiheadChunk.FieldType.GetField(
				"Buffer",
				BindingFlags.GetProperty | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.FlattenHierarchy
				| BindingFlags.Instance);
			var submitWriteStreamExpression = Expression.Field(r, fi_SubmitWriteStream);
			var headChunk =
				Expression.Condition(
					Expression.ReferenceNotEqual(submitWriteStreamExpression,
						Expression.Constant(null, submitWriteStreamExpression.Type)),
					Expression.Field(
						Expression.Property(
							Expression.Convert(submitWriteStreamExpression, connectStreamType), piBufferedData),
						fiheadChunk),
					Expression.Constant(null, fiheadChunk.FieldType));
			var body =
				Expression.Condition(
					Expression.ReferenceNotEqual(headChunk, Expression.Constant(null, headChunk.Type)),
					Expression.Field(headChunk, piBuffer), Expression.Constant(null, piBuffer.FieldType));
			var debugExpression = Expression.Lambda<Func<HttpRequestMessage, byte[]>>(body, r);
			return debugExpression.Compile();
		}
	}
}
