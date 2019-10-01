using System;
using System.Net;
using System.Threading.Tasks;
using EventStore.Core.Tests.Helpers;
using Xunit;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Http.ArgumentPassing {
	namespace matching {
		[Trait("Category", "LongRunning")]
		public class when_matching_against_simple_placeholders : HttpBehaviorSpecification {
			private JObject _response;

			protected override Task Given() => Task.CompletedTask;
			protected override Task When() => Task.CompletedTask;

			[Theory]
			[InlineData("1", "1", "2", "2")]
			[InlineData("1", "1", "%41", "A")]
			[InlineData("1", "1", "$", "$")]
			[InlineData("1", "1", "%24", "$")]
			[InlineData("%24", "$", "2", "2")]
			[InlineData("$", "$", "2", "2")]
			[InlineData("$", "$", "йцукен", "йцукен")]
			[InlineData("$", "$", "%D0%B9%D1%86%D1%83%D0%BA%D0%B5%D0%BD", "йцукен")]
			[InlineData("йцукен", "йцукен", "2", "2")]
			[InlineData("%D0%B9%D1%86%D1%83%D0%BA%D0%B5%D0%BD", "йцукен", "2", "2")]
//            [InlineData("%3F", "?", "2", "2")] // ?
//            [InlineData("%2F", "/", "2", "2")] // /
			[InlineData("%20", " ", "2", "2")] // space
			[InlineData("%25", "%", "2", "2")] // %
			public async Task returns_ok_status_code(string _a, string _ra, string _b, string _rb) {
				_response = await GetJson2<JObject>("/test-encoding/" + _a, "b=" + _b);
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
				HelperExtensions.AssertJson(new {a = _ra, b = _rb}, _response);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_matching_against_placeholders_with_reserved_characters : HttpBehaviorSpecification {
			private JObject _response;

			protected override Task Given() => Task.CompletedTask;
			protected override Task When() => Task.CompletedTask;

			[Theory(Skip = "Only demonstrates differences between .NET and Mono")]
			[InlineData("%24", "$", "2", "2")]
			[InlineData("$", "$", "2", "2")]
			[InlineData("%3F", "?", "2", "2")] // ?
			[InlineData("%2F", "/", "2", "2")] // /
			[InlineData("%20", " ", "2", "2")] // space
			[InlineData("%25", "%", "2", "2")] // %
			public async Task returns_ok_status_code(string _a, string _ra, string _b, string _rb) {
				_response = await GetJson2<JObject>("/test-encoding-reserved-" + _a, "?b=" + _b);
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
				Console.WriteLine(_response.ToString());
				HelperExtensions.AssertJson(new {a = _ra, b = _rb}, _response);
			}
		}
	}
}
