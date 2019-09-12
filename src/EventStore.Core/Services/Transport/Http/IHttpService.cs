using System;
using System.Collections.Generic;
using EventStore.Transport.Http.EntityManagement;

namespace EventStore.Core.Services.Transport.Http {
	public interface IHttpController {
		void Subscribe(IHttpService service);
	}

	public interface IHttpSender {
		void SubscribeSenders(HttpMessagePipe pipe);
	}

	public interface IHttpForwarder {
		bool ForwardRequest(HttpEntityManager manager);
	}

	public interface IHttpService {
		IEnumerable<string> ListenPrefixes { get; }
		ServiceAccessibility Accessibility { get; }

		List<UriToActionMatch> GetAllUriMatches(Uri uri);
		void SetupController(IHttpController controller);

		void RegisterCustomAction(ControllerAction action,
			Func<HttpEntityManager, UriTemplateMatch, RequestParams> handler);

		void RegisterAction(ControllerAction action, Action<HttpEntityManager, UriTemplateMatch> handler);
	}
}
