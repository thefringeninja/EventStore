using System;
using System.IO;
using System.Xml;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Atom;
using Xunit;

namespace EventStore.Core.Tests.Services.Transport.Http {
	public class feed_element_must : IDisposable {
		private const string FeedUrl = "http://127.0.0.1/streams/test";
		private FeedElement _feed;
		private XmlWriter _writer;

		public feed_element_must() {
			_feed = new FeedElement();
			_writer = XmlWriter.Create(new MemoryStream());
		}

		public void Dispose() {
			_writer.Close();
		}

		[Fact]
		public void have_non_empty_title() {
			//_feed.SetTitle("Event stream 'test'");
			_feed.SetId(FeedUrl);
			_feed.SetUpdated(DateTime.UtcNow);
			_feed.SetAuthor(AtomSpecs.Author);
			_feed.AddLink("self", FeedUrl, null);

			Assert.Throws<ArgumentNullException>(() => _feed.SetTitle(null));
			Assert.Throws<AtomSpecificationViolationException>(() => _feed.WriteXml(_writer));
		}

		[Fact]
		public void have_non_empty_id() {
			_feed.SetTitle("Event stream 'test'");
			//_feed.SetId(FeedUrl);
			_feed.SetUpdated(DateTime.UtcNow);
			_feed.SetAuthor(AtomSpecs.Author);
			_feed.AddLink("self", FeedUrl, null);

			Assert.Throws<ArgumentNullException>(() => _feed.SetId(null));
			Assert.Throws<AtomSpecificationViolationException>(() => _feed.WriteXml(_writer));
		}

		[Fact]
		public void have_non_empty_updated() {
			_feed.SetTitle("Event stream 'test'");
			_feed.SetId(FeedUrl);
			//_feed.SetUpdated(DateTime.UtcNow);
			_feed.SetAuthor(AtomSpecs.Author);
			_feed.AddLink("self", FeedUrl, null);

			Assert.Throws<AtomSpecificationViolationException>(() => _feed.WriteXml(_writer));
		}

		[Fact]
		public void format_dates_in_RFC_3339() {
			_feed.SetTitle("Event stream 'test'");
			_feed.SetId(FeedUrl);
			_feed.SetUpdated(DateTime.UtcNow);
			_feed.SetAuthor(AtomSpecs.Author);
			_feed.AddLink("self", FeedUrl, null);

			_feed.WriteXml(_writer);
		}

		[Fact]
		public void be_treated_as_valid_with_empty_entries_list() {
			_feed.SetTitle("Event stream 'test'");
			_feed.SetId(FeedUrl);
			_feed.SetUpdated(DateTime.UtcNow);
			_feed.SetAuthor(AtomSpecs.Author);
			_feed.AddLink("self", FeedUrl, null);

			_feed.WriteXml(_writer);
		}

		[Fact]
		public void have_non_empty_author() {
			_feed.SetTitle("Event stream 'test'");
			_feed.SetId(FeedUrl);
			_feed.SetUpdated(DateTime.UtcNow);
			//_feed.SetAuthor(AtomSpecs.Author);
			_feed.AddLink("self", FeedUrl, null);

			Assert.Throws<ArgumentNullException>(() => _feed.SetAuthor(null));
			Assert.Throws<AtomSpecificationViolationException>(() => _feed.WriteXml(_writer));
		}

		[Fact]
		public void have_at_least_one_link() {
			_feed.SetTitle("Event stream 'test'");
			_feed.SetId(FeedUrl);
			_feed.SetUpdated(DateTime.UtcNow);
			_feed.SetAuthor(AtomSpecs.Author);
			//_feed.AddLink("self", FeedUrl, null);

			Assert.Throws<AtomSpecificationViolationException>(() => _feed.WriteXml(_writer));
		}
	}

	public class entry_element_must {
		[Fact]
		public void have_all_fields_filled() {
			var writer = XmlWriter.Create(new MemoryStream());

			var entry = new EntryElement();
			Assert.Throws<AtomSpecificationViolationException>(() => entry.WriteXml(writer));

			entry.SetTitle("test #0");
			Assert.Throws<AtomSpecificationViolationException>(() => entry.WriteXml(writer));

			entry.SetId("guid");
			Assert.Throws<AtomSpecificationViolationException>(() => entry.WriteXml(writer));

			entry.SetUpdated(DateTime.UtcNow);
			Assert.Throws<AtomSpecificationViolationException>(() => entry.WriteXml(writer));

			entry.SetAuthor(AtomSpecs.Author);
			Assert.Throws<AtomSpecificationViolationException>(() => entry.WriteXml(writer));

			entry.SetSummary("Entry #0");

			entry.WriteXml(writer);
			writer.Close();
		}
	}

	public class link_element_must {
		[Fact]
		public void have_href_attribute() {
			var link = new LinkElement(null);
			var writer = XmlWriter.Create(new MemoryStream());

			Assert.Throws<AtomSpecificationViolationException>(() => link.WriteXml(writer));
			writer.Close();
		}
	}

	public class person_element_must {
		[Fact]
		public void have_exactly_one_name_attribute() {
			var person = new PersonElement(null);
			var writer = XmlWriter.Create(new MemoryStream());

			Assert.Throws<AtomSpecificationViolationException>(() => person.WriteXml(writer));
			writer.Close();
		}
	}

	public class service_document_must {
		[Fact]
		public void have_at_least_one_workspace() {
			var writer = XmlWriter.Create(new MemoryStream());
			var doc = new ServiceDocument();

			Assert.Throws<AtomSpecificationViolationException>(() => doc.WriteXml(writer));
			writer.Close();
		}
	}

	public class workspace_must {
		[Fact]
		public void contain_title() {
			var writer = XmlWriter.Create(new MemoryStream());
			var workspace = new WorkspaceElement();

			Assert.Throws<AtomSpecificationViolationException>(() => workspace.WriteXml(writer));
			writer.Close();
		}
	}

	public class collection_element_must : IDisposable {
		private CollectionElement _collection;
		private XmlWriter _writer;

		public collection_element_must() {
			_writer = XmlWriter.Create(new MemoryStream());
			_collection = new CollectionElement();
		}

		public void Dispose() {
			_writer.Close();
		}

		[Fact]
		public void contain_title() {
			//_collection.SetTitle("title");
			_collection.SetUri("http://127.0.0.1/streams/test");
			_collection.AddAcceptType(ContentType.Atom);

			Assert.Throws<AtomSpecificationViolationException>(() => _collection.WriteXml(_writer));
		}

		[Fact]
		public void contain_uri() {
			_collection.SetTitle("title");
			//_collection.SetUri("http://127.0.0.1/streams/test");
			_collection.AddAcceptType(ContentType.Atom);

			Assert.Throws<AtomSpecificationViolationException>(() => _collection.WriteXml(_writer));
		}
	}

	public class accept_element_must {
		[Fact]
		public void contain_value() {
			var writer = XmlWriter.Create(new MemoryStream());
			var accept = new AcceptElement(null);

			Assert.Throws<AtomSpecificationViolationException>(() => accept.WriteXml(writer));
			writer.Close();
		}
	}
}
