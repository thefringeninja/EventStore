using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using EventStore.Core.Services.VNode;
using Xunit;

namespace EventStore.Core.Tests.Services.VNode {
	internal abstract class P : Message {
		private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

		public override int MsgTypeId {
			get { return TypeId; }
		}
	}

	internal class A : P {
		private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

		public override int MsgTypeId {
			get { return TypeId; }
		}
	}

	internal class B : P {
		private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

		public override int MsgTypeId {
			get { return TypeId; }
		}
	}

	internal class C : Message {
		private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

		public override int MsgTypeId {
			get { return TypeId; }
		}
	}

	public class vnode_fsm_should {
		[Fact]
		public void allow_ignoring_messages_by_common_ancestor() {
			var fsm = new VNodeFSMBuilder(() => VNodeState.Master)
				.InAnyState()
				.When<P>().Ignore()
				.WhenOther().Do(x => throw new Exception($"{x.GetType().Name} slipped through"))
				.Build();

			fsm.Handle(new A());
			fsm.Handle(new B());
		}

		[Fact]
		public void handle_specific_message_even_if_base_message_is_ignored() {
			bool aHandled = false;
			var fsm = new VNodeFSMBuilder(() => VNodeState.Master)
				.InAnyState()
				.When<P>().Ignore()
				.When<A>().Do(x => aHandled = true)
				.WhenOther().Do(x => throw new Exception($"{x.GetType().Name} slipped through"))
				.Build();

			fsm.Handle(new A());
			fsm.Handle(new B());

			Assert.True(aHandled);
		}
	}
}
