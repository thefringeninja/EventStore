using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Core.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Helpers {
	public class length_prefix_suffix_framer_should {
		[Fact]
		public void correctly_frame_byte_array() {
			var framer = new LengthPrefixSuffixFramer(r => { });
			var data = new byte[] {0x7, 0x17, 0x27};
			var framedData = MergeBytes(framer.FrameData(new ArraySegment<byte>(data)));

			Assert.Equal(11, framedData.Length);

			Assert.Equal(0x03, framedData[0]);
			Assert.Equal(0x00, framedData[1]);
			Assert.Equal(0x00, framedData[2]);
			Assert.Equal(0x00, framedData[3]);

			Assert.Equal(0x07, framedData[4]);
			Assert.Equal(0x17, framedData[5]);
			Assert.Equal(0x27, framedData[6]);

			Assert.Equal(0x03, framedData[7]);
			Assert.Equal(0x00, framedData[8]);
			Assert.Equal(0x00, framedData[9]);
			Assert.Equal(0x00, framedData[10]);
		}

		private byte[] MergeBytes(IEnumerable<ArraySegment<byte>> frameData) {
			var bytes = new List<byte>();
			foreach (var segm in frameData) {
				for (int i = segm.Offset; i < segm.Offset + segm.Count; ++i) {
					bytes.Add(segm.Array[i]);
				}
			}

			return bytes.ToArray();
		}

		[Fact]
		public void unframe_record_when_provided_exactly_enough_data_in_one_call() {
			int unframedCnt = 0;
			var framer = new LengthPrefixSuffixFramer(r => {
				unframedCnt += 1;
				Assert.Equal(new byte[] {0x07, 0x17, 0x27}, ReadAll(r));
			});

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {
				0x03, 0x00, 0x00, 0x00,
				0x07, 0x17, 0x27,
				0x03, 0x00, 0x00, 0x00
			}));

			Assert.Equal(1, unframedCnt);
		}

		[Fact]
		public void unframe_record_when_provided_with_small_chunks_of_data_at_a_time() {
			int unframedCnt = 0;
			var framer = new LengthPrefixSuffixFramer(r => {
				unframedCnt += 1;
				Assert.Equal(new byte[] {0x07, 0x17, 0x27}, ReadAll(r));
			});

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {0x03, 0x00}));
			framer.UnFrameData(new ArraySegment<byte>(new byte[] {0x00, 0x00}));
			framer.UnFrameData(new ArraySegment<byte>(new byte[] {0x07, 0x17, 0x27}));
			framer.UnFrameData(new ArraySegment<byte>(new byte[] {0x03, 0x00}));

			Assert.Equal(0, unframedCnt);
			framer.UnFrameData(new ArraySegment<byte>(new byte[] {0x00, 0x00}));
			Assert.Equal(1, unframedCnt);
		}

		[Fact]
		public void unframe_two_consecutive_records() {
			int unframedCnt = 0;
			var framer = new LengthPrefixSuffixFramer(r => {
				if (unframedCnt == 0)
					Assert.Equal(new byte[] {0x07, 0x17, 0x27}, ReadAll(r));
				else if (unframedCnt == 1)
					Assert.Equal(new byte[] {0x05, 0x15}, ReadAll(r));
				else
					throw new Exception();

				unframedCnt += 1;
			});

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {
				0x03, 0x00, 0x00, 0x00,
				0x07, 0x17, 0x27
			}));

			Assert.Equal(0, unframedCnt);

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {
				0x03, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,
				0x05, 0x15
			}));

			Assert.Equal(1, unframedCnt);

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {0x02, 0x00, 0x00, 0x00}));

			Assert.Equal(2, unframedCnt);
		}

		[Fact]
		public void discard_data_when_reset_and_continue_unframing_from_blank_slate() {
			int unframedCnt = 0;
			var framer = new LengthPrefixSuffixFramer(r => {
				if (unframedCnt == 0)
					Assert.Equal(new byte[] {0x07, 0x17, 0x27}, ReadAll(r));
				else if (unframedCnt == 1)
					Assert.Equal(new byte[] {0x05, 0x15}, ReadAll(r));
				else
					throw new Exception();

				unframedCnt += 1;
			});

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {
				0x03, 0x00, 0x00, 0x00,
				0x07, 0x17, 0x27,
				0x03, 0x00, 0x00, 0x00,
				0xAA, 0xBB, 0xCC, 0x00,
				0x01, 0x02, 0x03, 0x04,
				0x05, 0x06, 0x07, 0x08
			}));

			Assert.Equal(1, unframedCnt);

			framer.Reset();

			framer.UnFrameData(new ArraySegment<byte>(new byte[] {
				0x02, 0x00, 0x00, 0x00,
				0x05, 0x15,
				0x02, 0x00, 0x00, 0x00
			}));

			Assert.Equal(2, unframedCnt);
		}

		private byte[] ReadAll(BinaryReader br) {
			var buf = new byte[100000];
			int read = br.Read(buf, 0, buf.Length);

			var res = new byte[read];
			Buffer.BlockCopy(buf, 0, res, 0, read);
			return res;
		}
	}
}
