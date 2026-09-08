using System.Text;
using discordx.EnvelopeCodecs;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace discordx.Tests.EnvelopeCodecs
{
    [TestClass]
    public class DiscordPresentationCodecTests
    {
        private const string Uuid = "00000000-0000-0000-0000-000000000000";

        [TestMethod]
        public void DecimalCodec_RoundTripsUtf8AndRejectsMalformedText()
        {
            var codec = new DecimalDiscordEnvelopeCodec();
            var context = DiscordEnvelopeContext.ForRequest(Uuid);
            var source = Encoding.UTF8.GetBytes("snowman ☃");
            var encoded = codec.Encode(source, context);

            CollectionAssert.AreEqual(source, codec.Decode(encoded, context));
            Assert.IsTrue(encoded.All(Char.IsDigit));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("12", context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("25x", context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("256", context));
        }

        [TestMethod]
        public void Base64Codec_IsCanonicalAndRejectsMalformedText()
        {
            var codec = new Base64DiscordEnvelopeCodec();
            var context = DiscordEnvelopeContext.ForRequest(Uuid);
            var source = Enumerable.Range(0, 256).Select(value => (byte)value).ToArray();
            var encoded = codec.Encode(source, context);

            CollectionAssert.AreEqual(source, codec.Decode(encoded, context));
            Assert.AreEqual("e30=", codec.Encode(Encoding.UTF8.GetBytes("{}"), context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("e30", context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("e30=\n", context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("e31=", context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("e3$=", context));
        }

        [TestMethod]
        public void EmojiCodec_RoundTripsBytesAndRejectsNoncanonicalText()
        {
            var codec = new EmojiDiscordEnvelopeCodec();
            var context = DiscordEnvelopeContext.ForRequest(Uuid);
            var source = Encoding.UTF8.GetBytes("{} snowman ☃");
            var encoded = codec.Encode(source, context);

            CollectionAssert.AreEqual(source, codec.Decode(encoded, context));
            Assert.AreEqual("😫😉😫😨", codec.Encode(Encoding.UTF8.GetBytes("{}"), context));
            Assert.IsFalse(encoded.Any(Char.IsAsciiLetterOrDigit));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("😁", context));
            Assert.ThrowsException<DiscordEnvelopeException>(() => codec.Decode("💩😁", context));
        }
    }
}
