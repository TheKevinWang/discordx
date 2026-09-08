using System.Text;
using discordx.EnvelopeCodecs;
using discordx.Models.Server;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace discordx.Tests.EnvelopeCodecs
{
    [TestClass]
    public class FixedTransportEnvelopeProtocolTests
    {
        private const string Uuid = "00000000-0000-0000-0000-000000000000";
        private const string Client = "11111111-1111-1111-1111-111111111111";
        private static readonly byte[] MasterKey = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();

        [TestMethod]
        public void SharedFixture_MatchesJsonAndBinaryFormatBytes()
        {
            var fixture = JObject.Parse(File.ReadAllText(Path.Combine(
                AppContext.BaseDirectory, "Fixtures", "discord-dual-envelope-v1-vectors.json")));
            foreach (var vector in (JArray)fixture["vectors"]!)
            {
                var body = Convert.FromBase64String((string)vector["body_base64"]!);
                var expected = Convert.FromBase64String((string)vector["envelope_base64"]!);
                var direction = (string)vector["direction"]! == "agent-to-server"
                    ? TransportDirection.AgentToServer : TransportDirection.ServerToAgent;
                var useBase64 = (bool)vector["use_base64"]!;
                var route = direction == TransportDirection.AgentToServer ? Uuid : Client;
                ITransportEnvelopeFormat format = (string)vector["format"]! switch
                {
                    "json-v1" => new JsonV1TransportEnvelopeFormat(),
                    "binary-v1" => new BinaryV1TransportEnvelopeFormat(
                        useBase64 ? AgentMessageFormat.Legacy : AgentMessageFormat.RawV1),
                    _ => throw new AssertFailedException("Unknown fixture format"),
                };
                var message = new TransportEnvelopeMessage(
                    body,
                    direction == TransportDirection.AgentToServer ? route : Uuid,
                    direction == TransportDirection.AgentToServer,
                    direction == TransportDirection.ServerToAgent ? route : null,
                    useBase64 ? AgentMessageFormat.Legacy : AgentMessageFormat.RawV1,
                    1,
                    true);
                CollectionAssert.AreEqual(expected, format.Serialize(message, direction), (string)vector["name"]!);
                CollectionAssert.AreEqual(body, format.Deserialize(expected, direction).Message.ToArray());
            }
        }

        [TestMethod]
        public void JsonV1_Plain_RoundTripsRawUtf8WithoutWireMetadata()
        {
            var body = Encoding.UTF8.GetBytes(Uuid + "{\"path\":\"C:\\\\tmp\",\"snow\":\"☃\",\"line\":\"a\\nb\"}");
            var protocol = FixedTransportEnvelopeProtocol.Create(
                "json-v1", "plain", "none", "single", Array.Empty<byte>(), useBase64: false);

            var document = protocol.Encode(Request(body, AgentMessageFormat.RawV1), TransportDirection.AgentToServer);
            Assert.IsTrue(document.StartsWith("{\"message\":", StringComparison.Ordinal));
            Assert.IsFalse(document.Contains("envelope_version", StringComparison.Ordinal));
            Assert.IsFalse(document.Contains("envelope_codec", StringComparison.Ordinal));
            var decoded = protocol.Decode(document, TransportDirection.AgentToServer);
            CollectionAssert.AreEqual(body, decoded.Message.ToArray());
        }

        [TestMethod]
        public void JsonV1_RejectsRedundantMetadataDuplicatesUnknownFieldsAndWrongDirection()
        {
            var protocol = FixedTransportEnvelopeProtocol.Create(
                "json-v1", "plain", "none", "single", Array.Empty<byte>(), useBase64: false);
            var body = Uuid + "{}";
            var invalid = new[]
            {
                $"{{\"message\":\"{body}\",\"sender_id\":\"{Uuid}\",\"to_server\":true,\"envelope_version\":1}}",
                $"{{\"message\":\"{body}\",\"message\":\"{body}\",\"sender_id\":\"{Uuid}\",\"to_server\":true}}",
                $"{{\"message\":\"{body}\",\"sender_id\":\"{Uuid}\",\"to_server\":true,\"unknown\":1}}",
            };
            foreach (var document in invalid)
                Assert.ThrowsException<DiscordEnvelopeException>(() =>
                    protocol.Decode(document, TransportDirection.AgentToServer));
            var valid = protocol.Encode(Request(Encoding.UTF8.GetBytes(body), AgentMessageFormat.RawV1), TransportDirection.AgentToServer);
            Assert.ThrowsException<DiscordEnvelopeException>(() =>
                protocol.Decode(valid, TransportDirection.ServerToAgent));
        }

        [TestMethod]
        public void BinaryV1_Base64_RoundTripsEveryByteWithoutUtf8Conversion()
        {
            var suffix = Enumerable.Range(0, 256).Select(value => (byte)value).ToArray();
            var body = Encoding.ASCII.GetBytes(Uuid).Concat(suffix).ToArray();
            var protocol = FixedTransportEnvelopeProtocol.Create(
                "binary-v1", "base64", "none", "single", Array.Empty<byte>(), useBase64: false);

            var document = protocol.Encode(Request(body, AgentMessageFormat.RawV1), TransportDirection.AgentToServer);
            var envelope = Convert.FromBase64String(document);
            Assert.AreEqual(0x03, envelope[0]);
            CollectionAssert.AreEqual(body, envelope[BinaryV1TransportEnvelopeFormat.HeaderLength..]);
            CollectionAssert.AreEqual(
                body,
                protocol.Decode(document, TransportDirection.AgentToServer).Message.ToArray());
        }

        [TestMethod]
        public void BinaryV1_RejectsReservedFlagsShortFramesAndRouteMismatch()
        {
            var format = new BinaryV1TransportEnvelopeFormat(AgentMessageFormat.RawV1);
            var valid = format.Serialize(Request(Encoding.ASCII.GetBytes(Uuid + "{}"), AgentMessageFormat.RawV1), TransportDirection.AgentToServer);
            valid[0] |= 0x80;
            Assert.ThrowsException<DiscordEnvelopeException>(() =>
                format.Deserialize(valid, TransportDirection.AgentToServer));
            Assert.ThrowsException<DiscordEnvelopeException>(() =>
                format.Deserialize(new byte[37], TransportDirection.AgentToServer));
            valid[0] = 0x03;
            valid[^1] ^= 1;
            var mismatched = Encoding.ASCII.GetBytes(Client + "{}");
            mismatched.CopyTo(valid, BinaryV1TransportEnvelopeFormat.HeaderLength);
            Assert.ThrowsException<DiscordEnvelopeException>(() =>
                format.Deserialize(valid, TransportDirection.AgentToServer));
        }

        [TestMethod]
        public void BinaryV1_SupportsHistoricalBase64FramingAndResponseRoutes()
        {
            var inner = Encoding.UTF8.GetBytes(Client + "{\"tasks\":[]}");
            var body = Encoding.ASCII.GetBytes(Convert.ToBase64String(inner));
            var protocol = FixedTransportEnvelopeProtocol.Create(
                "binary-v1", "decimal", "none", "single", Array.Empty<byte>(), useBase64: true);
            var response = new TransportEnvelopeMessage(
                body, Uuid, false, Client, AgentMessageFormat.Legacy, 1, true);

            var document = protocol.Encode(response, TransportDirection.ServerToAgent);
            var decoded = protocol.Decode(document, TransportDirection.ServerToAgent);
            Assert.AreEqual(AgentMessageFormat.Legacy, decoded.MessageFormat);
            CollectionAssert.AreEqual(body, decoded.Message.ToArray());
        }

        [TestMethod]
        public void FixedProtocol_RejectsInvalidPlainCompositionsAndOldSavedNames()
        {
            Assert.ThrowsException<DiscordEnvelopeException>(() => FixedTransportEnvelopeProtocol.Create(
                "binary-v1", "plain", "none", "single", Array.Empty<byte>(), false));
            Assert.ThrowsException<DiscordEnvelopeException>(() => FixedTransportEnvelopeProtocol.Create(
                "json-v1", "plain", "chacha20-v1", "single", MasterKey, false));
            Assert.ThrowsException<DiscordEnvelopeException>(() => FixedTransportEnvelopeProtocol.Create(
                "json-v1", "legacy-json", "none", "single", Array.Empty<byte>(), false));
        }

        [TestMethod]
        public void ChaCha20_MatchesIetfBlockVectorAtCounterOne()
        {
            var nonce = Convert.FromHexString("000000090000004A00000000");
            var output = ChaCha20TransportProtection.Transform(new byte[64], MasterKey, nonce, initialCounter: 1);
            Assert.AreEqual(
                "10F1E7E4D13B5915500FDD1FA32071C4C7D1F4C733C068030422AA9AC3D46C4E" +
                "D2826446079FAA0914C2D705D98B02A2B5129CD1DE164EB9CBD083E8A2503C4E",
                Convert.ToHexString(output));
        }

        [TestMethod]
        public void AesHmac_AuthenticatesDirectionBeforeDecrypting()
        {
            var protection = new Aes256HmacTransportProtection(MasterKey, directional: false);
            var entropy = new FixedEntropy(Enumerable.Repeat((byte)0xA5, 16).ToArray());
            var source = Encoding.UTF8.GetBytes("protected wrapper");
            var packet = protection.Protect(source, TransportDirection.AgentToServer, entropy);
            CollectionAssert.AreEqual(source, protection.Unprotect(packet, TransportDirection.AgentToServer));
            Assert.ThrowsException<DiscordEnvelopeException>(() =>
                protection.Unprotect(packet, TransportDirection.ServerToAgent));
        }

        [TestMethod]
        public void TrackingRoute_DropsAResponseAfterConfigurationChanges()
        {
            var route = DiscordTrackingRoute.ForFixed(Uuid, "0123456789abcdef").Serialize();
            Assert.IsTrue(DiscordTrackingRoute.TryParseFixed(route, "0123456789abcdef", out var parsed));
            Assert.AreEqual(Uuid, parsed.ClientId);
            Assert.IsFalse(DiscordTrackingRoute.TryParseFixed(route, "fedcba9876543210", out _));
        }

        private static TransportEnvelopeMessage Request(byte[] body, AgentMessageFormat format) =>
            new(body, Uuid, true, null, format, 1, true);

        private sealed class FixedEntropy : ITransportEntropy
        {
            private readonly byte[] _bytes;
            public FixedEntropy(byte[] bytes) => _bytes = bytes;
            public byte[] GetBytes(int count)
            {
                Assert.AreEqual(count, _bytes.Length);
                return _bytes.ToArray();
            }
        }
    }
}
