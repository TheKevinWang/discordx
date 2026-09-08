using System.Text;
using discordx.Models.Server;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using JsonDocument = System.Text.Json.JsonDocument;
using JsonDocumentOptions = System.Text.Json.JsonDocumentOptions;
using JsonCommentHandling = System.Text.Json.JsonCommentHandling;

namespace discordx.EnvelopeCodecs
{
    internal sealed class JsonV1TransportEnvelopeFormat : ITransportEnvelopeFormat
    {
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);
        private static readonly JsonLoadSettings StrictLoadSettings = new()
        {
            CommentHandling = CommentHandling.Load,
            DuplicatePropertyNameHandling = DuplicatePropertyNameHandling.Error,
            LineInfoHandling = LineInfoHandling.Ignore,
        };
        private static readonly HashSet<string> AllowedFields = new(StringComparer.Ordinal)
        {
            "message", "sender_id", "to_server", "client_id", "message_format", "id", "final",
        };

        public string Name => "json-v1";

        public byte[] Serialize(TransportEnvelopeMessage message, TransportDirection direction)
        {
            ValidateModel(message, direction);
            var value = new JObject
            {
                ["message"] = StrictUtf8.GetString(message.Message.Span),
                ["sender_id"] = message.SenderId,
                ["to_server"] = message.ToServer,
            };
            if (message.ClientId is not null) value["client_id"] = message.ClientId;
            if (message.MessageFormat == AgentMessageFormat.RawV1) value["message_format"] = "raw-v1";
            value["id"] = message.Id ?? 1;
            value["final"] = message.Final ?? true;
            return StrictUtf8.GetBytes(value.ToString(Formatting.None));
        }

        public TransportEnvelopeMessage Deserialize(ReadOnlySpan<byte> bytes, TransportDirection expectedDirection)
        {
            try
            {
                var json = StrictUtf8.GetString(bytes);
                using var document = JsonDocument.Parse(json, new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = 32,
                });
                var value = JObject.Parse(json, StrictLoadSettings);
                if (value.Properties().Any(property => !AllowedFields.Contains(property.Name)))
                    throw new DiscordEnvelopeException("JSON envelope contains unknown fields");
                if (value["message"]?.Type != JTokenType.String ||
                    value["sender_id"]?.Type != JTokenType.String ||
                    value["to_server"]?.Type != JTokenType.Boolean)
                    throw new DiscordEnvelopeException("JSON envelope is missing required fields");
                if (value["id"] is JToken id && !IsIntegerOne(id))
                    throw new DiscordEnvelopeException("JSON envelope has invalid id");
                if (value["final"] is JToken final &&
                    (final.Type != JTokenType.Boolean || !(bool)final))
                    throw new DiscordEnvelopeException("JSON envelope has invalid final flag");
                var formatText = (string?)value["message_format"];
                if (formatText is not null && formatText != "raw-v1")
                    throw new DiscordEnvelopeException("JSON envelope has unsupported message_format");
                var model = new TransportEnvelopeMessage(
                    StrictUtf8.GetBytes((string)value["message"]!),
                    (string)value["sender_id"]!,
                    (bool)value["to_server"]!,
                    (string?)value["client_id"],
                    formatText == "raw-v1" ? AgentMessageFormat.RawV1 : AgentMessageFormat.Legacy,
                    value["id"] is null ? null : 1,
                    value["final"] is null ? null : true);
                ValidateModel(model, expectedDirection);
                return model;
            }
            catch (Exception exception) when (
                exception is JsonException or System.Text.Json.JsonException or DecoderFallbackException)
            {
                throw new DiscordEnvelopeException("JSON transport envelope rejected", exception);
            }
        }

        private static bool IsIntegerOne(JToken value) =>
            value.Type == JTokenType.Integer && value.ToString(Formatting.None) == "1";

        private static void ValidateModel(TransportEnvelopeMessage message, TransportDirection direction)
        {
            var toServer = direction == TransportDirection.AgentToServer;
            if (message.ToServer != toServer) throw new DiscordEnvelopeException("JSON envelope direction mismatch");
            RequireCanonicalUuid(message.SenderId, "sender_id");
            string route;
            if (toServer)
            {
                if (message.ClientId is not null) throw new DiscordEnvelopeException("Agent request must omit client_id");
                route = message.SenderId;
            }
            else
            {
                if (message.ClientId is null) throw new DiscordEnvelopeException("Server response requires client_id");
                RequireCanonicalUuid(message.ClientId, "client_id");
                route = message.ClientId;
            }
            ValidateBody(message.Message.Span, route, message.MessageFormat);
            if (message.Id is not null && message.Id != 1) throw new DiscordEnvelopeException("JSON envelope has invalid id");
            if (message.Final is not null && message.Final != true) throw new DiscordEnvelopeException("JSON envelope has invalid final flag");
        }

        internal static void RequireCanonicalUuid(string value, string field)
        {
            if (!Guid.TryParseExact(value, "D", out var uuid) || uuid.ToString("D") != value)
                throw new DiscordEnvelopeException($"Transport {field} is not canonical");
        }

        internal static void ValidateBody(ReadOnlySpan<byte> body, string route, AgentMessageFormat format)
        {
            if (body.Length == 0) throw new DiscordEnvelopeException("Transport message body is empty");
            byte[] decoded;
            if (format == AgentMessageFormat.Legacy)
            {
                string text;
                try { text = StrictUtf8.GetString(body); decoded = Convert.FromBase64String(text); }
                catch (Exception exception) when (exception is DecoderFallbackException or FormatException)
                { throw new DiscordEnvelopeException("Legacy body is not canonical Base64", exception); }
                if (Convert.ToBase64String(decoded) != text)
                    throw new DiscordEnvelopeException("Legacy body is not canonical Base64");
            }
            else
            {
                decoded = body.ToArray();
            }
            if (decoded.Length < 36 || !decoded.AsSpan(0, 36).SequenceEqual(Encoding.ASCII.GetBytes(route)))
                throw new DiscordEnvelopeException("Message UUID does not match its route");
        }
    }
}
