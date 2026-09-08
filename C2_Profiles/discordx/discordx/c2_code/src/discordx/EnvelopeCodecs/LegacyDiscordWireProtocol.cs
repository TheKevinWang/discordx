using System.Text;
using discordx.Models.Server;
using Newtonsoft.Json;

namespace discordx.EnvelopeCodecs
{
    internal sealed record LegacyDiscordRequest(
        ReadOnlyMemory<byte> Message,
        string TrackingId,
        AgentMessageFormat MessageFormat);

    internal static class LegacyDiscordWireProtocol
    {
        private static readonly JsonSerializerSettings ResponseSerializerSettings = new()
        {
            NullValueHandling = NullValueHandling.Ignore,
        };

        public static bool TryDecodeRequest(string document, out LegacyDiscordRequest request)
        {
            request = default!;
            try
            {
                var wrapper = JsonConvert.DeserializeObject<LegacyDiscordMessageWrapper>(document);
                if (wrapper is null || !wrapper.ToServer ||
                    wrapper.Message is null || String.IsNullOrEmpty(wrapper.SenderId))
                {
                    return false;
                }

                var messageFormat = wrapper.MessageFormat switch
                {
                    null or "" => AgentMessageFormat.Legacy,
                    "raw-v1" => AgentMessageFormat.RawV1,
                    _ => throw new DiscordEnvelopeException("Legacy wrapper has unsupported message_format"),
                };

                request = new LegacyDiscordRequest(
                    Encoding.UTF8.GetBytes(wrapper.Message),
                    wrapper.SenderId,
                    messageFormat);
                return true;
            }
            catch (JsonException)
            {
                return false;
            }
            catch (DiscordEnvelopeException)
            {
                return false;
            }
        }

        public static string EncodeResponse(
            ReadOnlyMemory<byte> message,
            string clientId,
            string serverId)
        {
            var wrapper = new LegacyDiscordMessageWrapper
            {
                Message = Encoding.UTF8.GetString(message.Span),
                SenderId = serverId,
                ToServer = false,
                ClientId = clientId,
            };
            return JsonConvert.SerializeObject(wrapper, ResponseSerializerSettings);
        }

        private sealed class LegacyDiscordMessageWrapper
        {
            [JsonProperty("message")]
            public string? Message { get; set; }

            [JsonProperty("sender_id")]
            public string? SenderId { get; set; }

            [JsonProperty("to_server")]
            public bool ToServer { get; set; }

            [JsonProperty("client_id")]
            public string? ClientId { get; set; }

            [JsonProperty("message_format")]
            public string? MessageFormat { get; set; }
        }
    }
}
