using Discord.Net.Rest;
using Discord.Net.WebSockets;
using discordx.EnvelopeCodecs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace discordx.Clients
{
    internal static class DiscordProviderEndpoints
    {
        internal static string NormalizeApiOrigin(string value) =>
            NormalizeOrigin(value, "API", "https", "http", allowEmpty: false);

        internal static string NormalizeGatewayOrigin(string value) =>
            NormalizeOrigin(value, "Gateway", "wss", "ws", allowEmpty: true);

        internal static string NormalizeCdnOrigin(string value) =>
            NormalizeOrigin(value, "CDN", "https", "http", allowEmpty: false);

        internal static string ApiBaseUrl(string apiOrigin) =>
            NormalizeApiOrigin(apiOrigin) + "/api/v10/";

        internal static string? GatewayHost(string gatewayOrigin)
        {
            var normalized = NormalizeGatewayOrigin(gatewayOrigin);
            return normalized.Length == 0 ? null : normalized;
        }

        internal static RestClientProvider RestClientProvider(string apiOrigin)
        {
            var baseUrl = ApiBaseUrl(apiOrigin);
            var isSpacebar = !String.Equals(
                NormalizeApiOrigin(apiOrigin),
                "https://discord.com",
                StringComparison.OrdinalIgnoreCase);
            return _ =>
            {
                var client = DefaultRestClientProvider.Instance(baseUrl);
                return isSpacebar ? new SpacebarRestClient(client) : client;
            };
        }

        internal static WebSocketProvider WebSocketProvider(string gatewayOrigin)
        {
            if (String.IsNullOrEmpty(NormalizeGatewayOrigin(gatewayOrigin)))
                return DefaultWebSocketProvider.Instance;
            return () => new UncompressedWebSocketClient(
                DefaultWebSocketProvider.Instance());
        }

        internal static string WithoutGatewayCompression(string value)
        {
            var builder = new UriBuilder(value);
            var query = builder.Query.TrimStart('?').Split('&',
                StringSplitOptions.RemoveEmptyEntries);
            builder.Query = String.Join("&", query.Where(part =>
                !part.StartsWith("compress=", StringComparison.OrdinalIgnoreCase)));
            return builder.Uri.AbsoluteUri;
        }

        internal static string NormalizeGatewayReady(string message)
        {
            try
            {
                var frame = JObject.Parse(message);
                if (String.Equals((string?)frame["t"], "READY", StringComparison.Ordinal) &&
                    frame["d"] is JObject payload &&
                    payload.TryGetValue("read_state", out var readState) &&
                    readState is not JArray)
                {
                    // Spacebar currently emits this as an object. Discord.Net 3.14.1
                    // expects an array, and its converter leaves the reader positioned
                    // inside the object so later READY fields are not populated.
                    payload["read_state"] = new JArray();
                    return frame.ToString(Formatting.None);
                }
            }
            catch (JsonException)
            {
                // Preserve the original frame so Discord.Net owns malformed-frame handling.
            }
            return message;
        }

        internal static string NormalizeRestJson(string value)
        {
            try
            {
                var root = JToken.Parse(value);
                var objects = new List<JObject>();
                if (root is JObject rootObject)
                    objects.Add(rootObject);
                if (root is JContainer container)
                    objects.AddRange(container.Descendants().OfType<JObject>());

                var changed = false;
                foreach (var message in objects)
                {
                    if (message["id"] is not null &&
                        message["channel_id"] is not null &&
                        message["author"] is not null &&
                        message["timestamp"] is not null)
                    {
                        foreach (var property in message.Properties()
                            .Where(property => property.Value.Type == JTokenType.Null)
                            .ToArray())
                        {
                            // Spacebar also emits null for absent optional message
                            // objects such as `thread`. Discord.Net dereferences an
                            // Optional<T> once it is specified, so omission is the
                            // compatible representation of Spacebar's null value.
                            property.Remove();
                            changed = true;
                        }
                    }
                    if (message["attachments"] is not JArray attachments)
                        continue;
                    foreach (var attachment in attachments.OfType<JObject>())
                    {
                        foreach (var property in attachment.Properties()
                            .Where(property => property.Value.Type == JTokenType.Null)
                            .ToArray())
                        {
                            // Spacebar serializes absent optional attachment fields as
                            // null. Discord.Net 3.14.1's Optional<T> converter treats
                            // that as a specified value and rejects null value types.
                            // Removing only null attachment properties preserves the
                            // intended "unspecified" meaning for every optional field.
                            property.Remove();
                            changed = true;
                        }
                    }
                }
                return changed ? root.ToString(Formatting.None) : value;
            }
            catch (JsonException)
            {
                return value;
            }
        }

        internal static IReadOnlyDictionary<string, object> NormalizeSpacebarMultipart(
            IReadOnlyDictionary<string, object> multipartParams)
        {
            var normalized = new Dictionary<string, object>(multipartParams);
            if (!normalized.TryGetValue("payload_json", out var payload) ||
                payload is not string json)
            {
                return normalized;
            }
            try
            {
                var body = JObject.Parse(json);
                if (body.Remove("attachments"))
                    normalized["payload_json"] = body.ToString(Formatting.None);
            }
            catch (JsonException)
            {
                // Preserve malformed payload JSON so the provider returns the
                // authoritative validation error.
            }
            return normalized;
        }

        internal static string ValidateAttachmentUrl(string cdnOrigin, string value)
        {
            try
            {
                var expected = new Uri(NormalizeCdnOrigin(cdnOrigin), UriKind.Absolute);
                var actual = new Uri(value, UriKind.Absolute);
                if (!String.Equals(expected.Scheme, actual.Scheme, StringComparison.OrdinalIgnoreCase) ||
                    !String.Equals(expected.IdnHost, actual.IdnHost, StringComparison.OrdinalIgnoreCase) ||
                    expected.Port != actual.Port || !String.IsNullOrEmpty(actual.UserInfo))
                {
                    throw new DiscordEnvelopeException(
                        "Attachment URL is outside the configured CDN origin");
                }
                return actual.AbsoluteUri;
            }
            catch (DiscordEnvelopeException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new DiscordEnvelopeException("Attachment URL is invalid", exception);
            }
        }

        private static string NormalizeOrigin(
            string value,
            string name,
            string secureScheme,
            string localScheme,
            bool allowEmpty)
        {
            var text = (value ?? String.Empty).Trim();
            if (text.Length == 0 && allowEmpty)
                return String.Empty;
            if (!Uri.TryCreate(text, UriKind.Absolute, out var uri) ||
                (uri.Scheme != secureScheme && uri.Scheme != localScheme) ||
                String.IsNullOrEmpty(uri.Host) || !String.IsNullOrEmpty(uri.UserInfo) ||
                (uri.AbsolutePath != "/" && uri.AbsolutePath != String.Empty) ||
                !String.IsNullOrEmpty(uri.Query) || !String.IsNullOrEmpty(uri.Fragment))
            {
                throw new InvalidOperationException(
                    $"Discord {name} provider origin must be a root origin");
            }
            if (uri.Scheme == localScheme &&
                (!IPAddress.TryParse(uri.Host, out var address) || !IsLocalAddress(address)))
            {
                throw new InvalidOperationException(
                    $"Discord {name} provider origin permits {localScheme} only for a local IP address");
            }
            return uri.GetLeftPart(UriPartial.Authority).ToLowerInvariant();
        }

        private static bool IsLocalAddress(IPAddress address)
        {
            if (IPAddress.IsLoopback(address) || address.IsIPv6LinkLocal)
                return true;
            var bytes = address.GetAddressBytes();
            if (address.AddressFamily == AddressFamily.InterNetwork)
            {
                return bytes[0] == 10 ||
                    (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31) ||
                    (bytes[0] == 192 && bytes[1] == 168) ||
                    (bytes[0] == 169 && bytes[1] == 254);
            }
            return address.AddressFamily == AddressFamily.InterNetworkV6 &&
                (bytes[0] & 0xfe) == 0xfc;
        }

        private sealed class SpacebarRestClient : IRestClient
        {
            private readonly IRestClient _inner;

            internal SpacebarRestClient(IRestClient inner)
            {
                _inner = inner;
            }

            public void Dispose() => _inner.Dispose();
            public void SetHeader(string key, string value) => _inner.SetHeader(key, value);
            public void SetCancelToken(CancellationToken cancelToken) =>
                _inner.SetCancelToken(cancelToken);

            public async Task<RestResponse> SendAsync(
                string method,
                string endpoint,
                CancellationToken cancelToken,
                bool headerOnly = false,
                string? reason = null,
                IEnumerable<KeyValuePair<string, IEnumerable<string>>>? requestHeaders = null) =>
                await NormalizeResponseAsync(
                    await _inner.SendAsync(method, endpoint, cancelToken, headerOnly,
                        reason, requestHeaders),
                    cancelToken);

            public async Task<RestResponse> SendAsync(
                string method,
                string endpoint,
                string json,
                CancellationToken cancelToken,
                bool headerOnly = false,
                string? reason = null,
                IEnumerable<KeyValuePair<string, IEnumerable<string>>>? requestHeaders = null) =>
                await NormalizeResponseAsync(
                    await _inner.SendAsync(method, endpoint, json, cancelToken,
                        headerOnly, reason, requestHeaders),
                    cancelToken);

            public async Task<RestResponse> SendAsync(
                string method,
                string endpoint,
                IReadOnlyDictionary<string, object> multipartParams,
                CancellationToken cancelToken,
                bool headerOnly = false,
                string? reason = null,
                IEnumerable<KeyValuePair<string, IEnumerable<string>>>? requestHeaders = null) =>
                await NormalizeResponseAsync(
                    await _inner.SendAsync(method, endpoint,
                        NormalizeSpacebarMultipart(multipartParams),
                        cancelToken, headerOnly, reason, requestHeaders),
                    cancelToken);

            private static async Task<RestResponse> NormalizeResponseAsync(
                RestResponse response,
                CancellationToken cancelToken)
            {
                if (response.Stream is null)
                    return response;

                using var source = response.Stream;
                using var copy = new MemoryStream();
                await source.CopyToAsync(copy, cancelToken);
                var bytes = copy.ToArray();
                try
                {
                    var text = new UTF8Encoding(false, true).GetString(bytes);
                    var normalized = NormalizeRestJson(text);
                    bytes = Encoding.UTF8.GetBytes(normalized);
                }
                catch (DecoderFallbackException)
                {
                    // Preserve an unexpected non-JSON response byte-for-byte.
                }
                return new RestResponse(
                    response.StatusCode,
                    response.Headers,
                    new MemoryStream(bytes, writable: false));
            }
        }

        private sealed class UncompressedWebSocketClient : IWebSocketClient
        {
            private readonly IWebSocketClient _inner;

            internal UncompressedWebSocketClient(IWebSocketClient inner)
            {
                _inner = inner;
                _inner.TextMessage += OnTextMessage;
            }

            public event Func<byte[], int, int, Task> BinaryMessage
            {
                add => _inner.BinaryMessage += value;
                remove => _inner.BinaryMessage -= value;
            }
            public event Func<string, Task>? TextMessage;
            public event Func<Exception, Task> Closed
            {
                add => _inner.Closed += value;
                remove => _inner.Closed -= value;
            }

            public void Dispose() => _inner.Dispose();
            public void SetHeader(string key, string value) => _inner.SetHeader(key, value);
            public void SetCancelToken(CancellationToken cancelToken) =>
                _inner.SetCancelToken(cancelToken);
            public Task ConnectAsync(string host) =>
                _inner.ConnectAsync(WithoutGatewayCompression(host));
            public Task DisconnectAsync(int closeCode = 1000) =>
                _inner.DisconnectAsync(closeCode);
            public Task SendAsync(byte[] data, int index, int count, bool isText) =>
                _inner.SendAsync(data, index, count, isText);

            private Task OnTextMessage(string message)
            {
                var handler = TextMessage;
                return handler is null
                    ? Task.CompletedTask
                    : handler(NormalizeGatewayReady(message));
            }
        }
    }
}
