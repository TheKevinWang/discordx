using discordx.Models.Server;
using Discord;
using discordx.EnvelopeCodecs;
using Discord.WebSocket;
using System.Text;
using System.Threading.Channels;
using IDiscordClient = discordx.Models.Server.IDiscordClient;

namespace discordx.Clients
{
    public class DiscordClient : IDiscordClient
    {
        private readonly DiscordSocketClient _discordClient;
        private readonly HttpClient _httpClient;
        private readonly IMythicClient _mythicClient;
        private readonly SemaphoreSlim _outboundWriteLock = new(1, 1);
        private AutoResetEvent _ready = new AutoResetEvent(false);
        private ITextChannel _channel;
        private readonly string _uuid;
        private readonly IServerConfig _config;
        private readonly FixedTransportEnvelopeProtocol _envelopeProtocol;
        private readonly BoundedMessageIdCache _suppressedMessageIds = new(1024);
        private const string ProcessedMessageReaction = "✅";
        private static readonly TimeSpan OutboundWriteTimeout = TimeSpan.FromSeconds(30);
        public DiscordClient(IMythicClient mythicClient, IServerConfig config)
        {
            _envelopeProtocol = FixedTransportEnvelopeProtocol.Create(
                config.TransportEnvelopeFormat,
                config.TransportPresentation,
                config.TransportProtection,
                config.TransportKeyMode,
                config.TransportKey,
                config.UseBase64);
            var discordConfig = new DiscordSocketConfig()
            {
                GatewayIntents = GatewayIntents.AllUnprivileged | GatewayIntents.MessageContent
            };
            _uuid = Guid.NewGuid().ToString();
            _config = config;
            _discordClient = new DiscordSocketClient(discordConfig);
            _discordClient.MessageReceived += MessageReceivedAsync;
            _discordClient.Ready += _client_Ready;
            _httpClient = new HttpClient();
            _mythicClient = mythicClient;
            _mythicClient.ReceiveFromMythicAsync();
            _mythicClient.OnMessageReceived += _mythicClient_OnMessageReceived;

        }
        private async Task _client_Ready()
        {
            _channel = (ITextChannel)_discordClient.GetChannel(ulong.Parse(_config.ChannelID));

            if (_channel is null)
            {
                Console.WriteLine("[WriteToChannel] Unable to get channel: Channel is null");
                Environment.Exit(0);
            }
            await this.CatchUp();
            _ready.Set();
        }

        private async void _mythicClient_OnMessageReceived(object? sender, PushC2Services.PushC2MessageFromMythic e)
        {
            try
            {
                Console.WriteLine("[MythicClient] Received outbound Mythic message");
                // Mythic can push the next task while Discord is still uploading
                // the preceding attachment.  Serialize those sends so a second
                // task is never abandoned by an unobserved concurrent Task.
                await RunSerializedAsync(
                    _outboundWriteLock,
                    () => this.WriteToChannel(e.Message.ToByteArray(), e.TrackingID));
                Console.WriteLine("[MythicClient] Delivered outbound Mythic message to Discord");
            }
            catch (Exception exception)
            {
                Console.WriteLine($"[WriteToChannel] Failed to deliver Mythic task: {exception}");
            }
        }

        internal static async Task RunSerializedAsync(SemaphoreSlim gate, Func<Task> operation)
        {
            await gate.WaitAsync();
            try
            {
                await operation();
            }
            finally
            {
                gate.Release();
            }
        }

        internal static async Task AwaitOutboundWriteAsync(Func<Task> operation, TimeSpan timeout)
        {
            await operation().WaitAsync(timeout);
        }

        internal static async Task<bool> ForwardMessageToMythicAsync(
            IMythicClient mythicClient,
            string senderId,
            ReadOnlyMemory<byte> message,
            AgentMessageFormat format,
            Func<Task> markProcessedAsync,
            Func<Task> deleteAsync)
        {
            var forwarded = await mythicClient.SendToMythic(senderId, message, format);
            if (!forwarded)
            {
                Console.WriteLine("[MessageReceivedAsync] Failed to forward message to Mythic; leaving Discord message in place");
                return false;
            }
            Console.WriteLine("[MessageReceivedAsync] Forwarded inbound agent message to Mythic");

            try
            {
                await markProcessedAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"[MessageReceivedAsync] Failed to mark forwarded Discord message: {e.Message}");
            }

            try
            {
                await deleteAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"[MessageReceivedAsync] {e.Message}");
            }
            return true;
        }

        private async Task CatchUp()
        {
            try
            {
                var messages = await _channel.GetMessagesAsync().FlattenAsync();
                foreach (var message in messages
                    .Where(message => ShouldProcessCatchUpMessage(
                        message.Reactions.Any(reaction =>
                            reaction.Key.Name == ProcessedMessageReaction &&
                            reaction.Value.IsMe),
                        _suppressedMessageIds.Contains(message.Id)))
                    .OrderBy(message => message.Timestamp))
                {
                    await this.MessageReceivedAsync(message);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[CatchUp] {e.ToString()}");
            }
        }

        internal static bool ShouldProcessCatchUpMessage(
            bool processedByCurrentUser,
            bool rejectedByCurrentProcess)
        {
            return !processedByCurrentUser && !rejectedByCurrentProcess;
        }

        internal static async Task RunCatchUpPollingAsync(
            Func<Task> pollAsync,
            TimeSpan interval,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(interval, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    await pollAsync();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[CatchUp] Poll failed: {e}");
                }
            }
        }

        internal static string ResolveFixedTrackingId(
            string senderId,
            string configurationFingerprint) =>
            DiscordTrackingRoute.ForFixed(senderId, configurationFingerprint).Serialize();

        public async Task Start()
        {
            if (_discordClient.LoginState != LoginState.LoggedIn)
            {
                await _discordClient.LoginAsync(TokenType.Bot, _config.BotToken);
            }

            if (_discordClient.ConnectionState != ConnectionState.Connected)
            {
                await _discordClient.StartAsync();
            }

            if (_discordClient.LoginState != LoginState.LoggedIn)
            {
                Console.WriteLine("[Start] Failed to login to discord");
            }

            if (_channel is null && !_ready.WaitOne(TimeSpan.FromSeconds(30)))
            {
                Console.WriteLine("[Start] Timed out waiting for the Discord client to become ready");
                return;
            }
            await RunCatchUpPollingAsync(
                CatchUp,
                TimeSpan.FromSeconds(2),
                CancellationToken.None);
        }

        private async Task MessageReceivedAsync(IMessage message)
        {
            // Gateway delivery and CatchUp can observe the same message before
            // the first forward completes. Claim it before any awaited work so
            // exactly one path can submit it to Mythic.
            if (!_suppressedMessageIds.TryAdd(message.Id))
            {
                return;
            }

            if (_config.WireProtocol == "legacy")
            {
                await HandleLegacyMessageAsync(message);
                return;
            }

            string channelDocument;
            var attachment = message.Attachments.FirstOrDefault();
            if (attachment is not null)
            {
                try
                {
                    channelDocument = await GetFileContentsAsync(attachment.Url);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[MessageReceivedAsync] Attachment read failed: {e.Message}");
                    _suppressedMessageIds.Remove(message.Id);
                    return;
                }
            }
            else
            {
                channelDocument = message.Content;
            }

            TransportEnvelopeMessage discordMessage;
            try
            {
                discordMessage = _envelopeProtocol.Decode(
                    channelDocument,
                    TransportDirection.AgentToServer);
            }
            catch (DiscordEnvelopeException e)
            {
                Console.WriteLine($"[MessageReceivedAsync] Envelope rejected: {e.Message}");
                _suppressedMessageIds.Add(message.Id);
                return;
            }

            if (discordMessage is null || !discordMessage.ToServer)
            {
                _suppressedMessageIds.Add(message.Id);
                return;
            }

            if (discordMessage.ToServer) //It belongs to us
            {
                try
                {
                    var forwarded = await ForwardMessageToMythicAsync(
                        _mythicClient,
                        ResolveFixedTrackingId(
                            discordMessage.SenderId,
                            _config.ConfigurationFingerprint),
                        discordMessage.Message,
                        discordMessage.MessageFormat,
                        () => message.AddReactionAsync(new Emoji(ProcessedMessageReaction)),
                        () => message.DeleteAsync());
                    if (!forwarded)
                    {
                        _suppressedMessageIds.Remove(message.Id);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[MessageReceivedAsync] {e.Message}");
                    _suppressedMessageIds.Remove(message.Id);
                }
            }
        }

        private async Task HandleLegacyMessageAsync(IMessage message)
        {
            string channelDocument;
            var attachment = message.Attachments.FirstOrDefault(candidate =>
                candidate.Filename.EndsWith("server", StringComparison.Ordinal));
            if (attachment is not null)
            {
                try
                {
                    channelDocument = await GetFileContentsAsync(attachment.Url);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[MessageReceivedAsync] Legacy attachment read failed: {e.Message}");
                    _suppressedMessageIds.Remove(message.Id);
                    return;
                }
            }
            else
            {
                channelDocument = message.Content;
            }

            if (!LegacyDiscordWireProtocol.TryDecodeRequest(channelDocument, out var legacyMessage))
            {
                Console.WriteLine("[MessageReceivedAsync] Legacy wrapper rejected");
                return;
            }

            try
            {
                var forwarded = await ForwardMessageToMythicAsync(
                    _mythicClient,
                    legacyMessage.TrackingId,
                    legacyMessage.Message,
                    legacyMessage.MessageFormat,
                    () => message.AddReactionAsync(new Emoji(ProcessedMessageReaction)),
                    () => message.DeleteAsync());
                if (!forwarded)
                {
                    _suppressedMessageIds.Remove(message.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[MessageReceivedAsync] {e.Message}");
                _suppressedMessageIds.Remove(message.Id);
            }
        }

        public async Task WriteToChannel(ReadOnlyMemory<byte> message, string id)
        {
            if (_config.WireProtocol == "legacy")
            {
                await WriteLegacyMessageToChannel(message, id);
                return;
            }

            if (!DiscordTrackingRoute.TryParseFixed(
                id,
                _config.ConfigurationFingerprint,
                out var route))
            {
                Console.WriteLine("[WriteToChannel] Invalid tracking route or configuration generation mismatch");
                return;
            }
            var framedMessage = FrameServerMessage(route.ClientId, message, _config.UseBase64);
            var discordMessage = new TransportEnvelopeMessage(
                framedMessage,
                _uuid,
                false,
                route.ClientId,
                _config.UseBase64 ? AgentMessageFormat.Legacy : AgentMessageFormat.RawV1,
                1,
                true);

            if(_channel is null)
            {
                Console.WriteLine("[WriteToChannel] Unable to get channel: Channel is null");
                return;
            }

            string serialized;
            try
            {
                serialized = _envelopeProtocol.Encode(
                    discordMessage,
                    TransportDirection.ServerToAgent);
            }
            catch (DiscordEnvelopeException e)
            {
                Console.WriteLine($"[WriteToChannel] Envelope encoding failed: {e.Message}");
                return;
            }
            if (serialized.Length > 1900)
            {
                using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(serialized)))
                {
                    try { 
                        await AwaitOutboundWriteAsync(
                            () => _channel.SendFileAsync(stream, "message.txt"),
                            OutboundWriteTimeout);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"[WriteToChannel] {e.ToString()}");
                    }
                }
            }
            else
            {
                try
                {
                    await AwaitOutboundWriteAsync(
                        () => _channel.SendMessageAsync(serialized),
                        OutboundWriteTimeout);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[WriteToChannel] {e.ToString()}");
                }
            }

        }

        private async Task WriteLegacyMessageToChannel(ReadOnlyMemory<byte> message, string id)
        {
            if (_channel is null)
            {
                Console.WriteLine("[WriteToChannel] Unable to get channel: Channel is null");
                return;
            }

            var serialized = LegacyDiscordWireProtocol.EncodeResponse(message, id, _uuid);
            if (serialized.Length > 1950)
            {
                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(serialized));
                try
                {
                    await AwaitOutboundWriteAsync(
                        () => _channel.SendFileAsync(stream, id),
                        OutboundWriteTimeout);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[WriteToChannel] {e}");
                }
                return;
            }

            try
            {
                await AwaitOutboundWriteAsync(
                    () => _channel.SendMessageAsync(serialized),
                    OutboundWriteTimeout);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[WriteToChannel] {e}");
            }
        }

        internal static byte[] FrameServerMessage(
            string route,
            ReadOnlyMemory<byte> message,
            bool useBase64)
        {
            var routeBytes = Encoding.ASCII.GetBytes(route);
            var frame = new byte[checked(routeBytes.Length + message.Length)];
            routeBytes.CopyTo(frame, 0);
            message.Span.CopyTo(frame.AsSpan(routeBytes.Length));
            return useBase64
                ? Encoding.ASCII.GetBytes(Convert.ToBase64String(frame))
                : frame;
        }
        private async Task<string> GetFileContentsAsync(string url)
        {
            try
            {
                using (HttpResponseMessage response = await _httpClient.GetAsync(
                    url,
                    HttpCompletionOption.ResponseHeadersRead))
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new DiscordEnvelopeException($"Attachment HTTP status {response.StatusCode}");
                    }
                    if (response.Content.Headers.ContentLength is long length &&
                        length > FixedTransportEnvelopeProtocol.MaximumDocumentUtf8Bytes)
                    {
                        throw new DiscordEnvelopeException("Attachment exceeds the UTF-8 byte limit");
                    }
                    await using var source = await response.Content.ReadAsStreamAsync();
                    using var destination = new MemoryStream();
                    var buffer = new byte[8192];
                    while (true)
                    {
                        var count = await source.ReadAsync(buffer);
                        if (count == 0)
                        {
                            break;
                        }
                        if (destination.Length + count > FixedTransportEnvelopeProtocol.MaximumDocumentUtf8Bytes)
                        {
                            throw new DiscordEnvelopeException("Attachment exceeds the UTF-8 byte limit");
                        }
                        destination.Write(buffer, 0, count);
                    }
                    return new UTF8Encoding(false, true).GetString(destination.ToArray());
                }
            }
            catch (Exception e)
            {
                throw new DiscordEnvelopeException("Unable to read bounded attachment", e);
            }
        }
    }
}
