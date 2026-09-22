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
        private sealed record OutboundRequest(PushC2Services.PushC2MessageFromMythic Message,
            TaskCompletionSource<bool> Completion);
        private readonly Channel<OutboundRequest> _standardOutbound =
            Channel.CreateBounded<OutboundRequest>(128);
        private readonly Channel<OutboundRequest> _socksOutbound =
            Channel.CreateBounded<OutboundRequest>(128);
        private readonly Task _standardWorker;
        private readonly Task _socksWorker;
        private readonly Channel<IMessage> _socksCleanup = Channel.CreateBounded<IMessage>(1024);
        private readonly Task _socksCleanupWorker;
        private AutoResetEvent _ready = new AutoResetEvent(false);
        private readonly TaskCompletionSource<bool> _channelsReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private ITextChannel _channel;
        private ITextChannel? _socksChannel;
        private readonly string _uuid;
        private readonly IServerConfig _config;
        private readonly FixedTransportEnvelopeProtocol _envelopeProtocol;
        private readonly BoundedMessageIdCache _suppressedMessageIds = new(1024);
        private readonly object _socksRecoveryGate = new();
        private readonly SemaphoreSlim _socksRecoveryLock = new(1, 1);
        private readonly SemaphoreSlim _socksIngressLock = new(1, 1);
        private readonly Channel<IMessage> _socksLiveBuffer = Channel.CreateBounded<IMessage>(2048);
        private bool _recoveringSocks;
        private bool _socksRecoveryOverflow;
        private bool _socksRetryScheduled;
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
                GatewayIntents = GatewayIntents.AllUnprivileged | GatewayIntents.MessageContent,
                RestClientProvider = DiscordProviderEndpoints.RestClientProvider(
                    config.ProviderApiOrigin),
                WebSocketProvider = DiscordProviderEndpoints.WebSocketProvider(
                    config.ProviderGatewayOrigin),
                GatewayHost = DiscordProviderEndpoints.GatewayHost(
                    config.ProviderGatewayOrigin),
            };
            _uuid = Guid.NewGuid().ToString();
            _config = config;
            _recoveringSocks = !String.IsNullOrEmpty(config.SocksChannelID);
            _discordClient = new DiscordSocketClient(discordConfig);
            _discordClient.Log += message =>
            {
                Console.WriteLine($"[Discord.Net] {message.Severity}: {message.Message} {message.Exception}");
                return Task.CompletedTask;
            };
            _discordClient.Disconnected += exception =>
            {
                Console.WriteLine($"[Discord.Net] Disconnected: {exception}");
                return Task.CompletedTask;
            };
            _discordClient.MessageReceived += MessageReceivedAsync;
            _discordClient.Ready += _client_Ready;
            _httpClient = new HttpClient(new HttpClientHandler
            {
                AllowAutoRedirect = false,
            });
            _mythicClient = mythicClient;
            _mythicClient.ReceiveFromMythicAsync();
            _mythicClient.OnMessageReceived += _mythicClient_OnMessageReceived;
            _standardWorker = Task.Run(() => RunOutboundWorkerAsync(_standardOutbound.Reader,
                PushC2Services.DeliveryLane.Standard));
            _socksWorker = Task.Run(() => RunOutboundWorkerAsync(_socksOutbound.Reader,
                PushC2Services.DeliveryLane.Socks));
            _socksCleanupWorker = Task.Run(RunSocksCleanupAsync);

        }
        private async Task _client_Ready()
        {
            _channel = (ITextChannel)_discordClient.GetChannel(ulong.Parse(_config.ChannelID));

            if (_channel is null)
            {
                Console.WriteLine("[WriteToChannel] Unable to get channel: Channel is null");
                Environment.Exit(0);
            }
            if (!String.IsNullOrEmpty(_config.SocksChannelID))
            {
                _socksChannel = _discordClient.GetChannel(ulong.Parse(_config.SocksChannelID)) as ITextChannel;
                if (_socksChannel is null)
                    throw new InvalidOperationException("Configured SOCKS channel is unavailable");
                lock (_socksRecoveryGate) _recoveringSocks = true;
            }
            _channelsReady.TrySetResult(true);
            _ready.Set();
            await this.CatchUp();
            if (_socksChannel is not null)
            {
                await RecoverSocksChannelAsync();
            }
        }

        private async void _mythicClient_OnMessageReceived(object? sender, PushC2Services.PushC2MessageFromMythic e)
        {
            try
            {
                Console.WriteLine("[MythicClient] Received outbound Mythic message");
                var queue = e.DeliveryLane == PushC2Services.DeliveryLane.Socks
                    ? _socksOutbound : _standardOutbound;
                var completion = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                await queue.Writer.WriteAsync(new OutboundRequest(e, completion));
                if (!await completion.Task)
                    Console.WriteLine($"[WriteToChannel] {e.DeliveryLane} send failed; batch remains eligible for delivery retry");
            }
            catch (Exception exception)
            {
                Console.WriteLine($"[WriteToChannel] Failed to deliver Mythic task: {exception}");
            }
        }

        private async Task RunOutboundWorkerAsync(
            ChannelReader<OutboundRequest> reader,
            PushC2Services.DeliveryLane lane)
        {
            await RunOutboundLaneAsync(reader, async request =>
            {
                var delivered = false;
                try
                {
                    Console.WriteLine($"[WriteToChannel] {lane} waiting for channel readiness");
                    await _channelsReady.Task;
                    Console.WriteLine($"[WriteToChannel] {lane} channel ready; starting write");
                    await WriteToChannel(request.Message.Message.ToByteArray(),
                        request.Message.TrackingID, lane);
                    Console.WriteLine($"[WriteToChannel] {lane} write completed");
                    delivered = true;
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"[WriteToChannel] {lane} delivery failed: {exception}");
                }
                if (lane == PushC2Services.DeliveryLane.Socks &&
                    !String.IsNullOrEmpty(request.Message.OutboundID))
                {
                    try
                    {
                        if (!await _mythicClient.ReportOutboundDeliveryAsync(
                            request.Message.OutboundID, delivered))
                            Console.WriteLine("[WriteToChannel] SOCKS outbound result could not be reported to Mythic");
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine($"[WriteToChannel] SOCKS outbound result reporting failed: {exception}");
                    }
                }
                request.Completion.TrySetResult(delivered);
            });
        }

        internal static async Task RunOutboundLaneAsync<T>(ChannelReader<T> reader, Func<T, Task> send)
        {
            await foreach (var message in reader.ReadAllAsync())
                await send(message);
        }

        private async Task RunSocksCleanupAsync()
        {
            var accepted = new Dictionary<ulong, IMessage>();
            DateTimeOffset oldest = DateTimeOffset.MinValue;
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                while (accepted.Count < 100 && _socksCleanup.Reader.TryRead(out var message))
                {
                    if (accepted.TryAdd(message.Id, message) && oldest == DateTimeOffset.MinValue)
                        oldest = DateTimeOffset.UtcNow;
                }
                if (accepted.Count == 0) continue;
                var age = DateTimeOffset.UtcNow - oldest;
                if (accepted.Count < 100 &&
                    (accepted.Count == 1 ? age < TimeSpan.FromSeconds(5) :
                        age < TimeSpan.FromSeconds(1))) continue;

                try
                {
                    await DeleteAcceptedSocksMessagesAsync(accepted.Values.ToArray());
                }
                catch (Exception error)
                {
                    Console.WriteLine($"[SOCKS cleanup] Retrying accepted messages: {error.Message}");
                    continue;
                }
                accepted.Clear();
                oldest = DateTimeOffset.MinValue;
            }
        }

        private async Task DeleteAcceptedSocksMessagesAsync(IMessage[] messages)
        {
            if (_socksChannel is null || messages.Length == 0) return;
            var recent = messages.Where(message =>
                DateTimeOffset.UtcNow - message.Timestamp < TimeSpan.FromDays(13.9)).ToArray();
            var canBulk = _socksChannel is SocketTextChannel socketChannel &&
                socketChannel.Guild.CurrentUser.GetPermissions(socketChannel).ManageMessages;
            if (canBulk && recent.Length >= 2)
            {
                try
                {
                    await _socksChannel.DeleteMessagesAsync(recent.Select(message => message.Id));
                    foreach (var message in recent) _suppressedMessageIds.Add(message.Id);
                    messages = messages.Except(recent).ToArray();
                }
                catch (Exception error)
                {
                    Console.WriteLine($"[SOCKS cleanup] Bulk delete failed: {error.Message}");
                }
            }
            foreach (var message in messages)
            {
                try
                {
                    await message.DeleteAsync();
                }
                catch (Exception error)
                {
                    Console.WriteLine($"[SOCKS cleanup] Retaining {message.Id}: {error.Message}");
                    _suppressedMessageIds.Remove(message.Id);
                }
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
            Func<Task> deleteAsync,
            string? ingressID = null,
            PushC2Services.DeliveryLane ingressLane = PushC2Services.DeliveryLane.Standard)
        {
            var forwarded = await mythicClient.SendToMythic(senderId, message, format,
                ingressID, ingressLane);
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

        private async Task RecoverSocksChannelAsync()
        {
            if (_socksChannel is null) return;
            await _socksRecoveryLock.WaitAsync();
            try
            {
                lock (_socksRecoveryGate) _socksRecoveryOverflow = false;
                var retained = new Dictionary<ulong, IMessage>();
                var page = (await _socksChannel.GetMessagesAsync(100).FlattenAsync()).ToArray();
                while (page.Length > 0)
                {
                    foreach (var message in page) retained.TryAdd(message.Id, message);
                    if (retained.Count > 5000)
                        throw new InvalidOperationException("SOCKS recovery exceeds 5000 retained messages");
                    if (page.Length < 100) break;
                    var before = page.Min(message => message.Id);
                    page = (await _socksChannel.GetMessagesAsync(before, Direction.Before, 100)
                        .FlattenAsync()).ToArray();
                }
                foreach (var message in retained.Values.OrderBy(message => message.Id))
                    await ProcessMessageAsync(message, true);

                for (var pass = 0; pass < 10; pass++)
                {
                    var buffered = new Dictionary<ulong, IMessage>();
                    lock (_socksRecoveryGate)
                    {
                        while (_socksLiveBuffer.Reader.TryRead(out var message))
                            buffered.TryAdd(message.Id, message);
                        if (buffered.Count == 0 && !_socksRecoveryOverflow)
                        {
                            _recoveringSocks = false;
                            return;
                        }
                        if (_socksRecoveryOverflow)
                            throw new InvalidOperationException("SOCKS Gateway buffer overflowed during recovery");
                    }
                    foreach (var message in buffered.Values.OrderBy(message => message.Id))
                        await ProcessMessageAsync(message, true);
                }
                throw new InvalidOperationException("SOCKS recovery did not catch up with live events");
            }
            catch (Exception error)
            {
                Console.WriteLine($"[SOCKS recovery] Retaining messages for retry: {error.Message}");
                bool schedule;
                lock (_socksRecoveryGate)
                {
                    schedule = !_socksRetryScheduled;
                    _socksRetryScheduled = true;
                }
                if (schedule) _ = Task.Run(async () =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(30));
                    lock (_socksRecoveryGate)
                    {
                        _socksRetryScheduled = false;
                        if (!_recoveringSocks) return;
                    }
                    await RecoverSocksChannelAsync();
                });
            }
            finally
            {
                _socksRecoveryLock.Release();
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

        private Task MessageReceivedAsync(IMessage message) => ProcessMessageAsync(message, false);

        private async Task ProcessMessageAsync(IMessage message, bool fromRecovery)
        {
            var isSocksChannel = _socksChannel is not null &&
                message.Channel.Id == _socksChannel.Id;
            if (!isSocksChannel && (_channel is null || message.Channel.Id != _channel.Id))
            {
                return;
            }
            // Attachment downloads and Mythic ingress receipts can complete at
            // different times. Keep SOCKS messages in Gateway/recovery order so
            // later TCP records cannot overtake an earlier batch.
            if (!isSocksChannel)
            {
                await ProcessSelectedMessageAsync(message, false, fromRecovery);
                return;
            }
            await _socksIngressLock.WaitAsync();
            try
            {
                await ProcessSelectedMessageAsync(message, true, fromRecovery);
            }
            finally
            {
                _socksIngressLock.Release();
            }
        }

        private async Task ProcessSelectedMessageAsync(IMessage message,
            bool isSocksChannel, bool fromRecovery)
        {
            // Nuwa and this profile can post with the same bot token. Author
            // identity alone cannot distinguish their directions; only known
            // outbound IDs or the fixed envelope's direction can do that.
            if (isSocksChannel && !fromRecovery)
            {
                lock (_socksRecoveryGate)
                {
                    if (_recoveringSocks)
                    {
                        if (!_socksLiveBuffer.Writer.TryWrite(message))
                            _socksRecoveryOverflow = true;
                        return;
                    }
                }
            }
            // Gateway delivery and CatchUp can observe the same message before
            // the first forward completes. Claim it before any awaited work so
            // exactly one path can submit it to Mythic.
            if (!_suppressedMessageIds.TryAdd(message.Id))
            {
                return;
            }

            if (_config.WireProtocol == "legacy" && !isSocksChannel)
            {
                await HandleLegacyMessageAsync(message);
                return;
            }
            if (_config.WireProtocol == "legacy" && isSocksChannel)
            {
                _suppressedMessageIds.Remove(message.Id);
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
                        isSocksChannel ? () => Task.CompletedTask :
                            () => message.AddReactionAsync(new Emoji(ProcessedMessageReaction)),
                        isSocksChannel ? () => _socksCleanup.Writer.WriteAsync(message).AsTask() :
                            () => message.DeleteAsync(),
                        message.Id.ToString(),
                        isSocksChannel ? PushC2Services.DeliveryLane.Socks :
                            PushC2Services.DeliveryLane.Standard);
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
                    () => message.DeleteAsync(),
                    message.Id.ToString());
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

        public Task WriteToChannel(ReadOnlyMemory<byte> message, string id) =>
            WriteToChannel(message, id, PushC2Services.DeliveryLane.Standard);

        public async Task WriteToChannel(ReadOnlyMemory<byte> message, string id,
            PushC2Services.DeliveryLane lane)
        {
            if (lane == PushC2Services.DeliveryLane.Socks && _socksChannel is null)
                throw new InvalidOperationException("SOCKS delivery requires socks_channel");
            var channel = lane == PushC2Services.DeliveryLane.Socks ? _socksChannel : _channel;
            if (_config.WireProtocol == "legacy")
            {
                if (lane == PushC2Services.DeliveryLane.Socks)
                    throw new InvalidOperationException("SOCKS delivery requires fixed binary envelope");
                await WriteLegacyMessageToChannel(message, id);
                return;
            }

            if (!DiscordTrackingRoute.TryParseFixed(
                id,
                _config.ConfigurationFingerprint,
                out var route))
            {
                throw new InvalidOperationException("Invalid tracking route or configuration generation mismatch");
            }
            // Mythic's raw-v1 response already includes the UUID route. The
            // historical Base64 path uses the profile-owned framing below.
            var clientId = _config.UseBase64
                ? route.ClientId : ReadRawServerFrameRoute(message);
            var framedMessage = _config.UseBase64
                ? FrameServerMessage(route.ClientId, message, true)
                : PreserveRawServerFrame(clientId, message);
            var discordMessage = new TransportEnvelopeMessage(
                framedMessage,
                _uuid,
                false,
                clientId,
                _config.UseBase64 ? AgentMessageFormat.Legacy : AgentMessageFormat.RawV1,
                1,
                true);

            if(channel is null)
            {
                throw new InvalidOperationException("Unable to get Discord channel");
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
                throw new InvalidOperationException($"Envelope encoding failed: {e.Message}", e);
            }
            if (serialized.Length > 1900)
            {
                using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(serialized)))
                {
                    IMessage? posted = null;
                    await AwaitOutboundWriteAsync(
                        async () => posted = await channel.SendFileAsync(stream, "message.txt"),
                        OutboundWriteTimeout);
                    if (posted is null)
                        throw new InvalidOperationException("Discord attachment write returned no message");
                    _suppressedMessageIds.Add(posted.Id);
                }
            }
            else
            {
                IUserMessage? posted = null;
                await AwaitOutboundWriteAsync(
                    async () => posted = await channel.SendMessageAsync(serialized),
                    OutboundWriteTimeout);
                if (posted is null)
                    throw new InvalidOperationException("Discord text write returned no message");
                _suppressedMessageIds.Add(posted.Id);
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

        internal static byte[] PreserveRawServerFrame(string route, ReadOnlyMemory<byte> message)
        {
            var routeBytes = Encoding.ASCII.GetBytes(route);
            if (routeBytes.Length != 36 || message.Length <= routeBytes.Length ||
                !message.Span[..routeBytes.Length].SequenceEqual(routeBytes))
                throw new InvalidOperationException("Mythic raw-v1 response is missing its UUID route");
            return message.ToArray();
        }

        internal static string ReadRawServerFrameRoute(ReadOnlyMemory<byte> message)
        {
            if (message.Length <= 36)
                throw new InvalidOperationException("Mythic raw-v1 response has no UUID route");
            var route = Encoding.ASCII.GetString(message.Span[..36]);
            if (!Guid.TryParseExact(route, "D", out var parsed) ||
                !String.Equals(parsed.ToString("D"), route, StringComparison.Ordinal))
                throw new InvalidOperationException("Mythic raw-v1 response has an invalid UUID route");
            return route;
        }
        private async Task<string> GetFileContentsAsync(string url)
        {
            try
            {
                var validatedUrl = DiscordProviderEndpoints.ValidateAttachmentUrl(
                    _config.ProviderCdnOrigin, url);
                using (HttpResponseMessage response = await _httpClient.GetAsync(
                    validatedUrl,
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
