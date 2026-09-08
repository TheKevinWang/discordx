using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using PushC2Services;
using discordx.Models.Server;

namespace discordx.Clients
{
    public sealed class GrpcMythicPushConnectionFactory : IMythicPushConnectionFactory
    {
        public IMythicPushConnection Create()
        {
            return new GrpcMythicPushConnection();
        }
    }

    internal sealed class GrpcMythicPushConnection : IMythicPushConnection
    {
        private const string C2ProfileName = "discordx";
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(20);
        private readonly GrpcChannel _channel;
        private readonly PushC2.PushC2Client _client;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private AsyncDuplexStreamingCall<PushC2MessageFromAgent, PushC2MessageFromMythic>? _connector;
        private CancellationTokenSource? _heartbeatCancellation;
        private Task? _heartbeatTask;

        public GrpcMythicPushConnection()
        {
#if DEBUG
            var mythicAddress = "http://10.30.26.108:17444";
#else
            var mythicAddress = "http://127.0.0.1:17444";
#endif
            var httpHandler = CreateHttpHandler();
            _channel = GrpcChannel.ForAddress(mythicAddress, new GrpcChannelOptions
            {
                HttpHandler = httpHandler,
            });
            _client = new PushC2.PushC2Client(_channel);
        }

        internal static SocketsHttpHandler CreateHttpHandler()
        {
            return new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            };
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_connector is not null)
            {
                return;
            }

            _connector = _client.StartPushC2StreamingOneToMany(cancellationToken: cancellationToken);
            await WriteAsync(new PushC2MessageFromAgent
            {
                C2ProfileName = C2ProfileName,
            }, cancellationToken);
            _heartbeatCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _heartbeatTask = Task.Run(() => SendHeartbeatsAsync(_heartbeatCancellation.Token), CancellationToken.None);
        }

        public async Task SendToMythicAsync(
            string id,
            ReadOnlyMemory<byte> data,
            AgentMessageFormat format,
            CancellationToken cancellationToken)
        {
            if (_connector is null)
            {
                throw new InvalidOperationException("Mythic connection has not been established");
            }

            await WriteAsync(CreateAgentMessage(id, data, format), cancellationToken);
        }

        internal static PushC2MessageFromAgent CreateAgentMessage(
            string id,
            ReadOnlyMemory<byte> data,
            AgentMessageFormat format)
        {
            var request = new PushC2MessageFromAgent
            {
                C2ProfileName = C2ProfileName,
                TrackingID = id,
                RemoteIP = "",
            };

            switch (format)
            {
                case AgentMessageFormat.Legacy:
                    request.Base64Message = ByteString.CopyFrom(data.Span);
                    break;
                case AgentMessageFormat.RawV1:
                    request.Message = ByteString.CopyFrom(data.Span);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported agent message format");
            }

            return request;
        }

        public async IAsyncEnumerable<PushC2MessageFromMythic> ReadAllAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (_connector is null)
            {
                throw new InvalidOperationException("Mythic connection has not been established");
            }

            await foreach (var message in _connector.ResponseStream.ReadAllAsync(cancellationToken))
            {
                yield return message;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_heartbeatCancellation is not null)
            {
                _heartbeatCancellation.Cancel();
            }
            if (_heartbeatTask is not null)
            {
                try
                {
                    await _heartbeatTask;
                }
                catch (OperationCanceledException)
                {
                }
                _heartbeatTask = null;
            }
            _heartbeatCancellation?.Dispose();
            _heartbeatCancellation = null;

            if (_connector is not null)
            {
                try
                {
                    await _connector.RequestStream.CompleteAsync();
                }
                catch
                {
                }
                _connector.Dispose();
                _connector = null;
            }

            _writeLock.Dispose();
            _channel.Dispose();
        }

        private async Task SendHeartbeatsAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(HeartbeatInterval, cancellationToken);
                await WriteAsync(new PushC2MessageFromAgent
                {
                    C2ProfileName = C2ProfileName,
                }, cancellationToken);
            }
        }

        private async Task WriteAsync(PushC2MessageFromAgent message, CancellationToken cancellationToken)
        {
            if (_connector is null)
            {
                throw new InvalidOperationException("Mythic connection has not been established");
            }

            await _writeLock.WaitAsync(cancellationToken);
            try
            {
                await _connector.RequestStream.WriteAsync(message);
            }
            finally
            {
                _writeLock.Release();
            }
        }
    }
}
