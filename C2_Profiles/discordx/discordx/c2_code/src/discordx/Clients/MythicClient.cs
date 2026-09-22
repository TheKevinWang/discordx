using discordx.Models.Server;
using PushC2Services;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace discordx.Clients
{
    public class MythicClient : IMythicClient, IAsyncDisposable
    {
        private readonly IMythicPushConnectionFactory _connectionFactory;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private readonly Channel<PendingSend> _sendQueue = Channel.CreateBounded<PendingSend>(
            new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true, SingleWriter = false });
        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _receipts = new();
        private readonly Task _sendWorker;
        private readonly object _receiveLoopLock = new();
        private readonly CancellationTokenSource _shutdown = new();
        private readonly TimeSpan _reconnectDelay;
        private IMythicPushConnection? _connection;
        private Task? _receiveLoopTask;
        private sealed record PendingSend(string TrackingID, byte[] Data, AgentMessageFormat Format,
            string? IngressID, DeliveryLane Lane, TaskCompletionSource<bool> Written,
            string? OutboundID = null, bool OutboundSuccess = false);
        public event EventHandler<PushC2MessageFromMythic> OnMessageReceived;

        public MythicClient(
            IMythicPushConnectionFactory connectionFactory,
            TimeSpan? reconnectDelay = null)
        {
            _connectionFactory = connectionFactory;
            _reconnectDelay = reconnectDelay ?? TimeSpan.FromSeconds(2);
            _sendWorker = Task.Run(ProcessSendQueueAsync);
        }

        public async Task<bool> SendToMythic(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format,
            string? ingressID = null, DeliveryLane ingressLane = DeliveryLane.Standard)
        {
            await ReceiveFromMythicAsync();
            TaskCompletionSource<bool>? receipt = null;
            if (ingressID is not null)
            {
                receipt = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (!_receipts.TryAdd(ingressID, receipt))
                {
                    return false;
                }
            }
            var written = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                await _sendQueue.Writer.WriteAsync(new PendingSend(id, data.ToArray(), format,
                    ingressID, ingressLane, written), _shutdown.Token);
                if (!await written.Task)
                {
                    return false;
                }
                return receipt is null ||
                    await receipt.Task.WaitAsync(TimeSpan.FromSeconds(30), _shutdown.Token);
            }
            catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
            {
                return false;
            }
            catch (TimeoutException)
            {
                return false;
            }
            finally
            {
                if (ingressID is not null) _receipts.TryRemove(ingressID, out _);
            }
        }

        public async Task<bool> ReportOutboundDeliveryAsync(string outboundID, bool success)
        {
            await ReceiveFromMythicAsync();
            var written = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                await _sendQueue.Writer.WriteAsync(new PendingSend(String.Empty,
                    Array.Empty<byte>(), AgentMessageFormat.RawV1, null,
                    DeliveryLane.Socks, written, outboundID, success), _shutdown.Token);
                return await written.Task;
            }
            catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
            {
                return false;
            }
        }

        private async Task ProcessSendQueueAsync()
        {
            await foreach (var request in _sendQueue.Reader.ReadAllAsync(_shutdown.Token))
            {
                var attempts = request.IngressID is null ? 2 : 1;
                var sent = false;
                for (var attempt = 0; attempt < attempts && !sent; attempt++)
                {
                    try
                    {
                        var connection = await EnsureConnectedAsync(_shutdown.Token);
                        if (request.OutboundID is not null)
                            await connection.SendOutboundReceiptAsync(request.OutboundID,
                                request.OutboundSuccess, _shutdown.Token);
                        else
                            await connection.SendToMythicAsync(request.TrackingID, request.Data,
                                request.Format, request.IngressID, request.Lane, _shutdown.Token);
                        sent = true;
                    }
                    catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"[SendToMythic] {e}");
                        await ResetConnectionAsync();
                    }
                }
                request.Written.TrySetResult(sent);
            }
        }

        public async Task ReceiveFromMythicAsync()
        {
            lock (_receiveLoopLock)
            {
                _receiveLoopTask ??= Task.Run(async () =>
                {
                    while (!_shutdown.IsCancellationRequested)
                    {
                        try
                        {
                            var connection = await EnsureConnectedAsync(_shutdown.Token);
                            await foreach (var message in connection.ReadAllAsync(_shutdown.Token))
                            {
                                if (message.IsIngressReceipt)
                                {
                                    if (_receipts.TryGetValue(message.IngressID, out var receipt))
                                    {
                                        receipt.TrySetResult(message.Success);
                                    }
                                }
                                else
                                {
                                    OnMessageReceived?.Invoke(this, message);
                                }
                            }
                            Console.WriteLine("[MythicClient] Mythic stream closed, reconnecting");
                        }
                        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"[MythicClient] {e}");
                        }

                        await ResetConnectionAsync();
                        try
                        {
                            await Task.Delay(_reconnectDelay, _shutdown.Token);
                        }
                        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                });
            }
            await Task.CompletedTask;
        }

        private async Task<IMythicPushConnection> EnsureConnectedAsync(CancellationToken cancellationToken)
        {
            if (_connection is not null)
            {
                return _connection;
            }

            await _connectionLock.WaitAsync(cancellationToken);
            try
            {
                if (_connection is not null)
                {
                    return _connection;
                }

                var connection = _connectionFactory.Create();
                try
                {
                    await connection.ConnectAsync(cancellationToken);
                    _connection = connection;
                    Console.WriteLine("[MythicClient] Connected to Mythic push stream");
                    return connection;
                }
                catch
                {
                    await connection.DisposeAsync();
                    throw;
                }
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private async Task ResetConnectionAsync()
        {
            IMythicPushConnection? connection = null;

            await _connectionLock.WaitAsync();
            try
            {
                connection = _connection;
                _connection = null;
            }
            finally
            {
                _connectionLock.Release();
            }

            if (connection is not null)
            {
                foreach (var receipt in _receipts.Values) receipt.TrySetResult(false);
                try
                {
                    await connection.DisposeAsync();
                }
                catch (Exception e)
                {
                    // A stream can already be faulted when its reader asks to
                    // reconnect.  Disposal is best-effort here: allowing that
                    // error to escape would terminate the long-lived receive
                    // loop and strand all later Mythic task delivery.
                    Console.WriteLine($"[MythicClient] Failed to dispose closed Mythic stream: {e}");
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _shutdown.Cancel();
            _sendQueue.Writer.TryComplete();

            try { await _sendWorker; }
            catch (OperationCanceledException) { }

            if (_receiveLoopTask is not null)
            {
                try
                {
                    await _receiveLoopTask;
                }
                catch (OperationCanceledException)
                {
                }
            }

            await ResetConnectionAsync();
            _shutdown.Dispose();
            _connectionLock.Dispose();
        }
    }
}
