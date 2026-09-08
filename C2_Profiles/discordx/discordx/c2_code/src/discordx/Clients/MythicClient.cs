using discordx.Models.Server;
using PushC2Services;

namespace discordx.Clients
{
    public class MythicClient : IMythicClient, IAsyncDisposable
    {
        private readonly IMythicPushConnectionFactory _connectionFactory;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private readonly SemaphoreSlim _sendLock = new(1, 1);
        private readonly object _receiveLoopLock = new();
        private readonly CancellationTokenSource _shutdown = new();
        private readonly TimeSpan _reconnectDelay;
        private IMythicPushConnection? _connection;
        private Task? _receiveLoopTask;
        public event EventHandler<PushC2MessageFromMythic> OnMessageReceived;

        public MythicClient(
            IMythicPushConnectionFactory connectionFactory,
            TimeSpan? reconnectDelay = null)
        {
            _connectionFactory = connectionFactory;
            _reconnectDelay = reconnectDelay ?? TimeSpan.FromSeconds(2);
        }

        public async Task<bool> SendToMythic(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format)
        {
            await _sendLock.WaitAsync(_shutdown.Token);
            try
            {
                for (var attempt = 0; attempt < 2; attempt++)
                {
                    try
                    {
                        var connection = await EnsureConnectedAsync(_shutdown.Token);
                        await connection.SendToMythicAsync(id, data, format, _shutdown.Token);
                        return true;
                    }
                    catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                    {
                        return false;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"[SendToMythic] {e}");
                        await ResetConnectionAsync();
                    }
                }
                return false;
            }
            finally
            {
                _sendLock.Release();
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
                                OnMessageReceived?.Invoke(this, message);
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
            _sendLock.Dispose();
        }
    }
}
