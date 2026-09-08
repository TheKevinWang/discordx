using discordx.Clients;
using discordx.Models.Server;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using PushC2Services;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace discordx.Tests.Clients
{
    [TestClass]
    public class MythicClientTests
    {
        [TestMethod]
        public async Task SendToMythic_RetriesWithFreshConnectionAfterFailure()
        {
            var firstConnection = new FakeConnection { ThrowOnSend = true };
            var secondConnection = new FakeConnection();
            var factory = new FakeConnectionFactory(firstConnection, secondConnection);

            await using var client = new MythicClient(factory, TimeSpan.FromMilliseconds(10));

            var forwarded = await client.SendToMythic(
                "tracking-1", System.Text.Encoding.UTF8.GetBytes("payload-1"), AgentMessageFormat.RawV1);

            Assert.IsTrue(forwarded);
            Assert.AreEqual(2, factory.CreateCount);
            Assert.AreEqual(1, firstConnection.SendAttempts);
            Assert.AreEqual(1, secondConnection.SendAttempts);
            Assert.AreEqual("tracking-1", secondConnection.LastTrackingId);
            Assert.AreEqual("payload-1", secondConnection.LastMessage);
            Assert.AreEqual(AgentMessageFormat.RawV1, secondConnection.LastFormat);
        }

        [TestMethod]
        public async Task ReceiveFromMythicAsync_ReconnectsAfterStreamEnds()
        {
            var firstConnection = new FakeConnection(
                new[]
                {
                    new PushC2MessageFromMythic
                    {
                        TrackingID = "tracking-2",
                        Message = Google.Protobuf.ByteString.CopyFromUtf8("hello"),
                    }
                });
            var secondConnection = new BlockingConnection();
            var factory = new FakeConnectionFactory(firstConnection, secondConnection);
            var received = new TaskCompletionSource<PushC2MessageFromMythic>(TaskCreationOptions.RunContinuationsAsynchronously);

            await using var client = new MythicClient(factory, TimeSpan.FromMilliseconds(10));
            client.OnMessageReceived += (_, message) => received.TrySetResult(message);

            await client.ReceiveFromMythicAsync();

            var message = await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.AreEqual("tracking-2", message.TrackingID);
            Assert.AreEqual("hello", message.Message.ToStringUtf8());
            await WaitForConditionAsync(() => factory.CreateCount >= 2, TimeSpan.FromSeconds(2));
            Assert.IsTrue(secondConnection.ConnectCount >= 1);
        }

        [TestMethod]
        public async Task ReceiveFromMythicAsync_ReconnectsWhenClosedConnectionDisposalFails()
        {
            var firstConnection = new DisposeFailingConnection();
            var secondConnection = new FakeConnection(
                new[]
                {
                    new PushC2MessageFromMythic
                    {
                        TrackingID = "tracking-after-dispose-failure",
                        Message = Google.Protobuf.ByteString.CopyFromUtf8("reconnected"),
                    }
                });
            var thirdConnection = new BlockingConnection();
            var factory = new FakeConnectionFactory(firstConnection, secondConnection, thirdConnection);
            var received = new TaskCompletionSource<PushC2MessageFromMythic>(TaskCreationOptions.RunContinuationsAsynchronously);

            await using var client = new MythicClient(factory, TimeSpan.FromMilliseconds(10));
            client.OnMessageReceived += (_, message) => received.TrySetResult(message);

            await client.ReceiveFromMythicAsync();

            var message = await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.AreEqual("tracking-after-dispose-failure", message.TrackingID);
            Assert.AreEqual("reconnected", message.Message.ToStringUtf8());
            Assert.IsTrue(firstConnection.DisposeAttempts >= 1);
            Assert.IsTrue(secondConnection.ConnectCount >= 1);
        }

        private static async Task WaitForConditionAsync(Func<bool> predicate, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (predicate())
                {
                    return;
                }

                await Task.Delay(25);
            }

            Assert.Fail("Timed out waiting for condition");
        }

        private sealed class FakeConnectionFactory : IMythicPushConnectionFactory
        {
            private readonly ConcurrentQueue<IMythicPushConnection> _connections;

            public FakeConnectionFactory(params IMythicPushConnection[] connections)
            {
                _connections = new ConcurrentQueue<IMythicPushConnection>(connections);
            }

            public int CreateCount { get; private set; }

            public IMythicPushConnection Create()
            {
                CreateCount++;
                if (_connections.TryDequeue(out var connection))
                {
                    return connection;
                }

                throw new InvalidOperationException("No fake connections remaining");
            }
        }

        private class FakeConnection : IMythicPushConnection
        {
            private readonly Queue<PushC2MessageFromMythic> _messages;

            public FakeConnection(IEnumerable<PushC2MessageFromMythic>? messages = null)
            {
                _messages = new Queue<PushC2MessageFromMythic>(messages ?? Enumerable.Empty<PushC2MessageFromMythic>());
            }

            public int ConnectCount { get; private set; }
            public int SendAttempts { get; private set; }
            public bool ThrowOnSend { get; set; }
            public string? LastTrackingId { get; private set; }
            public byte[]? LastMessageBytes { get; private set; }
            public string? LastMessage => LastMessageBytes is null ? null : System.Text.Encoding.UTF8.GetString(LastMessageBytes);
            public AgentMessageFormat? LastFormat { get; private set; }

            public Task ConnectAsync(CancellationToken cancellationToken)
            {
                ConnectCount++;
                return Task.CompletedTask;
            }

            public virtual async IAsyncEnumerable<PushC2MessageFromMythic> ReadAllAsync(
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                while (_messages.TryDequeue(out var message))
                {
                    yield return message;
                }

                await Task.CompletedTask;
            }

            public Task SendToMythicAsync(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format, CancellationToken cancellationToken)
            {
                SendAttempts++;
                LastTrackingId = id;
                LastMessageBytes = data.ToArray();
                LastFormat = format;
                if (ThrowOnSend)
                {
                    ThrowOnSend = false;
                    throw new InvalidOperationException("simulated send failure");
                }

                return Task.CompletedTask;
            }

            public virtual ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class BlockingConnection : FakeConnection
        {
            public override async IAsyncEnumerable<PushC2MessageFromMythic> ReadAllAsync(
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
                yield break;
            }
        }

        private sealed class DisposeFailingConnection : FakeConnection
        {
            public int DisposeAttempts { get; private set; }

            public override ValueTask DisposeAsync()
            {
                DisposeAttempts++;
                throw new InvalidOperationException("simulated disposal failure");
            }
        }
    }
}
