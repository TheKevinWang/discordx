using discordx.Clients;
using discordx.EnvelopeCodecs;
using discordx.Models.Server;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using PushC2Services;

namespace discordx.Tests.Clients
{
    [TestClass]
    public class DiscordClientTests
    {
        [TestMethod]
        public void ShouldProcessCatchUpMessage_UsesDurableProcessedMarker()
        {
            Assert.IsTrue(DiscordClient.ShouldProcessCatchUpMessage(
                processedByCurrentUser: false,
                rejectedByCurrentProcess: false));
            Assert.IsFalse(DiscordClient.ShouldProcessCatchUpMessage(
                processedByCurrentUser: true,
                rejectedByCurrentProcess: false));
            Assert.IsFalse(DiscordClient.ShouldProcessCatchUpMessage(
                processedByCurrentUser: false,
                rejectedByCurrentProcess: true));
        }

        [TestMethod]
        public void BoundedMessageIdCache_EvictsOldestRejectedMessage()
        {
            var cache = new BoundedMessageIdCache(capacity: 2);

            cache.Add(10);
            cache.Add(20);
            cache.Add(30);

            Assert.IsFalse(cache.Contains(10));
            Assert.IsTrue(cache.Contains(20));
            Assert.IsTrue(cache.Contains(30));
        }

        [TestMethod]
        public async Task BoundedMessageIdCache_TryAddClaimsMessageOnlyOnceAndCanReleaseIt()
        {
            var cache = new BoundedMessageIdCache(capacity: 2);

            var claims = await Task.WhenAll(Enumerable.Range(0, 32)
                .Select(_ => Task.Run(() => cache.TryAdd(10))));

            Assert.AreEqual(1, claims.Count(claimed => claimed));
            Assert.IsTrue(cache.Contains(10));

            cache.Remove(10);

            Assert.IsFalse(cache.Contains(10));
            Assert.IsTrue(cache.TryAdd(10));
        }

        [TestMethod]
        public async Task RunCatchUpPollingAsync_PollsUntilCancelled()
        {
            using var cancellation = new CancellationTokenSource();
            var pollCount = 0;

            await DiscordClient.RunCatchUpPollingAsync(
                () =>
                {
                    pollCount++;
                    if (pollCount == 2)
                    {
                        cancellation.Cancel();
                    }
                    return Task.CompletedTask;
                },
                TimeSpan.FromMilliseconds(1),
                cancellation.Token);

            Assert.AreEqual(2, pollCount);
        }

        [TestMethod]
        public async Task RunCatchUpPollingAsync_ContinuesAfterTransientFailure()
        {
            using var cancellation = new CancellationTokenSource();
            var pollCount = 0;

            await DiscordClient.RunCatchUpPollingAsync(
                () =>
                {
                    pollCount++;
                    if (pollCount == 1)
                    {
                        throw new HttpRequestException("transient");
                    }
                    cancellation.Cancel();
                    return Task.CompletedTask;
                },
                TimeSpan.FromMilliseconds(1),
                cancellation.Token);

            Assert.AreEqual(2, pollCount);
        }

        [TestMethod]
        public async Task RunSerializedAsync_DeliversConcurrentMessagesInOrder()
        {
            using var gate = new SemaphoreSlim(1, 1);
            var releaseFirst = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var firstStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var deliveryOrder = new List<string>();

            var first = DiscordClient.RunSerializedAsync(gate, async () =>
            {
                deliveryOrder.Add("first-start");
                firstStarted.SetResult(true);
                await releaseFirst.Task;
                deliveryOrder.Add("first-complete");
            });
            await firstStarted.Task;

            var second = DiscordClient.RunSerializedAsync(gate, () =>
            {
                deliveryOrder.Add("second");
                return Task.CompletedTask;
            });

            await Task.Delay(10);
            CollectionAssert.AreEqual(new[] { "first-start" }, deliveryOrder);

            releaseFirst.SetResult(true);
            await Task.WhenAll(first, second);

            CollectionAssert.AreEqual(
                new[] { "first-start", "first-complete", "second" },
                deliveryOrder);
        }

        [TestMethod]
        public async Task AwaitOutboundWriteAsync_ReleasesTheQueueWhenDiscordDoesNotRespond()
        {
            var neverCompletes = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            await Assert.ThrowsExceptionAsync<TimeoutException>(() =>
                DiscordClient.AwaitOutboundWriteAsync(
                    () => neverCompletes.Task,
                    TimeSpan.FromMilliseconds(10)));
        }

        [TestMethod]
        public async Task ForwardMessageToMythicAsync_DeletesMessageOnlyAfterSuccessfulForward()
        {
            var mythicClient = new FakeMythicClient { SendResult = true };
            var markCount = 0;
            var deleteCount = 0;
            var callOrder = new List<string>();

            var result = await DiscordClient.ForwardMessageToMythicAsync(
                mythicClient,
                "sender-1",
                System.Text.Encoding.UTF8.GetBytes("message-1"),
                AgentMessageFormat.Legacy,
                () =>
                {
                    markCount++;
                    callOrder.Add("mark");
                    return Task.CompletedTask;
                },
                () =>
                {
                    deleteCount++;
                    callOrder.Add("delete");
                    return Task.CompletedTask;
                });

            Assert.IsTrue(result);
            Assert.AreEqual(1, markCount);
            Assert.AreEqual(1, deleteCount);
            CollectionAssert.AreEqual(new[] { "mark", "delete" }, callOrder);
            Assert.AreEqual("sender-1", mythicClient.LastSenderId);
            Assert.AreEqual("message-1", mythicClient.LastMessage);
        }

        [TestMethod]
        public async Task ForwardMessageToMythicAsync_LeavesMessageWhenForwardFails()
        {
            var mythicClient = new FakeMythicClient { SendResult = false };
            var markCount = 0;
            var deleteCount = 0;

            var result = await DiscordClient.ForwardMessageToMythicAsync(
                mythicClient,
                "sender-2",
                System.Text.Encoding.UTF8.GetBytes("message-2"),
                AgentMessageFormat.Legacy,
                () =>
                {
                    markCount++;
                    return Task.CompletedTask;
                },
                () =>
                {
                    deleteCount++;
                    return Task.CompletedTask;
                });

            Assert.IsFalse(result);
            Assert.AreEqual(0, markCount);
            Assert.AreEqual(0, deleteCount);
            Assert.AreEqual("sender-2", mythicClient.LastSenderId);
            Assert.AreEqual("message-2", mythicClient.LastMessage);
        }

        [TestMethod]
        public void FrameServerMessage_PreservesArbitraryRawBytesAndHistoricalBase64()
        {
            const string route = "00000000-0000-0000-0000-000000000000";
            var message = new byte[] { 0, 0xff, 0x80, 0x41 };
            var expected = System.Text.Encoding.ASCII.GetBytes(route).Concat(message).ToArray();

            CollectionAssert.AreEqual(
                expected,
                DiscordClient.FrameServerMessage(route, message, false));
            CollectionAssert.AreEqual(
                expected,
                Convert.FromBase64String(System.Text.Encoding.ASCII.GetString(
                    DiscordClient.FrameServerMessage(route, message, true))));
        }

        [TestMethod]
        public void LegacyWireProtocol_AcceptsAthenaShapeAndPreservesOriginalReplyWrapper()
        {
            const string payloadUuid = "00000000-0000-0000-0000-000000000000";
            const string senderId = "11111111-1111-1111-1111-111111111111";
            const string serverId = "22222222-2222-2222-2222-222222222222";
            var agentMessage = Convert.ToBase64String(
                System.Text.Encoding.UTF8.GetBytes(payloadUuid + "{\"action\":\"checkin\"}"));
            var request =
                $"{{\"message\":\"{agentMessage}\",\"sender_id\":\"{senderId}\",\"to_server\":true,\"client_id\":\"\"}}";

            Assert.IsTrue(LegacyDiscordWireProtocol.TryDecodeRequest(request, out var decoded));
            Assert.AreEqual(senderId, decoded.TrackingId);
            Assert.AreEqual(AgentMessageFormat.Legacy, decoded.MessageFormat);
            Assert.AreEqual(agentMessage, System.Text.Encoding.UTF8.GetString(decoded.Message.Span));

            var response = LegacyDiscordWireProtocol.EncodeResponse(
                System.Text.Encoding.UTF8.GetBytes("mythic-response"), senderId, serverId);
            using var document = System.Text.Json.JsonDocument.Parse(response);
            var root = document.RootElement;
            Assert.AreEqual("mythic-response", root.GetProperty("message").GetString());
            Assert.AreEqual(serverId, root.GetProperty("sender_id").GetString());
            Assert.IsFalse(root.GetProperty("to_server").GetBoolean());
            Assert.AreEqual(senderId, root.GetProperty("client_id").GetString());
            Assert.IsFalse(root.TryGetProperty("message_format", out _));
        }

        private sealed class FakeMythicClient : IMythicClient
        {
            public bool SendResult { get; set; }
            public string? LastSenderId { get; private set; }
            public byte[]? LastMessageBytes { get; private set; }
            public string? LastMessage => LastMessageBytes is null ? null : System.Text.Encoding.UTF8.GetString(LastMessageBytes);
            public AgentMessageFormat? LastFormat { get; private set; }
            public event EventHandler<PushC2MessageFromMythic>? OnMessageReceived;

            public Task ReceiveFromMythicAsync()
            {
                return Task.CompletedTask;
            }

            public Task<bool> SendToMythic(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format)
            {
                LastSenderId = id;
                LastMessageBytes = data.ToArray();
                LastFormat = format;
                return Task.FromResult(SendResult);
            }
        }
    }
}
