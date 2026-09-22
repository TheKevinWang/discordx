using discordx.Clients;
using discordx.EnvelopeCodecs;
using discordx.Models.Server;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using PushC2Services;
using System.Threading.Channels;

namespace discordx.Tests.Clients
{
    [TestClass]
    public class DiscordClientTests
    {
        [TestMethod]
        public void DiscordProviderEndpoints_DerivePinnedApiAndGatewayAddresses()
        {
            Assert.AreEqual(
                "http://127.0.0.1:3301/api/v10/",
                DiscordProviderEndpoints.ApiBaseUrl("http://127.0.0.1:3301"));
            Assert.AreEqual(
                "ws://127.0.0.1:3302",
                DiscordProviderEndpoints.GatewayHost("ws://127.0.0.1:3302"));
            Assert.IsNull(DiscordProviderEndpoints.GatewayHost(String.Empty));
            Assert.AreEqual(
                "ws://127.0.0.1:3302/?v=10&encoding=json",
                DiscordProviderEndpoints.WithoutGatewayCompression(
                    "ws://127.0.0.1:3302?v=10&encoding=json&compress=zlib-stream"));
        }

        [TestMethod]
        public void DiscordProviderEndpoints_NormalizesSpacebarReadyReadState()
        {
            const string ready = "{\"op\":0,\"t\":\"READY\",\"d\":{\"read_state\":{},\"private_channels\":[]}}";

            var normalized = JObject.Parse(
                DiscordProviderEndpoints.NormalizeGatewayReady(ready));

            Assert.IsInstanceOfType(normalized["d"]?["read_state"], typeof(JArray));
            Assert.IsInstanceOfType(normalized["d"]?["private_channels"], typeof(JArray));
        }

        [TestMethod]
        public void DiscordProviderEndpoints_DoesNotRewriteOtherGatewayEvents()
        {
            const string message = "{\"op\":0,\"t\":\"MESSAGE_CREATE\",\"d\":{\"read_state\":{}}}";

            Assert.AreEqual(message, DiscordProviderEndpoints.NormalizeGatewayReady(message));
        }

        [TestMethod]
        public void DiscordProviderEndpoints_RemovesNullSpacebarAttachmentFields()
        {
            const string response = "[{\"id\":\"1\",\"channel_id\":\"2\",\"author\":{},\"timestamp\":\"2026-09-21T00:00:00Z\",\"thread\":null,\"attachments\":[{\"id\":\"3\",\"height\":null,\"width\":null,\"flags\":null}]}]";

            var normalized = JArray.Parse(
                DiscordProviderEndpoints.NormalizeRestJson(response));
            var message = (JObject?)normalized[0];
            var attachment = (JObject?)message?["attachments"]?[0];

            Assert.IsNotNull(message);
            Assert.IsNotNull(attachment);
            Assert.IsNull(message.Property("thread"));
            Assert.IsNull(attachment.Property("height"));
            Assert.IsNull(attachment.Property("width"));
            Assert.IsNull(attachment.Property("flags"));
        }

        [TestMethod]
        public void DiscordProviderEndpoints_PreservesUnrelatedNullDimensions()
        {
            const string response = "{\"height\":null,\"nested\":{\"width\":null}}";

            Assert.AreEqual(response, DiscordProviderEndpoints.NormalizeRestJson(response));
        }

        [TestMethod]
        public void DiscordProviderEndpoints_RemovesSpacebarMultipartAttachmentPlaceholders()
        {
            var source = new Dictionary<string, object>
            {
                ["payload_json"] = "{\"content\":\"\",\"attachments\":[{\"id\":\"0\",\"filename\":\"message.txt\"}]}",
                ["files[0]"] = new byte[] { 0, 128, 255 },
            };

            var normalized = DiscordProviderEndpoints.NormalizeSpacebarMultipart(source);

            Assert.IsNotNull(normalized["payload_json"]);
            Assert.IsNull(JObject.Parse((string)normalized["payload_json"])["attachments"]);
            CollectionAssert.AreEqual(
                (byte[])source["files[0]"],
                (byte[])normalized["files[0]"]);
            Assert.IsNotNull(JObject.Parse((string)source["payload_json"])["attachments"]);
        }

        [TestMethod]
        public void DiscordProviderEndpoints_RejectCrossOriginAttachments()
        {
            Assert.AreEqual(
                "http://127.0.0.1:3303/attachments/message.txt",
                DiscordProviderEndpoints.ValidateAttachmentUrl(
                    "http://127.0.0.1:3303",
                    "http://127.0.0.1:3303/attachments/message.txt"));
            Assert.ThrowsException<DiscordEnvelopeException>(() =>
                DiscordProviderEndpoints.ValidateAttachmentUrl(
                    "http://127.0.0.1:3303",
                    "http://127.0.0.1:3304/attachments/message.txt"));
        }

        [TestMethod]
        public void DiscordProviderEndpoints_RejectPublicPlaintextOrigins()
        {
            Assert.ThrowsException<InvalidOperationException>(() =>
                DiscordProviderEndpoints.NormalizeApiOrigin("http://example.com"));
            Assert.ThrowsException<InvalidOperationException>(() =>
                DiscordProviderEndpoints.NormalizeGatewayOrigin("ws://example.com"));
        }

        [TestMethod]
        public async Task BlockedSocksUploadDoesNotHoldNormalOutboundWorker()
        {
            var socks = Channel.CreateBounded<int>(1);
            var normal = Channel.CreateBounded<int>(1);
            var holdSocks = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var socksStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var normalDelivered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var socksWorker = DiscordClient.RunOutboundLaneAsync(socks.Reader, async _ =>
            {
                socksStarted.TrySetResult(true);
                await holdSocks.Task;
            });
            var normalWorker = DiscordClient.RunOutboundLaneAsync(normal.Reader, _ =>
            {
                normalDelivered.TrySetResult(true);
                return Task.CompletedTask;
            });
            await socks.Writer.WriteAsync(1);
            await socksStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
            await normal.Writer.WriteAsync(2);
            await normalDelivered.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.IsFalse(socksWorker.IsCompleted);
            holdSocks.SetResult(true);
            socks.Writer.Complete();
            normal.Writer.Complete();
            await Task.WhenAll(socksWorker, normalWorker);
        }

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
        public void PreserveRawServerFrame_DoesNotPrefixAnAlreadyFramedMythicResponse()
        {
            const string route = "00000000-0000-0000-0000-000000000000";
            var frame = System.Text.Encoding.ASCII.GetBytes(route)
                .Concat(new byte[] { 0x09, 0x00, 0x80, 0xff }).ToArray();
            CollectionAssert.AreEqual(frame, DiscordClient.PreserveRawServerFrame(route, frame));
            Assert.AreEqual(route, DiscordClient.ReadRawServerFrameRoute(frame));
            Assert.ThrowsException<InvalidOperationException>(() =>
                DiscordClient.PreserveRawServerFrame(route, new byte[] { 0x09, 0x00 }));
            Assert.ThrowsException<InvalidOperationException>(() =>
                DiscordClient.ReadRawServerFrameRoute(new byte[] { 0x09, 0x00 }));
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

            public Task<bool> ReportOutboundDeliveryAsync(string outboundID, bool success) =>
                Task.FromResult(true);

            public Task<bool> SendToMythic(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format,
                string? ingressID = null, DeliveryLane ingressLane = DeliveryLane.Standard)
            {
                LastSenderId = id;
                LastMessageBytes = data.ToArray();
                LastFormat = format;
                return Task.FromResult(SendResult);
            }
        }
    }
}
