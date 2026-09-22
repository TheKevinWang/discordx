using PushC2Services;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace discordx.Models.Server
{
    public interface IMythicPushConnection : IAsyncDisposable
    {
        Task ConnectAsync(CancellationToken cancellationToken);
        Task SendToMythicAsync(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format,
            string? ingressID, DeliveryLane ingressLane, CancellationToken cancellationToken);
        Task SendOutboundReceiptAsync(string outboundID, bool success,
            CancellationToken cancellationToken);
        IAsyncEnumerable<PushC2MessageFromMythic> ReadAllAsync(CancellationToken cancellationToken);
    }

    public interface IMythicPushConnectionFactory
    {
        IMythicPushConnection Create();
    }
}
