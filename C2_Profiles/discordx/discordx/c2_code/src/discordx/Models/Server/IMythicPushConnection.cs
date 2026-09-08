using PushC2Services;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace discordx.Models.Server
{
    public interface IMythicPushConnection : IAsyncDisposable
    {
        Task ConnectAsync(CancellationToken cancellationToken);
        Task SendToMythicAsync(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format, CancellationToken cancellationToken);
        IAsyncEnumerable<PushC2MessageFromMythic> ReadAllAsync(CancellationToken cancellationToken);
    }

    public interface IMythicPushConnectionFactory
    {
        IMythicPushConnection Create();
    }
}
