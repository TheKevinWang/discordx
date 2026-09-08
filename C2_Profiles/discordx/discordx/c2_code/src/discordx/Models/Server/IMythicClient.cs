using PushC2Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace discordx.Models.Server
{
    public interface IMythicClient
    {
        Task<bool> SendToMythic(string id, ReadOnlyMemory<byte> data, AgentMessageFormat format);
        Task ReceiveFromMythicAsync();
        public event EventHandler<PushC2MessageFromMythic> OnMessageReceived;
    }
}
