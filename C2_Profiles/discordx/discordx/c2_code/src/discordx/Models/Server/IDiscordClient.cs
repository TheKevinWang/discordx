using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace discordx.Models.Server
{
    public interface IDiscordClient
    {
        public Task WriteToChannel(ReadOnlyMemory<byte> data, string id);
        public Task Start();
    }
}
