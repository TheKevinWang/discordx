using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace discordx.Models.Server
{
    public interface IServerConfig
    {
        public string BotToken { get; set; }
        public string ChannelID { get; set; }
        public string WireProtocol { get; }
        public string TransportEnvelopeFormat { get; }
        public string TransportPresentation { get; }
        public string TransportProtection { get; }
        public string TransportKeyMode { get; }
        public byte[] TransportKey { get; }
        public bool UseBase64 { get; }
        public string ConfigurationFingerprint { get; }
        public bool IsValid();
    }
}
