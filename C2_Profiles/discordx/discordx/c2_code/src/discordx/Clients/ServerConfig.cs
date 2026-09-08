using discordx.Models.Server;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace discordx.Clients
{
    public class ServerConfig : IServerConfig
    {
        public ServerConfig()
        {
#if DEBUG
            string configText = File.ReadAllText(@"../../../../discordx/dev_config.json");
            var configValues = JsonSerializer.Deserialize<Dictionary<string, string>>(configText) ??
                throw new InvalidOperationException("Discord configuration is empty");
#else
            string configText = File.ReadAllText(@"config.json");
            var configValues = JsonSerializer.Deserialize<Dictionary<string, string>>(configText) ??
                throw new InvalidOperationException("Discord configuration is empty");
#endif
            BotToken = Required(configValues, "botToken");
            ChannelID = Required(configValues, "channelID");
            WireProtocol = Value(configValues, "wireProtocol", "fixed");
            TransportEnvelopeFormat = Required(configValues, "transportEnvelopeFormat");
            TransportPresentation = Required(configValues, "transportPresentation");
            TransportProtection = Required(configValues, "transportProtection");
            TransportKeyMode = Required(configValues, "transportKeyMode");
            UseBase64 = ParseBoolean(Required(configValues, "useBase64"), "useBase64");
            TransportKey = ParseKey(Value(configValues, "transportKey", String.Empty), TransportProtection);
            ValidateTransportConfiguration();
            ConfigurationFingerprint = ComputeFingerprint();
        }
        public string BotToken { get; set; } = String.Empty;
        public string ChannelID { get; set; } = String.Empty;
        public string WireProtocol { get; } = "fixed";
        public string TransportEnvelopeFormat { get; } = "json-v1";
        public string TransportPresentation { get; } = "plain";
        public string TransportProtection { get; } = "none";
        public string TransportKeyMode { get; } = "single";
        public byte[] TransportKey { get; } = Array.Empty<byte>();
        public bool UseBase64 { get; }
        public string ConfigurationFingerprint { get; } = String.Empty;
        public bool IsValid()
        {
            return !String.IsNullOrWhiteSpace(BotToken) && ulong.TryParse(ChannelID, out _);
        }

        private static string Required(IReadOnlyDictionary<string, string> values, string name)
        {
            var value = Value(values, name, String.Empty);
            if (String.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException($"Discord configuration requires {name}");
            }
            return value;
        }

        private static string Value(
            IReadOnlyDictionary<string, string> values,
            string name,
            string fallback) => values.TryGetValue(name, out var value) ? value : fallback;

        private static byte[] ParseKey(string value, string protection)
        {
            if (protection == "none")
            {
                if (!String.IsNullOrEmpty(value))
                {
                    throw new InvalidOperationException("Unprotected Discord transport must not contain a key");
                }
                return Array.Empty<byte>();
            }
            try
            {
                var key = Convert.FromBase64String(value);
                if (key.Length != 32 || Convert.ToBase64String(key) != value)
                {
                    throw new InvalidOperationException("Discord transport key is invalid");
                }
                return key;
            }
            catch (FormatException exception)
            {
                throw new InvalidOperationException("Discord transport key is invalid", exception);
            }
        }

        private static bool ParseBoolean(string value, string name) => value switch
        {
            "true" => true,
            "false" => false,
            _ => throw new InvalidOperationException($"Discord configuration requires Boolean {name}"),
        };

        private void ValidateTransportConfiguration()
        {
            if (WireProtocol is not ("fixed" or "legacy") ||
                TransportEnvelopeFormat is not ("json-v1" or "binary-v1") ||
                TransportPresentation is not ("plain" or "base64" or "decimal" or "emoji") ||
                TransportProtection is not ("none" or "xor-obfuscation-v1" or "chacha20-v1" or "aes256-hmac-v1") ||
                TransportKeyMode is not ("single" or "directional"))
            {
                throw new InvalidOperationException("Discord transport configuration contains an unsupported choice");
            }
            if (TransportPresentation == "plain" &&
                (TransportEnvelopeFormat != "json-v1" || TransportProtection != "none"))
            {
                throw new InvalidOperationException("plain requires json-v1 and transport protection none");
            }
        }

        private string ComputeFingerprint()
        {
            var keyDigest = SHA256.HashData(TransportKey);
            var publicConfiguration = String.Join("\0", new[]
            {
                ChannelID,
                TransportEnvelopeFormat,
                TransportPresentation,
                TransportProtection,
                TransportKeyMode,
                UseBase64 ? "true" : "false",
                Convert.ToHexString(keyDigest),
            });
            return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(publicConfiguration)))
                [..16].ToLowerInvariant();
        }
    }
}
