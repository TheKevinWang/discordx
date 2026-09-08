using System.Security.Cryptography;
using System.Text;
using discordx.Models.Server;

namespace discordx.EnvelopeCodecs
{
    internal interface ITransportPresentation
    {
        string Name { get; }
        string Encode(ReadOnlySpan<byte> packet);
        byte[] Decode(string document);
    }

    internal sealed class PlainTransportPresentation : ITransportPresentation
    {
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);
        public string Name => "plain";
        public string Encode(ReadOnlySpan<byte> packet) => StrictUtf8.GetString(packet);
        public byte[] Decode(string document) => StrictUtf8.GetBytes(document);
    }

    internal sealed class CodecTransportPresentation : ITransportPresentation
    {
        private readonly IDiscordEnvelopeCodec _codec;
        public CodecTransportPresentation(IDiscordEnvelopeCodec codec) => _codec = codec;
        public string Name => _codec.Name;
        public string Encode(ReadOnlySpan<byte> packet) =>
            _codec.Encode(packet, DiscordEnvelopeContext.ForRequest(String.Empty).WithCodec(Name));
        public byte[] Decode(string document) =>
            _codec.Decode(document, DiscordEnvelopeContext.ForRequest(String.Empty).WithCodec(Name));
    }

    internal sealed class FixedTransportEnvelopeProtocol
    {
        public const int MaximumDocumentUtf8Bytes = 2_097_152;
        public const int MaximumEnvelopeBytes = 524_288;
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);
        private readonly ITransportEnvelopeFormat _format;
        private readonly ITransportPresentation _presentation;
        private readonly ITransportProtection _protection;
        private readonly ITransportEntropy _entropy;

        private FixedTransportEnvelopeProtocol(
            ITransportEnvelopeFormat format,
            ITransportPresentation presentation,
            ITransportProtection protection,
            ITransportEntropy entropy)
        {
            if (presentation.Name == "plain" && (format.Name != "json-v1" || protection.Name != "none"))
                throw new DiscordEnvelopeException("plain presentation requires json-v1 format and none protection");
            _format = format;
            _presentation = presentation;
            _protection = protection;
            _entropy = entropy;
        }

        public string FormatName => _format.Name;
        public string PresentationName => _presentation.Name;
        public string ProtectionName => _protection.Name;

        public static FixedTransportEnvelopeProtocol Create(
            string envelopeFormatName,
            string presentationName,
            string protectionName,
            string keyMode,
            ReadOnlySpan<byte> masterKey,
            bool useBase64,
            ITransportEntropy? entropy = null)
        {
            var directional = keyMode switch
            {
                "single" => false,
                "directional" => true,
                _ => throw new DiscordEnvelopeException("Unsupported transport key mode"),
            };
            var messageFormat = useBase64 ? AgentMessageFormat.Legacy : AgentMessageFormat.RawV1;
            ITransportEnvelopeFormat format = envelopeFormatName switch
            {
                "json-v1" => new JsonV1TransportEnvelopeFormat(),
                "binary-v1" => new BinaryV1TransportEnvelopeFormat(messageFormat),
                _ => throw new DiscordEnvelopeException("Unsupported transport envelope format"),
            };
            ITransportPresentation presentation = presentationName switch
            {
                "plain" => new PlainTransportPresentation(),
                "base64" => new CodecTransportPresentation(new Base64DiscordEnvelopeCodec()),
                "decimal" => new CodecTransportPresentation(new DecimalDiscordEnvelopeCodec()),
                "emoji" => new CodecTransportPresentation(new EmojiDiscordEnvelopeCodec()),
                _ => throw new DiscordEnvelopeException("Unsupported transport presentation"),
            };
            ITransportProtection protection = protectionName switch
            {
                "none" when masterKey.Length == 0 => new NoneTransportProtection(),
                "xor-obfuscation-v1" => new XorTransportProtection(masterKey, directional),
                "chacha20-v1" => new ChaCha20TransportProtection(masterKey, directional),
                "aes256-hmac-v1" => new Aes256HmacTransportProtection(masterKey, directional),
                "none" => throw new DiscordEnvelopeException("Unprotected transport must not carry a key"),
                _ => throw new DiscordEnvelopeException("Unsupported transport protection"),
            };
            return new FixedTransportEnvelopeProtocol(
                format, presentation, protection, entropy ?? new SystemTransportEntropy());
        }

        public string Encode(TransportEnvelopeMessage message, TransportDirection direction)
        {
            var envelope = _format.Serialize(message, direction);
            if (envelope.Length > MaximumEnvelopeBytes)
                throw new DiscordEnvelopeException("Transport envelope exceeds the byte limit");
            var packet = _protection.Protect(envelope, direction, _entropy);
            if (packet.Length > MaximumEnvelopeBytes + _protection.FixedOverheadBytes)
                throw new DiscordEnvelopeException("Transport protection exceeded its declared overhead");
            var document = _presentation.Encode(packet);
            ValidateDocument(document);
            return document;
        }

        public TransportEnvelopeMessage Decode(string document, TransportDirection expectedDirection)
        {
            ValidateDocument(document);
            try
            {
                var packet = _presentation.Decode(document);
                if (packet.Length > MaximumEnvelopeBytes + _protection.FixedOverheadBytes)
                    throw new DiscordEnvelopeException("Transport packet exceeds the byte limit");
                var envelope = _protection.Unprotect(packet, expectedDirection);
                if (envelope.Length > MaximumEnvelopeBytes)
                    throw new DiscordEnvelopeException("Transport envelope exceeds the byte limit");
                return _format.Deserialize(envelope, expectedDirection);
            }
            catch (Exception exception) when (
                exception is DecoderFallbackException or EncoderFallbackException or
                    CryptographicException or FormatException)
            {
                throw new DiscordEnvelopeException("Transport envelope rejected", exception);
            }
        }

        private static void ValidateDocument(string document)
        {
            int byteCount;
            try { byteCount = StrictUtf8.GetByteCount(document); }
            catch (EncoderFallbackException exception)
            { throw new DiscordEnvelopeException("Channel document is not valid UTF-8 text", exception); }
            if (byteCount > MaximumDocumentUtf8Bytes)
                throw new DiscordEnvelopeException("Channel document exceeds the UTF-8 byte limit");
            if (document.Length > 0 && (Char.IsWhiteSpace(document[0]) || Char.IsWhiteSpace(document[^1])))
                throw new DiscordEnvelopeException("Channel document has boundary whitespace");
            foreach (var character in document)
                if (character == '\0' || Char.IsControl(character))
                    throw new DiscordEnvelopeException("Channel document contains control characters");
        }
    }
}
