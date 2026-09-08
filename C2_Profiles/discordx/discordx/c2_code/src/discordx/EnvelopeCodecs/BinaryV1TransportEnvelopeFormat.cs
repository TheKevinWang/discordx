using System.Text;
using discordx.Models.Server;

namespace discordx.EnvelopeCodecs
{
    internal sealed class BinaryV1TransportEnvelopeFormat : ITransportEnvelopeFormat
    {
        internal const int HeaderLength = 37;
        internal const byte DirectionFlag = 0x01;
        internal const byte RawV1Flag = 0x02;
        private const byte AssignedFlags = DirectionFlag | RawV1Flag;
        private static readonly Encoding StrictAscii = Encoding.GetEncoding(
            "us-ascii", EncoderFallback.ExceptionFallback, DecoderFallback.ExceptionFallback);
        private readonly AgentMessageFormat _messageFormat;

        internal BinaryV1TransportEnvelopeFormat(AgentMessageFormat messageFormat) =>
            _messageFormat = messageFormat;

        public string Name => "binary-v1";

        public byte[] Serialize(TransportEnvelopeMessage message, TransportDirection direction)
        {
            if (message.MessageFormat != _messageFormat)
                throw new DiscordEnvelopeException("Binary envelope framing does not match listener use_base64");
            var toServer = direction == TransportDirection.AgentToServer;
            if (message.ToServer != toServer) throw new DiscordEnvelopeException("Binary envelope direction mismatch");
            var route = toServer ? message.SenderId : message.ClientId ?? String.Empty;
            JsonV1TransportEnvelopeFormat.RequireCanonicalUuid(route, toServer ? "sender_id" : "client_id");
            JsonV1TransportEnvelopeFormat.ValidateBody(message.Message.Span, route, _messageFormat);
            var result = new byte[HeaderLength + message.Message.Length];
            result[0] = (byte)((toServer ? DirectionFlag : 0) |
                (_messageFormat == AgentMessageFormat.RawV1 ? RawV1Flag : 0));
            StrictAscii.GetBytes(route).CopyTo(result, 1);
            message.Message.Span.CopyTo(result.AsSpan(HeaderLength));
            return result;
        }

        public TransportEnvelopeMessage Deserialize(ReadOnlySpan<byte> bytes, TransportDirection expectedDirection)
        {
            if (bytes.Length <= HeaderLength) throw new DiscordEnvelopeException("Binary envelope is too short");
            var flags = bytes[0];
            if ((flags & ~AssignedFlags) != 0) throw new DiscordEnvelopeException("Binary envelope has reserved flag bits");
            var toServer = (flags & DirectionFlag) != 0;
            if (toServer != (expectedDirection == TransportDirection.AgentToServer))
                throw new DiscordEnvelopeException("Binary envelope direction mismatch");
            var format = (flags & RawV1Flag) != 0 ? AgentMessageFormat.RawV1 : AgentMessageFormat.Legacy;
            if (format != _messageFormat)
                throw new DiscordEnvelopeException("Binary envelope framing does not match listener use_base64");
            string route;
            try { route = StrictAscii.GetString(bytes.Slice(1, 36)); }
            catch (DecoderFallbackException exception)
            { throw new DiscordEnvelopeException("Binary envelope route is not ASCII", exception); }
            JsonV1TransportEnvelopeFormat.RequireCanonicalUuid(route, toServer ? "sender_id" : "client_id");
            var body = bytes[HeaderLength..].ToArray();
            JsonV1TransportEnvelopeFormat.ValidateBody(body, route, format);
            return new TransportEnvelopeMessage(
                body,
                route,
                toServer,
                toServer ? null : route,
                format,
                1,
                true);
        }
    }
}
