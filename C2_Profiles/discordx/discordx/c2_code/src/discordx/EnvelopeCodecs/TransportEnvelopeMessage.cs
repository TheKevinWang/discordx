using discordx.Models.Server;

namespace discordx.EnvelopeCodecs
{
    internal sealed record TransportEnvelopeMessage(
        ReadOnlyMemory<byte> Message,
        string SenderId,
        bool ToServer,
        string? ClientId,
        AgentMessageFormat MessageFormat,
        int? Id = null,
        bool? Final = null);

    internal interface ITransportEnvelopeFormat
    {
        string Name { get; }
        byte[] Serialize(TransportEnvelopeMessage message, TransportDirection direction);
        TransportEnvelopeMessage Deserialize(ReadOnlySpan<byte> bytes, TransportDirection expectedDirection);
    }
}
