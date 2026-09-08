namespace discordx.EnvelopeCodecs
{
    internal sealed class Base64DiscordEnvelopeCodec : IDiscordEnvelopeCodec
    {
        public string Name => "base64";
        public DiscordEnvelopeCodecLimits Limits => new(4, 3, 2, false);

        public string Encode(ReadOnlySpan<byte> wrapperUtf8, DiscordEnvelopeContext context) =>
            Convert.ToBase64String(wrapperUtf8);

        public byte[] Decode(string value, DiscordEnvelopeContext context)
        {
            try
            {
                var decoded = Convert.FromBase64String(value);
                if (!String.Equals(
                    Convert.ToBase64String(decoded),
                    value,
                    StringComparison.Ordinal))
                {
                    throw new DiscordEnvelopeException("Base64 envelope is not canonical");
                }
                return decoded;
            }
            catch (FormatException exception)
            {
                throw new DiscordEnvelopeException(
                    "Base64 envelope is not valid canonical Base64",
                    exception);
            }
        }
    }
}
