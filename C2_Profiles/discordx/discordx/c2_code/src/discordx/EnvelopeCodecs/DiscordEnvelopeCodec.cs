using System.Text;
using System.Text.RegularExpressions;

namespace discordx.EnvelopeCodecs
{
    internal sealed class DiscordEnvelopeException : Exception
    {
        public DiscordEnvelopeException(string message) : base(message) { }
        public DiscordEnvelopeException(string message, Exception inner) : base(message, inner) { }
    }

    internal readonly record struct DiscordEnvelopeContext(
        string Direction,
        string SenderId,
        string ClientId,
        string C2Profile,
        string EnvelopeCodec,
        int EnvelopeVersion)
    {
        public static DiscordEnvelopeContext ForRequest(string senderId) =>
            new("agent-to-server", senderId, String.Empty, "discordx", String.Empty, 1);

        public static DiscordEnvelopeContext ForResponse(string clientId, string codecName) =>
            new("server-to-agent", String.Empty, clientId, "discordx", codecName, 1);

        public DiscordEnvelopeContext WithCodec(string codecName) => this with
        {
            EnvelopeCodec = codecName,
            EnvelopeVersion = 1,
        };
    }

    internal readonly record struct DiscordEnvelopeCodecLimits(
        int MaxExpansionNumerator,
        int MaxExpansionDenominator,
        int FixedOverheadBytes,
        bool UsesEntropy)
    {
        public int MaximumEncodedUtf8Bytes(int decodedByteCount)
        {
            if (decodedByteCount < 0 || MaxExpansionNumerator <= 0 ||
                MaxExpansionDenominator <= 0 || FixedOverheadBytes < 0)
            {
                throw new DiscordEnvelopeException("Invalid envelope codec resource declaration");
            }

            var result = checked(
                ((long)decodedByteCount * MaxExpansionNumerator + MaxExpansionDenominator - 1) /
                MaxExpansionDenominator + FixedOverheadBytes);
            if (result > Int32.MaxValue)
            {
                throw new DiscordEnvelopeException("Envelope codec expansion exceeds supported limits");
            }
            return (int)result;
        }
    }

    internal interface IDiscordEnvelopeCodec
    {
        string Name { get; }
        DiscordEnvelopeCodecLimits Limits { get; }
        string Encode(ReadOnlySpan<byte> wrapperUtf8, DiscordEnvelopeContext context);
        byte[] Decode(string value, DiscordEnvelopeContext context);
    }

    internal static class DiscordEnvelopeNames
    {
        private static readonly Regex ValidName = new(
            "^[a-z0-9][a-z0-9_-]{0,31}$",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public static bool IsValidCodecName(string? value) =>
            value is not null && ValidName.IsMatch(value);
    }
}
