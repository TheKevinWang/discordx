using System.Text;

namespace discordx.EnvelopeCodecs
{
    internal sealed class DecimalDiscordEnvelopeCodec : IDiscordEnvelopeCodec
    {
        public string Name => "decimal";
        public DiscordEnvelopeCodecLimits Limits => new(3, 1, 0, false);

        public string Encode(ReadOnlySpan<byte> wrapperUtf8, DiscordEnvelopeContext context)
        {
            var result = new char[checked(wrapperUtf8.Length * 3)];
            var output = 0;
            foreach (var value in wrapperUtf8)
            {
                result[output++] = (char)('0' + value / 100);
                result[output++] = (char)('0' + value % 100 / 10);
                result[output++] = (char)('0' + value % 10);
            }
            return new string(result);
        }

        public byte[] Decode(string value, DiscordEnvelopeContext context)
        {
            if (value.Length % 3 != 0)
            {
                throw new DiscordEnvelopeException("Decimal envelope length must be divisible by 3");
            }
            var result = new byte[value.Length / 3];
            for (var index = 0; index < value.Length; index += 3)
            {
                var a = value[index] - '0';
                var b = value[index + 1] - '0';
                var c = value[index + 2] - '0';
                if (a is < 0 or > 9 || b is < 0 or > 9 || c is < 0 or > 9)
                {
                    throw new DiscordEnvelopeException("Decimal envelope must contain only digits");
                }
                var decoded = a * 100 + b * 10 + c;
                if (decoded > Byte.MaxValue)
                {
                    throw new DiscordEnvelopeException("Decimal envelope group exceeds 255");
                }
                result[index / 3] = (byte)decoded;
            }
            return result;
        }
    }
}
