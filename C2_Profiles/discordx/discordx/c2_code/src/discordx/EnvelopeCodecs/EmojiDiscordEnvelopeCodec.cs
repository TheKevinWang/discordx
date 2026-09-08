using System.Text;

namespace discordx.EnvelopeCodecs
{
    internal sealed class EmojiDiscordEnvelopeCodec : IDiscordEnvelopeCodec
    {
        private static readonly string[] Alphabet =
        {
            "😁", "😂", "😅", "😳", "🥺", "🙄", "🤗", "😫",
            "🥰", "😏", "🤭", "😉", "😃", "😨", "😰", "🤑",
        };

        public string Name => "emoji";
        public DiscordEnvelopeCodecLimits Limits => new(8, 1, 0, false);

        public string Encode(ReadOnlySpan<byte> wrapperUtf8, DiscordEnvelopeContext context)
        {
            var result = new StringBuilder(checked(wrapperUtf8.Length * 4));
            foreach (var value in wrapperUtf8)
            {
                result.Append(Alphabet[value >> 4]);
                result.Append(Alphabet[value & 15]);
            }
            return result.ToString();
        }

        public byte[] Decode(string value, DiscordEnvelopeContext context)
        {
            var nibbles = new List<byte>(value.Length);
            var cursor = 0;
            while (cursor < value.Length)
            {
                var matched = false;
                for (byte nibble = 0; nibble < Alphabet.Length; nibble++)
                {
                    var token = Alphabet[nibble];
                    if (value.AsSpan(cursor).StartsWith(token.AsSpan(), StringComparison.Ordinal))
                    {
                        nibbles.Add(nibble);
                        cursor += token.Length;
                        matched = true;
                        break;
                    }
                }
                if (!matched)
                {
                    throw new DiscordEnvelopeException("Emoji envelope contains a noncanonical token");
                }
            }

            if (nibbles.Count % 2 != 0)
            {
                throw new DiscordEnvelopeException("Emoji envelope must contain an even number of tokens");
            }

            var result = new byte[nibbles.Count / 2];
            for (var index = 0; index < nibbles.Count; index += 2)
            {
                result[index / 2] = (byte)((nibbles[index] << 4) | nibbles[index + 1]);
            }
            return result;
        }
    }
}
