using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace discordx.EnvelopeCodecs
{
    internal enum TransportDirection
    {
        AgentToServer,
        ServerToAgent,
    }

    internal static class TransportDirectionNames
    {
        public static string Get(TransportDirection direction) => direction switch
        {
            TransportDirection.AgentToServer => "agent-to-server",
            TransportDirection.ServerToAgent => "server-to-agent",
            _ => throw new DiscordEnvelopeException("Unsupported transport direction"),
        };

        public static byte[] Label(TransportDirection direction) =>
            Encoding.UTF8.GetBytes(Get(direction));
    }

    internal interface ITransportEntropy
    {
        byte[] GetBytes(int count);
    }

    internal sealed class SystemTransportEntropy : ITransportEntropy
    {
        public byte[] GetBytes(int count)
        {
            if (count < 0)
            {
                throw new DiscordEnvelopeException("Invalid entropy length");
            }
            return RandomNumberGenerator.GetBytes(count);
        }
    }

    internal interface ITransportProtection
    {
        string Name { get; }
        int FixedOverheadBytes { get; }
        byte[] Protect(ReadOnlySpan<byte> wrapperUtf8, TransportDirection direction, ITransportEntropy entropy);
        byte[] Unprotect(ReadOnlySpan<byte> packet, TransportDirection expectedDirection);
    }

    internal abstract class KeyedTransportProtection : ITransportProtection
    {
        private readonly byte[] _masterKey;
        private readonly bool _directional;

        protected KeyedTransportProtection(ReadOnlySpan<byte> masterKey, bool directional)
        {
            if (masterKey.Length != 32)
            {
                throw new DiscordEnvelopeException("Transport protection requires a 32-byte key");
            }
            _masterKey = masterKey.ToArray();
            _directional = directional;
        }

        public abstract string Name { get; }
        public abstract int FixedOverheadBytes { get; }
        public abstract byte[] Protect(ReadOnlySpan<byte> wrapperUtf8, TransportDirection direction, ITransportEntropy entropy);
        public abstract byte[] Unprotect(ReadOnlySpan<byte> packet, TransportDirection expectedDirection);

        protected byte[] DirectionKey(TransportDirection direction)
        {
            if (!_directional)
            {
                return _masterKey.ToArray();
            }
            return Derive(_masterKey, TransportDirectionNames.Get(direction));
        }

        protected static byte[] Derive(ReadOnlySpan<byte> root, string label)
        {
            var labelBytes = Encoding.UTF8.GetBytes(label);
            var material = new byte[root.Length + 1 + labelBytes.Length];
            root.CopyTo(material);
            labelBytes.CopyTo(material.AsSpan(root.Length + 1));
            return SHA256.HashData(material);
        }
    }

    internal sealed class NoneTransportProtection : ITransportProtection
    {
        public string Name => "none";
        public int FixedOverheadBytes => 0;
        public byte[] Protect(ReadOnlySpan<byte> wrapperUtf8, TransportDirection direction, ITransportEntropy entropy) =>
            wrapperUtf8.ToArray();
        public byte[] Unprotect(ReadOnlySpan<byte> packet, TransportDirection expectedDirection) => packet.ToArray();
    }

    internal sealed class XorTransportProtection : KeyedTransportProtection
    {
        public XorTransportProtection(ReadOnlySpan<byte> masterKey, bool directional)
            : base(masterKey, directional) { }

        public override string Name => "xor-obfuscation-v1";
        public override int FixedOverheadBytes => 8;

        public override byte[] Protect(
            ReadOnlySpan<byte> wrapperUtf8,
            TransportDirection direction,
            ITransportEntropy entropy)
        {
            var salt = entropy.GetBytes(8);
            if (salt.Length != 8)
            {
                throw new DiscordEnvelopeException("Entropy source returned an invalid length");
            }
            var output = new byte[checked(salt.Length + wrapperUtf8.Length)];
            salt.CopyTo(output, 0);
            Apply(wrapperUtf8, output.AsSpan(8), DirectionKey(direction), salt);
            return output;
        }

        public override byte[] Unprotect(ReadOnlySpan<byte> packet, TransportDirection expectedDirection)
        {
            if (packet.Length < 8)
            {
                throw new DiscordEnvelopeException("Transport protection rejected packet");
            }
            var output = new byte[packet.Length - 8];
            Apply(packet[8..], output, DirectionKey(expectedDirection), packet[..8]);
            return output;
        }

        private static void Apply(
            ReadOnlySpan<byte> input,
            Span<byte> output,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> salt)
        {
            for (var index = 0; index < input.Length; index++)
            {
                var saltByte = salt[index % 8];
                var mask = (byte)(key[(index + saltByte) % 32] ^ saltByte);
                output[index] = (byte)(input[index] ^ mask);
            }
        }
    }

    internal sealed class ChaCha20TransportProtection : KeyedTransportProtection
    {
        public ChaCha20TransportProtection(ReadOnlySpan<byte> masterKey, bool directional)
            : base(masterKey, directional) { }

        public override string Name => "chacha20-v1";
        public override int FixedOverheadBytes => 12;

        public override byte[] Protect(
            ReadOnlySpan<byte> wrapperUtf8,
            TransportDirection direction,
            ITransportEntropy entropy)
        {
            var nonce = entropy.GetBytes(12);
            if (nonce.Length != 12)
            {
                throw new DiscordEnvelopeException("Entropy source returned an invalid length");
            }
            var output = new byte[checked(12 + wrapperUtf8.Length)];
            nonce.CopyTo(output, 0);
            Transform(wrapperUtf8, DirectionKey(direction), nonce, 1).CopyTo(output, 12);
            return output;
        }

        public override byte[] Unprotect(ReadOnlySpan<byte> packet, TransportDirection expectedDirection)
        {
            if (packet.Length < 12)
            {
                throw new DiscordEnvelopeException("Transport protection rejected packet");
            }
            return Transform(packet[12..], DirectionKey(expectedDirection), packet[..12], 1);
        }

        internal static byte[] Transform(
            ReadOnlySpan<byte> input,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> nonce,
            uint initialCounter)
        {
            if (key.Length != 32 || nonce.Length != 12)
            {
                throw new DiscordEnvelopeException("Invalid ChaCha20 key or nonce length");
            }
            var blockCount = ((ulong)input.Length + 63) / 64;
            if (blockCount > (ulong)UInt32.MaxValue - initialCounter + 1)
            {
                throw new DiscordEnvelopeException("ChaCha20 counter would wrap");
            }

            var output = new byte[input.Length];
            var offset = 0;
            var counter = initialCounter;
            while (offset < input.Length)
            {
                var keyStream = Block(key, nonce, counter);
                var count = Math.Min(64, input.Length - offset);
                for (var index = 0; index < count; index++)
                {
                    output[offset + index] = (byte)(input[offset + index] ^ keyStream[index]);
                }
                offset += count;
                counter++;
            }
            return output;
        }

        private static byte[] Block(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce, uint counter)
        {
            var state = new uint[16]
            {
                0x61707865, 0x3320646e, 0x79622d32, 0x6b206574,
                0, 0, 0, 0, 0, 0, 0, 0,
                counter, 0, 0, 0,
            };
            for (var index = 0; index < 8; index++)
            {
                state[4 + index] = BinaryPrimitives.ReadUInt32LittleEndian(key[(index * 4)..]);
            }
            for (var index = 0; index < 3; index++)
            {
                state[13 + index] = BinaryPrimitives.ReadUInt32LittleEndian(nonce[(index * 4)..]);
            }
            var working = state.ToArray();
            for (var round = 0; round < 10; round++)
            {
                QuarterRound(working, 0, 4, 8, 12);
                QuarterRound(working, 1, 5, 9, 13);
                QuarterRound(working, 2, 6, 10, 14);
                QuarterRound(working, 3, 7, 11, 15);
                QuarterRound(working, 0, 5, 10, 15);
                QuarterRound(working, 1, 6, 11, 12);
                QuarterRound(working, 2, 7, 8, 13);
                QuarterRound(working, 3, 4, 9, 14);
            }
            var output = new byte[64];
            for (var index = 0; index < 16; index++)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(
                    output.AsSpan(index * 4), unchecked(working[index] + state[index]));
            }
            return output;
        }

        private static void QuarterRound(uint[] state, int a, int b, int c, int d)
        {
            state[a] = unchecked(state[a] + state[b]); state[d] ^= state[a]; state[d] = RotateLeft(state[d], 16);
            state[c] = unchecked(state[c] + state[d]); state[b] ^= state[c]; state[b] = RotateLeft(state[b], 12);
            state[a] = unchecked(state[a] + state[b]); state[d] ^= state[a]; state[d] = RotateLeft(state[d], 8);
            state[c] = unchecked(state[c] + state[d]); state[b] ^= state[c]; state[b] = RotateLeft(state[b], 7);
        }

        private static uint RotateLeft(uint value, int count) =>
            (value << count) | (value >> (32 - count));
    }

    internal sealed class Aes256HmacTransportProtection : KeyedTransportProtection
    {
        public Aes256HmacTransportProtection(ReadOnlySpan<byte> masterKey, bool directional)
            : base(masterKey, directional) { }

        public override string Name => "aes256-hmac-v1";
        public override int FixedOverheadBytes => 64;

        public override byte[] Protect(
            ReadOnlySpan<byte> wrapperUtf8,
            TransportDirection direction,
            ITransportEntropy entropy)
        {
            var iv = entropy.GetBytes(16);
            if (iv.Length != 16)
            {
                throw new DiscordEnvelopeException("Entropy source returned an invalid length");
            }
            var root = DirectionKey(direction);
            var ciphertext = Encrypt(wrapperUtf8, Derive(root, "aes256-hmac-v1/encryption"), iv);
            var authenticated = AuthenticationInput(direction, iv, ciphertext);
            var tag = HMACSHA256.HashData(Derive(root, "aes256-hmac-v1/authentication"), authenticated);
            var packet = new byte[checked(16 + ciphertext.Length + tag.Length)];
            iv.CopyTo(packet, 0);
            ciphertext.CopyTo(packet, 16);
            tag.CopyTo(packet, 16 + ciphertext.Length);
            return packet;
        }

        public override byte[] Unprotect(ReadOnlySpan<byte> packet, TransportDirection expectedDirection)
        {
            if (packet.Length < 64 || (packet.Length - 48) % 16 != 0)
            {
                throw new DiscordEnvelopeException("Transport protection rejected packet");
            }
            var iv = packet[..16];
            var ciphertext = packet[16..^32];
            var suppliedTag = packet[^32..];
            var root = DirectionKey(expectedDirection);
            var expectedTag = HMACSHA256.HashData(
                Derive(root, "aes256-hmac-v1/authentication"),
                AuthenticationInput(expectedDirection, iv, ciphertext));
            if (!CryptographicOperations.FixedTimeEquals(expectedTag, suppliedTag))
            {
                throw new DiscordEnvelopeException("Transport protection rejected packet");
            }
            try
            {
                return Decrypt(ciphertext, Derive(root, "aes256-hmac-v1/encryption"), iv);
            }
            catch (CryptographicException exception)
            {
                throw new DiscordEnvelopeException("Transport protection rejected packet", exception);
            }
        }

        private static byte[] AuthenticationInput(
            TransportDirection direction,
            ReadOnlySpan<byte> iv,
            ReadOnlySpan<byte> ciphertext)
        {
            var label = Encoding.UTF8.GetBytes(TransportDirectionNames.Get(direction) + "\0");
            var output = new byte[label.Length + iv.Length + ciphertext.Length];
            label.CopyTo(output, 0);
            iv.CopyTo(output.AsSpan(label.Length));
            ciphertext.CopyTo(output.AsSpan(label.Length + iv.Length));
            return output;
        }

        private static byte[] Encrypt(ReadOnlySpan<byte> plaintext, byte[] key, ReadOnlySpan<byte> iv)
        {
            using var aes = Aes.Create();
            aes.KeySize = 256;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.Key = key;
            aes.IV = iv.ToArray();
            using var encryptor = aes.CreateEncryptor();
            return encryptor.TransformFinalBlock(plaintext.ToArray(), 0, plaintext.Length);
        }

        private static byte[] Decrypt(ReadOnlySpan<byte> ciphertext, byte[] key, ReadOnlySpan<byte> iv)
        {
            using var aes = Aes.Create();
            aes.KeySize = 256;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.Key = key;
            aes.IV = iv.ToArray();
            using var decryptor = aes.CreateDecryptor();
            return decryptor.TransformFinalBlock(ciphertext.ToArray(), 0, ciphertext.Length);
        }
    }
}
