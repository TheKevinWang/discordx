namespace discordx.EnvelopeCodecs
{
    internal readonly record struct DiscordTrackingRoute(string ClientId, string ConfigurationFingerprint)
    {
        private const string Prefix = "dte1";

        public static DiscordTrackingRoute ForFixed(string clientId, string configurationFingerprint)
        {
            ValidateUuid(clientId);
            ValidateFingerprint(configurationFingerprint);
            return new(clientId, configurationFingerprint);
        }

        public string Serialize()
        {
            ValidateUuid(ClientId);
            ValidateFingerprint(ConfigurationFingerprint);
            return $"{Prefix}:{ConfigurationFingerprint}:{ClientId}";
        }

        public static bool TryParseFixed(
            string value,
            string expectedConfigurationFingerprint,
            out DiscordTrackingRoute route)
        {
            route = default;
            try
            {
                ValidateFingerprint(expectedConfigurationFingerprint);
                var pieces = value.Split(':');
                if (pieces.Length != 3 || pieces[0] != Prefix ||
                    !String.Equals(pieces[1], expectedConfigurationFingerprint, StringComparison.Ordinal))
                {
                    return false;
                }
                route = ForFixed(pieces[2], pieces[1]);
                return true;
            }
            catch (DiscordEnvelopeException)
            {
                route = default;
                return false;
            }
        }

        private static void ValidateUuid(string value)
        {
            if (!Guid.TryParseExact(value, "D", out var parsed) ||
                !String.Equals(parsed.ToString("D"), value, StringComparison.Ordinal))
            {
                throw new DiscordEnvelopeException("Tracking route requires a canonical UUID");
            }
        }

        private static void ValidateFingerprint(string value)
        {
            if (value.Length != 16 || value.Any(character =>
                character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f')))
            {
                throw new DiscordEnvelopeException("Tracking route requires a configuration fingerprint");
            }
        }
    }
}
