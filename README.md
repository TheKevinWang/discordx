# discordx

## Fixed transport envelopes

One running Discord profile and channel use one fixed envelope format,
presentation, and outer-protection profile. The listener never probes another
format, presentation, key, protection profile, or plaintext fallback. Select
`json-v1` or `binary-v1`, then `plain` or `base64`, plus `none`,
`xor-obfuscation-v1`, `chacha20-v1`, or `aes256-hmac-v1`. Plain is valid only
for unprotected JSON. Mismatched and malformed documents fail closed and are
not forwarded to Mythic.

The pipeline is the selected envelope serializer, then protection, then
presentation. JSON uses the routing fields `sender_id`, `to_server`, and
`client_id`. Binary is one flags byte, one 36-byte canonical route UUID, and
the inner message bytes. Neither envelope carries a magic marker, version, or
codec/negotiation name.
Oversized documents use the neutral attachment name `message.txt`; filenames
do not identify an agent, direction, codec, or profile. Documents are bounded
at 2,097,152 presented UTF-8 bytes and wrappers at 524,288 bytes.

New DiscordX instances default to raw-v1 UUID framing inside `binary-v1`,
directional `chacha20-v1` outer protection, and Base64 outer presentation.
ChaCha20 provides confidentiality but not integrity; select `aes256-hmac-v1`
when the outer wrapper must also detect modification.

`xor-obfuscation-v1` is unauthenticated obfuscation. `chacha20-v1` is IETF
ChaCha20 with a transmitted 12-byte nonce, counter 1, and no authentication
tag; nonce reuse breaks confidentiality and modification is not detected.
`aes256-hmac-v1` is AES-256-CBC with encrypt-then-HMAC-SHA256. The outer key is
shared across the listener, not per agent. Independent inner Mythic protection
is still required for per-payload or per-callback isolation. Version 1 does not
add cryptographic replay prevention.
Discord account/channel metadata, timestamps, message timing and length, and
content-versus-attachment remain observable; the feature does not claim to
make Discord traffic undetectable.

A C2 profile that uses the Discord REST API for communication. V2 added support for Push profiles to help with rate limiting issues.

## How to install an agent in this format within Mythic

When it's time for you to test out your install or for another user to install your c2 profile, it's pretty simple. Within Mythic you can run:

* `sudo ./mythic-cli install github https://github.com/MythicC2Profiles/discordx` to install the main branch

## Configuring Proper Tokens

- Navigate to https://discord.com/developers/applications
- Click New Application, Enter a name for your bot and click Create.
- Navigate to Bot and turn on MESSAGE CONTENT INTENT
- Next hit “Reset Token” and save your Token to use in Mythic
- Navigate to Settings > Oauth2 and grab your ClientID
- Replace the ClientID with yours and Navigate to the URL : https://discord.com/api/oauth2/authorize?client_id=<ClientID>&permissions=0&scope=bot
- Select Your Server from the Menu and Authorize. Your bot should now appear your Discord Server


## Getting Channel ID

- In Discord go to Settings -> Advanced -> and enable "Developer Mode"
- Go to your server and right click the channel you want your comms to happen in
- Right Click the Text Channel you wish to use "Copy ID" and the channel ID will be copied to your clipboard
  
## Configuring C2 Profile in Mythic
- Navigate to https://[ServerIP]:7443/new/payloadtypes
- Start profile > View/Edit Config 
- Enter your botToken And ChannelID
- Select `wire_protocol`:
  - `fixed` (the default) uses the configured fixed transport envelope.
  - `legacy` reproduces the original Discord JSON wrapper and is the drop-in
    setting for agents built against the existing Discord profile. It preserves
    the original tracking IDs and response attachment names. It does not apply
    a fixed envelope, presentation encoding, or outer protection.
- `use_base64` is an optional Boolean payload parameter. New fixed DiscordX
  instances default it to `false` for raw-v1 framing; existing saved values
  remain explicit compatibility choices.
- Choose one `transport_envelope_format`, `transport_presentation`,
  `transport_protection`, and `transport_key_mode`. Protected profiles require
  a canonical Base64 32-byte `transport_key`.
- `base64` transport presentation uses canonical padded RFC 4648 text. It is
  an encoding of the complete wrapper, not the legacy `use_base64` UUID-framing
  switch and not encryption.
- Save one C2 instance and reuse it for the running listener and every matching
  payload. Separately materialized unsaved defaults generate different keys.
  For `none`, an automatically materialized key is discarded and never written
  to runtime configuration.
- Changing any fixed transport choice or key restarts the profile. Rotate by
  coordinating new payloads and a new channel when old payloads must remain
  active; no old-key fallback exists. Keep differently configured deployments
  in separate channels.
- Each versioned protection profile owns its salt, nonce, or IV construction.
  Version 1 requests 8 bytes for XOR, 12 bytes for ChaCha20, or 16 bytes for
  AES from the implementation's one internal byte source. That source can
  change in a future implementation and is not a listener setting.
- Start profile

## Fixed Discord framing

Set `wire_protocol` to `fixed` for agents that implement the fixed transport
contract. A listener runs one wire protocol at a time; use a separate channel
and listener configuration for legacy agents.

The fixed transport remains independent of Mythic message framing. With
`use_base64=true`, the envelope body contains the established canonical Base64
UUID frame. With `use_base64=false`, the body is raw-v1 and begins with the
36-byte route UUID. JSON requires that complete body to be strict UTF-8;
binary-v1 may carry arbitrary body bytes. Unknown flags/selectors, route
mismatches, malformed prefixes, and oversized bodies are rejected without
forwarding.

Agents using raw framing set `use_base64=false` and send `raw-v1`. Agents using
the historical UUID envelope set `use_base64=true`. The framing selection is
saved in the listener configuration and included in the binary flags; it is
never guessed from message contents.

## Troubleshooting
- If your bot is offline run: sudo ./mythic-cli discordx restart
