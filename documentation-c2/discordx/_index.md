+++
title = "discordx"
chapter = false
weight = 5
+++

## Overview
This C2 profile consists of a server that listens for new event messages on a specific discord channel, on receive of a new message, it deserializes the message and passes the contents on to the Mythic server via the standard REST API. It then takes the result, serializes it, and writes the result to that same channel.

The `wire_protocol` listener setting selects one protocol for the running
channel. `fixed` is the default and uses one fixed transport envelope format,
presentation, and protection. It never probes plaintext, another format,
presentation, key, or downgrade fallback. Choose `json-v1` or `binary-v1`,
then protect it before `plain`, `base64`, `decimal`, or `emoji` presentation.
Plain is valid only for unprotected JSON. Base64 presentation is canonical
padded RFC 4648 text and is independent of the historical `use_base64`
UUID-framing switch. Malformed, mismatched, and oversized fixed documents fail
closed. Presented and wrapper limits are 2,097,152 and 524,288 UTF-8 bytes
respectively; large fixed documents use a neutral attachment filename.

New DiscordX instances default to Nuwa's third-party C2 stack: `binary-v1`
with raw-v1 UUID framing (`use_base64=false`), directional `chacha20-v1`
protection, and Base64 outer presentation. Nuwa's independent
`codec_profile` build parameter defaults to `raw`. ChaCha20 provides
confidentiality but not integrity; choose `aes256-hmac-v1` for authenticated
outer protection. See Nuwa's Configuration page for the complete recommended
configuration bundles, including layered authentication, readable development,
and historical-framing compatibility.

Set `wire_protocol` to `legacy` to reproduce the original Discord profile for
existing agents. Legacy requests and responses use the unframed JSON wrapper,
the agent-supplied tracking ID, and its historical response attachment name.
It deliberately has no fixed outer presentation or protection. One listener
uses one protocol, so place legacy and fixed agents on separate channels.

### Discord C2 Workflow
{{<mermaid>}}
sequenceDiagram
    participant M as Mythic
    participant H as Discord Channel
    participant A as Agent
    A ->>+ H: Write to channel for tasking
    H ->>+ M: forward request to Mythic
    M -->>- H: reply with tasking
    H -->>- A: Write to channel with tasking
{{< /mermaid >}}

Legend:

1.) The agent writes a message to the discord channel indicating it's serverbound

2.) The server gets a notification of the message, deletes it, and forwards the message to Mythic

2a.) If the presented transport document is too large for an inline message, the complete document is read from a bounded attachment with a neutral filename

3.) The server receives the response from the Mythic server, serializes it, and writes it to the discord channel

3a.) If the presented response document is too large for an inline message, the complete document is written to a bounded attachment with a neutral filename

4.) The agent polls for new messages in the channel, waiting for messages designated for its GUID

5.) The agent deserializes the message and performs the requested tasks

## Configuration Options
The profile reads a private runtime `config.json` containing the required token,
channel, and fixed transport settings. Mythic writes it atomically with mode
`0600`.

```JSON
{
  "botToken": "OTkzMTY4MDUxMDY5NTE3OD...ZfPTf03-mgU",
  "channelID": "9931...734622",
  "wireProtocol": "fixed",
  "transportEnvelopeFormat": "binary-v1",
  "transportPresentation": "base64",
  "transportProtection": "chacha20-v1",
  "transportKeyMode": "directional",
  "transportKey": "base64-encoded-32-byte-listener-key",
  "useBase64": "false"
}
```

Reuse one saved C2 instance for the listener and all matching payloads. A new
unsaved instance may materialize a different randomized key. For `none`, any
materialized key is discarded. There is no mixed-mode channel or old-key
fallback. Coordinate key rotation with new payloads and use a new channel when
old payloads must remain active. Keep differently configured deployments in
separate channels. Each versioned
protection profile owns its salt, nonce, or IV construction. Version 1 requests
8 bytes for XOR, 12 bytes for ChaCha20, or 16 bytes for AES from the
implementation's one internal byte source. That source can change in a future
implementation and is not a listener setting.


## Generating a token

- Navigate to https://discord.com/developers/applications
- Click New Application, Enter your Bot name and click create.
- Next hit “Reset Token” and save your Token
- Navigate to Settings > Oauth2 and grab your ClientID
- Replace the ClientID with yours and navigate to the URL : https://discord.com/api/oauth2/authorize?client_id=<ClientID>&permissions=0&scope=bot
- Select your server from the menu and Authorise. Your bot should now appear your Discord server.

### Profile Options
#### Discord Channel ID
The channel ID where discord messages will be written to.

#### Discord Bot Token
A token that will be used by the agent to read and write messages to the associate channel.

#### Message Checks
How many times to check for a response from the server before assuming something went wrong with the message send.

#### Time Between Checks
How long to wait between each message check.

#### Callback Interval
A number to indicate how many seconds the agent should wait in between tasking requests.

#### Callback Jitter
Percentage of jitter effect for callback interval.

#### Crypto Type

This is the independent inner Mythic message-protection setting. Nuwa defaults
it to `none` for the compact raw inner stack. Select Nuwa's versioned
AES/HMAC profile when per-payload or per-callback isolation is required; the
outer listener key is shared by every payload assigned to that listener.

#### Perform Key Exchange

Select `T` to perform Mythic's `staging_rsa` exchange before normal check-in.
The agent uses its embedded Base64 32-byte AES key to protect an initial request
containing a newly generated RSA public key. Mythic returns a fresh
per-execution AES session key encrypted to that public key, and the agent uses
the session key for subsequent traffic. Select `F` to skip staging and keep
using the embedded static AES key. This setting applies only when `AESPSK`
encryption is enabled, and the selected payload type must implement
`staging_rsa`. With `AESPSK` set to `none`, the inner Mythic message is
unencrypted.

#### User Agent
The User Agent to be passed in the HTTP requests for calls to the REST API

#### Proxy Host
If you need to manually specify a proxy endpoint, do that here. This follows the same format as the callback host.

#### Proxy Password
If you need to authenticate to the proxy endpoint, specify the password here.

#### Proxy Username
If you need to authenticate to the proxy endpoint, specify the username here.

#### Proxy Port
If you need to manually specify a proxy endpoint, this is where you specify the associated port number.

#### Kill Date
Date for the agent to automatically exit, typically the after an assessment is finished.

## OPSEC
`none` provides no outer confidentiality. XOR is obfuscation only. Regular
ChaCha20 hides content only while its 12-byte nonce does not repeat under the
listener key and does not detect modification. AES/HMAC is the authenticated
outer option. All outer modes share one listener key across matching agents;
per-agent isolation depends on independent inner Mythic protection. Discord
metadata, timing, document length, and content-versus-attachment remain
observable, and version 1 has no cryptographic replay prevention.

## Development

All of the code for the server is .NET 7 and located in `C2_Profiles/discordx/discordx/c2_code`.

The server reverses the one configured format, presentation, and protection,
strictly validates the route and expected direction, and forwards accepted
bytes to Mythic. The channel packet has no clear magic, version, algorithm, or
codec selector.

`json-v1` serializes into compact JSON like this:

```
{
  "message":"UUID-prefixed Mythic message",
  "sender_id":"GUID",
  "to_server":false,
  "id":1,
  "final":true
}
```

`binary-v1` is one flags byte, a 36-byte canonical lowercase ASCII route UUID,
and the remaining inner message bytes. Flag bit 0 is direction, bit 1 is
raw-v1 versus historical Base64 framing, and all other bits must be zero.
`use_base64` selects that inner framing and is independent of the fixed outer
transport. JSON bodies must be strict UTF-8; binary bodies may contain any
bytes. Both formats require the decoded inner frame UUID to match the route.

The `message` parameter contains the Mythic message. Empty messages remain
valid. When the presented transport document exceeds 1,900 UTF-16 code units,
the entire document is uploaded as a bounded UTF-8 attachment named
`message.txt`; attachment filenames never route messages.

The `sender_id` is a guid generated by the agent to be included with every message. Any messages with an agents generated GUID and the to_server parameter set to `false` are intended to be processed by the agent.

THe `to_server` parameter indicates the intended recipient. If set to `true` the server will process the message, if `false` the agent is meant to.

The `id` parameter is not currently in use, so safe to just set it to `1` or `0` for now.

The `final` parameter is not currently in use, so safe to just set it to `true` for now.
