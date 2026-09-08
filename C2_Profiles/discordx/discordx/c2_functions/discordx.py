from mythic_container.C2ProfileBase import *
from pathlib import Path
import base64
import binascii
import json
import os
import tempfile

class DiscordX(C2Profile):
    name = "discordx"
    description = "discordx"
    author = "@tr41nwr3ck & @checkymander"
    is_p2p = False
    is_server_routed = True
    server_binary_path = Path(os.path.join(".", "discordx", "c2_code", "discordx"))
    server_folder_path = Path(os.path.join(".", "discordx", "c2_code"))
    parameters = [
        C2ProfileParameter(
            name="discord_token",
            description="A Bot Token for sending messages",
            default_value="",
            #verifier_regex="",
            required=True,
        ),
        C2ProfileParameter(
            name="bot_channel",
            description="The channel ID for the messages",
            default_value="",
            required=True,
        ),
        C2ProfileParameter(
            name="wire_protocol",
            description=(
                "Choose fixed for Nuwa's configured transport envelope or legacy to preserve "
                "the original Discord JSON wrapper for existing agents. One running listener "
                "uses exactly one wire protocol."
            ),
            default_value="fixed",
            choices=["fixed", "legacy"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="transport_envelope_format",
            description=(
                "Fixed channel envelope selected by the saved listener. json-v1 uses compact "
                "JSON and requires the complete inner frame to be valid UTF-8; binary-v1 keeps "
                "the inner frame as bytes and is the recommended default for Nuwa. The listener "
                "never probes another format."
            ),
            default_value="binary-v1",
            choices=["json-v1", "binary-v1"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="transport_presentation",
            description=(
                "Converts the fixed envelope bytes to Discord channel text. Choose plain only for "
                "unprotected json-v1, or Base64, decimal digits, or emoji symbols for either format. "
                "This formatting choice does not encrypt or authenticate the "
                "traffic; use Transport Protection for that. Every payload "
                "must use the same saved Discord C2 configuration as the listener."
            ),
            default_value="base64",
            choices=["plain", "base64", "decimal", "emoji"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="transport_protection",
            description="Fixed outer protection: none is compatibility, XOR is obfuscation only, ChaCha20 is the recommended Nuwa default but provides unauthenticated encryption whose nonce reuse compromises confidentiality and whose modifications are not detected, and AES/HMAC is authenticated encryption.",
            default_value="chacha20-v1",
            choices=["none", "xor-obfuscation-v1", "chacha20-v1", "aes256-hmac-v1"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="transport_key_mode",
            description="single uses one listener key in both directions. directional derives independent agent-to-server and server-to-agent roots. This is an outer listener key, not per-agent encryption.",
            default_value="directional",
            choices=["single", "directional"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="transport_key",
            description="Canonical Base64 encoding of exactly 32 listener-key bytes. Required for every protected profile and shared by all payloads on this fixed listener.",
            default_value="",
            randomize=True,
            format_string="[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=",
            verifier_regex="^[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=$",
            required=False,
        ),
        C2ProfileParameter(
            name="use_base64",
            description="Use the historical Base64 UUID envelope instead of Nuwa's recommended raw-v1 inner framing",
            default_value=False,
            parameter_type=ParameterType.Boolean,
            required=False,
        ),
        C2ProfileParameter(
            name="message_checks",
            description="The number of times to attempt to send a message or check for a response from the server before assuming a failure",
            default_value="10",
            required=False,
        ),
        C2ProfileParameter(
            name="time_between_checks",
            description="The amount of time the agent should wait between checks in seconds",
            default_value="10",
            required=False,
        ),
        C2ProfileParameter(
            name="callback_interval",
            description="Callback Interval in seconds",
            default_value="60",
            verifier_regex="^[0-9]+$",
            required=False,
        ),
        C2ProfileParameter(
            name="callback_jitter",
            description="Callback Jitter in percent",
            default_value="10",
            verifier_regex="^[0-9]+$",
            required=False,
        ),
        C2ProfileParameter(
            name="encrypted_exchange_check",
            description=(
                "Select T to perform Mythic's staging_rsa exchange before check-in and replace "
                "the embedded static AES key as the active traffic key with a fresh per-execution "
                "session key. Select F to skip staging and keep using the embedded static AES key. "
                "This setting applies only when AESPSK encryption is enabled, and the selected "
                "payload type must implement staging_rsa; with AESPSK set to none, the inner "
                "Mythic message is unencrypted."
            ),
            choices=["T", "F"],
            parameter_type=ParameterType.ChooseOne,
            default_value="F",
            required=False,
        ),
        C2ProfileParameter(
            name="AESPSK",
            description=(
                "Inner Message Protection (AESPSK). AESPSK is the legacy Mythic parameter name; "
                "the selected payload type defines the available protection profiles, and Mythic "
                "generates and manages their key material. This is independent of outer Transport "
                "Protection."
            ),
            default_value="none",
            parameter_type=ParameterType.ChooseOne,
            choices=["aes256_hmac", "none"],
            required=False,
            crypto_type=True
        ),
        C2ProfileParameter(
            name="user_agent",
            description="User Agent",
            default_value="Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko",
            required=False,
        ),
        C2ProfileParameter(
            name="proxy_host",
            description="Proxy Host",
            default_value="",
            required=False,
            verifier_regex=r"^$|^(http|https)://[a-zA-Z0-9]+",
        ),
        C2ProfileParameter(
            name="proxy_port",
            description="Proxy Port",
            default_value="",
            verifier_regex="^$|^[0-9]+$",
            required=False,
        ),
        C2ProfileParameter(
            name="proxy_user",
            description="Proxy Username",
            default_value="",
            required=False,
        ),
        C2ProfileParameter(
            name="proxy_pass",
            description="Proxy Password",
            default_value="",
            required=False,
        ),
        C2ProfileParameter(
            name="killdate",
            description="Kill Date",
            parameter_type=ParameterType.Date,
            default_value=365,
            required=False,
        ),
    ]

    def _config_path(self) -> Path:
        return self.server_folder_path / "config.json"

    def _runtime_config(self, parameters: dict) -> dict[str, str]:
        token = str(parameters.get("discord_token", "")).strip()
        channel = str(parameters.get("bot_channel", "")).strip()
        if not token:
            raise ValueError("discord_token is required")
        if not channel:
            raise ValueError("bot_channel is required")
        if not channel.isdigit():
            raise ValueError("bot_channel must be a numeric Discord channel ID")
        wire_protocol = str(parameters.get("wire_protocol", "fixed")).strip()
        if wire_protocol not in {"fixed", "legacy"}:
            raise ValueError("unsupported wire protocol")
        envelope_format = str(parameters.get("transport_envelope_format", "binary-v1")).strip()
        presentation = str(parameters.get("transport_presentation", "base64")).strip()
        protection = str(parameters.get("transport_protection", "chacha20-v1")).strip()
        key_mode = str(parameters.get("transport_key_mode", "directional")).strip()
        key = str(parameters.get("transport_key", "")).strip()
        if envelope_format not in {"json-v1", "binary-v1"}:
            raise ValueError("unsupported transport envelope format")
        if presentation not in {"plain", "base64", "decimal", "emoji"}:
            raise ValueError("unsupported transport presentation")
        if protection not in {"none", "xor-obfuscation-v1", "chacha20-v1", "aes256-hmac-v1"}:
            raise ValueError("unsupported transport protection")
        if key_mode not in {"single", "directional"}:
            raise ValueError("unsupported transport key mode")
        if presentation == "plain" and (envelope_format != "json-v1" or protection != "none"):
            raise ValueError(
                "plain transport presentation requires json-v1 and transport protection none"
            )
        use_base64 = parameters.get("use_base64", False)
        if not isinstance(use_base64, bool):
            raise ValueError("use_base64 must be Boolean")
        if protection == "none":
            key = ""
        else:
            try:
                decoded_key = base64.b64decode(key, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError("transport key must be canonical Base64") from exc
            if len(decoded_key) != 32:
                raise ValueError("transport key must decode to exactly 32 bytes")
            if base64.b64encode(decoded_key).decode("ascii") != key:
                raise ValueError("transport key must use canonical Base64")
        runtime_config = {
            "botToken": token,
            "channelID": channel,
            "wireProtocol": wire_protocol,
            "transportEnvelopeFormat": envelope_format,
            "transportPresentation": presentation,
            "transportProtection": protection,
            "transportKeyMode": key_mode,
            "useBase64": "true" if use_base64 else "false",
        }
        if protection != "none":
            runtime_config["transportKey"] = key
        return runtime_config

    @staticmethod
    def _atomic_write_private_json(config_path: Path, value: dict[str, str]) -> None:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{config_path.name}.", suffix=".tmp", dir=config_path.parent
        )
        temporary_path = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
                descriptor = -1
                json.dump(value, output, indent=2)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_path, config_path)
            os.chmod(config_path, 0o600)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass

    async def config_check(self, inputMsg: C2ConfigCheckMessage) -> C2ConfigCheckMessageResponse:
        try:
            runtime_config = self._runtime_config(inputMsg.Parameters)
            config_path = self._config_path()
            existing_config = None
            if config_path.exists():
                existing_config = json.loads(config_path.read_text())
            self._atomic_write_private_json(config_path, runtime_config)
            return C2ConfigCheckMessageResponse(
                Success=True,
                Message="Wrote discordx runtime configuration",
                RestartInternalServer=existing_config != runtime_config,
            )
        except Exception as exc:
            return C2ConfigCheckMessageResponse(
                Success=False,
                Error=str(exc),
            )
