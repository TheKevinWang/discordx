from mythic_container.C2ProfileBase import *
from pathlib import Path
import base64
import binascii
import ipaddress
import json
import os
import tempfile
import uuid
from urllib.parse import urlsplit, urlunsplit

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
            name="socks_channel",
            description="Optional shared channel ID for SOCKS traffic; must differ from bot_channel",
            default_value="",
            required=False,
        ),
        C2ProfileParameter(
            name="listener_id",
            description="Optional immutable canonical listener UUID. Leave blank to let Mythic assign one when the saved instance is first created.",
            default_value="",
            verifier_regex=r"^$|^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            required=False,
        ),
        C2ProfileParameter(
            name="listener_enabled",
            description="Enable this logical listener in the shared Discordx runtime",
            default_value=True,
            parameter_type=ParameterType.Boolean,
            required=False,
        ),
        C2ProfileParameter(
            name="discord_provider_kind",
            description="Discord service provider. Self-hosted providers are intended for explicit compatibility testing.",
            default_value="discord",
            choices=["discord", "spacebar", "mock"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="discord_api_version",
            description="Discord-compatible REST API version",
            default_value="10",
            choices=["10"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(
            name="discord_test_only_allow_insecure_transport",
            description="Permit explicit HTTP/WS origins for a local Spacebar or mock compatibility lab",
            default_value=False,
            parameter_type=ParameterType.Boolean,
            required=False,
        ),
        C2ProfileParameter(
            name="provider_api_origin",
            description=(
                "Discord-compatible API origin. Keep the default for Discord; local Spacebar "
                "labs may use plain HTTP only on loopback or private addresses."
            ),
            default_value="https://discord.com",
            required=False,
        ),
        C2ProfileParameter(
            name="provider_gateway_origin",
            description=(
                "Optional explicit Discord-compatible Gateway origin. Leave blank for Discord "
                "discovery; local Spacebar labs use their pinned ws:// endpoint."
            ),
            default_value="",
            required=False,
        ),
        C2ProfileParameter(
            name="provider_cdn_origin",
            description=(
                "Expected attachment origin. Keep the default for Discord; local Spacebar labs "
                "set their pinned CDN endpoint so cross-origin downloads fail closed."
            ),
            default_value="https://cdn.discordapp.com",
            required=False,
        ),
        C2ProfileParameter(
            name="server_egress_proxy_mode",
            description="Network route used by this server-side bot worker",
            default_value="direct",
            choices=["direct", "http", "socks5"],
            parameter_type=ParameterType.ChooseOne,
            required=False,
        ),
        C2ProfileParameter(name="server_egress_proxy_url", description="HTTP(S) or SOCKS5 server proxy root URL", default_value="", required=False),
        C2ProfileParameter(name="server_egress_proxy_username", description="Optional write-only server proxy username", default_value="", required=False),
        C2ProfileParameter(name="server_egress_proxy_password", description="Optional write-only server proxy password", default_value="", required=False),
        C2ProfileParameter(
            name="server_egress_proxy_remote_dns",
            description="Resolve provider hostnames through the SOCKS5 proxy",
            default_value=True,
            parameter_type=ParameterType.Boolean,
            required=False,
        ),
        C2ProfileParameter(
            name="server_ingress_mode", description="Server discovery path", default_value="gateway",
            choices=["gateway", "polling"], parameter_type=ParameterType.ChooseOne, required=False,
        ),
        C2ProfileParameter(
            name="server_poll_strategy", description="Polling cadence planner", default_value="adaptive",
            choices=["adaptive", "fixed"], parameter_type=ParameterType.ChooseOne, required=False,
        ),
        C2ProfileParameter(name="server_poll_interval_seconds", description="Fixed/fallback polling interval (2-300 seconds)", default_value="15", verifier_regex=r"^[0-9]+$", required=False),
        C2ProfileParameter(name="server_poll_min_interval_seconds", description="Adaptive hot interval (2-60 seconds)", default_value="2", verifier_regex=r"^[0-9]+$", required=False),
        C2ProfileParameter(name="server_poll_base_interval_seconds", description="Adaptive base interval (5-300 seconds)", default_value="15", verifier_regex=r"^[0-9]+$", required=False),
        C2ProfileParameter(name="server_poll_max_interval_seconds", description="Adaptive cold interval (15-3600 seconds)", default_value="300", verifier_regex=r"^[0-9]+$", required=False),
        C2ProfileParameter(name="server_poll_build_warm_seconds", description="Post-build adaptive warm duration (0-3600 seconds)", default_value="900", verifier_regex=r"^[0-9]+$", required=False),
        C2ProfileParameter(name="server_reconciliation_interval_seconds", description="Gateway cursor reconciliation interval; zero disables it (0 or 60-3600 seconds)", default_value="300", verifier_regex=r"^[0-9]+$", required=False),
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

    @staticmethod
    def _normalize_provider_origin(
        value: object,
        *,
        name: str,
        secure_scheme: str,
        local_scheme: str,
        allow_empty: bool = False,
    ) -> str:
        text = str(value or "").strip()
        if not text and allow_empty:
            return ""
        try:
            parsed = urlsplit(text)
            port = parsed.port
        except ValueError as exc:
            raise ValueError(f"{name} provider origin is malformed") from exc
        if (
            parsed.scheme.lower() not in {secure_scheme, local_scheme}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"{name} provider origin must be a root origin")
        scheme = parsed.scheme.lower()
        host = parsed.hostname.lower()
        if scheme == local_scheme:
            try:
                address = ipaddress.ip_address(host)
            except ValueError as exc:
                raise ValueError(
                    f"{name} provider origin permits {local_scheme} only for a local IP address"
                ) from exc
            if not (address.is_loopback or address.is_private or address.is_link_local):
                raise ValueError(
                    f"{name} provider origin permits {local_scheme} only for a local IP address"
                )
        display_host = f"[{host}]" if ":" in host else host
        netloc = display_host if port is None else f"{display_host}:{port}"
        return urlunsplit((scheme, netloc, "", "", ""))

    def _runtime_config(self, parameters: dict) -> dict[str, str]:
        token = str(parameters.get("discord_token", "")).strip()
        channel = str(parameters.get("bot_channel", "")).strip()
        if not token:
            raise ValueError("discord_token is required")
        if not channel:
            raise ValueError("bot_channel is required")
        if not channel.isdigit():
            raise ValueError("bot_channel must be a numeric Discord channel ID")
        socks_channel = str(parameters.get("socks_channel", "")).strip()
        if socks_channel and (not socks_channel.isdigit() or socks_channel == channel):
            raise ValueError("socks_channel must be a different numeric Discord channel ID")
        listener_id = str(parameters.get("listener_id", "")).strip()
        if listener_id:
            try:
                parsed_listener_id = uuid.UUID(listener_id)
            except ValueError as exc:
                raise ValueError("listener_id must be a canonical UUID") from exc
            if str(parsed_listener_id) != listener_id or parsed_listener_id.version != 4:
                raise ValueError("listener_id must be a canonical UUIDv4")
        listener_enabled = parameters.get("listener_enabled", True)
        if not isinstance(listener_enabled, bool):
            raise ValueError("listener_enabled must be Boolean")
        provider_api_origin = self._normalize_provider_origin(
            parameters.get("provider_api_origin", "https://discord.com"),
            name="API",
            secure_scheme="https",
            local_scheme="http",
        )
        provider_gateway_origin = self._normalize_provider_origin(
            parameters.get("provider_gateway_origin", ""),
            name="Gateway",
            secure_scheme="wss",
            local_scheme="ws",
            allow_empty=True,
        )
        provider_cdn_origin = self._normalize_provider_origin(
            parameters.get("provider_cdn_origin", "https://cdn.discordapp.com"),
            name="CDN",
            secure_scheme="https",
            local_scheme="http",
        )
        provider_kind = str(parameters.get("discord_provider_kind", "")).strip().lower()
        if not provider_kind:
            provider_kind = "discord" if provider_api_origin == "https://discord.com" else "spacebar"
        if provider_kind not in {"discord", "spacebar", "mock"}:
            raise ValueError("unsupported Discord provider kind")
        try:
            provider_api_version = int(parameters.get("discord_api_version", 10))
        except (TypeError, ValueError) as exc:
            raise ValueError("Discord API version must be numeric") from exc
        if provider_api_version != 10:
            raise ValueError("unsupported Discord API version")
        provider_insecure = parameters.get("discord_test_only_allow_insecure_transport", False)
        if not isinstance(provider_insecure, bool):
            raise ValueError("discord_test_only_allow_insecure_transport must be Boolean")
        official = (
            provider_api_origin == "https://discord.com"
            and provider_gateway_origin in {"", "wss://gateway.discord.gg"}
            and provider_cdn_origin == "https://cdn.discordapp.com"
        )
        if provider_kind == "discord" and (not official or provider_insecure):
            raise ValueError("official Discord provider endpoints are fixed")
        endpoint_insecure = any(origin.startswith(("http://", "ws://")) for origin in (
            provider_api_origin, provider_gateway_origin, provider_cdn_origin
        ))
        if provider_kind in {"spacebar", "mock"}:
            if not provider_gateway_origin:
                raise ValueError("custom Discord providers require an explicit Gateway origin")
            if endpoint_insecure and not provider_insecure:
                raise ValueError("insecure provider transport requires explicit test-only opt-in")

        egress_mode = str(parameters.get("server_egress_proxy_mode", "direct")).strip().lower()
        egress_url = str(parameters.get("server_egress_proxy_url", "")).strip()
        egress_username = str(parameters.get("server_egress_proxy_username", ""))
        egress_password = str(parameters.get("server_egress_proxy_password", ""))
        egress_remote_dns = parameters.get("server_egress_proxy_remote_dns", True)
        if not isinstance(egress_remote_dns, bool):
            raise ValueError("server_egress_proxy_remote_dns must be Boolean")
        if egress_mode == "direct":
            if egress_url or egress_username or egress_password:
                raise ValueError("direct server egress forbids proxy settings")
        elif egress_mode in {"http", "socks5"}:
            try:
                parsed_proxy = urlsplit(egress_url)
            except ValueError as exc:
                raise ValueError("server proxy URL is malformed") from exc
            expected_schemes = {"http", "https"} if egress_mode == "http" else {"socks5"}
            if (
                parsed_proxy.scheme.lower() not in expected_schemes or not parsed_proxy.hostname
                or parsed_proxy.username is not None or parsed_proxy.password is not None
                or parsed_proxy.path not in {"", "/"} or parsed_proxy.query or parsed_proxy.fragment
            ):
                raise ValueError("server proxy URL must be a credential-free root URL matching its mode")
            if egress_mode == "socks5" and not egress_remote_dns:
                raise ValueError("SOCKS5 server egress requires remote DNS")
        else:
            raise ValueError("unsupported server egress proxy mode")

        ingress_mode = str(parameters.get("server_ingress_mode", "gateway")).strip().lower()
        poll_strategy = str(parameters.get("server_poll_strategy", "adaptive")).strip().lower()
        if ingress_mode not in {"gateway", "polling"} or poll_strategy not in {"adaptive", "fixed"}:
            raise ValueError("unsupported server ingress or polling strategy")
        timing_names = {
            "server_poll_interval_seconds": (15, 2, 300),
            "server_poll_min_interval_seconds": (2, 2, 60),
            "server_poll_base_interval_seconds": (15, 5, 300),
            "server_poll_max_interval_seconds": (300, 15, 3600),
            "server_poll_build_warm_seconds": (900, 0, 3600),
        }
        timings = {}
        for name, (default, minimum, maximum) in timing_names.items():
            try:
                timings[name] = int(parameters.get(name, default))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be numeric") from exc
            if not minimum <= timings[name] <= maximum:
                raise ValueError(f"{name} is outside its supported range")
        if not (
            timings["server_poll_min_interval_seconds"]
            <= timings["server_poll_base_interval_seconds"]
            <= timings["server_poll_max_interval_seconds"]
        ):
            raise ValueError("adaptive polling requires minimum <= base <= maximum")
        try:
            reconciliation = int(parameters.get("server_reconciliation_interval_seconds", 300))
        except (TypeError, ValueError) as exc:
            raise ValueError("server_reconciliation_interval_seconds must be numeric") from exc
        if reconciliation != 0 and not 60 <= reconciliation <= 3600:
            raise ValueError("server reconciliation interval must be zero or 60-3600 seconds")
        if ingress_mode == "polling":
            try:
                response_window = (int(parameters.get("message_checks", 10)) - 1) * int(parameters.get("time_between_checks", 10))
            except (TypeError, ValueError) as exc:
                raise ValueError("message_checks and time_between_checks must be numeric") from exc
            if response_window < 2 * timings["server_poll_min_interval_seconds"]:
                raise ValueError("Nuwa response window must cover at least two minimum polling intervals")
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
            "providerApiOrigin": provider_api_origin,
            "providerGatewayOrigin": provider_gateway_origin,
            "providerCdnOrigin": provider_cdn_origin,
        }
        if protection != "none":
            runtime_config["transportKey"] = key
        if socks_channel:
            runtime_config["socksChannelID"] = socks_channel
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
            self._runtime_config(inputMsg.Parameters)
            return C2ConfigCheckMessageResponse(
                Success=True,
                Message="Validated Discordx listener parameters; active registry unchanged",
                RestartInternalServer=False,
            )
        except Exception as exc:
            return C2ConfigCheckMessageResponse(
                Success=False,
                Error=str(exc),
            )
