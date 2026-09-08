from __future__ import annotations

import importlib.util
import asyncio
import base64
import json
import os
import re
import stat
import sys
import types
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "C2_Profiles" / "discordx" / "discordx" / "c2_functions" / "discordx.py"


def _load_profile_module():
    fake_root = types.ModuleType("mythic_container")
    fake_base = types.ModuleType("mythic_container.C2ProfileBase")

    class C2Profile:
        pass

    class C2ProfileParameter:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class C2ConfigCheckMessage:
        def __init__(self, c2_profile_name: str, parameters: dict, **kwargs):
            self.Name = c2_profile_name
            self.Parameters = parameters

    class C2ConfigCheckMessageResponse:
        def __init__(self, Success: bool, Error: str = "", Message: str = "", RestartInternalServer: bool = False, **kwargs):
            self.Success = Success
            self.Error = Error
            self.Message = Message
            self.RestartInternalServer = RestartInternalServer

    class ParameterType:
        Boolean = "Boolean"
        ChooseOne = "ChooseOne"
        Date = "Date"

    fake_base.C2Profile = C2Profile
    fake_base.C2ProfileParameter = C2ProfileParameter
    fake_base.C2ConfigCheckMessage = C2ConfigCheckMessage
    fake_base.C2ConfigCheckMessageResponse = C2ConfigCheckMessageResponse
    fake_base.ParameterType = ParameterType
    fake_root.C2ProfileBase = fake_base

    sys.modules["mythic_container"] = fake_root
    sys.modules["mythic_container.C2ProfileBase"] = fake_base

    spec = importlib.util.spec_from_file_location("discord_profile_under_test", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module, fake_base


def test_config_check_writes_runtime_config(tmp_path):
    module, fake_base = _load_profile_module()
    profile = module.DiscordX()
    profile.server_folder_path = tmp_path

    response = asyncio.run(profile.config_check(
        fake_base.C2ConfigCheckMessage(
            c2_profile_name="discordx",
            parameters={
                "discord_token": "bot-token-value",
                "bot_channel": "1234567890",
                "transport_envelope_format": "binary-v1",
                "transport_presentation": "decimal",
                "transport_protection": "chacha20-v1",
                "transport_key_mode": "directional",
                "transport_nonce_strategy": "random",
                "transport_key": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
                "use_base64": False,
            },
        )
    ))

    assert response.Success is True
    assert response.RestartInternalServer is True
    assert json.loads((tmp_path / "config.json").read_text()) == {
        "botToken": "bot-token-value",
        "channelID": "1234567890",
        "wireProtocol": "fixed",
        "transportEnvelopeFormat": "binary-v1",
        "transportPresentation": "decimal",
        "transportProtection": "chacha20-v1",
        "transportKeyMode": "directional",
        "useBase64": "false",
        "transportKey": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
    }
    assert stat.S_IMODE((tmp_path / "config.json").stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".config.json.*.tmp"))


def test_config_check_rejects_invalid_channel_id(tmp_path):
    module, fake_base = _load_profile_module()
    profile = module.DiscordX()
    profile.server_folder_path = tmp_path

    response = asyncio.run(profile.config_check(
        fake_base.C2ConfigCheckMessage(
            c2_profile_name="discordx",
            parameters={
                "discord_token": "bot-token-value",
                "bot_channel": "not-a-number",
            },
        )
    ))

    assert response.Success is False
    assert "channel" in response.Error.lower()
    assert not (tmp_path / "config.json").exists()


def test_nuwa_third_party_stack_is_the_new_listener_default():
    module, _ = _load_profile_module()
    profile = module.DiscordX()

    parameters = {parameter.kwargs["name"]: parameter.kwargs for parameter in profile.parameters}

    assert parameters["wire_protocol"]["default_value"] == "fixed"
    assert parameters["transport_envelope_format"]["default_value"] == "binary-v1"
    assert parameters["transport_presentation"]["default_value"] == "base64"
    assert parameters["transport_protection"]["default_value"] == "chacha20-v1"
    assert parameters["transport_key_mode"]["default_value"] == "directional"
    assert parameters["use_base64"]["default_value"] is False
    assert parameters["AESPSK"]["default_value"] == "none"
    assert parameters["encrypted_exchange_check"]["default_value"] == "F"

    transport_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
    runtime = profile._runtime_config(
        {
            "discord_token": "bot-token-value",
            "bot_channel": "1234567890",
            "transport_key": transport_key,
        }
    )
    assert runtime == {
        "botToken": "bot-token-value",
        "channelID": "1234567890",
        "wireProtocol": "fixed",
        "transportEnvelopeFormat": "binary-v1",
        "transportPresentation": "base64",
        "transportProtection": "chacha20-v1",
        "transportKeyMode": "directional",
        "useBase64": "false",
        "transportKey": transport_key,
    }
    assert runtime["useBase64"] == "false"


def test_encrypted_exchange_help_explains_modes_and_agent_requirement():
    module, _ = _load_profile_module()
    profile = module.DiscordX()
    parameters = {parameter.kwargs["name"]: parameter.kwargs for parameter in profile.parameters}

    help_text = parameters["encrypted_exchange_check"]["description"]
    for expected in (
        "staging_rsa",
        "fresh per-execution session key",
        "embedded static AES key",
        "AESPSK",
        "payload type must implement",
    ):
        assert expected in help_text


def test_aespsk_help_names_inner_message_protection_and_legacy_identifier():
    module, _ = _load_profile_module()
    parameters = {
        parameter.kwargs["name"]: parameter.kwargs
        for parameter in module.DiscordX().parameters
    }

    help_text = parameters["AESPSK"]["description"]
    assert help_text.startswith("Inner Message Protection (AESPSK)")
    assert "legacy Mythic parameter name" in help_text
    assert "generates and manages" in help_text


def test_transport_parameters_are_fixed_listener_choices_with_accessible_help():
    module, fake_base = _load_profile_module()
    profile = module.DiscordX()
    parameters = {parameter.kwargs["name"]: parameter.kwargs for parameter in profile.parameters}

    assert parameters["transport_envelope_format"]["choices"] == ["json-v1", "binary-v1"]
    assert parameters["transport_presentation"]["choices"] == ["plain", "base64", "decimal", "emoji"]
    assert parameters["transport_protection"]["choices"] == [
        "none", "xor-obfuscation-v1", "chacha20-v1", "aes256-hmac-v1",
    ]
    assert parameters["transport_key_mode"]["choices"] == ["single", "directional"]
    assert "transport_nonce_strategy" not in parameters
    presentation_help = parameters["transport_presentation"]["description"].lower()
    assert "discord channel" in presentation_help
    assert "plain" in presentation_help
    assert "base64" in presentation_help
    assert "digits" in presentation_help
    assert "emoji" in presentation_help
    assert "does not encrypt" in presentation_help
    assert "unauthenticated" in parameters["transport_protection"]["description"].lower()
    assert parameters["transport_key"]["randomize"] is True


def test_transport_key_parameter_accepts_every_canonical_32_byte_base64_tail():
    module, _ = _load_profile_module()
    profile = module.DiscordX()
    parameter = next(
        parameter.kwargs for parameter in profile.parameters
        if parameter.kwargs["name"] == "transport_key"
    )

    canonical_keys = [
        base64.b64encode(bytes(31) + bytes([last_byte])).decode("ascii")
        for last_byte in range(256)
    ]
    canonical_tail_characters = {key[-2] for key in canonical_keys}

    assert canonical_tail_characters == set("AEIMQUYcgkosw048")
    assert parameter["format_string"] == "[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]="
    assert all(re.fullmatch(parameter["verifier_regex"], key) for key in canonical_keys)

    noncanonical_tail_characters = set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    ) - canonical_tail_characters
    assert all(
        re.fullmatch(parameter["verifier_regex"], canonical_keys[0][:-2] + tail + "=") is None
        for tail in noncanonical_tail_characters
    )


def test_none_discards_an_auto_materialized_transport_key():
    module, _ = _load_profile_module()
    runtime = module.DiscordX()._runtime_config({
        "discord_token": "bot-token-value",
        "bot_channel": "1234567890",
        "transport_envelope_format": "json-v1",
        "transport_presentation": "plain",
        "transport_protection": "none",
        "transport_key": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
    })

    assert "transportKey" not in runtime


def test_runtime_config_preserves_base64_transport_presentation():
    module, _ = _load_profile_module()
    runtime = module.DiscordX()._runtime_config({
        "discord_token": "bot-token-value",
        "bot_channel": "1234567890",
        "transport_presentation": "base64",
        "transport_protection": "none",
    })

    assert runtime["transportPresentation"] == "base64"


def test_wire_protocol_selects_the_explicit_legacy_compatibility_listener():
    module, fake_base = _load_profile_module()
    profile = module.DiscordX()
    parameters = {parameter.kwargs["name"]: parameter.kwargs for parameter in profile.parameters}

    assert parameters["wire_protocol"] == {
        "name": "wire_protocol",
        "description": (
            "Choose fixed for Nuwa's configured transport envelope or legacy to preserve "
            "the original Discord JSON wrapper for existing agents. One running listener "
            "uses exactly one wire protocol."
        ),
        "default_value": "fixed",
        "choices": ["fixed", "legacy"],
        "parameter_type": fake_base.ParameterType.ChooseOne,
        "required": False,
    }

    runtime = profile._runtime_config({
        "discord_token": "bot-token-value",
        "bot_channel": "1234567890",
        "wire_protocol": "legacy",
        "transport_envelope_format": "json-v1",
        "transport_presentation": "plain",
        "transport_protection": "none",
        "transport_key_mode": "single",
    })

    assert runtime["wireProtocol"] == "legacy"


@pytest.mark.parametrize(
    ("updates", "fragment"),
    [
        ({"transport_presentation": "plain", "transport_protection": "chacha20-v1"}, "plain"),
        ({"transport_envelope_format": "binary-v1", "transport_presentation": "plain"}, "plain"),
        ({"transport_envelope_format": "legacy-json"}, "format"),
        ({"transport_presentation": "bogus"}, "presentation"),
        ({"transport_protection": "bogus"}, "protection"),
        ({"transport_key_mode": "bogus"}, "key mode"),
        ({"wire_protocol": "bogus"}, "wire protocol"),
        ({"transport_protection": "chacha20-v1", "transport_key": "not-base64"}, "key"),
        ({"transport_protection": "chacha20-v1", "transport_key": "YQ=="}, "32"),
    ],
)
def test_runtime_config_rejects_invalid_fixed_transport_settings(updates, fragment):
    module, _ = _load_profile_module()
    parameters = {
        "discord_token": "bot-token-value",
        "bot_channel": "1234567890",
        "transport_envelope_format": "json-v1",
        "transport_presentation": "decimal",
        "transport_protection": "none",
        "transport_key_mode": "single",
        "transport_key": "",
    }
    parameters.update(updates)

    with pytest.raises(ValueError, match=fragment):
        module.DiscordX()._runtime_config(parameters)


def test_config_check_replaces_existing_file_and_keeps_key_out_of_response(tmp_path):
    module, fake_base = _load_profile_module()
    profile = module.DiscordX()
    profile.server_folder_path = tmp_path
    config_path = tmp_path / "config.json"
    config_path.write_text("{}")
    os.chmod(config_path, 0o644)
    key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

    response = asyncio.run(profile.config_check(fake_base.C2ConfigCheckMessage(
        c2_profile_name="discordx",
        parameters={
            "discord_token": "bot-token-value",
            "bot_channel": "1234567890",
            "transport_presentation": "emoji",
            "transport_protection": "aes256-hmac-v1",
            "transport_key_mode": "single",
            "transport_key": key,
        },
    )))

    assert response.Success is True
    assert response.RestartInternalServer is True
    assert key not in response.Message
    assert key not in response.Error
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
