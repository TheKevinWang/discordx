from __future__ import annotations

import ast
from pathlib import Path


PROFILE_REPOSITORY = Path(__file__).resolve().parents[1]
PROFILE_ROOT = PROFILE_REPOSITORY / "C2_Profiles" / "discordx"
PROFILE_MODULE = PROFILE_ROOT / "discordx" / "c2_functions" / "discordx.py"
README = PROFILE_REPOSITORY / "README.md"


def test_discordx_is_the_only_custom_profile_identity():
    assert PROFILE_ROOT.is_dir()
    assert not (PROFILE_REPOSITORY / "C2_Profiles" / "discord").exists()
    assert PROFILE_MODULE.is_file()

    module = ast.parse(PROFILE_MODULE.read_text(encoding="utf-8"))
    profile_class = next(
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == "DiscordX"
    )
    assignments = {
        target.id: node.value.value
        for node in profile_class.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance((target := node.targets[0]), ast.Name)
        and isinstance(node.value, ast.Constant)
    }

    assert assignments["name"] == "discordx"
    assert assignments["description"] == "discordx"


def test_discordx_entrypoints_and_push_identity_agree():
    main_source = (PROFILE_ROOT / "main.py").read_text(encoding="utf-8")
    push_source = (
        PROFILE_ROOT
        / "discordx"
        / "c2_code"
        / "src"
        / "discordx"
        / "Clients"
        / "GrpcMythicPushConnection.cs"
    ).read_text(encoding="utf-8")

    assert "from discordx.c2_functions.discordx import *" in main_source
    assert 'C2ProfileName = "discordx"' in push_source
    assert (PROFILE_ROOT / "discordx" / "c2_code" / "src" / "discordx.sln").is_file()
    assert (
        PROFILE_ROOT
        / "discordx"
        / "c2_code"
        / "tests"
        / "discordx.Tests"
        / "discordx.Tests.csproj"
    ).is_file()


def test_public_readme_is_agent_and_dummy_codec_neutral():
    readme = README.read_text(encoding="utf-8").lower()

    for private_implementation_detail in ("nuwa", "decimal", "emoji"):
        assert private_implementation_detail not in readme
