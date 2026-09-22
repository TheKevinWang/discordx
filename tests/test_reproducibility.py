from pathlib import Path


DISCORD_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_BASE = (
    "itsafeaturemythic/mythic_python_dotnet"
    "@sha256:62a9295dc8803d6b9497f7a290540d41cab33a0f138526731c9d51e47b77ff33"
)


def test_discord_container_base_is_immutable():
    dockerfile = (
        DISCORD_ROOT / "C2_Profiles" / "discordx" / "Dockerfile"
    ).read_text(encoding="utf-8")

    assert dockerfile.splitlines()[0] == f"FROM {EXPECTED_BASE} AS runtime-base"
    assert ":latest" not in dockerfile
    assert "golang:1.26.0-bookworm@sha256:2a0ba12e116687098780d3ce700f9ce3cb340783779646aafbabed748fa6677c" in dockerfile
