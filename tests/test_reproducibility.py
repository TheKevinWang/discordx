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

    assert dockerfile.splitlines()[0] == f"FROM {EXPECTED_BASE}"
    assert ":latest" not in dockerfile
