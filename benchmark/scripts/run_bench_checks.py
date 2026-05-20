"""Pre-flight checks for local and cloud benchmark modes."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_BENCH_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(_BENCH_DIR))


class PreflightError(Exception):
    """A pre-flight check failed."""


def check_local(delays: list[int]) -> None:
    """Verify local mode prerequisites are met."""
    # cargo build must exist
    root = _BENCH_DIR
    binary = root / "node"
    if not binary.exists():
        raise PreflightError(
            f"Binary not found: {binary}. Run 'cargo build --release --features benchmark' first."
        )

    if not delays or all(d == 0 for d in delays):
        return

    # Dummynet requires sudo and dnctl
    if shutil.which("dnctl") is None:
        raise PreflightError(
            "dnctl not found. Dummynet requires macOS (or install ipfw/dummynet)."
        )

    # Verify sudo works with hardcoded password (same as run_bench_pipeline)
    sudo_pass = os.environ.get("SWEEP_SUDO_PASSWORD", "561280")
    proc = subprocess.run(
        ["sudo", "-S", "echo", "sudo_ok"],
        input=sudo_pass + "\n",
        capture_output=True,
        text=True,
        timeout=10,
    )
    if "sudo_ok" not in proc.stdout:
        raise PreflightError(
            f"sudo with password-stdin failed (check password or sudo permissions)"
        )


def check_cloud(settings_path: str | Path) -> None:
    """Verify cloud mode prerequisites."""
    try:
        from benchmark.settings import Settings
        resolved_settings = Settings.resolve_path(str(settings_path))
        s = Settings.load(str(resolved_settings))
    except Exception as e:
        raise PreflightError(f"Failed to load settings: {e}") from e

    # Settings stores provider under different keys; check heuristically
    provider = getattr(s, "cloud_provider", None)
    if provider not in ("aws", "gcp"):
        # Try to detect from settings.json raw content
        import json
        raw = json.loads(Path(resolved_settings).read_text())
        provider = raw.get("provider", raw.get("cloud_provider", ""))

    if provider == "gcp":
        if shutil.which("gcloud") is None:
            raise PreflightError("gcloud CLI not found. Install Google Cloud SDK.")
    else:
        if shutil.which("aws") is None:
            raise PreflightError(
                "aws CLI not found. Install AWS CLI. (For GCP, set provider: gcp in settings.json)"
            )
