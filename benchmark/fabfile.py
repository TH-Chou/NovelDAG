"""Stable Fabric entrypoint for the benchmark workspace.

The implementation lives in scripts/fabfile.py so the supporting Python tools can
stay together. This wrapper keeps the traditional workflow working:

    cd benchmark
    fab local
    fab info
"""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys

from invoke.tasks import Task

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

spec = spec_from_file_location("_benchmark_scripts_fabfile", SCRIPTS / "fabfile.py")
if spec is None or spec.loader is None:
    raise RuntimeError(f"Cannot load Fabric tasks from {SCRIPTS / 'fabfile.py'}")

module = module_from_spec(spec)
spec.loader.exec_module(module)

for name, value in vars(module).items():
    if isinstance(value, Task):
        globals()[name] = value

__all__ = [name for name, value in globals().items() if isinstance(value, Task)]
