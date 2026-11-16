"""Make the script folders importable (the scripts are run by path, not installed as modules)."""
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "smart_clames"
for sub in ("streaming", "synthetic_data", "database"):
    sys.path.insert(0, str(SRC / sub))
