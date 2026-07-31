"""Put the repo root on sys.path so `processor.*` and `producer.*` import cleanly.

Both services are packages rather than loose scripts specifically so their
same-named `config` modules cannot shadow each other.
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
