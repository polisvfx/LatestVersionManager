"""
Latest Version Manager - PySide6 GUI Application (entry point).

The implementation lives in the ``app/`` package. This file remains as a
script entry point so existing tooling (PyInstaller spec, start scripts)
continues to work unchanged.
"""

import os
import sys

# Ensure src/ is on sys.path so `import lvm` resolves cleanly from source
_ROOT = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(_ROOT, "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from app._common import main


if __name__ == "__main__":
    main()
