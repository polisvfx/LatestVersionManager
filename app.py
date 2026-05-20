"""
Latest Version Manager - PySide6 GUI Application (entry point).

The implementation lives in the ``app/`` package. This file remains as a
script entry point so existing tooling (PyInstaller spec, start scripts)
continues to work unchanged.
"""

from app._common import main


if __name__ == "__main__":
    main()
