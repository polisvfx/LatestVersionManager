# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for Latest Version Manager (LVM)
#
# Build commands:
#   Windows:  pyinstaller lvm.spec
#   macOS:    pyinstaller lvm.spec
#   Linux:    pyinstaller lvm.spec
#
# Output will be in dist/LatestVersionManager/

import sys
from pathlib import Path

block_cipher = None

# Collect all PySide6 plugins needed for the app
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

# ── Analysis ──────────────────────────────────────────────────────────────────
a = Analysis(
    ['app.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        # Bundle the SVG resource
        ('resources/mp_logo.svg', 'resources'),
        # Bundle the src package explicitly (it's a namespace package)
        ('src/lvm/*.py', 'src/lvm'),
    ],
    hiddenimports=[
        # watchdog uses platform-specific backends selected at runtime
        'watchdog.observers',
        'watchdog.observers.polling',
        # Platform-specific watchdog backends
        'watchdog.observers.fsevents',    # macOS
        'watchdog.observers.inotify',     # Linux
        'watchdog.observers.read_directory_changes',  # Windows
        # PySide6 modules used via dynamic import
        'PySide6.QtSvg',
        'PySide6.QtXml',
        # src package
        'src',
        'src.lvm',
        'src.lvm.models',
        'src.lvm.config',
        'src.lvm.scanner',
        'src.lvm.promoter',
        'src.lvm.history',
        'src.lvm.discovery',
        'src.lvm.watcher',
        'src.lvm.elevation',
        'src.lvm.task_tokens',
        'src.lvm.timecode',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Only exclude heavy third-party packages we definitely don't need.
        # Do NOT exclude stdlib modules — many are pulled in transitively
        # (e.g. urllib is required by pathlib, email by logging, etc.)
        'tkinter',
        'matplotlib',
        'numpy',
        'scipy',
        'pandas',
        'PIL',
        'IPython',
        'jupyter',
        'notebook',
        'pytest',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ── PYZ (compiled .pyc archive) ───────────────────────────────────────────────
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# ── EXE ───────────────────────────────────────────────────────────────────────
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='LatestVersionManager',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,          # Compress with UPX if available (reduces size ~30%)
    console=False,     # No terminal window — GUI app
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Windows: embed an icon if one exists as .ico
    # icon='resources/mp_logo.ico',  # Uncomment after converting SVG → ICO
)

# ── COLLECT (gather all files into dist folder) ───────────────────────────────
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='LatestVersionManager',
)

# ── macOS: wrap in .app bundle ────────────────────────────────────────────────
if sys.platform == 'darwin':
    app = BUNDLE(
        coll,
        name='LatestVersionManager.app',
        # icon='resources/mp_logo.icns',  # Uncomment after converting SVG → ICNS
        bundle_identifier='com.polisvfx.lvm',
        info_plist={
            'CFBundleName': 'Latest Version Manager',
            'CFBundleDisplayName': 'Latest Version Manager',
            'CFBundleVersion': '0.1.0',
            'CFBundleShortVersionString': '0.1.0',
            'NSHighResolutionCapable': True,
            'LSMinimumSystemVersion': '11.0',
            # Needed so macOS doesn't sandbox file access
            'NSAppleEventsUsageDescription': 'LVM needs to access the file system to manage versioned files.',
        },
    )
