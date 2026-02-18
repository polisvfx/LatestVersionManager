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

# Read version from the package — CI may have patched this before running PyInstaller
_ns = {}
exec(Path('src/lvm/__init__.py').read_text(), _ns)
APP_VERSION = _ns.get('__version__', '0.0.0')

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
        # Bundle the pre-rendered PNG (used by the app on all platforms at runtime)
        ('resources/mp_logo_256.png', 'resources'),
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
# Icon embedding:
#   Windows — .ico is embedded directly in the PE header (shown in Explorer)
#   macOS   — icon is set on the .app BUNDLE below, not on the raw EXE
#   Linux   — PyInstaller cannot embed icons in ELF binaries; the Qt window
#              icon is set at runtime from the bundled mp_logo_256.png
if sys.platform == 'win32':
    _exe_icon = 'resources/mp_logo.ico'
else:
    _exe_icon = None   # macOS uses BUNDLE; Linux has no PE icon concept

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
    icon=_exe_icon,
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
        icon='resources/mp_logo.icns',
        bundle_identifier='com.polisvfx.lvm',
        info_plist={
            'CFBundleName': 'Latest Version Manager',
            'CFBundleDisplayName': 'Latest Version Manager',
            'CFBundleVersion': APP_VERSION,
            'CFBundleShortVersionString': APP_VERSION,
            'NSHighResolutionCapable': True,
            'LSMinimumSystemVersion': '11.0',
            # Needed so macOS doesn't sandbox file access
            'NSAppleEventsUsageDescription': 'LVM needs to access the file system to manage versioned files.',
        },
    )
