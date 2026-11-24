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

# Collect the bundled ffmpeg binary from imageio-ffmpeg
_ffmpeg_binaries = collect_data_files('imageio_ffmpeg', subdir='binaries')

# ── Analysis ──────────────────────────────────────────────────────────────────
a = Analysis(
    ['app.py'],
    pathex=['.', 'src'],
    binaries=[],
    datas=[
        # Bundle the SVG resource
        ('resources/mp_logo.svg', 'resources'),
        # Bundle the pre-rendered PNG (used by the app on all platforms at runtime)
        ('resources/mp_logo_256.png', 'resources'),
        # Bundle the NLE companion scripts so the bridge can launch them
        # in frozen builds (companions_dir() reads sys._MEIPASS / "companions").
        ('companions/resolve/*.py', 'companions/resolve'),
        ('companions/premiere/*.jsx', 'companions/premiere'),
        # CEP panel for Premiere — users install this from the bundled
        # tree to their Adobe CEP extensions folder.
        ('companions/premiere/lvm_panel/CSXS/*.xml', 'companions/premiere/lvm_panel/CSXS'),
        ('companions/premiere/lvm_panel/lib/*.js',   'companions/premiere/lvm_panel/lib'),
        ('companions/premiere/lvm_panel/*.html',     'companions/premiere/lvm_panel'),
        ('companions/premiere/lvm_panel/*.js',       'companions/premiere/lvm_panel'),
        ('companions/premiere/lvm_panel/*.jsx',      'companions/premiere/lvm_panel'),
    ] + _ffmpeg_binaries,
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
        # lvm core package (unified under src/lvm -> lvm.*)
        'lvm',
        'lvm.auto_promote',
        'lvm.config',
        'lvm.conflicts',
        'lvm.dirpoll',
        'lvm.discovery',
        'lvm.elevation',
        'lvm.fast_copy',
        'lvm.fileutil',
        'lvm.fsinfo',
        'lvm.history',
        'lvm.hook_trust',
        'lvm.hooks',
        'lvm.lockfile',
        'lvm.log_handler',
        'lvm.models',
        'lvm.nle_bridge',
        'lvm.patterns',
        'lvm.promoter',
        'lvm.render_check',
        'lvm.scan_cache',
        'lvm.scanner',
        'lvm.task_tokens',
        'lvm.templates',
        'lvm.thumbnail',
        'lvm.timecode',
        'lvm.updater',
        'lvm.watcher',
        # Stdlib modules used by the bundled NLE companion script
        # (companions/ is shipped as data, so PyInstaller can't see what
        # those scripts import — list anything they need here).
        'glob',
        'importlib.util',
        # Bundled ffmpeg binary provider
        'imageio_ffmpeg',
        'imageio_ffmpeg._utils',
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
    upx=False,         # UPX breaks macOS notarization and triggers AV false positives
    console=False,     # No terminal window — GUI app
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,       # Signing is handled post-build in CI
    entitlements_file='resources/entitlements.plist' if sys.platform == 'darwin' else None,
    icon=_exe_icon,
)

# ── COLLECT (gather all files into dist folder) ───────────────────────────────
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
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
