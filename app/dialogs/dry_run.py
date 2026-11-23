"""dry_run dialog module."""

import html

from app._common import *  # noqa: F401,F403
from app._common import (
    _STATUS_MARKERS,
    _STATUS_LABELS,
    _STATUS_COLORS,
    _OVERRIDE_COLOR,
    _CONFLICT_COLOR,
    _DEFAULT_GROUP_COLOR_HEX,
    _GROUP_COLOR_CACHE,
    _REVEAL_LABEL,
    _BASE_DIR,
    _PLATFORM,
    _SINGLE_FILE_EXTS,
    _group_qcolor,
    _load_app_icon,
    _preview_sample_suffix,
    _expand_group_token,
    _resolve_group_root,
)


class DryRunDialog(QDialog):
    """Preview dialog showing the file mapping before promotion."""

    def __init__(self, dry_run_data: dict, version: VersionInfo,
                 source: WatchedSource, current: HistoryEntry = None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Promote Preview — {source.name} → {version.version_string}")
        self.setMinimumSize(750, 500)
        self.resize(850, 550)

        layout = QVBoxLayout(self)

        # Summary header
        header = QLabel(
            f"<b>{html.escape(source.name)}</b> → <b>{html.escape(version.version_string)}</b> &nbsp; "
            f"({dry_run_data['total_files']} files, "
            f"{html.escape(version.total_size_human)}, {html.escape(str(dry_run_data['link_mode']))} mode)"
        )
        header.setStyleSheet("font-size: 13pt; padding: 4px;")
        layout.addWidget(header)

        target_label = QLabel(f"Target: {dry_run_data['target_dir']}")
        target_label.setTextFormat(Qt.PlainText)
        target_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; padding: 0 4px 4px;")
        layout.addWidget(target_label)

        # Active promotion hooks banner (visibility before executing shell commands)
        pre_cmd = (getattr(source, "pre_promote_cmd", "") or "").strip()
        post_cmd = (getattr(source, "post_promote_cmd", "") or "").strip()
        if pre_cmd or post_cmd:
            hook_frame = QFrame()
            hook_frame.setStyleSheet(
                "QFrame { background-color: #222a35; border: 1px solid #446688; "
                "border-radius: 4px; padding: 6px; }"
            )
            hook_layout = QVBoxLayout(hook_frame)
            hook_layout.setContentsMargins(8, 4, 8, 4)
            hook_title = QLabel("<b>Shell hooks configured for this promotion:</b>")
            hook_title.setStyleSheet("color: #8cc4ff; font-size: 11pt;")
            hook_layout.addWidget(hook_title)
            if pre_cmd:
                lbl = QLabel(f"Pre-promote: {pre_cmd}")
                lbl.setTextFormat(Qt.PlainText)
                lbl.setWordWrap(True)
                lbl.setStyleSheet("color: #d0d8e0; font-family: monospace; font-size: 10pt;")
                hook_layout.addWidget(lbl)
            if post_cmd:
                lbl = QLabel(f"Post-promote: {post_cmd}")
                lbl.setTextFormat(Qt.PlainText)
                lbl.setWordWrap(True)
                lbl.setStyleSheet("color: #d0d8e0; font-family: monospace; font-size: 10pt;")
                hook_layout.addWidget(lbl)
            layout.addWidget(hook_frame)

        # Frame range mismatch warning
        self._warnings = []
        if current:
            if (current.frame_range and version.frame_range
                    and current.frame_range != version.frame_range):
                self._warnings.append(
                    f"Frame range changed: {current.frame_range} → {version.frame_range}"
                )
            if (current.frame_count and version.frame_count
                    and current.frame_count != version.frame_count):
                self._warnings.append(
                    f"Frame count changed: {current.frame_count} → {version.frame_count}"
                )
            if (current.start_timecode and version.start_timecode
                    and current.start_timecode != version.start_timecode):
                self._warnings.append(
                    f"Timecode changed: {current.start_timecode} → {version.start_timecode}"
                )

        if self._warnings:
            warn_frame = QFrame()
            warn_frame.setStyleSheet(
                "QFrame { background-color: #3a3a1a; border: 1px solid #6a6a2d; "
                "border-radius: 4px; padding: 6px; }"
            )
            warn_layout = QVBoxLayout(warn_frame)
            warn_layout.setContentsMargins(8, 4, 8, 4)
            for w in self._warnings:
                warn_label = QLabel(f"\u26a0 {w}")
                warn_label.setStyleSheet("color: #ffaa00; font-size: 12pt;")
                warn_layout.addWidget(warn_label)
            layout.addWidget(warn_frame)

        # File mapping tree
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Source File", "→ Target Name", "Size"])
        self.tree.setRootIsDecorated(False)
        self.tree.setAlternatingRowColors(True)
        header_view = self.tree.header()
        header_view.setStretchLastSection(False)
        header_view.setSectionResizeMode(0, QHeaderView.Stretch)
        header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        header_view.setSectionResizeMode(2, QHeaderView.ResizeToContents)

        for item in dry_run_data["file_map"]:
            src_name = os.path.basename(item["source"])
            size = item["size_bytes"]
            for unit in ("B", "KB", "MB", "GB"):
                if size < 1024:
                    size_str = f"{size:.1f} {unit}"
                    break
                size /= 1024
            else:
                size_str = f"{size:.1f} TB"

            tree_item = QTreeWidgetItem([src_name, item["target_name"], size_str])
            # Highlight if name actually changed
            if src_name != item["target_name"]:
                tree_item.setForeground(1, QColor("#4ec9a0"))
            self.tree.addTopLevelItem(tree_item)

        layout.addWidget(self.tree)

        # Keep / Replace info
        if current:
            replace_label = QLabel(
                f"This will replace the current version ({current.version})."
            )
            replace_label.setStyleSheet("color: #cc8833; font-size: 11pt; padding: 4px;")
            layout.addWidget(replace_label)

        # Buttons
        btn_box = QDialogButtonBox()
        self.btn_promote = btn_box.addButton("Promote", QDialogButtonBox.AcceptRole)
        self.btn_promote.setStyleSheet(
            "QPushButton { background-color: #336699; color: white; padding: 8px 20px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
        )
        btn_box.addButton(QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)


# ---------------------------------------------------------------------------
# Source editor dialog (with override toggles)
# ---------------------------------------------------------------------------

