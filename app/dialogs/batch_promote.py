"""batch_promote dialog module."""

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


class BatchPromoteReviewDialog(QDialog):
    """Review table for batch promotion with per-source checkboxes."""

    def __init__(self, promote_list, source_status, promoters, already_current, skipped, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Batch Promotion Review")
        self.setMinimumSize(900, 500)

        layout = QVBoxLayout(self)

        summary = QLabel(f"<b>{len(promote_list)}</b> source(s) will be promoted")
        summary.setStyleSheet("font-size: 13pt; padding: 4px;")
        layout.addWidget(summary)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["", "Source", "Current", "Target Version", "Files", "Frame Range", "Timecode", "Status"])
        self.tree.setRootIsDecorated(False)
        self.tree.setAlternatingRowColors(True)
        header = self.tree.header()
        header.resizeSection(0, 30)

        for source, version in promote_list:
            promoter = promoters.get(source.name)
            current = promoter.get_current_version() if promoter else None
            current_ver = current.version if current else "---"

            row_status = "normal"
            status_text = "OK"
            if version.frame_range and "gaps detected" in version.frame_range:
                row_status = "red"
                status_text = "GAPS"
            elif current:
                if current.frame_range and version.frame_range and current.frame_range != version.frame_range:
                    row_status = "orange"
                    status_text = "Range changed"
                elif current.start_timecode and version.start_timecode and current.start_timecode != version.start_timecode:
                    row_status = "orange"
                    status_text = "TC changed"

            batch_frame_display = version.frame_range or "---"
            if version.sub_sequences:
                batch_frame_display += f" (+{len(version.sub_sequences)} layer{'s' if len(version.sub_sequences) > 1 else ''})"
            item = QTreeWidgetItem([
                "", source.name, current_ver, version.version_string,
                str(version.file_count), batch_frame_display,
                version.start_timecode or "---", status_text,
            ])
            item.setCheckState(0, Qt.Checked)
            item.setData(0, Qt.UserRole, (source, version))

            color_map = {"normal": "#4ec9a0", "orange": "#ffaa00", "red": "#ff6666"}
            color = QColor(color_map[row_status])
            for col in range(1, 8):
                item.setForeground(col, color)

            self.tree.addTopLevelItem(item)

        layout.addWidget(self.tree)

        if already_current or skipped:
            info_parts = []
            if already_current:
                info_parts.append(f"{len(already_current)} already current")
            if skipped:
                info_parts.append(f"{len(skipped)} skipped")
            info_label = QLabel(", ".join(info_parts))
            info_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; padding: 4px;")
            layout.addWidget(info_label)

        btn_row = QHBoxLayout()
        btn_select_all = QPushButton("Select All")
        btn_select_all.clicked.connect(lambda: self._set_all_checked(True))
        btn_deselect_all = QPushButton("Deselect All")
        btn_deselect_all.clicked.connect(lambda: self._set_all_checked(False))
        btn_row.addWidget(btn_select_all)
        btn_row.addWidget(btn_deselect_all)
        btn_row.addStretch()

        btn_promote = QPushButton("Promote Selected")
        btn_promote.setStyleSheet(
            "QPushButton { background-color: #336699; color: white; padding: 8px 20px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
        )
        btn_promote.clicked.connect(self.accept)
        btn_row.addWidget(btn_promote)

        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)

        layout.addLayout(btn_row)

    def _set_all_checked(self, checked):
        state = Qt.Checked if checked else Qt.Unchecked
        for i in range(self.tree.topLevelItemCount()):
            self.tree.topLevelItem(i).setCheckState(0, state)

    def get_selected(self):
        selected = []
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if item.checkState(0) == Qt.Checked:
                selected.append(item.data(0, Qt.UserRole))
        return selected


class UndoPromoteDialog(QDialog):
    """Confirmation dialog for Undo Promote.

    Shows current -> target diff for a single source with a Steps spinbox.
    Reuses the diff/coloring conventions from BatchPromoteReviewDialog.
    """

    def __init__(self, source, promoter, scanner, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Undo Last Promote")
        self.setMinimumSize(720, 240)
        self._source = source
        self._promoter = promoter
        self._scanner = scanner
        self._versions = scanner.scan()
        self._history = promoter.get_history()
        self._target_version = None  # VersionInfo for accepted undo

        layout = QVBoxLayout(self)

        header = QLabel(f"<b>{source.name}</b>")
        header.setStyleSheet("font-size: 13pt; padding: 4px;")
        layout.addWidget(header)

        steps_row = QHBoxLayout()
        steps_row.addWidget(QLabel("Undo steps:"))
        self.steps_spin = QSpinBox()
        self.steps_spin.setMinimum(1)
        self.steps_spin.setMaximum(max(1, len(self._history) - 1))
        self.steps_spin.setValue(1)
        self.steps_spin.valueChanged.connect(self._refresh)
        steps_row.addWidget(self.steps_spin)
        steps_row.addStretch()
        layout.addLayout(steps_row)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(
            ["Source", "Current", "Undo Target", "Files", "Frame Range", "Timecode", "Status"]
        )
        self.tree.setRootIsDecorated(False)
        layout.addWidget(self.tree)

        self.warn_label = QLabel("")
        self.warn_label.setStyleSheet("color: #ff6666; padding: 4px;")
        self.warn_label.setVisible(False)
        layout.addWidget(self.warn_label)

        hint = QLabel("Tip: Ctrl+Shift+Z skips this dialog and undoes one step immediately.")
        hint.setStyleSheet("color: #8c8c8c; font-size: 10pt; padding: 2px 4px;")
        layout.addWidget(hint)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_ok = QPushButton("Undo")
        self.btn_ok.setStyleSheet(
            "QPushButton { background-color: #336699; color: white; padding: 8px 20px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
        )
        self.btn_ok.clicked.connect(self.accept)
        btn_row.addWidget(self.btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        self._refresh()

    def _refresh(self):
        from src.lvm.models import version_strings_match
        self.tree.clear()
        self.warn_label.setVisible(False)
        self._target_version = None

        n = self.steps_spin.value()
        if len(self._history) <= n:
            self.warn_label.setText(
                f"Not enough history to undo {n} step(s) — only {len(self._history)} entries."
            )
            self.warn_label.setVisible(True)
            self.btn_ok.setEnabled(False)
            return

        current = self._history[0]
        target = self._history[n]

        # Match the historical entry against currently-scanned versions
        target_version = None
        for v in self._versions:
            if version_strings_match(v.version_string, target.version, v.version_number):
                target_version = v
                break

        if target_version is None:
            self.warn_label.setText(
                f"Source files for {target.version} no longer exist on disk."
            )
            self.warn_label.setVisible(True)
            self.btn_ok.setEnabled(False)
            return

        self._target_version = target_version

        row_status = "normal"
        status_text = "OK"
        if current.frame_range and target_version.frame_range and current.frame_range != target_version.frame_range:
            row_status = "orange"
            status_text = "Range changed"
        elif current.start_timecode and target_version.start_timecode and current.start_timecode != target_version.start_timecode:
            row_status = "orange"
            status_text = "TC changed"

        item = QTreeWidgetItem([
            self._source.name,
            current.version,
            target_version.version_string,
            str(target_version.file_count),
            target_version.frame_range or "---",
            target_version.start_timecode or "---",
            status_text,
        ])
        color_map = {"normal": "#4ec9a0", "orange": "#ffaa00", "red": "#ff6666"}
        color = QColor(color_map[row_status])
        for col in range(0, 7):
            item.setForeground(col, color)
        self.tree.addTopLevelItem(item)
        self.btn_ok.setEnabled(True)

    def get_target_version(self):
        """Return the VersionInfo to re-promote, or None if invalid."""
        return self._target_version

    def get_steps(self) -> int:
        return self.steps_spin.value()


# ---------------------------------------------------------------------------
# Obsolete Layer Conflict Dialog
# ---------------------------------------------------------------------------

