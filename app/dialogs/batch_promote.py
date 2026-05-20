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


# ---------------------------------------------------------------------------
# Obsolete Layer Conflict Dialog
# ---------------------------------------------------------------------------

