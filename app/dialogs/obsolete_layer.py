"""obsolete_layer dialog module."""

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


class ObsoleteLayerDialog(QDialog):
    """Asks the user what to do when the new version is missing layers
    that the previously promoted version had in the latest directory.

    The dialog presents the list of obsolete layers and offers three actions:
    - **Keep**: leave the old layer files in the latest directory
    - **Delete**: remove them (default promotion behaviour)
    - **Skip**: do not promote this source at all

    An "Apply to all" checkbox (on by default) lets the user apply the same
    decision to every subsequent source with a layer conflict in the current
    batch.
    """

    # Result codes matching the three buttons
    KEEP = 1
    DELETE = 2
    SKIP = 3

    def __init__(self, source_name: str, version_string: str,
                 obsolete_layers: list[dict], conflict_count: int,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle("Obsolete Layer Conflict")
        self.setMinimumWidth(480)
        self.choice = self.DELETE  # default

        layout = QVBoxLayout(self)

        # Header
        if conflict_count > 1:
            count_label = QLabel(
                f"<b>{conflict_count}</b> source(s) in this promotion have layer conflicts."
            )
            count_label.setStyleSheet("color: #ffaa00; font-size: 12pt; padding-bottom: 4px;")
            layout.addWidget(count_label)

        header = QLabel(
            f"<b>{source_name}</b> — promoting to <b>{version_string}</b>"
        )
        header.setWordWrap(True)
        layout.addWidget(header)

        desc = QLabel(
            "The new version is missing the following layers that are "
            "currently in the latest directory:"
        )
        desc.setWordWrap(True)
        desc.setStyleSheet("padding: 4px 0;")
        layout.addWidget(desc)

        # Layer list
        layer_list = QTreeWidget()
        layer_list.setHeaderLabels(["Layer", "Files"])
        layer_list.setRootIsDecorated(False)
        layer_list.setAlternatingRowColors(True)
        for layer in obsolete_layers:
            item = QTreeWidgetItem([layer["name"], str(layer["file_count"])])
            item.setForeground(0, QColor("#ffaa00"))
            layer_list.addTopLevelItem(item)
        layer_list.header().setStretchLastSection(False)
        layer_list.header().setSectionResizeMode(0, QHeaderView.Stretch)
        layer_list.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        layer_list.setMaximumHeight(min(30 + len(obsolete_layers) * 26, 200))
        layout.addWidget(layer_list)

        # Apply to all checkbox
        self.apply_all_cb = QCheckBox("Apply to all current layer conflicts")
        self.apply_all_cb.setChecked(True)
        layout.addWidget(self.apply_all_cb)

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        btn_keep = QPushButton("Keep")
        btn_keep.setToolTip("Leave old layer files in the latest directory")
        btn_keep.setStyleSheet(
            "QPushButton { padding: 6px 18px; }"
        )
        btn_keep.clicked.connect(lambda: self._finish(self.KEEP))
        btn_row.addWidget(btn_keep)

        btn_delete = QPushButton("Delete")
        btn_delete.setToolTip("Remove obsolete layer files from the latest directory")
        btn_delete.setStyleSheet(
            "QPushButton { background-color: #8b2500; color: white; padding: 6px 18px; "
            "border-radius: 3px; }"
            "QPushButton:hover { background-color: #a83200; }"
        )
        btn_delete.clicked.connect(lambda: self._finish(self.DELETE))
        btn_row.addWidget(btn_delete)

        btn_skip = QPushButton("Skip Promotion")
        btn_skip.setToolTip("Do not promote this source")
        btn_skip.setStyleSheet(
            "QPushButton { padding: 6px 18px; }"
        )
        btn_skip.clicked.connect(lambda: self._finish(self.SKIP))
        btn_row.addWidget(btn_skip)

        layout.addLayout(btn_row)

    @property
    def apply_to_all(self) -> bool:
        return self.apply_all_cb.isChecked()

    def _finish(self, choice):
        self.choice = choice
        self.accept()


# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------

