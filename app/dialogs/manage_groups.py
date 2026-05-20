"""manage_groups dialog module."""

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
from app.widgets import _GROUP_COLOR_PALETTE


class ManageGroupsDialog(QDialog):
    """Dialog for managing source groups (add, rename, recolor, delete)."""

    def __init__(self, config: ProjectConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Groups")
        self.setMinimumWidth(450)
        self.setMinimumHeight(350)
        self._config = config
        # Work on a copy so we can cancel
        self._groups: dict[str, dict] = {k: dict(v) for k, v in config.groups.items()}

        layout = QVBoxLayout(self)

        self.group_list = QListWidget()
        self.group_list.setAlternatingRowColors(True)
        layout.addWidget(self.group_list)

        btn_row = QHBoxLayout()
        self.btn_add = QPushButton("Add Group")
        self.btn_add.clicked.connect(self._add_group)
        btn_row.addWidget(self.btn_add)

        self.btn_rename = QPushButton("Rename")
        self.btn_rename.clicked.connect(self._rename_group)
        btn_row.addWidget(self.btn_rename)

        self.btn_color = QPushButton("Change Color")
        self.btn_color.clicked.connect(self._change_color)
        btn_row.addWidget(self.btn_color)

        self.btn_root = QPushButton("Set Root Dir...")
        self.btn_root.clicked.connect(self._set_root_dir)
        btn_row.addWidget(self.btn_root)

        self.btn_clear_root = QPushButton("Clear Root")
        self.btn_clear_root.clicked.connect(self._clear_root_dir)
        btn_row.addWidget(self.btn_clear_root)

        self.btn_delete = QPushButton("Delete")
        self.btn_delete.clicked.connect(self._delete_group)
        btn_row.addWidget(self.btn_delete)

        layout.addLayout(btn_row)

        # Help text
        root_help = QLabel(
            "{group_root} token resolves to the group's root directory.\n"
            "If unset, {group_root} falls back to {project_root}."
        )
        root_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addWidget(root_help)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.group_list.currentItemChanged.connect(self._on_group_selected)
        self._rebuild_list()

    def _on_group_selected(self):
        item = self.group_list.currentItem()
        if item:
            name = item.data(Qt.UserRole)
            has_root = bool(self._groups[name].get("root_dir", ""))
            self.btn_clear_root.setEnabled(has_root)
        else:
            self.btn_clear_root.setEnabled(False)

    def _next_color(self) -> str:
        used = {v.get("color", "") for v in self._groups.values()}
        for c in _GROUP_COLOR_PALETTE:
            if c not in used:
                return c
        return _GROUP_COLOR_PALETTE[len(self._groups) % len(_GROUP_COLOR_PALETTE)]

    def _rebuild_list(self):
        self.group_list.clear()
        for name, props in sorted(self._groups.items()):
            color = props.get("color", "#8c8c8c")
            root = props.get("root_dir", "")
            count = sum(1 for s in self._config.watched_sources if s.group == name)
            root_label = f"  \u2502 root: {root}" if root else ""
            item = QListWidgetItem(f"  {name}  ({count} sources){root_label}")
            item.setData(Qt.UserRole, name)
            item.setForeground(QColor(color))
            if root:
                item.setToolTip(f"Root directory: {root}")
            self.group_list.addItem(item)
        self._on_group_selected()

    def _add_group(self):
        name, ok = QInputDialog.getText(self, "New Group", "Group name:")
        if not ok or not name.strip():
            return
        name = name.strip()
        if name in self._groups:
            QMessageBox.warning(self, "Duplicate", f"Group '{name}' already exists.")
            return
        self._groups[name] = {"color": self._next_color()}
        self._rebuild_list()

    def _rename_group(self):
        item = self.group_list.currentItem()
        if not item:
            return
        old_name = item.data(Qt.UserRole)
        new_name, ok = QInputDialog.getText(self, "Rename Group", "New name:", text=old_name)
        if not ok or not new_name.strip() or new_name.strip() == old_name:
            return
        new_name = new_name.strip()
        if new_name in self._groups:
            QMessageBox.warning(self, "Duplicate", f"Group '{new_name}' already exists.")
            return
        self._groups[new_name] = self._groups.pop(old_name)
        # Update sources referencing old name
        for source in self._config.watched_sources:
            if source.group == old_name:
                source.group = new_name
        self._rebuild_list()

    def _change_color(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        current = QColor(self._groups[name].get("color", "#8c8c8c"))
        color = QColorDialog.getColor(current, self, f"Color for {name}")
        if color.isValid():
            self._groups[name]["color"] = color.name()
            self._rebuild_list()

    def _set_root_dir(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        current = self._groups[name].get("root_dir", "")
        path = QFileDialog.getExistingDirectory(
            self, f"Root Directory for '{name}'", current or ""
        )
        if path:
            self._groups[name]["root_dir"] = path
        self._rebuild_list()

    def _clear_root_dir(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        self._groups[name]["root_dir"] = ""
        self._rebuild_list()

    def _delete_group(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        count = sum(1 for s in self._config.watched_sources if s.group == name)
        if count > 0:
            reply = QMessageBox.question(
                self, "Delete Group",
                f"Group '{name}' has {count} source(s) assigned.\n\n"
                f"Delete the group and unassign those sources?"
            )
            if reply != QMessageBox.Yes:
                return
            for source in self._config.watched_sources:
                if source.group == name:
                    source.group = ""
        del self._groups[name]
        self._rebuild_list()

    def apply_to_config(self, config: ProjectConfig):
        config.groups = dict(self._groups)


# ---------------------------------------------------------------------------
# Update Dialog
# ---------------------------------------------------------------------------

