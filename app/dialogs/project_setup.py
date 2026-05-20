"""project_setup dialog module."""

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
from app.widgets import TagInputWidget


class ProjectSetupDialog(QDialog):
    """Dialog for setting up a new LVM project."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("New Project")
        self.setMinimumWidth(500)

        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("My VFX Project")
        layout.addRow("Project Name:", self.name_edit)

        # Template dropdown (Feature #17)
        self.template_combo = QComboBox()
        self.template_combo.addItem("(none)", "")
        try:
            from src.lvm.templates import list_templates
            for t in list_templates():
                self.template_combo.addItem(f"{t['name']} [{t['location']}]", t["path"])
        except Exception:
            logger.warning("Could not load templates", exc_info=True)
        layout.addRow("From Template:", self.template_combo)

        # Project Root — the logical root of the project
        self.root_edit = QLineEdit()
        self.root_browse_btn = QPushButton("Browse...")
        self.root_browse_btn.clicked.connect(self._browse_root)
        root_row = QHBoxLayout()
        root_row.addWidget(self.root_edit, 1)
        root_row.addWidget(self.root_browse_btn)
        root_help = QLabel("The root directory of the project (used for {project_root} token).")
        root_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addRow("Project Root:", root_row)
        layout.addRow("", root_help)

        # Project Save — where the JSON config is saved (hidden until root is set)
        self.save_label = QLabel("Project Save:")
        self.save_edit = QLineEdit()
        self.save_browse_btn = QPushButton("Browse...")
        self.save_browse_btn.clicked.connect(self._browse_save)
        save_row = QHBoxLayout()
        save_row.addWidget(self.save_edit, 1)
        save_row.addWidget(self.save_browse_btn)
        save_help = QLabel("Where the project file (.json) is saved. Defaults to Project Root.")
        save_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        self._save_label = self.save_label
        self._save_row_widget = QWidget()
        self._save_row_widget.setLayout(save_row)
        self._save_help = save_help
        layout.addRow(self.save_label, self._save_row_widget)
        layout.addRow("", save_help)

        # Initially hide save location until root is chosen
        self._set_save_visible(False)
        self._save_user_edited = False
        self.root_edit.textChanged.connect(self._on_root_changed)
        self.save_edit.textChanged.connect(self._on_save_edited)

        # Filters
        filter_label = QLabel(
            "Filters are used when discovering versioned content.\n"
            "Whitelist: only include folders matching these keywords.\n"
            "Blacklist: skip folders matching these keywords."
        )
        filter_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-top: 6px;")
        layout.addRow("", filter_label)

        self.whitelist_edit = TagInputWidget(placeholder="Type and press comma to add...")
        layout.addRow("Whitelist:", self.whitelist_edit)

        self.blacklist_edit = TagInputWidget(placeholder="Type and press comma to add...")
        layout.addRow("Blacklist:", self.blacklist_edit)

        # Task tokens
        task_sep = QLabel("Task Names")
        task_sep.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addRow("", task_sep)

        task_help = QLabel(
            "Common task names in your pipeline that can be stripped\n"
            "from filenames. Use % as a counted wildcard (e.g. comp_%% matches comp_mp)."
        )
        task_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addRow("", task_help)

        self.tasks_edit = QLineEdit()
        self.tasks_edit.setPlaceholderText("comp, grade, dmp, fx, roto, paint")
        layout.addRow("Task Names:", self.tasks_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def _set_save_visible(self, visible: bool):
        self._save_label.setVisible(visible)
        self._save_row_widget.setVisible(visible)
        self._save_help.setVisible(visible)

    def _on_root_changed(self, text: str):
        root = text.strip()
        if root:
            self._set_save_visible(True)
            # Auto-populate save with root if user hasn't manually edited it
            if not self._save_user_edited:
                self.save_edit.blockSignals(True)
                self.save_edit.setText(root)
                self.save_edit.blockSignals(False)
        else:
            self._set_save_visible(False)

    def _on_save_edited(self):
        # Mark as user-edited only when save differs from root
        if self.save_edit.text().strip() != self.root_edit.text().strip():
            self._save_user_edited = True

    def _browse_root(self):
        d = QFileDialog.getExistingDirectory(self, "Select Project Root")
        if d:
            self.root_edit.setText(d)

    def _browse_save(self):
        start = self.save_edit.text().strip() or self.root_edit.text().strip()
        d = QFileDialog.getExistingDirectory(self, "Select Project Save Location", start)
        if d:
            self.save_edit.setText(d)

    def get_project_info(self) -> dict:
        wl = self.whitelist_edit.tags()
        bl = self.blacklist_edit.tags()
        tasks = [t.strip() for t in self.tasks_edit.text().split(",") if t.strip()]
        root = self.root_edit.text().strip()
        save = self.save_edit.text().strip() or root
        template_path = self.template_combo.currentData() or ""
        return {
            "project_name": self.name_edit.text().strip() or "Untitled",
            "project_root": root,
            "save_dir": save,
            "project_dir": save,  # backwards compat — save location
            "name_whitelist": wl,
            "name_blacklist": bl,
            "task_tokens": tasks,
            "template_path": template_path,
        }


# ---------------------------------------------------------------------------
# Tag Input Widget (for whitelist / blacklist)
# ---------------------------------------------------------------------------

