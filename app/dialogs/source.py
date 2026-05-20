"""source dialog module."""

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


class SourceDialog(QDialog):
    """Dialog for adding or editing a watched source."""

    def __init__(self, source: WatchedSource = None, project_config: ProjectConfig = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Source" if source else "Add Source")
        self.setMinimumWidth(550)
        self._project_config = project_config

        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.name_edit = QLineEdit()
        layout.addRow("Name:", self.name_edit)

        # Source directory
        self.source_dir_edit = QLineEdit()
        self.source_browse_btn = QPushButton("Browse...")
        self.source_browse_btn.clicked.connect(self._browse_source)
        source_row = QHBoxLayout()
        source_row.addWidget(self.source_dir_edit, 1)
        source_row.addWidget(self.source_browse_btn)
        layout.addRow("Source Directory:", source_row)

        # --- Overrideable fields ---
        override_label = QLabel("Fields below inherit project defaults unless overridden:")
        override_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-top: 8px;")
        layout.addRow("", override_label)

        # Latest target
        self.override_latest_check = QCheckBox("Override")
        self.target_dir_edit = QLineEdit()
        self.target_browse_btn = QPushButton("Browse...")
        self.target_browse_btn.clicked.connect(self._browse_target)
        self.override_latest_check.toggled.connect(lambda on: self._toggle_override(
            on, self.target_dir_edit, self.target_browse_btn,
            self._get_default_latest()))
        target_row = QHBoxLayout()
        target_row.addWidget(self.override_latest_check)
        target_row.addWidget(self.target_dir_edit, 1)
        target_row.addWidget(self.target_browse_btn)
        layout.addRow("Latest Target:", target_row)

        # Version pattern
        self.override_pattern_check = QCheckBox("Override")
        self.pattern_edit = QLineEdit()
        self.override_pattern_check.toggled.connect(lambda on: self._toggle_override(
            on, self.pattern_edit, None,
            project_config.default_version_pattern if project_config else "_v{version}"))
        pattern_row = QHBoxLayout()
        pattern_row.addWidget(self.override_pattern_check)
        pattern_row.addWidget(self.pattern_edit, 1)
        layout.addRow("Version Pattern:", pattern_row)

        # Date format — multi-select. Pick zero or more formats; each one
        # configured contributes to validation/stripping/sorting. Zero
        # checked == "(none)", same as the old single-value behaviour.
        self.override_date_format_check = QCheckBox("Override")
        self.date_format_checks: dict[str, QCheckBox] = {}
        date_row = QHBoxLayout()
        date_row.addWidget(self.override_date_format_check)
        for fmt in ("DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD"):
            cb = QCheckBox(fmt)
            self.date_format_checks[fmt] = cb
            date_row.addWidget(cb)
        date_row.addStretch(1)
        self.override_date_format_check.toggled.connect(
            lambda on: [cb.setEnabled(on) for cb in self.date_format_checks.values()]
        )
        layout.addRow("Date Format:", date_row)

        # File extensions
        self.override_ext_check = QCheckBox("Override")
        self.extensions_edit = QLineEdit()
        default_ext_str = " ".join(project_config.default_file_extensions if project_config else DEFAULT_FILE_EXTENSIONS)
        self.override_ext_check.toggled.connect(lambda on: self._toggle_override(
            on, self.extensions_edit, None, default_ext_str))
        ext_row = QHBoxLayout()
        ext_row.addWidget(self.override_ext_check)
        ext_row.addWidget(self.extensions_edit, 1)
        layout.addRow("File Extensions:", ext_row)

        # Link mode
        self.override_link_mode_check = QCheckBox("Override")
        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItems(["copy", "hardlink", "symlink"])
        self.override_link_mode_check.toggled.connect(lambda on: self.link_mode_combo.setEnabled(on))
        link_row = QHBoxLayout()
        link_row.addWidget(self.override_link_mode_check)
        link_row.addWidget(self.link_mode_combo, 1)
        layout.addRow("Link Mode:", link_row)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

        # Populate if editing
        if source:
            self.name_edit.setText(source.name)
            self.source_dir_edit.setText(source.source_dir)

            self.override_latest_check.setChecked(source.override_latest_target)
            self.target_dir_edit.setText(source.latest_target)

            self.override_pattern_check.setChecked(source.override_version_pattern)
            self.pattern_edit.setText(source.version_pattern)

            self.override_ext_check.setChecked(source.override_file_extensions)
            self.extensions_edit.setText(" ".join(source.file_extensions))

            self.override_date_format_check.setChecked(source.override_date_format)
            from src.lvm.task_tokens import parse_date_formats as _parse_dfmts
            for fmt in _parse_dfmts(source.date_format):
                if fmt in self.date_format_checks:
                    self.date_format_checks[fmt].setChecked(True)

            self.override_link_mode_check.setChecked(source.override_link_mode)
            self.link_mode_combo.setCurrentText(source.link_mode)
        else:
            # New source — start with defaults, overrides off
            self.override_latest_check.setChecked(False)
            self.override_pattern_check.setChecked(False)
            self.override_date_format_check.setChecked(False)
            self.override_ext_check.setChecked(False)
            self.override_link_mode_check.setChecked(False)

        # Apply initial toggle state
        self._toggle_override(self.override_latest_check.isChecked(),
                              self.target_dir_edit, self.target_browse_btn,
                              self._get_default_latest())
        self._toggle_override(self.override_pattern_check.isChecked(),
                              self.pattern_edit, None,
                              project_config.default_version_pattern if project_config else "_v{version}")
        self._toggle_override(self.override_ext_check.isChecked(),
                              self.extensions_edit, None, default_ext_str)
        for cb in self.date_format_checks.values():
            cb.setEnabled(self.override_date_format_check.isChecked())
        self.link_mode_combo.setEnabled(self.override_link_mode_check.isChecked())

    def _get_default_latest(self) -> str:
        if self._project_config and self._project_config.latest_path_template:
            return self._project_config.latest_path_template
        return "(set in Project Settings)"

    def _toggle_override(self, enabled: bool, line_edit: QLineEdit, browse_btn=None, default_text=""):
        line_edit.setEnabled(enabled)
        if browse_btn:
            browse_btn.setEnabled(enabled)
        if not enabled:
            line_edit.setPlaceholderText(f"Default: {default_text}")

    def _browse_source(self):
        d = QFileDialog.getExistingDirectory(self, "Select Source Directory")
        if d:
            self.source_dir_edit.setText(d)

    def _browse_target(self):
        d = QFileDialog.getExistingDirectory(self, "Select Latest Target Directory")
        if d:
            self.target_dir_edit.setText(d)

    def get_source(self) -> WatchedSource:
        exts_text = self.extensions_edit.text().strip()
        exts = exts_text.split() if exts_text else list(DEFAULT_FILE_EXTENSIONS)

        pc = self._project_config
        date_fmt = ",".join(
            fmt for fmt, cb in self.date_format_checks.items() if cb.isChecked()
        )
        return WatchedSource(
            name=self.name_edit.text().strip() or "Untitled",
            source_dir=self.source_dir_edit.text().strip(),
            version_pattern=self.pattern_edit.text().strip() or (pc.default_version_pattern if pc else "_v{version}"),
            file_extensions=exts,
            latest_target=self.target_dir_edit.text().strip(),
            link_mode=self.link_mode_combo.currentText(),
            date_format=date_fmt,
            override_version_pattern=self.override_pattern_check.isChecked(),
            override_date_format=self.override_date_format_check.isChecked(),
            override_file_extensions=self.override_ext_check.isChecked(),
            override_latest_target=self.override_latest_check.isChecked(),
            override_link_mode=self.override_link_mode_check.isChecked(),
        )


# ---------------------------------------------------------------------------
# Project Setup Dialog (simplified: name + dir + filters)
# ---------------------------------------------------------------------------

