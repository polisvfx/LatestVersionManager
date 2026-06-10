"""bulk edit dialog module."""

from PySide6.QtWidgets import QButtonGroup, QRadioButton

from app._common import *  # noqa: F401,F403


class BulkEditSourcesDialog(QDialog):
    """Apply selected field changes to several sources at once.

    Each row is gated by a "Change" checkbox — only checked rows are
    returned from :meth:`get_edits`; everything else keeps each source's
    current value. Override-able fields offer "Set" (override with a
    value) or "Use project default" (clear the override flag).
    """

    def __init__(self, sources: list, project_config: ProjectConfig = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit {len(sources)} Sources")
        self.setMinimumWidth(620)
        self._sources = sources
        self._config = project_config
        # field -> (change_checkbox, set_radio, inherit_radio, value_widget)
        self._rows: dict = {}

        pc = project_config
        first = sources[0] if sources else None

        layout = QVBoxLayout(self)
        names = ", ".join(s.name for s in sources[:5])
        if len(sources) > 5:
            names += f", … (+{len(sources) - 5} more)"
        hint = QLabel(
            f"Editing: {names}\n"
            f"Only fields with “Change” checked are applied — "
            f"everything else keeps each source's current value."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addWidget(hint)

        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        layout.addLayout(form)

        # --- Group (plain set, no override flag) ---
        self.group_change = QCheckBox("Change")
        self.group_combo = QComboBox()
        self.group_combo.setEditable(True)
        self.group_combo.addItem("")  # blank = remove from group
        for g in sorted(pc.groups.keys()) if pc else []:
            self.group_combo.addItem(g)
        if first:
            self.group_combo.setCurrentText(first.group)
        self.group_combo.setToolTip(
            "Pick an existing group, type a new name to create one, "
            "or leave blank to remove the sources from their groups"
        )
        self.group_combo.setEnabled(False)
        self.group_change.toggled.connect(self.group_combo.setEnabled)
        group_row = QHBoxLayout()
        group_row.addWidget(self.group_change)
        group_row.addWidget(self.group_combo, 1)
        form.addRow("Group:", group_row)

        # --- Link mode ---
        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItems(["copy", "hardlink", "symlink"])
        if first:
            self.link_mode_combo.setCurrentText(first.link_mode)
        self._add_override_row(
            form, "Link Mode:", "link_mode", self.link_mode_combo,
            pc.default_link_mode if pc else "copy",
        )

        # --- File extensions ---
        self.extensions_edit = QLineEdit()
        if first:
            self.extensions_edit.setText(" ".join(first.file_extensions))
        self._add_override_row(
            form, "File Extensions:", "file_extensions", self.extensions_edit,
            " ".join(pc.default_file_extensions) if pc else " ".join(DEFAULT_FILE_EXTENSIONS),
        )

        # --- Date format (multi-select checkboxes, same as SourceDialog) ---
        self.date_format_checks: dict[str, QCheckBox] = {}
        date_widget = QWidget()
        date_row = QHBoxLayout(date_widget)
        date_row.setContentsMargins(0, 0, 0, 0)
        for fmt in ("DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD"):
            cb = QCheckBox(fmt)
            self.date_format_checks[fmt] = cb
            date_row.addWidget(cb)
        date_row.addStretch(1)
        if first and first.date_format:
            from src.lvm.task_tokens import parse_date_formats as _parse_dfmts
            for fmt in _parse_dfmts(first.date_format):
                if fmt in self.date_format_checks:
                    self.date_format_checks[fmt].setChecked(True)
        self._add_override_row(
            form, "Date Format:", "date_format", date_widget,
            (pc.default_date_format or "(none)") if pc else "(none)",
        )

        # --- Version pattern ---
        self.pattern_edit = QLineEdit()
        if first:
            self.pattern_edit.setText(first.version_pattern)
        self._add_override_row(
            form, "Version Pattern:", "version_pattern", self.pattern_edit,
            pc.default_version_pattern if pc else "_v{version}",
        )

        # --- File rename template ---
        self.rename_edit = QLineEdit()
        if first:
            self.rename_edit.setText(first.file_rename_template)
        self.rename_edit.setPlaceholderText("e.g. {source_name}_latest")
        self._add_override_row(
            form, "File Rename:", "file_rename_template", self.rename_edit,
            (pc.default_file_rename_template or "(none)") if pc else "(none)",
        )

        # --- Block incomplete sequences ---
        self.block_incomplete_check = QCheckBox("Block promotion when sequence has frame gaps")
        if first:
            self.block_incomplete_check.setChecked(first.block_incomplete_sequences)
        self._add_override_row(
            form, "Frame Gaps:", "block_incomplete_sequences", self.block_incomplete_check,
            "block" if (pc and pc.block_incomplete_sequences) else "allow",
        )

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _add_override_row(self, form: QFormLayout, label: str, field: str,
                          value_widget, default_text: str):
        """One gated row: [Change] (• Set: <widget>  ○ Use project default)."""
        change_cb = QCheckBox("Change")
        set_radio = QRadioButton("Set:")
        inherit_radio = QRadioButton("Use project default")
        inherit_radio.setToolTip(f"Project default: {default_text}")
        set_radio.setChecked(True)
        btn_group = QButtonGroup(self)
        btn_group.addButton(set_radio)
        btn_group.addButton(inherit_radio)

        row = QHBoxLayout()
        row.addWidget(change_cb)
        row.addWidget(set_radio)
        row.addWidget(value_widget, 1)
        row.addWidget(inherit_radio)

        def _sync(*_args):
            on = change_cb.isChecked()
            set_radio.setEnabled(on)
            inherit_radio.setEnabled(on)
            value_widget.setEnabled(on and set_radio.isChecked())

        change_cb.toggled.connect(_sync)
        set_radio.toggled.connect(_sync)
        _sync()

        form.addRow(label, row)
        self._rows[field] = (change_cb, set_radio, inherit_radio, value_widget)

    def _widget_value(self, field: str, widget):
        if field == "link_mode":
            return widget.currentText()
        if field == "file_extensions":
            text = widget.text().strip()
            return text.split() if text else list(DEFAULT_FILE_EXTENSIONS)
        if field == "date_format":
            return ",".join(
                fmt for fmt, cb in self.date_format_checks.items() if cb.isChecked()
            )
        if field == "block_incomplete_sequences":
            return widget.isChecked()
        return widget.text().strip()

    def get_edits(self) -> dict:
        """Edits dict for ``lvm.config.apply_bulk_edits`` — checked rows only."""
        edits: dict = {}
        if self.group_change.isChecked():
            edits["group"] = {
                "action": "set",
                "value": self.group_combo.currentText().strip(),
            }
        for field, (change_cb, set_radio, _inherit_radio, widget) in self._rows.items():
            if not change_cb.isChecked():
                continue
            if set_radio.isChecked():
                edits[field] = {
                    "action": "override",
                    "value": self._widget_value(field, widget),
                }
            else:
                edits[field] = {"action": "inherit"}
        return edits
