"""naming_rule dialog module."""

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


class NamingRuleDialog(QDialog):
    """Dialog for choosing how sources should be named during ingest.

    Shown on the first discovery add when naming_configured is False.
    Presents naming options derived from a representative DiscoveryResult.
    """

    def __init__(self, results: list, task_patterns: list = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Choose Source Naming Convention")
        self.setMinimumWidth(600)
        self._results = results
        self._task_patterns = task_patterns or []
        self._chosen_rule = ""

        layout = QVBoxLayout(self)

        # Header
        header = QLabel("How should discovered sources be named?")
        header.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 8px;")
        layout.addWidget(header)

        # Show example path from first result
        if results:
            result = results[0]
            path_label = QLabel(f"Example path: {result.path}")
            path_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
            path_label.setWordWrap(True)
            layout.addWidget(path_label)

            if result.sample_filename:
                file_label = QLabel(f"Example file: {result.sample_filename}")
                file_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
                layout.addWidget(file_label)

        # Radio buttons for naming options
        options_group = QGroupBox("Naming Options")
        options_layout = QVBoxLayout(options_group)

        self._radio_buttons = []

        if results:
            options = get_naming_options(results[0], self._task_patterns)
            for opt in options:
                radio = QPushButton()
                radio.setCheckable(True)
                radio.setStyleSheet(
                    "QPushButton { text-align: left; padding: 8px 12px; "
                    "border: 1px solid #2a2a2a; border-radius: 4px; }"
                    "QPushButton:checked { background-color: #336699; border-color: #5588bb; }"
                    "QPushButton:hover { background-color: #242424; }"
                )
                radio.setText(opt["label"])
                radio.clicked.connect(lambda checked, r=opt["rule"]: self._select_rule(r))
                options_layout.addWidget(radio)
                self._radio_buttons.append((radio, opt["rule"]))

        layout.addWidget(options_group)

        # Preview section
        preview_label = QLabel("Preview (names for all selected sources):")
        preview_label.setStyleSheet("font-weight: bold; margin-top: 8px;")
        layout.addWidget(preview_label)

        self.preview_list = QListWidget()
        self.preview_list.setMaximumHeight(120)
        self.preview_list.setStyleSheet("color: #88aaff;")
        layout.addWidget(self.preview_list)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.ok_btn = QPushButton("OK")
        self.ok_btn.setEnabled(False)
        self.ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        # Auto-select first option if available
        if self._radio_buttons:
            self._radio_buttons[0][0].setChecked(True)
            self._select_rule(self._radio_buttons[0][1])

    def _select_rule(self, rule: str):
        """Update selection and preview when a naming rule is chosen."""
        self._chosen_rule = rule
        self.ok_btn.setEnabled(True)

        # Uncheck other buttons
        for radio, r in self._radio_buttons:
            if r != rule:
                radio.setChecked(False)
            else:
                radio.setChecked(True)

        # Update preview for all selected results
        self.preview_list.clear()
        for result in self._results:
            name = compute_source_name(result, rule, self._task_patterns)
            self.preview_list.addItem(f"  {name}")

    def get_chosen_rule(self) -> str:
        """Return the selected naming rule string."""
        return self._chosen_rule


# ---------------------------------------------------------------------------
# Discovery Dialog (multi-select + add to project)
# ---------------------------------------------------------------------------

