"""latest_path dialog module."""

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


class LatestPathDialog(QDialog):
    """Dialog for defining the latest path template.

    Shown when the user tries to promote or add sources but no latest path
    template has been configured. Provides a live preview of the resolved path.
    """

    def __init__(self, config: ProjectConfig, source: WatchedSource = None,
                 discovery_results: list = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Set Latest Path")
        self.setMinimumWidth(600)
        self._config = config
        self._source = source  # optional: specific source for preview
        self._discovery_results = discovery_results or []  # DiscoveryResults for preview

        layout = QVBoxLayout(self)

        # Header
        header = QLabel("A latest path template is required before promotion.")
        header.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 4px;")
        layout.addWidget(header)

        desc = QLabel(
            "This defines where promoted files are placed. The template is applied\n"
            "to each source, so you can use tokens to create unique paths per source."
        )
        desc.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-bottom: 8px;")
        layout.addWidget(desc)

        # Template input
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        token_help = QLabel(
            "Tokens: {source_dir}, {source_title}, {source_name}, {source_basename},\n"
            "{source_fullname}, {source_filename}, {project_root}, {group}, {group_root}\n"
            "If {group} is empty, the token and its trailing divider are omitted.\n"
            "{group_root} resolves to the group's root directory (falls back to {project_root}).\n"
            "{source_title} is the source's in-project display name.\n"
            "Examples: ../online  |  {group_root}/latest/{source_name}  |  online/{group}/{source_name}"
        )
        token_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        form.addRow("", token_help)

        default_template = config.latest_path_template or "{source_dir}/../{source_name}_latest"
        self.template_edit = QLineEdit(default_template)
        self.template_edit.setPlaceholderText("{source_dir}/../{source_name}_latest")
        self.template_edit.textChanged.connect(self._update_preview)
        form.addRow("Latest Path Template:", self.template_edit)

        # Browse button
        browse_row = QHBoxLayout()
        browse_row.addStretch()
        browse_btn = QPushButton("Browse for Directory...")
        browse_btn.clicked.connect(self._browse)
        browse_row.addWidget(browse_btn)
        form.addRow("", browse_row)

        # File rename template
        rename_help = QLabel(
            "Controls the output filename (frame number and extension are preserved).\n"
            "Tokens: {source_title}, {source_name}, {source_basename}, {source_fullname}"
        )
        rename_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        form.addRow("", rename_help)

        default_rename = config.default_file_rename_template or "{source_basename}_latest"
        self.rename_edit = QLineEdit(default_rename)
        self.rename_edit.setPlaceholderText("{source_basename}_latest")
        self.rename_edit.textChanged.connect(self._update_preview)
        form.addRow("File Rename Template:", self.rename_edit)

        layout.addLayout(form)

        # Preview
        preview_header = QLabel("Resolved Preview:")
        preview_header.setStyleSheet("font-weight: bold; margin-top: 8px;")
        layout.addWidget(preview_header)

        self.preview_label = QLabel("")
        self.preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")
        self.preview_label.setWordWrap(True)
        self.preview_label.setMinimumHeight(60)
        layout.addWidget(self.preview_label)

        # Buttons
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.ok_btn = btn_box.button(QDialogButtonBox.Ok)
        self.ok_btn.setEnabled(False)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

        self._update_preview()

    def _browse(self):
        d = QFileDialog.getExistingDirectory(self, "Select Latest Target Directory")
        if d:
            # Try to make relative to project_dir for portability
            if self._config.project_dir:
                try:
                    rel = os.path.relpath(d, self._config.project_dir).replace("\\", "/")
                    self.template_edit.setText(rel)
                    return
                except ValueError:
                    pass
            self.template_edit.setText(d)

    def _update_preview(self):
        tpl = self.template_edit.text().strip()
        rename_tpl = self.rename_edit.text().strip() or "{source_basename}_latest"
        self.ok_btn.setEnabled(bool(tpl))
        if not tpl:
            self.preview_label.setText("(enter a template above)")
            self.preview_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
            return

        config = self._config
        previews = []

        # If a specific source was given, show it first; also include
        # discovery results so the preview is meaningful before sources are
        # actually added to the project.
        sources = []
        if self._source:
            sources.append(self._source)
        sources.extend(s for s in config.watched_sources if s != self._source)
        # Build lightweight preview sources from discovery results
        if self._discovery_results:
            naming_rule = config.default_naming_rule or "parent:0"
            for dr in self._discovery_results:
                name = compute_source_name(dr, naming_rule, config.task_tokens)
                preview_src = WatchedSource(
                    name=name,
                    source_dir=dr.path,
                    sample_filename=dr.sample_filename or "",
                )
                if preview_src not in sources:
                    sources.append(preview_src)
        sources = sources[:4]

        if sources:
            for source in sources:
                tokens = derive_source_tokens(source.sample_filename or source.name,
                                              config.task_tokens, source_title=source.name)
                resolved = tpl
                resolved = resolved.replace("{project_root}", config.effective_project_root or "<project_root>")
                resolved = resolved.replace("{group_root}", _resolve_group_root(config, source.group) or "<project_root>")
                resolved = resolved.replace("{source_title}", tokens["source_title"])
                resolved = resolved.replace("{source_name}", tokens["source_name"])
                resolved = resolved.replace("{source_basename}", tokens["source_basename"])
                resolved = resolved.replace("{source_fullname}", tokens["source_fullname"])
                resolved = resolved.replace("{source_filename}", tokens["source_filename"])
                resolved = resolved.replace("{source_dir}", source.source_dir)
                resolved = _expand_group_token(resolved, source.group)
                # Relative paths resolve from the source directory
                p = Path(resolved)
                if not p.is_absolute() and source.source_dir:
                    p = Path(source.source_dir) / p
                elif not p.is_absolute() and config.project_dir:
                    p = Path(config.project_dir) / p
                try:
                    dir_str = str(p.resolve())
                except OSError:
                    dir_str = str(p)
                # Build sample renamed file
                rename_resolved = rename_tpl
                rename_resolved = rename_resolved.replace("{source_title}", tokens["source_title"])
                rename_resolved = rename_resolved.replace("{source_name}", tokens["source_name"])
                rename_resolved = rename_resolved.replace("{source_basename}", tokens["source_basename"])
                rename_resolved = rename_resolved.replace("{source_fullname}", tokens["source_fullname"])
                rename_resolved = _expand_group_token(rename_resolved, source.group)
                sample_file = f"{rename_resolved}{_preview_sample_suffix(source)}"
                group_tag = f" [{source.group}]" if source.group else ""
                previews.append(f"{source.name}{group_tag}: {str(Path(dir_str) / sample_file)}")
            if len(config.watched_sources) > 4:
                previews.append(f"... and {len(config.watched_sources) - 4} more")
        else:
            resolved = tpl
            resolved = resolved.replace("{project_root}", config.effective_project_root or "<project_root>")
            resolved = resolved.replace("{group_root}", "<group_root>")
            resolved = resolved.replace("{source_title}", "<source_title>")
            resolved = resolved.replace("{source_name}", "<source_name>")
            resolved = resolved.replace("{source_basename}", "<source_basename>")
            resolved = resolved.replace("{source_fullname}", "<source_fullname>")
            resolved = resolved.replace("{source_filename}", "<source_filename>")
            resolved = resolved.replace("{source_dir}", "<source_dir>")
            resolved = resolved.replace("{group}", "<group>")
            p = Path(resolved)
            try:
                dir_str = str(p.resolve())
            except OSError:
                dir_str = str(p)
            rename_resolved = rename_tpl
            rename_resolved = rename_resolved.replace("{source_title}", "<source_title>")
            rename_resolved = rename_resolved.replace("{source_name}", "<source_name>")
            rename_resolved = rename_resolved.replace("{source_basename}", "<source_basename>")
            rename_resolved = rename_resolved.replace("{source_fullname}", "<source_fullname>")
            rename_resolved = rename_resolved.replace("{group}", "<group>")
            sample_file = f"{rename_resolved}.####.exr"
            previews.append(str(Path(dir_str) / sample_file))

        self.preview_label.setText("\n".join(previews))
        self.preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")

    def get_template(self) -> str:
        return self.template_edit.text().strip()

    def get_rename_template(self) -> str:
        return self.rename_edit.text().strip() or "{source_basename}_latest"


# ---------------------------------------------------------------------------
# Naming Rule Dialog (first-ingest naming convention choice)
# ---------------------------------------------------------------------------

