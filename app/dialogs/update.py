"""update dialog module."""

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
from app.workers import UpdateCheckWorker, UpdateDownloadWorker


class UpdateDialog(QDialog):
    """Check for updates and optionally download + install."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Check for Updates")
        self.setMinimumSize(500, 350)
        self.resize(550, 420)

        self._release_info = None
        self._check_worker: Optional[UpdateCheckWorker] = None
        self._download_worker: Optional[UpdateDownloadWorker] = None
        self._downloaded_zip: Optional[str] = None
        self._temp_dir: Optional[str] = None

        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # Header
        self._header = QLabel(f"<b>{APP_NAME}</b> &mdash; v{APP_VERSION}")
        self._header.setStyleSheet("font-size: 14pt; padding: 4px;")
        layout.addWidget(self._header)

        # Status label
        self._status_label = QLabel("Checking for updates...")
        self._status_label.setWordWrap(True)
        layout.addWidget(self._status_label)

        # Release notes area (hidden initially)
        self._notes_group = QGroupBox("Release Notes")
        notes_layout = QVBoxLayout(self._notes_group)
        self._notes_text = QTextEdit()
        self._notes_text.setReadOnly(True)
        self._notes_text.setMaximumHeight(200)
        notes_layout.addWidget(self._notes_text)
        self._notes_group.setVisible(False)
        layout.addWidget(self._notes_group)

        # Progress bar (hidden initially)
        self._progress_bar = QProgressBar()
        self._progress_bar.setVisible(False)
        layout.addWidget(self._progress_bar)

        # Size label
        self._size_label = QLabel()
        self._size_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        self._size_label.setVisible(False)
        layout.addWidget(self._size_label)

        layout.addStretch()

        # Buttons
        self._btn_layout = QHBoxLayout()
        self._action_btn = QPushButton("Download && Install")
        self._action_btn.setVisible(False)
        self._action_btn.clicked.connect(self._on_action_clicked)
        self._btn_layout.addStretch()
        self._btn_layout.addWidget(self._action_btn)

        self._close_btn = QPushButton("Close")
        self._close_btn.clicked.connect(self.reject)
        self._btn_layout.addWidget(self._close_btn)
        layout.addLayout(self._btn_layout)

        # Start checking
        self._start_check()

    # -- Check phase --

    def _start_check(self):
        self._check_worker = UpdateCheckWorker(APP_VERSION, self)
        self._check_worker.finished.connect(self._on_check_finished)
        self._check_worker.error.connect(self._on_check_error)
        self._check_worker.start()

    def _on_check_finished(self, release_info):
        self._check_worker = None
        if release_info is None:
            self._status_label.setText(
                f"<span style='color:#4caf50;'>&#10004;</span> "
                f"You are running the latest version (v{APP_VERSION})."
            )
            self._close_btn.setText("OK")
            self._close_btn.setFocus()
            return

        self._release_info = release_info
        self._status_label.setText(
            f"<b>A new version is available: v{release_info.version}</b>"
        )

        # Show release notes
        if release_info.body:
            self._notes_text.setPlainText(release_info.body)
            self._notes_group.setVisible(True)

        # Show asset size
        size_mb = release_info.asset_size / (1024 * 1024)
        self._size_label.setText(f"Download size: {size_mb:.1f} MB")
        self._size_label.setVisible(True)

        # Show the action button
        from src.lvm.updater import is_frozen
        if is_frozen():
            self._action_btn.setText("Download && Install")
        else:
            self._action_btn.setText("View on GitHub")
        self._action_btn.setVisible(True)

    def _on_check_error(self, msg):
        self._check_worker = None
        self._status_label.setText(f"<span style='color:#e55;'>{msg}</span>")
        self._action_btn.setText("Retry")
        self._action_btn.setVisible(True)

    # -- Action button handler (context-sensitive) --

    def _on_action_clicked(self):
        btn_text = self._action_btn.text()

        if btn_text == "Retry":
            self._action_btn.setVisible(False)
            self._status_label.setText("Checking for updates...")
            self._start_check()

        elif btn_text == "View on GitHub":
            if self._release_info:
                from PySide6.QtGui import QDesktopServices
                QDesktopServices.openUrl(QUrl(self._release_info.html_url))

        elif btn_text.startswith("Download"):
            self._start_download()

        elif btn_text.startswith("Install"):
            self._start_install()

    # -- Download phase --

    def _start_download(self):
        if not self._release_info:
            return

        self._temp_dir = tempfile.mkdtemp(prefix="lvm_update_")
        self._action_btn.setEnabled(False)
        self._action_btn.setText("Downloading...")
        self._progress_bar.setValue(0)
        self._progress_bar.setVisible(True)

        self._download_worker = UpdateDownloadWorker(
            self._release_info, self._temp_dir, self
        )
        self._download_worker.progress.connect(self._on_download_progress)
        self._download_worker.finished.connect(self._on_download_finished)
        self._download_worker.error.connect(self._on_download_error)
        self._download_worker.start()

    def _on_download_progress(self, current, total):
        if total > 0:
            self._progress_bar.setMaximum(total)
            self._progress_bar.setValue(current)
            pct = int(current / total * 100)
            mb_done = current / (1024 * 1024)
            mb_total = total / (1024 * 1024)
            self._size_label.setText(
                f"Downloading: {mb_done:.1f} / {mb_total:.1f} MB ({pct}%)"
            )

    def _on_download_finished(self, zip_path):
        self._download_worker = None
        self._downloaded_zip = zip_path
        self._progress_bar.setVisible(False)
        self._status_label.setText(
            "Update downloaded. Click <b>Install &amp; Restart</b> to apply.\n\n"
            "The application will close, update, and relaunch automatically."
        )
        self._size_label.setVisible(False)
        self._action_btn.setText("Install && Restart")
        self._action_btn.setEnabled(True)

    def _on_download_error(self, msg):
        self._download_worker = None
        self._progress_bar.setVisible(False)
        self._status_label.setText(
            f"<span style='color:#e55;'>Download failed: {msg}</span>"
        )
        self._action_btn.setText("Download && Install")
        self._action_btn.setEnabled(True)

    # -- Install phase --

    def _start_install(self):
        if not self._downloaded_zip or not self._temp_dir:
            return

        from src.lvm.updater import (
            extract_update, create_updater_script, launch_updater, get_install_dir,
        )

        install_dir = get_install_dir()
        if not install_dir:
            QMessageBox.warning(
                self, "Update Error",
                "Cannot determine the installation directory."
            )
            return

        try:
            self._status_label.setText("Extracting update...")
            self._action_btn.setEnabled(False)
            QApplication.processEvents()

            extract_dir = Path(self._temp_dir) / "extracted"
            extracted = extract_update(Path(self._downloaded_zip), extract_dir)

            script = create_updater_script(
                extracted_dir=extracted,
                install_dir=install_dir,
                executable_path=Path(sys.executable),
                pid=os.getpid(),
            )

            launch_updater(script)

            # Quit the application so the updater can replace files
            QApplication.quit()

        except Exception as e:
            self._status_label.setText(
                f"<span style='color:#e55;'>Install failed: {e}</span>"
            )
            self._action_btn.setText("Download && Install")
            self._action_btn.setEnabled(True)

    # -- Cleanup on close --

    def reject(self):
        # Stop any running workers
        if self._download_worker and self._download_worker.isRunning():
            self._download_worker.terminate()
            self._download_worker.wait(2000)
        if self._check_worker and self._check_worker.isRunning():
            self._check_worker.terminate()
            self._check_worker.wait(2000)
        # Clean up temp dir if download wasn't installed
        if self._temp_dir and not self._downloaded_zip:
            import shutil
            try:
                shutil.rmtree(self._temp_dir, ignore_errors=True)
            except Exception:
                pass
        super().reject()


# ---------------------------------------------------------------------------
# About Dialog
# ---------------------------------------------------------------------------

