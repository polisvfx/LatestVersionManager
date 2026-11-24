"""update dialog module."""

import html
import shutil

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
from app.workers import UpdateCheckWorker, UpdateDownloadWorker, UpdateExtractWorker


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
        self._extract_worker: Optional[UpdateExtractWorker] = None
        self._downloaded_zip: Optional[str] = None
        self._temp_dir: Optional[str] = None
        self._installed: bool = False

        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # Header
        self._header = QLabel(
            f"<b>{html.escape(APP_NAME)}</b> &mdash; v{html.escape(APP_VERSION)}"
        )
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
        w = UpdateCheckWorker(APP_VERSION, self)
        self._check_worker = w
        w.completed.connect(self._on_check_finished)
        w.error.connect(self._on_check_error)
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_check_finished(self, release_info):
        self._check_worker = None
        if release_info is None:
            self._status_label.setText(
                f"<span style='color:#4caf50;'>&#10004;</span> "
                f"You are running the latest version (v{html.escape(APP_VERSION)})."
            )
            self._close_btn.setText("OK")
            self._close_btn.setFocus()
            return

        self._release_info = release_info
        self._status_label.setText(
            f"<b>A new version is available: v{html.escape(str(release_info.version))}</b>"
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
        from lvm.updater import is_frozen
        if is_frozen():
            self._action_btn.setText("Download && Install")
        else:
            self._action_btn.setText("View on GitHub")
        self._action_btn.setVisible(True)

    def _on_check_error(self, msg):
        self._check_worker = None
        self._status_label.setText(f"<span style='color:#e55;'>{html.escape(str(msg))}</span>")
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
            if self._release_info and self._release_info.html_url:
                url = QUrl(self._release_info.html_url)
                if url.scheme().lower() == "https":
                    from PySide6.QtGui import QDesktopServices
                    QDesktopServices.openUrl(url)

        elif btn_text.startswith("Download"):
            self._start_download()

        elif btn_text.startswith("Install"):
            self._start_install()

    # -- Download phase --

    def _start_download(self):
        if not self._release_info:
            return

        if self._temp_dir:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
        self._temp_dir = tempfile.mkdtemp(prefix="lvm_update_")
        self._action_btn.setEnabled(False)
        self._action_btn.setText("Downloading...")
        self._progress_bar.setValue(0)
        self._progress_bar.setVisible(True)

        w = UpdateDownloadWorker(self._release_info, self._temp_dir, self)
        self._download_worker = w
        w.progress.connect(self._on_download_progress)
        w.completed.connect(self._on_download_finished)
        w.error.connect(self._on_download_error)
        w.finished.connect(w.deleteLater)
        w.start()

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
            "Update downloaded and verified. Click <b>Install &amp; Restart</b> to apply.\n\n"
            "The application will close, update, and relaunch automatically."
        )
        self._size_label.setVisible(False)
        self._action_btn.setText("Install && Restart")
        self._action_btn.setEnabled(True)

    def _on_download_error(self, msg):
        self._download_worker = None
        self._progress_bar.setVisible(False)
        self._status_label.setText(
            f"<span style='color:#e55;'>Download failed: {html.escape(str(msg))}</span>"
        )
        self._action_btn.setText("Download && Install")
        self._action_btn.setEnabled(True)

    # -- Install phase --

    def _start_install(self):
        if not self._downloaded_zip or not self._temp_dir:
            return

        from lvm.updater import get_install_dir

        install_dir = get_install_dir()
        if not install_dir:
            QMessageBox.warning(
                self, "Update Error",
                "Cannot determine the installation directory."
            )
            return

        self._status_label.setText("Extracting update...")
        self._action_btn.setEnabled(False)
        self._close_btn.setEnabled(False)
        self._progress_bar.setMaximum(0)
        self._progress_bar.setVisible(True)

        extract_dir = str(Path(self._temp_dir) / "extracted")
        w = UpdateExtractWorker(self._downloaded_zip, extract_dir, self)
        self._extract_worker = w
        w.completed.connect(self._on_extract_finished)
        w.error.connect(self._on_extract_error)
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_extract_finished(self, extracted_path: str):
        self._extract_worker = None
        self._progress_bar.setVisible(False)
        try:
            from lvm.updater import (
                create_updater_script, launch_updater, get_install_dir,
            )
            install_dir = get_install_dir()
            if not install_dir:
                raise RuntimeError("Cannot determine the installation directory.")

            script = create_updater_script(
                extracted_dir=Path(extracted_path),
                install_dir=install_dir,
                executable_path=Path(sys.executable),
                pid=os.getpid(),
                cleanup_dir=Path(self._temp_dir) if self._temp_dir else None,
            )
            self._installed = True
            launch_updater(script)

            # Close parent MainWindow cleanly so closeEvent saves state and
            # stops workers/watcher rather than abrupt QApplication.quit().
            parent_win = self.parent()
            self.accept()
            if parent_win is not None and hasattr(parent_win, "close"):
                parent_win.close()
            else:
                QApplication.quit()
        except Exception as e:
            self._on_extract_error(str(e))

    def _on_extract_error(self, msg: str):
        self._extract_worker = None
        self._progress_bar.setVisible(False)
        self._close_btn.setEnabled(True)
        self._status_label.setText(
            f"<span style='color:#e55;'>Install failed: {html.escape(str(msg))}</span>"
        )
        self._action_btn.setText("Download && Install")
        self._action_btn.setEnabled(True)

    # -- Cleanup on close --

    def _stop_workers(self):
        if self._download_worker is not None:
            self._download_worker.cancel()
            try:
                self._download_worker.disconnect()
            except RuntimeError:
                pass
            if self._download_worker.isRunning():
                self._download_worker.wait(3000)
            self._download_worker = None

        for attr in ("_check_worker", "_extract_worker"):
            w = getattr(self, attr, None)
            if w is not None:
                try:
                    w.disconnect()
                except RuntimeError:
                    pass
                if w.isRunning():
                    w.wait(3000)
                setattr(self, attr, None)

        # Clean up temp dir whenever the update was NOT handed off to the updater script
        if self._temp_dir and not self._installed:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None

    def reject(self):
        self._stop_workers()
        super().reject()

    def closeEvent(self, event):
        self._stop_workers()
        super().closeEvent(event)
