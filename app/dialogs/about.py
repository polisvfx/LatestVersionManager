"""about dialog module."""

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


class AboutDialog(QDialog):
    """About dialog showing application info and credits."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"About {APP_NAME}")
        self.setFixedSize(400, 340)

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(24, 24, 24, 24)

        # Logo
        if LOGO_PATH.exists():
            logo_pixmap = QPixmap(64, 64)
            logo_pixmap.fill(Qt.transparent)
            renderer = QSvgRenderer(str(LOGO_PATH))
            painter = QPainter(logo_pixmap)
            renderer.render(painter)
            painter.end()
            logo_label = QLabel()
            logo_label.setPixmap(logo_pixmap)
            logo_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(logo_label)

        # App name and version
        title = QLabel(f"<h2>{APP_NAME}</h2>")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        version = QLabel(f"Version {APP_VERSION}")
        version.setAlignment(Qt.AlignCenter)
        version.setStyleSheet("color: #8c8c8c;")
        layout.addWidget(version)

        # Separator
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color: #2a2a2a;")
        layout.addWidget(line)

        # Author info
        info = QLabel(
            '<p style="text-align:center;">'
            '<b>Author:</b> Maris Polis<br>'
            '<a href="https://marispolis.com" style="color:#6699cc;">marispolis.com</a><br>'
            '<a href="mailto:mp@marispolis.com" style="color:#6699cc;">mp@marispolis.com</a><br><br>'
            '<a href="https://www.linkedin.com/in/maris-polis-2bb404191/" style="color:#6699cc;">LinkedIn</a>'
            '&nbsp;&nbsp;|&nbsp;&nbsp;'
            '<a href="https://github.com/polisvfx/LatestVersionManager" style="color:#6699cc;">GitHub</a>'
            '</p>'
        )
        info.setOpenExternalLinks(True)
        info.setAlignment(Qt.AlignCenter)
        layout.addWidget(info)

        layout.addStretch()

        # Close button
        btn_box = QDialogButtonBox(QDialogButtonBox.Close)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)


# ---------------------------------------------------------------------------
# Batch Promote Review Dialog
# ---------------------------------------------------------------------------

