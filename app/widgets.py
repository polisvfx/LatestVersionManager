"""Custom Qt widgets used by the GUI."""

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


class VersionTreeWidget(QTreeWidget):
    """QTreeWidget subclass that accepts file/directory drops for manual version import."""

    files_dropped = Signal(list)  # list of Path objects (files and/or directories)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragEnterEvent(event)

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragMoveEvent(event)

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            paths = []
            for url in event.mimeData().urls():
                local = url.toLocalFile()
                if local:
                    paths.append(Path(local))
            if paths:
                self.files_dropped.emit(paths)
            event.acceptProposedAction()
        else:
            super().dropEvent(event)


# ---------------------------------------------------------------------------
# Dry-run preview dialog
# ---------------------------------------------------------------------------


class FlowLayout(QVBoxLayout):
    """Simple flow layout that wraps widgets into rows."""

    def __init__(self, parent=None, spacing=4):
        super().__init__(parent)
        self._rows: list[QHBoxLayout] = []
        self._spacing = spacing
        self.setSpacing(spacing)
        self.setContentsMargins(0, 0, 0, 0)
        self._add_row()

    def _add_row(self):
        row = QHBoxLayout()
        row.setSpacing(self._spacing)
        row.setContentsMargins(0, 0, 0, 0)
        row.addStretch()
        super().addLayout(row)
        self._rows.append(row)
        return row

    def addWidget(self, widget):
        row = self._rows[-1]
        row.insertWidget(row.count() - 1, widget)

    def removeWidget(self, widget):
        for row in self._rows:
            row.removeWidget(widget)
        widget.setParent(None)



class TagWidget(QFrame):
    """A single removable tag pill with an X button on the right."""

    removed = Signal(str)

    def __init__(self, text: str, parent=None):
        super().__init__(parent)
        self._text = text
        self.setFrameShape(QFrame.NoFrame)
        self.setStyleSheet(
            "TagWidget {"
            "  background: #1e2530; border: 1px solid #333333; border-radius: 10px;"
            "  padding: 1px 6px 1px 2px;"
            "}"
            "TagWidget:hover { background: #28333f; }"
        )

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 1, 2, 1)
        layout.setSpacing(2)

        label = QLabel(text)
        label.setStyleSheet("background: transparent; border: none; color: #e0e0e0; padding: 0;")
        layout.addWidget(label)

        close_btn = QPushButton("\u00d7")
        close_btn.setFixedSize(16, 16)
        close_btn.setCursor(Qt.PointingHandCursor)
        close_btn.setStyleSheet(
            "QPushButton {"
            "  background: transparent; border: none; color: #8c8c8c;"
            "  font-size: 13pt; font-weight: bold; padding: 0; margin: 0;"
            "}"
            "QPushButton:hover { color: #ff6b6b; }"
        )
        close_btn.clicked.connect(lambda: self.removed.emit(self._text))
        layout.addWidget(close_btn)

        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    @property
    def text(self):
        return self._text



class TagInputWidget(QWidget):
    """Container that displays tags as removable pills with a text input.

    Typing a comma converts the preceding text into a tag.
    """

    tagsChanged = Signal()  # Emitted when tags are added or removed

    def __init__(self, initial_tags: list[str] = None, placeholder: str = "", parent=None):
        super().__init__(parent)
        self._tags: list[str] = []

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(4)

        self._tag_container = QWidget()
        self._flow = FlowLayout(self._tag_container, spacing=4)
        outer.addWidget(self._tag_container)

        self._input = QLineEdit()
        self._input.setPlaceholderText(placeholder)
        self._input.textChanged.connect(self._on_text_changed)
        self._input.returnPressed.connect(self._commit_input)
        outer.addWidget(self._input)

        if initial_tags:
            for tag in initial_tags:
                self._add_tag(tag)
        self._update_container_visibility()

    def _on_text_changed(self, text: str):
        if "," in text:
            parts = text.split(",")
            for part in parts[:-1]:
                word = part.strip()
                if word:
                    self._add_tag(word)
            self._input.setText(parts[-1].lstrip())

    def _commit_input(self):
        word = self._input.text().strip().rstrip(",")
        if word:
            self._add_tag(word)
            self._input.clear()

    def _add_tag(self, text: str):
        text = text.strip()
        if not text or text in self._tags:
            return
        self._tags.append(text)
        tag_w = TagWidget(text)
        tag_w.removed.connect(self._remove_tag)
        self._flow.addWidget(tag_w)
        self._update_container_visibility()
        self.tagsChanged.emit()

    def _remove_tag(self, text: str):
        if text in self._tags:
            self._tags.remove(text)
        for i in range(self._tag_container.layout().count()):
            item = self._tag_container.layout().itemAt(i)
            if item and isinstance(item, QHBoxLayout):
                for j in range(item.count()):
                    sub = item.itemAt(j)
                    if sub and sub.widget() and isinstance(sub.widget(), TagWidget):
                        if sub.widget().text == text:
                            self._flow.removeWidget(sub.widget())
                            self._update_container_visibility()
                            self.tagsChanged.emit()
                            return

    def _update_container_visibility(self):
        self._tag_container.setVisible(bool(self._tags))

    def tags(self) -> list[str]:
        """Return the current list of tags, including any uncommitted input."""
        result = list(self._tags)
        pending = self._input.text().strip().rstrip(",")
        if pending and pending not in result:
            result.append(pending)
        return result


# ---------------------------------------------------------------------------
# Collapsible Section Widget
# ---------------------------------------------------------------------------


class CollapsibleSection(QWidget):
    """A collapsible section with a toggle header and animated content area."""

    def __init__(self, title: str, parent=None, collapsed: bool = False):
        super().__init__(parent)
        self._collapsed = collapsed

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Header button
        self._toggle_btn = QToolButton()
        self._toggle_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self._toggle_btn.setText(title)
        self._toggle_btn.setCheckable(True)
        self._toggle_btn.setChecked(not collapsed)
        self._toggle_btn.setArrowType(Qt.DownArrow if not collapsed else Qt.RightArrow)
        self._toggle_btn.setStyleSheet(
            "QToolButton { border: none; font-weight: bold; font-size: 12pt;"
            " padding: 6px 4px; color: #e0e0e0; }"
            "QToolButton:hover { color: #fff; background: #242424; }"
        )
        self._toggle_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._toggle_btn.clicked.connect(self._on_toggle)

        # Separator line under header
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setStyleSheet("color: #2a2a2a;")

        # Content area
        self._content = QWidget()
        self._content_layout = QFormLayout(self._content)
        self._content_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        self._content_layout.setContentsMargins(8, 4, 4, 8)
        self._content.setVisible(not collapsed)

        main_layout.addWidget(self._toggle_btn)
        main_layout.addWidget(separator)
        main_layout.addWidget(self._content)

    def content_layout(self) -> QFormLayout:
        """Return the QFormLayout inside the collapsible content area."""
        return self._content_layout

    def _on_toggle(self, checked: bool = None):
        if checked is None:
            checked = self._toggle_btn.isChecked()
        self._collapsed = not checked
        self._toggle_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        self._content.setVisible(checked)


# ---------------------------------------------------------------------------
# Project Settings Dialog
# ---------------------------------------------------------------------------


class SourceItemDelegate(QStyledItemDelegate):
    """Custom delegate that renders group tags as colored labels in the source list."""

    # Role to store (group_name, group_color) tuple
    GROUP_ROLE = Qt.UserRole + 1

    def paint(self, painter: QPainter, option, index):
        group_data = index.data(self.GROUP_ROLE)

        # If no group data, just use default painting
        if not group_data:
            super().paint(painter, option, index)
            return

        # Draw everything except the text via the base style
        opt = option.__class__(option)
        self.initStyleOption(opt, index)

        # Strip the bullet+group from the display text for base measurement
        full_text = opt.text or ""
        marker = "  \u2022"
        if marker in full_text:
            main_text = full_text[:full_text.index(marker)]
        else:
            main_text = full_text

        # Let base class draw selection background + icon, but with truncated text
        opt.text = main_text
        style = opt.widget.style() if opt.widget else QApplication.style()
        style.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, painter, opt.widget)

        # Now draw the group pill on top
        group_name, group_color_str = group_data
        group_color = QColor(group_color_str)

        painter.save()

        font = opt.font
        fm = QFontMetrics(font)
        text_rect = style.subElementRect(QStyle.SubElement.SE_ItemViewItemText, opt, opt.widget)

        tag_font = QFont(font)
        base_size = font.pointSize()
        if base_size <= 0:
            base_size = font.pixelSize()
            if base_size <= 0:
                base_size = 9
        tag_font.setPointSize(max(base_size - 1, 7))
        tag_fm = QFontMetrics(tag_font)
        tag_width = tag_fm.horizontalAdvance(f" {group_name} ") + 6
        tag_height = tag_fm.height() + 2
        tag_x = text_rect.right() - tag_width - 2
        tag_y = text_rect.center().y() - tag_height // 2

        # Draw rounded pill background
        pill_color = QColor(group_color)
        pill_color.setAlpha(60)
        painter.setBrush(QBrush(pill_color))
        painter.setPen(QPen(group_color, 1))
        painter.setRenderHint(QPainter.Antialiasing)
        from PySide6.QtCore import QRectF
        painter.drawRoundedRect(QRectF(tag_x, tag_y, tag_width, tag_height), 4, 4)

        # Draw tag text
        painter.setFont(tag_font)
        painter.setPen(QPen(group_color))
        painter.drawText(
            int(tag_x), int(tag_y), int(tag_width), int(tag_height),
            Qt.AlignCenter, group_name,
        )

        painter.restore()


# ---------------------------------------------------------------------------
# Manage Groups Dialog
# ---------------------------------------------------------------------------

# Default palette for auto-assigning colors to new groups
_GROUP_COLOR_PALETTE = [
    "#4a90d9", "#d94a4a", "#4ad94a", "#d9a64a", "#9b59b6",
    "#1abc9c", "#e67e22", "#e74c3c", "#3498db", "#2ecc71",
]


