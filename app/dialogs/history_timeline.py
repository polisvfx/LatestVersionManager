"""History Timeline dialog — Gantt-style view of promotions across sources.

Reads `.latest_history.json` sidecars (no new persistence) and renders one row
per source. Each bar spans from a promote's `set_at` to the next promote (or
"now"), so the user can scrub their project's promote history visually.
"""

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

from PySide6.QtWidgets import (
    QGraphicsView, QGraphicsScene, QGraphicsRectItem, QGraphicsSimpleTextItem,
    QGraphicsLineItem, QDateEdit, QSlider,
)
from PySide6.QtCore import QDate, QRectF, QPointF, QSize


# ---------------------------------------------------------------------------
# Background worker — fans out the N sidecar reads across a thread pool.
# ---------------------------------------------------------------------------


class HistoryLoadWorker(QThread):
    """Loads every source's .latest_history.json off the UI thread."""
    finished = Signal(list)   # list of (source, history_entries_newest_first)
    error = Signal(str)

    def __init__(self, sources: list, parent=None):
        super().__init__(parent)
        self._sources = list(sources)

    def run(self):
        try:
            from src.lvm.history import HistoryManager

            def _load_one(source):
                if not source.latest_target:
                    return source, []
                path = os.path.join(source.latest_target, source.history_filename)
                if not os.path.exists(path):
                    return source, []
                try:
                    return source, HistoryManager(path).get_history()
                except Exception as e:
                    logger.warning("History load failed for %s: %s", source.name, e)
                    return source, []

            results = []
            worker_count = min(8, max(1, len(self._sources)))
            if worker_count > 1:
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = [executor.submit(_load_one, s) for s in self._sources]
                    for future in as_completed(futures):
                        results.append(future.result())
            else:
                for s in self._sources:
                    results.append(_load_one(s))
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))


# ---------------------------------------------------------------------------
# Graphics items
# ---------------------------------------------------------------------------


class _BarItem(QGraphicsRectItem):
    """A single promote bar. Holds the source name and emits selection via
    a parent callback when clicked."""

    def __init__(self, rect: QRectF, source_name: str, on_click):
        super().__init__(rect)
        self._source_name = source_name
        self._on_click = on_click
        self.setAcceptHoverEvents(False)  # tooltip is enough; no hover cost
        self.setCursor(Qt.PointingHandCursor)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton and self._on_click:
            self._on_click(self._source_name)
            event.accept()
            return
        super().mousePressEvent(event)


class _TimelineView(QGraphicsView):
    """QGraphicsView that asks for a logical zoom step on Ctrl+wheel.

    The view itself never calls ``scale()`` — that would stretch text and
    tick marks along with the bars. Instead it emits a multiplier and the
    dialog re-draws the scene with a new px-per-day, so only the bar widths
    and tick spacing change.
    """

    zoom_requested = Signal(float, QPointF)  # factor, anchor in view coords

    def __init__(self, scene, parent=None):
        super().__init__(scene, parent)
        self.setRenderHint(QPainter.Antialiasing, False)
        self.setDragMode(QGraphicsView.ScrollHandDrag)

    def wheelEvent(self, event):
        if event.modifiers() & Qt.ControlModifier:
            factor = 1.15 if event.angleDelta().y() > 0 else 1 / 1.15
            self.zoom_requested.emit(factor, QPointF(event.position()))
            event.accept()
            return
        super().wheelEvent(event)


# ---------------------------------------------------------------------------
# Dialog
# ---------------------------------------------------------------------------


# Module-level so tests can build expected bar counts without instantiating Qt.
ROW_HEIGHT = 22
ROW_PADDING = 4
LABEL_WIDTH = 180
HEADER_HEIGHT = 28
MIN_BAR_WIDTH_PX = 4

# Lightness factors (Qt.lighter/darker units) cycled across consecutive bars
# in a row. Includes both lighter and darker steps so the pattern reads as
# "stripes" rather than a fade direction.
_SHADE_FACTORS = (115, 90, 135, 75)


def _shade(color: QColor, factor: int) -> QColor:
    """Return a lighter or darker variant of *color*. ``factor`` uses the
    same convention as ``QColor.lighter`` — 100 == no change, >100 brighter,
    <100 darker."""
    if factor >= 100:
        return color.lighter(factor)
    return color.darker(int(round(10000 / factor)))


def build_timeline_model(
    loaded: list,
    name_filter: str = "",
    group_filter: str = "",
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    now: Optional[datetime] = None,
) -> dict:
    """Pure function: turn loaded history into rendering data.

    Kept separate from any Qt object so tests can assert on the bar set
    without spinning a QApplication.

    Returns dict with:
      rows: list of dicts {source, bars: [{start, end, entry}], ...}
      time_min, time_max: datetime bounds across all bars
    """
    now = now or datetime.now()
    name_filter = (name_filter or "").lower()

    rows = []
    time_min: Optional[datetime] = None
    time_max: Optional[datetime] = None

    for source, entries in loaded:
        if name_filter and name_filter not in source.name.lower():
            continue
        if group_filter and source.group != group_filter:
            continue

        # entries are newest-first per HistoryManager; sort oldest-first so each
        # bar runs from its set_at to the *next* (newer) entry's set_at.
        parsed = []
        for e in entries:
            try:
                t = datetime.fromisoformat(e.set_at)
            except (ValueError, TypeError):
                continue
            parsed.append((t, e))
        parsed.sort(key=lambda x: x[0])

        bars = []
        for i, (t, entry) in enumerate(parsed):
            end = parsed[i + 1][0] if i + 1 < len(parsed) else now
            # Apply date-range filter on the bar's start.
            if date_from and t < date_from:
                continue
            if date_to and t > date_to:
                continue
            bars.append({"start": t, "end": end, "entry": entry})
            if time_min is None or t < time_min:
                time_min = t
            if time_max is None or end > time_max:
                time_max = end

        rows.append({"source": source, "bars": bars})

    return {
        "rows": rows,
        "time_min": time_min,
        "time_max": time_max,
    }


class HistoryTimelineDialog(QDialog):
    """Gantt-style view of promotions across all sources, over time."""

    source_activated = Signal(str)   # source name; parent listens & selects

    def __init__(self, config: ProjectConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("History Timeline")
        self.resize(1100, 600)

        self._config = config
        self._loaded: list = []          # list of (source, [HistoryEntry])
        self._model: dict = {"rows": [], "time_min": None, "time_max": None}
        self._worker: Optional[HistoryLoadWorker] = None
        # px-per-day that exactly fits the data into the viewport at zoom 100%.
        # Recomputed on resize / data load / filter change. Slider value is a
        # multiplier of this — so 100% always means "fill the available width".
        self._fit_px_per_day = 16.0
        self._px_per_day = self._fit_px_per_day

        # Frozen-column items — repositioned every time the user scrolls
        # horizontally so source names stay pinned to the left edge.
        self._frozen_items: list = []

        self._build_ui()
        self._load_async()

    # --- UI -----------------------------------------------------------------

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # Filter bar
        filter_bar = QHBoxLayout()
        filter_bar.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("source name…")
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._rerender)
        filter_bar.addWidget(self.search_edit, 1)

        filter_bar.addWidget(QLabel("Group:"))
        self.group_combo = QComboBox()
        self.group_combo.addItem("All groups", "")
        for g in sorted(self._config.groups.keys()):
            self.group_combo.addItem(g, g)
        self.group_combo.currentIndexChanged.connect(self._rerender)
        filter_bar.addWidget(self.group_combo)

        filter_bar.addWidget(QLabel("From:"))
        self.date_from = QDateEdit()
        self.date_from.setCalendarPopup(True)
        self.date_from.setDisplayFormat("yyyy-MM-dd")
        self.date_from.setSpecialValueText(" ")
        self.date_from.setMinimumDate(QDate(1970, 1, 1))
        self.date_from.setDate(self.date_from.minimumDate())  # acts as "no filter"
        self.date_from.dateChanged.connect(self._rerender)
        filter_bar.addWidget(self.date_from)

        filter_bar.addWidget(QLabel("To:"))
        self.date_to = QDateEdit()
        self.date_to.setCalendarPopup(True)
        self.date_to.setDisplayFormat("yyyy-MM-dd")
        self.date_to.setSpecialValueText(" ")
        self.date_to.setMinimumDate(QDate(1970, 1, 1))
        self.date_to.setDate(self.date_to.minimumDate())
        self.date_to.dateChanged.connect(self._rerender)
        filter_bar.addWidget(self.date_to)

        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self._clear_filters)
        filter_bar.addWidget(clear_btn)

        layout.addLayout(filter_bar)

        # Zoom bar — 100% always means "fit the whole data span into the
        # viewport". Going below that would leave dead horizontal space, which
        # the user has explicitly asked us not to do.
        zoom_bar = QHBoxLayout()
        zoom_bar.addWidget(QLabel("Zoom:"))
        self.zoom_slider = QSlider(Qt.Horizontal)
        self.zoom_slider.setMinimum(100)
        self.zoom_slider.setMaximum(3200)
        self.zoom_slider.setValue(100)
        self.zoom_slider.valueChanged.connect(self._on_zoom_slider)
        zoom_bar.addWidget(self.zoom_slider, 1)
        self.zoom_label = QLabel("100%")
        self.zoom_label.setMinimumWidth(50)
        zoom_bar.addWidget(self.zoom_label)
        zoom_bar.addWidget(QLabel("  (Ctrl+wheel to zoom)"))
        layout.addLayout(zoom_bar)

        # Status / count
        self.status_label = QLabel("Loading history…")
        self.status_label.setStyleSheet("color: #888; padding: 2px 4px;")
        layout.addWidget(self.status_label)

        # Graphics scene
        self.scene = QGraphicsScene(self)
        self.view = _TimelineView(self.scene, self)
        self.view.setBackgroundBrush(QColor(18, 18, 18))
        self.view.zoom_requested.connect(self._on_wheel_zoom)
        self.view.horizontalScrollBar().valueChanged.connect(self._reposition_frozen)
        layout.addWidget(self.view, 1)

        # Close button
        btns = QDialogButtonBox(QDialogButtonBox.Close)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _clear_filters(self):
        self.search_edit.clear()
        self.group_combo.setCurrentIndex(0)
        # Reset dates back to the loaded data span rather than blanking them —
        # leaving the pickers empty after "Clear" hides the actual range users
        # just narrowed away from.
        self.date_from.blockSignals(True)
        self.date_to.blockSignals(True)
        self.date_from.setDate(self.date_from.minimumDate())
        self.date_to.setDate(self.date_to.minimumDate())
        self.date_from.blockSignals(False)
        self.date_to.blockSignals(False)
        self._prefill_date_range()
        self._rerender()

    def _recompute_fit_px_per_day(self) -> float:
        """How many pixels per day are needed to exactly fill the viewport?"""
        time_min = self._model.get("time_min")
        time_max = self._model.get("time_max")
        if not time_min or not time_max:
            return 16.0
        total_days = max(1.0 / 24.0,
                         (time_max - time_min).total_seconds() / 86400.0)
        # Width budget = viewport minus the frozen label column and a small
        # right-side breathing room (matches the +40 used in _draw).
        vp_w = max(200, self.view.viewport().width())
        budget = max(50.0, vp_w - LABEL_WIDTH - 40)
        return budget / total_days

    def _apply_zoom(self):
        """Recompute _px_per_day from the current slider + fit value."""
        mult = self.zoom_slider.value() / 100.0
        self._px_per_day = self._fit_px_per_day * mult

    def _on_zoom_slider(self, value: int):
        self.zoom_label.setText(f"{value}%")
        self._apply_zoom()
        self._draw()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        # When the dialog (and therefore the viewport) changes size, the
        # "fit" value moves under the slider — recompute and redraw so the
        # min-zoom level keeps occupying the full width.
        old_fit = self._fit_px_per_day
        self._fit_px_per_day = self._recompute_fit_px_per_day()
        if abs(self._fit_px_per_day - old_fit) > 0.01:
            self._apply_zoom()
            self._draw()

    def _on_wheel_zoom(self, factor: float, anchor_view: QPointF):
        # Capture the scene point under the cursor before redraw so we can
        # restore it after — gives a "zoom toward cursor" feel without using
        # a stretching view transform.
        anchor_scene_before = self.view.mapToScene(anchor_view.toPoint())
        cur = self.zoom_slider.value()
        new = max(self.zoom_slider.minimum(),
                  min(self.zoom_slider.maximum(), int(round(cur * factor))))
        if new == cur:
            return
        # Setting the slider triggers _on_zoom_slider → _draw.
        self.zoom_slider.setValue(new)
        # Re-centre so the cursor stays over the same date.
        anchor_scene_after = QPointF(
            LABEL_WIDTH + (anchor_scene_before.x() - LABEL_WIDTH) * (new / cur),
            anchor_scene_before.y(),
        )
        delta = anchor_scene_after - anchor_scene_before
        hbar = self.view.horizontalScrollBar()
        hbar.setValue(int(hbar.value() + delta.x()))

    # --- Loading ------------------------------------------------------------

    def _load_async(self):
        sources = list(self._config.watched_sources)
        if not sources:
            self.status_label.setText("No sources configured.")
            return
        self._worker = HistoryLoadWorker(sources, self)
        self._worker.finished.connect(self._on_loaded)
        self._worker.error.connect(self._on_load_error)
        self._worker.start()

    def _on_loaded(self, loaded: list):
        self._loaded = loaded
        self._prefill_date_range()
        self._rerender()

    def _prefill_date_range(self):
        """Snap the From/To pickers to the actual data span on first load.

        Only runs when the user hasn't touched the date fields yet (both still
        at the minimum sentinel) — re-opening the dialog with edits in flight
        would otherwise clobber them.
        """
        if (self.date_from.date() != self.date_from.minimumDate()
                or self.date_to.date() != self.date_to.minimumDate()):
            return

        earliest: Optional[datetime] = None
        latest: Optional[datetime] = None
        for _source, entries in self._loaded:
            for e in entries:
                try:
                    t = datetime.fromisoformat(e.set_at)
                except (ValueError, TypeError):
                    continue
                if earliest is None or t < earliest:
                    earliest = t
                if latest is None or t > latest:
                    latest = t

        if earliest is None or latest is None:
            return

        # Block signals so we set both fields without two redraws.
        self.date_from.blockSignals(True)
        self.date_to.blockSignals(True)
        self.date_from.setDate(QDate(earliest.year, earliest.month, earliest.day))
        self.date_to.setDate(QDate(latest.year, latest.month, latest.day))
        self.date_from.blockSignals(False)
        self.date_to.blockSignals(False)

    def _on_load_error(self, msg: str):
        self.status_label.setText(f"Failed to load history: {msg}")

    # --- Rendering ----------------------------------------------------------

    def _filter_args(self) -> dict:
        df = None
        if self.date_from.date() != self.date_from.minimumDate():
            d = self.date_from.date()
            df = datetime(d.year(), d.month(), d.day())
        dt = None
        if self.date_to.date() != self.date_to.minimumDate():
            d = self.date_to.date()
            # Include the whole "to" day.
            dt = datetime(d.year(), d.month(), d.day(), 23, 59, 59)
        return {
            "name_filter": self.search_edit.text().strip(),
            "group_filter": self.group_combo.currentData() or "",
            "date_from": df,
            "date_to": dt,
        }

    def _rerender(self):
        if not self._loaded:
            return
        self._model = build_timeline_model(self._loaded, **self._filter_args())
        # Filtering can shrink/grow the visible date range — recompute fit so
        # "100% = fit" stays true after every filter change.
        self._fit_px_per_day = self._recompute_fit_px_per_day()
        self._apply_zoom()
        self._draw()
        total_bars = sum(len(r["bars"]) for r in self._model["rows"])
        visible_sources = sum(1 for r in self._model["rows"] if r["bars"])
        self.status_label.setText(
            f"{visible_sources} source(s) with history, {total_bars} promote(s)."
        )

    def _draw(self):
        self.scene.clear()
        self._frozen_items = []
        rows = self._model["rows"]
        time_min = self._model["time_min"]
        time_max = self._model["time_max"]

        if not rows or time_min is None or time_max is None:
            self.scene.addText("No promotion history matches the current filters.",
                               QFont()).setDefaultTextColor(QColor("#888"))
            self.scene.setSceneRect(0, 0, 400, 80)
            return

        # Pixel span follows the user's zoom directly — no implicit stretch,
        # so zooming actually changes density on screen instead of being
        # cancelled out by an auto-fit minimum.
        total_seconds = max(1.0, (time_max - time_min).total_seconds())
        total_days = total_seconds / 86400.0
        px_per_day = self._px_per_day
        timeline_width = px_per_day * total_days
        scene_width = LABEL_WIDTH + timeline_width + 40
        scene_height = HEADER_HEIGHT + len(rows) * (ROW_HEIGHT + ROW_PADDING) + 20

        def t_to_x(t: datetime) -> float:
            return LABEL_WIDTH + ((t - time_min).total_seconds() / 86400.0) * px_per_day

        # Header — date ticks (one per visible day boundary, but cap at ~20 labels)
        self._draw_time_axis(time_min, time_max, t_to_x, scene_height)

        # Frozen background behind the source-name column. Painted first so
        # bars are drawn over it; repositioned on hscroll so it always covers
        # whatever bars slide under it.
        label_bg = QGraphicsRectItem(0, 0, LABEL_WIDTH, scene_height)
        label_bg.setBrush(QBrush(QColor(22, 22, 22)))
        label_bg.setPen(QPen(QColor("#2a2a2a"), 1))
        label_bg.setZValue(20)  # above bars, below tooltips
        self.scene.addItem(label_bg)
        self._frozen_items.append((label_bg, 0.0))  # base x = 0

        # Rows
        for i, row in enumerate(rows):
            y = HEADER_HEIGHT + i * (ROW_HEIGHT + ROW_PADDING)
            source = row["source"]

            # Row label (source name)
            label = QGraphicsSimpleTextItem(source.name)
            label.setBrush(QColor("#cccccc"))
            label.setPos(8, y + 3)
            label.setZValue(21)
            self.scene.addItem(label)
            self._frozen_items.append((label, 8.0))  # base x = 8

            # Faint baseline
            line = QGraphicsLineItem(LABEL_WIDTH, y + ROW_HEIGHT / 2,
                                     scene_width - 20, y + ROW_HEIGHT / 2)
            line.setPen(QPen(QColor("#2a2a2a"), 1, Qt.DotLine))
            self.scene.addItem(line)

            base_color = self._source_color(source)
            last_idx = len(row["bars"]) - 1
            for bi, bar in enumerate(row["bars"]):
                x1 = t_to_x(bar["start"])
                x2 = t_to_x(bar["end"])
                width = max(MIN_BAR_WIDTH_PX, x2 - x1)
                rect = QRectF(x1, y + 2, width, ROW_HEIGHT - 4)
                item = _BarItem(rect, source.name, self._on_bar_clicked)

                # Alternate four shades so adjacent promotes are distinguishable
                # at a glance. Modulated around the source's base group colour
                # so each row still reads as one block from across the room.
                shade = _SHADE_FACTORS[bi % len(_SHADE_FACTORS)]
                fill = _shade(base_color, shade)
                item.setBrush(QBrush(fill))

                entry = bar["entry"]
                is_current = bi == last_idx
                if getattr(entry, "pinned", False):
                    # Pinned promotes get a bright cyan border — matches the
                    # "deliberate" status colour used in the main source list.
                    item.setPen(QPen(QColor("#7abbe0"), 2))
                elif is_current:
                    # Currently-promoted bar gets a stronger border so users can
                    # spot "what's live right now" without reading version text.
                    item.setPen(QPen(fill.lighter(170), 2))
                else:
                    item.setPen(QPen(fill.darker(150), 1))

                item.setToolTip(self._tooltip_for(source, bar))
                self.scene.addItem(item)

                # Version label inside the bar, only if it fits. Auto-pick text
                # colour based on bar lightness so we stay readable on both
                # pale and dark group colours.
                self._maybe_label_bar(rect, entry.version, fill)

        # "Now" marker — a dim vertical line so users can see how stale each
        # row's most-recent promote is relative to today.
        now = datetime.now()
        if time_min <= now <= time_max:
            x = t_to_x(now)
            now_line = QGraphicsLineItem(x, HEADER_HEIGHT, x, scene_height - 4)
            now_line.setPen(QPen(QColor("#cc5544"), 1, Qt.DashLine))
            now_line.setZValue(10)
            self.scene.addItem(now_line)
            now_label = QGraphicsSimpleTextItem("now")
            now_label.setBrush(QColor("#cc5544"))
            now_label.setPos(x + 2, HEADER_HEIGHT - 14)
            now_label.setZValue(10)
            self.scene.addItem(now_label)

        self.scene.setSceneRect(0, 0, scene_width, scene_height)
        # Sync the frozen column to wherever the user is currently scrolled.
        self._reposition_frozen()

    def _reposition_frozen(self, *_):
        """Pin the source-name column to the left viewport edge.

        Called both when the user scrolls horizontally and right after a
        redraw. Each frozen item stores its "base x" (its position relative
        to the column); we slide them by the current horizontal scroll value
        so they appear stationary while the rest of the scene moves.
        """
        if not self._frozen_items:
            return
        offset = float(self.view.horizontalScrollBar().value())
        for item, base_x in self._frozen_items:
            pos = item.pos()
            item.setPos(base_x + offset, pos.y())

    def _maybe_label_bar(self, rect: QRectF, text: str, fill: QColor) -> None:
        """Draw the version string inside a bar if it'll fit."""
        if not text:
            return
        font = QFont()
        font.setPointSize(8)
        fm = QFontMetrics(font)
        # Need enough room for the text plus a couple px of padding on each side.
        if rect.width() < fm.horizontalAdvance(text) + 6:
            return
        item = QGraphicsSimpleTextItem(text)
        item.setFont(font)
        # Pick white-ish on dark fills, black-ish on light fills using HSV value.
        item.setBrush(QColor("#101010") if fill.value() > 170 else QColor("#f5f5f5"))
        text_rect = fm.boundingRect(text)
        item.setPos(rect.x() + 4,
                    rect.y() + (rect.height() - text_rect.height()) / 2)
        item.setZValue(2)
        self.scene.addItem(item)

    def _draw_time_axis(self, time_min, time_max, t_to_x, scene_height):
        # Choose tick spacing so we draw 6–20 labels.
        total_days = max(1.0, (time_max - time_min).total_seconds() / 86400.0)
        for candidate in (1, 2, 5, 7, 14, 30, 60, 90, 180, 365):
            if total_days / candidate <= 18:
                step_days = candidate
                break
        else:
            step_days = 365

        # Header background
        bg = QGraphicsRectItem(0, 0, t_to_x(time_max) + 40, HEADER_HEIGHT)
        bg.setBrush(QBrush(QColor("#222222")))
        bg.setPen(QPen(Qt.NoPen))
        self.scene.addItem(bg)

        # Start at the first whole-day boundary >= time_min
        cursor = time_min.replace(hour=0, minute=0, second=0, microsecond=0)
        if cursor < time_min:
            cursor = cursor + timedelta_days(1)
        while cursor <= time_max:
            x = t_to_x(cursor)
            tick = QGraphicsLineItem(x, 0, x, scene_height)
            tick.setPen(QPen(QColor("#2a2a2a"), 1))
            self.scene.addItem(tick)
            label = QGraphicsSimpleTextItem(cursor.strftime("%Y-%m-%d"))
            label.setBrush(QColor("#888"))
            label.setPos(x + 3, 6)
            self.scene.addItem(label)
            cursor = cursor + timedelta_days(step_days)

    def _source_color(self, source) -> QColor:
        """Colour bars by the source's group when available; otherwise grey."""
        if source.group and source.group in self._config.groups:
            hex_str = self._config.groups[source.group].get("color", _DEFAULT_GROUP_COLOR_HEX)
            return _group_qcolor(hex_str)
        return QColor("#4a6a8a")

    def _tooltip_for(self, source, bar) -> str:
        entry = bar["entry"]
        start = bar["start"].strftime("%Y-%m-%d %H:%M:%S")
        end = bar["end"].strftime("%Y-%m-%d %H:%M:%S")
        duration = bar["end"] - bar["start"]
        days = duration.total_seconds() / 86400.0
        if days >= 1:
            dur_str = f"{days:.1f} days"
        else:
            dur_str = f"{duration.total_seconds() / 3600.0:.1f} hours"
        frames = entry.frame_range or "—"
        return (f"<b>{source.name}</b><br>"
                f"Version: <b>{entry.version}</b><br>"
                f"Promoted: {start}<br>"
                f"Until: {end} ({dur_str})<br>"
                f"By: {entry.set_by or 'unknown'}<br>"
                f"Frames: {frames}")

    # --- Interactions -------------------------------------------------------

    def _on_bar_clicked(self, source_name: str):
        self.source_activated.emit(source_name)


# datetime.timedelta lives in stdlib datetime — _common imports only `datetime`
# the class. Importing here keeps the dialog file self-contained.
from datetime import timedelta as _timedelta


def timedelta_days(n: int):
    return _timedelta(days=n)
