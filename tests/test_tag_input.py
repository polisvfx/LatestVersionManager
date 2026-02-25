"""
Tests for TagInputWidget — verifies that the tag-based input produces
identical results to the old comma-separated QLineEdit approach.
"""

import sys
import unittest
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from PySide6.QtWidgets import QApplication

# A QApplication is required before any widget can be instantiated.
_app = QApplication.instance() or QApplication(sys.argv)

from app import TagInputWidget


def _old_parse(text: str) -> list[str]:
    """Replicate the old QLineEdit comma-split parsing from apply_to_config."""
    return [kw.strip() for kw in text.split(",") if kw.strip()]


class TestTagInputParity(unittest.TestCase):
    """Ensure TagInputWidget.tags() matches the old comma-split behaviour."""

    def _make_widget_from_list(self, tags: list[str]) -> TagInputWidget:
        """Create a TagInputWidget pre-populated with a list of tags."""
        return TagInputWidget(initial_tags=tags)

    def _make_widget_from_text(self, text: str) -> TagInputWidget:
        """Simulate typing comma-separated text into the widget."""
        w = TagInputWidget()
        # Feed the text character-by-character through the input field
        # to trigger _on_text_changed for each comma.
        for ch in text:
            w._input.setText(w._input.text() + ch)
        return w

    # -- Basic cases ----------------------------------------------------------

    def test_empty(self):
        old = _old_parse("")
        w = self._make_widget_from_list([])
        self.assertEqual(w.tags(), old)
        self.assertEqual(w.tags(), [])

    def test_single_word(self):
        old = _old_parse("comp")
        w = self._make_widget_from_list(["comp"])
        self.assertEqual(w.tags(), old)

    def test_multiple_words(self):
        text = "comp, grade, final"
        old = _old_parse(text)
        w = self._make_widget_from_list(["comp", "grade", "final"])
        self.assertEqual(w.tags(), old)

    def test_blacklist_defaults(self):
        text = "denoise, prerender, wip, temp"
        old = _old_parse(text)
        w = self._make_widget_from_list(["denoise", "prerender", "wip", "temp"])
        self.assertEqual(w.tags(), old)

    # -- Typed input (comma triggers tag creation) ----------------------------

    def test_typed_with_trailing_comma(self):
        """Typing 'comp,' should commit 'comp' as a tag."""
        text = "comp,"
        old = _old_parse(text)
        w = self._make_widget_from_text(text)
        self.assertEqual(w.tags(), old)

    def test_typed_multiple_commas(self):
        text = "comp, grade, final,"
        old = _old_parse(text)
        w = self._make_widget_from_text(text)
        self.assertEqual(w.tags(), old)

    def test_typed_pending_word(self):
        """Uncommitted text (no trailing comma) should still appear in tags()."""
        text = "comp, grade, final"
        old = _old_parse(text)
        w = self._make_widget_from_text(text)
        self.assertEqual(w.tags(), old)

    # -- Edge cases -----------------------------------------------------------

    def test_extra_commas(self):
        text = "comp,,, grade,,"
        old = _old_parse(text)
        w = self._make_widget_from_text(text)
        self.assertEqual(w.tags(), old)

    def test_whitespace_only(self):
        text = "  ,  ,  "
        old = _old_parse(text)
        w = self._make_widget_from_text(text)
        self.assertEqual(w.tags(), old)
        self.assertEqual(w.tags(), [])

    def test_leading_trailing_spaces(self):
        text = "  comp , grade ,  final  "
        old = _old_parse(text)
        w = self._make_widget_from_list(["comp", "grade", "final"])
        self.assertEqual(w.tags(), old)

    def test_duplicate_ignored(self):
        """Duplicates should be silently dropped, preserving first occurrence."""
        w = self._make_widget_from_list(["comp", "grade", "comp"])
        self.assertEqual(w.tags(), ["comp", "grade"])

    # -- Removal --------------------------------------------------------------

    def test_remove_tag(self):
        w = self._make_widget_from_list(["comp", "grade", "final"])
        w._remove_tag("grade")
        self.assertEqual(w.tags(), ["comp", "final"])

    def test_remove_nonexistent(self):
        w = self._make_widget_from_list(["comp", "grade"])
        w._remove_tag("nope")
        self.assertEqual(w.tags(), ["comp", "grade"])

    def test_remove_all(self):
        w = self._make_widget_from_list(["comp"])
        w._remove_tag("comp")
        self.assertEqual(w.tags(), [])


if __name__ == "__main__":
    unittest.main()
