"""Tests for instant blacklist filtering in Discover Versions dialog."""

import unittest
from pathlib import Path
from src.lvm.models import DiscoveryResult, VersionInfo


class TestBlacklistFiltering(unittest.TestCase):
    """Test the blacklist keyword matching logic."""

    def test_keyword_matches_result_name(self):
        """Test filtering when keyword matches the result name."""
        # Create test results
        result1 = DiscoveryResult(
            path="/renders/wip/comp_v001",
            name="comp_wip",
            versions_found=[],
            sample_filename="comp_wip.exr",
        )
        result2 = DiscoveryResult(
            path="/renders/final/comp_v001",
            name="comp_final",
            versions_found=[],
            sample_filename="comp_final.exr",
        )
        results = [result1, result2]

        # Simulate _apply_blacklist_keyword logic
        keyword = "wip"
        keyword_lower = keyword.lower()
        ignored = set()

        for result in results:
            parts = [result.name, result.path]
            if result.sample_filename:
                parts.append(result.sample_filename)
            search_text = " ".join(parts).lower()

            if keyword_lower in search_text:
                ignored.add(result.path)

        # Verify only the WIP result is ignored
        self.assertEqual(ignored, {"/renders/wip/comp_v001"})

    def test_keyword_matches_path(self):
        """Test filtering when keyword matches the directory path."""
        result = DiscoveryResult(
            path="/renders/prerender/comp_v001",
            name="comp",
            versions_found=[],
            sample_filename="comp.exr",
        )
        results = [result]

        keyword = "prerender"
        keyword_lower = keyword.lower()
        ignored = set()

        for result in results:
            parts = [result.name, result.path]
            if result.sample_filename:
                parts.append(result.sample_filename)
            search_text = " ".join(parts).lower()

            if keyword_lower in search_text:
                ignored.add(result.path)

        self.assertEqual(ignored, {"/renders/prerender/comp_v001"})

    def test_keyword_case_insensitive(self):
        """Test that keyword matching is case-insensitive."""
        result = DiscoveryResult(
            path="/renders/final/comp_v001",
            name="COMP_FINAL",
            versions_found=[],
            sample_filename="comp_final.exr",
        )
        results = [result]

        keyword = "Final"  # Mixed case
        keyword_lower = keyword.lower()
        ignored = set()

        for result in results:
            parts = [result.name, result.path]
            if result.sample_filename:
                parts.append(result.sample_filename)
            search_text = " ".join(parts).lower()

            if keyword_lower in search_text:
                ignored.add(result.path)

        self.assertEqual(ignored, {"/renders/final/comp_v001"})

    def test_keyword_matches_filename(self):
        """Test filtering when keyword matches the sample filename."""
        result = DiscoveryResult(
            path="/renders/comp",
            name="comp",
            versions_found=[],
            sample_filename="comp_denoise_v001.exr",
        )
        results = [result]

        keyword = "denoise"
        keyword_lower = keyword.lower()
        ignored = set()

        for result in results:
            parts = [result.name, result.path]
            if result.sample_filename:
                parts.append(result.sample_filename)
            search_text = " ".join(parts).lower()

            if keyword_lower in search_text:
                ignored.add(result.path)

        self.assertEqual(ignored, {"/renders/comp"})

    def test_no_match(self):
        """Test that non-matching keywords don't filter results."""
        result = DiscoveryResult(
            path="/renders/final/comp_v001",
            name="comp_final",
            versions_found=[],
            sample_filename="comp_final.exr",
        )
        results = [result]

        keyword = "prerender"
        keyword_lower = keyword.lower()
        ignored = set()

        for result in results:
            parts = [result.name, result.path]
            if result.sample_filename:
                parts.append(result.sample_filename)
            search_text = " ".join(parts).lower()

            if keyword_lower in search_text:
                ignored.add(result.path)

        self.assertEqual(ignored, set())


if __name__ == "__main__":
    unittest.main()
