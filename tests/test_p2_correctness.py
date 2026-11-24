"""Regression tests for Phase 2 core correctness, performance, CLI, and NLE fixes."""

import json
import os
import shutil
import struct
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from lvm.auto_promote import (
    PROMOTE,
    SKIP,
    WAIT_RENDER,
    SettleState,
    evaluate_auto_promote,
)
from lvm.config import _migrate_shared_history_filenames, apply_project_defaults
from lvm.conflicts import detect_target_conflicts
from lvm.discovery import _is_plausible_date, discover
from lvm.history import HistoryManager, has_newer_versions_since
from lvm.models import HistoryEntry, ProjectConfig, VersionInfo, WatchedSource, version_strings_match
from lvm.nle_bridge import collect_premiere_renames
from lvm.promoter import Promoter
from lvm.render_check import COMPLETE, INCOMPLETE, UNKNOWN, container_structure_status
from lvm.scan_cache import load_cache, save_cache
from lvm.scanner import VersionScanner, _detect_frame_range_for_group, _group_files_by_sequence
from lvm.task_tokens import (
    compose_nle_display_stem,
    compute_source_name,
    strip_frame_and_ext,
    strip_task_tokens,
)
from lvm.timecode import _decode_smpte_timecode, _read_dpx_timecode


class _TmpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_p2_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestTaskTokensAndModels(unittest.TestCase):
    def test_strip_task_tokens_coalesces_overlapping_spans(self):
        # Overlapping patterns ["comp", "comp_%%%"] must not corrupt surrounding text
        res = strip_task_tokens("hero_comp_mpo_final", ["comp", "comp_%%%"])
        self.assertEqual(res, "hero_final")

    def test_strip_frame_and_ext_preserves_dot_directory_names(self):
        self.assertEqual(strip_frame_and_ext("ep01.sh010_comp_v002"), "ep01.sh010_comp_v002")
        self.assertEqual(strip_frame_and_ext("ep01.sh010_comp_v002.1001.exr"), "ep01.sh010_comp_v002")
        self.assertEqual(strip_frame_and_ext("ep01.sh010_comp_v002.mov"), "ep01.sh010_comp_v002")

    def test_compose_nle_display_stem_single_pass(self):
        tokens = {
            "source_title": "{source_name}_custom",
            "source_name": "shot010",
            "source_basename": "shot010",
            "source_fullname": "shot010_v002",
            "source_filename": "shot010_v002.exr",
        }
        stem = compose_nle_display_stem(
            tokens, include_version=False, custom_enabled=True, custom_template="{source_title}",
        )
        self.assertEqual(stem, "{source_name}_custom")

    def test_compute_source_name_negative_parent_clamped(self):
        class _Dummy:
            path = "/a/b/shot010"
            name = "shot010"
            sample_filename = ""
        self.assertEqual(compute_source_name(_Dummy(), "parent:-3"), "shot010")

    def test_version_strings_match_dates(self):
        self.assertTrue(version_strings_match("2026-03-01", "2026-03-01", 0))
        self.assertFalse(version_strings_match("2026-03-01", "2026-03-02", 0))
        self.assertTrue(version_strings_match("2026-03-01 v002", "2026-03-01 v02", 2))
        self.assertFalse(version_strings_match("2026-03-01 v002", "2026-03-02 v002", 2))

    def test_project_config_null_fields_and_empty_rename_template(self):
        cfg = ProjectConfig.from_dict({
            "project_name": None,
            "default_file_rename_template": "",
            "default_file_extensions": None,
            "watched_sources": None,
        })
        self.assertEqual(cfg.project_name, "Untitled")
        self.assertEqual(cfg.default_file_rename_template, "")
        self.assertTrue(len(cfg.default_file_extensions) > 0)
        self.assertEqual(cfg.watched_sources, [])
        self.assertIn("default_file_rename_template", cfg.to_dict())
        self.assertEqual(cfg.to_dict()["default_file_rename_template"], "")


class TestScannerAndDiscovery(_TmpTest):
    def test_huge_frame_gap_is_linear_not_oom(self):
        # Frame numbers 1 and 99_999_999 would OOM set(range(...))
        files = [Path("shot.0001.exr"), Path("shot.99999999.exr")]
        range_str, count = _detect_frame_range_for_group(files)
        self.assertEqual(count, 2)
        self.assertIn("gaps detected", range_str)
        self.assertIn("2-99999998", range_str)

    def test_group_files_case_insensitive_extension(self):
        files = [Path("shot.1001.EXR"), Path("shot.1002.exr")]
        groups = _group_files_by_sequence(files)
        self.assertEqual(len(groups), 1)

    def test_invalid_version_pattern_regex_does_not_crash(self):
        src = WatchedSource(name="s", source_dir=str(self.tmp), version_pattern="[unclosed")
        scanner = VersionScanner(src)
        self.assertEqual(scanner.scan(), [])

    def test_discovery_date_validation_accepts_19th_ddmmyyyy(self):
        # 19032026 = 19 March 2026 (DDMMYYYY) must be recognized as plausible
        self.assertTrue(_is_plausible_date("19032026"))
        self.assertTrue(_is_plausible_date("20260319"))
        self.assertFalse(_is_plausible_date("99999999"))

    def test_discovery_skips_empty_version_dirs_and_dedups_flat_by_date_ver(self):
        empty_v = self.tmp / "empty_shot" / "shot_v001"
        empty_v.mkdir(parents=True)
        flat = self.tmp / "dated_flat"
        flat.mkdir()
        (flat / "260301_shot_v001.mov").write_bytes(b"a")
        (flat / "260302_shot_v001.mov").write_bytes(b"b")

        results = discover(str(self.tmp), max_depth=3)
        paths = [Path(r.path).name for r in results]
        self.assertNotIn("empty_shot", paths)
        self.assertIn("dated_flat", paths)
        dated_res = next(r for r in results if Path(r.path).name == "dated_flat")
        self.assertEqual(len(dated_res.versions_found), 2)


class TestAutoPromoteCorrectness(_TmpTest):
    def test_one_frame_render_of_multi_frame_sequence_waits(self):
        src_dir = self.tmp / "src"
        v1 = src_dir / "shot_v001"
        v2 = src_dir / "shot_v002"
        v1.mkdir(parents=True)
        v2.mkdir(parents=True)
        for f in (1001, 1002, 1003):
            (v1 / f"shot_v001.{f}.exr").write_bytes(b"x")
        (v2 / "shot_v002.1001.exr").write_bytes(b"y")

        target = self.tmp / "latest"
        src = WatchedSource(
            name="shot", source_dir=str(src_dir), latest_target=str(target),
            file_extensions=[".exr"], sample_filename="shot_v001.1001.exr",
        )
        scanner = VersionScanner(src)
        promoter = Promoter(src)
        v1_info = scanner.scan()[0]
        promoter.promote(v1_info, user="test")

        # Simulate settled v2 with only 1 frame (frame_range is None)
        state = SettleState("v002", 1, 1, None, first_seen=0.0)
        dec = evaluate_auto_promote(promoter, scanner, previous=state, now=10.0)
        self.assertEqual(dec.action, WAIT_RENDER)

    def test_max_wait_seconds_enforced_on_unfinished_container(self):
        src_dir = self.tmp / "src"
        src_dir.mkdir()
        v1 = src_dir / "shot_v001.mov"
        v2 = src_dir / "shot_v002.mov"
        # Minimal valid QT movie with ftyp + moov for v1
        ftyp = struct.pack(">I4s", 16, b"ftyp") + b"qt  \x00\x00\x00\x00"
        moov = struct.pack(">I4s", 16, b"moov") + b"\x00" * 8
        v1.write_bytes(ftyp + moov)
        # Unfinished v2: size-0 mdat with no moov
        v2.write_bytes(ftyp + struct.pack(">I4s", 0, b"mdat") + b"\x00" * 16)

        target = self.tmp / "latest"
        src = WatchedSource(
            name="shot", source_dir=str(src_dir), latest_target=str(target),
            file_extensions=[".mov"], sample_filename="shot_v001.mov",
        )
        scanner = VersionScanner(src)
        promoter = Promoter(src)
        promoter.promote(scanner.scan()[0], user="test")

        # Previous settle state started long ago -> exceeds max_wait_seconds
        state = SettleState("v002", 1, v2.stat().st_size, None, first_seen=0.0)
        dec = evaluate_auto_promote(
            promoter, scanner, previous=state, now=5000.0, max_wait_seconds=60.0,
        )
        self.assertEqual(dec.action, SKIP)
        self.assertIn("stopped waiting", dec.reason)


class TestTimecodeAndRenderCheck(_TmpTest):
    def test_dpx_timecode_masks_flags_and_drop_frame(self):
        # 01:02:03;04 with upper flag bits set in tens nibbles (0xC0 / 0x80) and drop-frame bit 6
        # h_t=0 + flag bits (0b1100 -> 0xC), h_u=1 -> 0xC1
        # m_t=0 + flag bit  (0b1000 -> 0x8), m_u=2 -> 0x82
        # s_t=0 + flag bit  (0b1000 -> 0x8), s_u=3 -> 0x83
        # f_t=0 + drop-frame bit 6 (0x40),   f_u=4 -> 0x44
        packed = 0xC1828344
        dpx = self.tmp / "frame.dpx"
        buf = bytearray(2048)
        buf[0:4] = b"SDPX"
        struct.pack_into(">I", buf, 1920, packed)
        dpx.write_bytes(bytes(buf))
        self.assertEqual(_read_dpx_timecode(dpx), "01:02:03;04")

    def test_invalid_bcd_nibble_rejected(self):
        # Units nibble 0xA is invalid BCD
        data = struct.pack("<II", 0x0102030A, 0)
        self.assertIsNone(_decode_smpte_timecode(data))

    def test_truncated_qt_and_avi_headers_detected_while_dummy_unknown(self):
        trunc_mov = self.tmp / "trunc.mov"
        trunc_mov.write_bytes(b"\x00\x00\x00\x14ft")
        self.assertEqual(container_structure_status(trunc_mov)[0], INCOMPLETE)

        dummy_mov = self.tmp / "dummy.mov"
        dummy_mov.write_bytes(b"v1")
        self.assertEqual(container_structure_status(dummy_mov)[0], UNKNOWN)

        trunc_avi = self.tmp / "trunc.avi"
        trunc_avi.write_bytes(b"RIFF\x00\x00")
        self.assertEqual(container_structure_status(trunc_avi)[0], INCOMPLETE)


class TestHistoryConflictsCacheConfigPromoter(_TmpTest):
    def test_history_defensive_copy_and_date_newer_versions(self):
        hm = HistoryManager(str(self.tmp / ".latest_history.json"))
        now = datetime.now()
        old_ts = (now - timedelta(seconds=30)).isoformat(timespec="seconds")
        e = HistoryEntry(version="2026-03-01", source=str(self.tmp / "d1"), set_by="u", set_at=old_ts)
        hm.record_promotion(e)
        h = hm.get_history()
        h.clear()  # mutating returned list must not corrupt internal cache
        self.assertEqual(len(hm.get_history()), 1)

        d2 = self.tmp / "d2"
        d2.mkdir()
        v1 = VersionInfo("2026-03-01", 0, str(self.tmp / "d1"), date_string="260301", date_sortable=20260301)
        v2 = VersionInfo("2026-03-02", 0, str(d2), date_string="260302", date_sortable=20260302)
        self.assertTrue(has_newer_versions_since(e, [v1, v2]))
        # Works even if v1 was archived/deleted from disk
        self.assertTrue(has_newer_versions_since(e, [v2]))

    def test_conflicts_container_with_trailing_digits_not_sequence(self):
        target = self.tmp / "online"
        target.mkdir()
        s1 = WatchedSource(
            name="s1", source_dir=str(self.tmp / "a"), latest_target=str(target),
            sample_filename="shot_2026.mov", file_rename_template="",
            history_filename=".latest_history_s1.json",
        )
        s2 = WatchedSource(
            name="s2", source_dir=str(self.tmp / "b"), latest_target=str(target),
            sample_filename="shot_2027.mov", file_rename_template="",
            history_filename=".latest_history_s2.json",
        )
        cfg = ProjectConfig(project_name="P", watched_sources=[s1, s2])
        self.assertEqual(detect_target_conflicts(cfg), [])

    def test_scan_cache_invalidates_on_task_tokens_change(self):
        cfg_path = str(self.tmp / "proj.json")
        src = WatchedSource(name="shot", source_dir=str(self.tmp))
        v = VersionInfo("v001", 1, str(self.tmp / "v001"))
        save_cache(cfg_path, [src], {"shot": [v]}, task_tokens=["comp"])
        self.assertIn("shot", load_cache(cfg_path, [src], task_tokens=["comp"]))
        self.assertNotIn("shot", load_cache(cfg_path, [src], task_tokens=["comp", "grade"]))

    def test_migrate_shared_history_preserves_first_source_filename(self):
        target = str(self.tmp / "shared")
        s1 = WatchedSource(name="shot1", source_dir="/a", latest_target=target)
        s2 = WatchedSource(name="shot2", source_dir="/b", latest_target=target)
        s3 = WatchedSource(name="shot1", source_dir="/c", latest_target=target)
        cfg = ProjectConfig(project_name="P", watched_sources=[s1, s2, s3])
        _migrate_shared_history_filenames(cfg)
        names = [s.history_filename for s in cfg.watched_sources]
        self.assertEqual(len(set(n.casefold() for n in names)), 3)
        self.assertEqual(s1.history_filename, ".latest_history.json")

    def test_promoter_drop_foreign_delimiter_and_flat_dry_run(self):
        src_dir = self.tmp / "flat"
        src_dir.mkdir()
        (src_dir / "shot_v001.1001.exr").write_bytes(b"111")
        (src_dir / "shot_v002.1001.exr").write_bytes(b"222")
        target = self.tmp / "latest"
        target.mkdir()
        # Foreign sidecar with stem "shot10" and own stem "shot1"
        (target / ".latest_history_shot10.json").write_text(
            json.dumps({"current": {"latest_basename": "shot10"}}), encoding="utf-8",
        )
        src = WatchedSource(
            name="shot", source_dir=str(src_dir), latest_target=str(target),
            file_extensions=[".exr"], sample_filename="shot_v001.1001.exr",
            history_filename=".latest_history_shot1.json",
        )
        p = Promoter(src)
        kept = p._drop_foreign_files(
            [target / "shot1.1001.exr", target / "shot10.1001.exr"],
            target, own_stem="shot1",
        )
        self.assertEqual([k.name for k in kept], ["shot1.1001.exr"])

        v1 = VersionInfo("v001", 1, str(src_dir), file_count=1)
        preview = p.dry_run(v1)
        self.assertEqual(preview["total_files"], 1)
        self.assertTrue(preview["file_map"][0]["source"].endswith("shot_v001.1001.exr"))

    def test_collect_premiere_renames_longest_stem_wins(self):
        latest = self.tmp / "online"
        latest.mkdir()
        (latest / "shot1.mov").write_bytes(b"a")
        (latest / "shot1_bg.mov").write_bytes(b"b")
        (latest / ".latest_history_a.json").write_text(json.dumps({
            "current": {"latest_basename": "shot1", "nle_display_stem": "shot1_v002"},
        }), encoding="utf-8")
        (latest / ".latest_history_b.json").write_text(json.dumps({
            "current": {"latest_basename": "shot1_bg", "nle_display_stem": "shot1_bg_v005"},
        }), encoding="utf-8")
        out = {Path(e["clip_match_key"]).name: e["new_display_name"]
               for e in collect_premiere_renames([str(latest)])}
        self.assertEqual(out["shot1.mov"], "shot1_v002")
        self.assertEqual(out["shot1_bg.mov"], "shot1_bg_v005")


class TestCLICorrectness(_TmpTest):
    def test_cmd_verify_exits_nonzero_on_failure(self):
        import main as cli_main
        target = self.tmp / "latest"
        target.mkdir()
        (target / "rogue.exr").write_bytes(b"x")
        cfg_path = self.tmp / "proj.json"
        cfg = ProjectConfig(
            project_name="P",
            watched_sources=[WatchedSource(
                name="s", source_dir=str(self.tmp), latest_target=str(target),
                override_latest_target=True,
                file_extensions=[".exr"],
            )],
        )
        from lvm.config import save_config
        save_config(cfg, str(cfg_path))
        args = mock.MagicMock(config=str(cfg_path))
        with self.assertRaises(SystemExit) as ctx:
            cli_main.cmd_verify(args)
        self.assertEqual(ctx.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
