"""
Tests for the watcher backends: network-path detection / backend choice
(src/lvm/fsinfo.py) and the directory poller (src/lvm/dirpoll.py).

Self-contained: temp directories, injected mount tables. Portable (no Qt).
"""

import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.dirpoll import DirectoryPoller
from lvm.fsinfo import (
    NATIVE, POLLING, choose_watch_backend, is_network_path,
    parse_macos_mount_output, parse_proc_mounts,
)

LINUX_MOUNTS = parse_proc_mounts(
    "/dev/nvme0n1p2 / ext4 rw,relatime 0 0\n"
    "//nas/show /mnt/show cifs rw,vers=3.1.1 0 0\n"
    "nas:/export/plates /mnt/plates nfs4 rw 0 0\n"
    "/dev/sdb1 /mnt/show/local\\040cache ext4 rw 0 0\n"
)

MAC_MOUNTS = parse_macos_mount_output(
    "/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)\n"
    "//artist@nas._smb._tcp.local/show on /Volumes/show (smbfs, nodev, nosuid, mounted by artist)\n"
    "/dev/disk5s1 on /Volumes/Scratch (apfs, local, nodev, nosuid, journaled)\n"
)


class TestMountParsing(unittest.TestCase):

    def test_proc_mounts(self):
        self.assertIn(("/mnt/show", "cifs"), LINUX_MOUNTS)
        self.assertIn(("/mnt/show/local cache", "ext4"), LINUX_MOUNTS)

    def test_macos_mount_output(self):
        self.assertIn(("/Volumes/show", "smbfs"), MAC_MOUNTS)
        self.assertIn(("/", "apfs"), MAC_MOUNTS)


class TestIsNetworkPath(unittest.TestCase):

    def test_linux(self):
        net = lambda p: is_network_path(p, platform="linux", mounts=LINUX_MOUNTS)
        self.assertTrue(net("/mnt/show/shots/sh010/renders"))
        self.assertTrue(net("/mnt/plates/a"))
        self.assertFalse(net("/home/artist/renders"))
        self.assertFalse(net("/mnt/showreel"), "prefix must stop at a path separator")
        self.assertFalse(net("/mnt/show/local cache/x"), "longest mount point wins")

    def test_macos(self):
        net = lambda p: is_network_path(p, platform="darwin", mounts=MAC_MOUNTS)
        self.assertTrue(net("/Volumes/show/sh010"))
        self.assertFalse(net("/Volumes/Scratch/sh010"))
        self.assertFalse(net("/Users/artist"))

    def test_windows_unc(self):
        self.assertTrue(is_network_path(r"\\nas\show\sh010", platform="win32"))
        self.assertTrue(is_network_path("//nas/show/sh010", platform="win32"))

    @unittest.skipUnless(sys.platform == "win32", "drive type lookup is Windows-only")
    def test_windows_local_drive(self):
        self.assertFalse(is_network_path(tempfile.gettempdir()))


class TestChooseWatchBackend(unittest.TestCase):

    def test_explicit_modes_win(self):
        self.assertEqual(choose_watch_backend("/mnt/show", "native",
                                              platform="linux", mounts=LINUX_MOUNTS), NATIVE)
        self.assertEqual(choose_watch_backend("/home/x", "polling",
                                              platform="linux", mounts=LINUX_MOUNTS), POLLING)

    def test_auto_polls_network_shares_on_linux_and_macos(self):
        self.assertEqual(choose_watch_backend("/mnt/show/sh010", "auto",
                                              platform="linux", mounts=LINUX_MOUNTS), POLLING)
        self.assertEqual(choose_watch_backend("/Volumes/show/sh010", "auto",
                                              platform="darwin", mounts=MAC_MOUNTS), POLLING)
        self.assertEqual(choose_watch_backend("/home/x", "auto",
                                              platform="linux", mounts=LINUX_MOUNTS), NATIVE)

    def test_auto_keeps_native_on_windows_shares(self):
        """ReadDirectoryChangesW sees changes from other machines over SMB."""
        self.assertEqual(choose_watch_backend(r"\\nas\show\sh010", "auto",
                                              platform="win32"), NATIVE)


class TestDirectoryPoller(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_poll_"))
        self.renders = self.tmp / "renders"
        self.renders.mkdir()
        (self.renders / "shot_v001").mkdir()
        self.calls = []
        self.poller = DirectoryPoller(self.calls.append, interval=0.05)
        self.poller.add("shot", str(self.renders))

    def tearDown(self):
        self.poller.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_first_poll_only_records(self):
        self.assertEqual(self.poller.poll_once(), [])
        self.assertEqual(self.calls, [])
        self.assertTrue(self.poller.is_reachable("shot"))

    def test_new_entry_triggers_callback(self):
        self.poller.poll_once()
        (self.renders / "shot_v002").mkdir()
        self.assertEqual(self.poller.poll_once(), ["shot"])
        self.assertEqual(self.poller.poll_once(), [], "unchanged dir must not fire again")
        self.assertEqual(self.calls, ["shot"])

    def test_removal_does_not_trigger(self):
        self.poller.poll_once()
        (self.renders / "shot_v001").rmdir()
        self.assertEqual(self.poller.poll_once(), [])

    def test_unreachable_then_back_fires_catch_up(self):
        self.poller.poll_once()
        moved = self.tmp / "offline"
        self.renders.rename(moved)               # share dropped
        self.assertEqual(self.poller.poll_once(), [])
        self.assertFalse(self.poller.is_reachable("shot"))
        moved.rename(self.renders)               # share back
        self.assertEqual(self.poller.poll_once(), ["shot"])
        self.assertTrue(self.poller.is_reachable("shot"))

    def test_missing_at_start_fires_when_it_appears(self):
        poller = DirectoryPoller(self.calls.append, interval=0.05)
        later = self.tmp / "not_mounted_yet"
        poller.add("late", str(later))
        self.assertEqual(poller.poll_once(), [])
        later.mkdir()
        self.assertEqual(poller.poll_once(), ["late"])

    def test_removed_key_is_not_polled(self):
        self.poller.poll_once()
        self.poller.remove("shot")
        (self.renders / "shot_v002").mkdir()
        self.assertEqual(self.poller.poll_once(), [])

    def test_background_thread_reports_changes(self):
        fired = threading.Event()
        poller = DirectoryPoller(lambda key: fired.set(), interval=0.05)
        poller.add("shot", str(self.renders))
        poller.start()
        try:
            time.sleep(0.15)                     # first poll records the state
            (self.renders / "shot_v002").mkdir()
            self.assertTrue(fired.wait(2.0), "poller thread did not report the new folder")
        finally:
            poller.stop()
        self.assertFalse(poller.is_alive())


if __name__ == "__main__":
    unittest.main()
