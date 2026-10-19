"""
Tests for the ephemeral rqlited launcher behind the ":memory:" scheme.
"""

import os
import unittest

from pyrqlite import _ephemeral
from pyrqlite._ephemeral import EphemeralRqlited


class EphemeralRqlitedTests(unittest.TestCase):
    def test_start_and_stop_cleans_up(self):
        with EphemeralRqlited() as node:
            tempdir = node._tempdir
            proc = node._proc
            self.assertTrue(os.path.isdir(tempdir))
            self.assertIsNone(proc.poll())
            self.assertTrue(node._test_readyz(*node.http))
        self.assertFalse(os.path.exists(tempdir))
        self.assertIsNotNone(proc.poll())
        self.assertIsNone(node._proc)
        self.assertIsNone(node._tempdir)

    def test_binary_that_exits_immediately_does_not_hang(self):
        # Point at a binary that exits at once. The readiness loop
        # used to spin forever here because it never checked whether
        # the process was still alive.
        saved = _ephemeral.RQLITED_PATH
        _ephemeral.RQLITED_PATH = "/bin/false"
        try:
            node = EphemeralRqlited()
            node._MAX_ATTEMPTS = 2
            with self.assertRaises(RuntimeError) as cm:
                node._start()
            self.assertIn("/bin/false", str(cm.exception))
            self.assertIsNone(node._tempdir)
            self.assertIsNone(node._proc)
        finally:
            _ephemeral.RQLITED_PATH = saved

    def test_missing_binary_raises_and_cleans_up(self):
        saved = _ephemeral.RQLITED_PATH
        _ephemeral.RQLITED_PATH = "/nonexistent/rqlited"
        try:
            node = EphemeralRqlited()
            with self.assertRaises(EnvironmentError):
                node._start()
            self.assertIsNone(node._tempdir)
        finally:
            _ephemeral.RQLITED_PATH = saved
