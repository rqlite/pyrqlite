"""
Connection behaviour that can be pinned without a running rqlited.
Constructing a Connection only builds an HTTPConnection object; no
socket is opened until a request is made.
"""

import base64
import gc
import sys
import unittest

import pyrqlite.dbapi2 as sqlite


class BasicAuthHeaderTests(unittest.TestCase):
    def _header(self, user, password):
        con = sqlite.connect(host="localhost", port=1, user=user, password=password)
        try:
            return con._headers.get("Authorization")
        finally:
            con.close()

    def test_no_credentials_no_header(self):
        con = sqlite.connect(host="localhost", port=1)
        try:
            self.assertNotIn("Authorization", con._headers)
        finally:
            con.close()

    def test_short_credentials(self):
        expected = "Basic " + base64.b64encode(b"alice:secret").decode("ascii")
        self.assertEqual(self._header("alice", "secret"), expected)

    def test_long_credentials_have_no_newline(self):
        # The base64 codec wraps its output every 76 characters, so
        # credentials longer than 57 bytes used to produce a header
        # with an embedded newline, which only the trailing strip missed.
        user = "someuser"
        password = "p" * 100
        header = self._header(user, password)
        self.assertNotIn("\n", header)
        raw = "{}:{}".format(user, password).encode("utf-8")
        self.assertEqual(header, "Basic " + base64.b64encode(raw).decode("ascii"))

    def test_unicode_credentials(self):
        header = self._header("usr", "pässwörd")
        encoded = header[len("Basic "):]
        self.assertEqual(base64.b64decode(encoded).decode("utf-8"), "usr:pässwörd")


class PartiallyConstructedConnectionTests(unittest.TestCase):
    @unittest.skipIf(not hasattr(sys, "unraisablehook"), "needs sys.unraisablehook")
    def test_failed_init_does_not_raise_in_del(self):
        # An unsupported scheme raises before _connection is assigned.
        # __del__ then ran close(), which blew up on the missing
        # attribute and printed "Exception ignored in __del__".
        caught = []
        saved = sys.unraisablehook
        sys.unraisablehook = lambda unraisable: caught.append(unraisable)
        try:
            with self.assertRaises(sqlite.ProgrammingError):
                sqlite.connect(scheme="ftp", host="localhost", port=1)
            gc.collect()
        finally:
            sys.unraisablehook = saved
        self.assertEqual(caught, [])

    def test_close_is_idempotent(self):
        con = sqlite.connect(host="localhost", port=1)
        con.close()
        con.close()


class HeaderHandlingTests(unittest.TestCase):
    def test_per_request_headers_do_not_leak(self):
        # The old mutable default argument never leaked in practice
        # because it was only read, but the signature now makes that
        # explicit: None means no extra headers.
        con = sqlite.connect(host="localhost", port=1, user="u", password="p")
        try:
            self.assertEqual(sorted(con._headers), ["Authorization"])
        finally:
            con.close()
