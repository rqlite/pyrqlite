"""
Cursor behaviour that can be pinned without a running rqlited, plus a
few end-to-end checks against an ephemeral node.
"""

import unittest

import pyrqlite.dbapi2 as sqlite
from pyrqlite.cursors import Cursor


class FetchAfterDMLTests(unittest.TestCase):
    """
    After UPDATE or DELETE, rowcount holds the number of rows affected
    while no rows are held. The fetch methods used to loop on rowcount
    and so hung forever (fetchall) or returned a list of None
    (fetchmany) in that state.
    """

    def _dml_cursor(self, affected):
        cu = Cursor(None)
        cu._rows = []
        cu.rownumber = 0
        cu.rowcount = affected
        return cu

    def test_fetchall_returns_empty(self):
        self.assertEqual(self._dml_cursor(3).fetchall(), [])

    def test_fetchmany_returns_empty(self):
        self.assertEqual(self._dml_cursor(3).fetchmany(10), [])

    def test_iteration_is_empty(self):
        self.assertEqual(list(self._dml_cursor(3)), [])

    def test_fetchone_returns_none(self):
        self.assertIsNone(self._dml_cursor(3).fetchone())

    def test_fetch_with_no_statement(self):
        cu = Cursor(None)
        self.assertIsNone(cu.fetchone())
        self.assertEqual(cu.fetchmany(5), [])
        self.assertEqual(cu.fetchall(), [])
        self.assertEqual(list(cu), [])


class FetchAfterDMLEndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def setUp(self):
        self.cu = self.cx.cursor()
        self.cu.execute("create table test(x integer)")
        self.cu.executemany("insert into test(x) values (?)", [(1,), (2,), (3,)])

    def tearDown(self):
        self.cu.execute("drop table test")
        self.cu.close()

    def test_fetchall_after_delete(self):
        self.cu.execute("delete from test")
        self.assertEqual(self.cu.rowcount, 3)
        self.assertEqual(self.cu.fetchall(), [])

    def test_fetchmany_after_update(self):
        self.cu.execute("update test set x = x + 1")
        self.assertEqual(self.cu.rowcount, 3)
        self.assertEqual(self.cu.fetchmany(10), [])
        self.assertEqual(list(self.cu), [])
