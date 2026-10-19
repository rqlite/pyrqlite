"""
Cursor behaviour that can be pinned without a running rqlited, plus a
few end-to-end checks against an ephemeral node.
"""

import unittest

import pyrqlite.dbapi2 as sqlite
from pyrqlite.cursors import Cursor


class NamedParameterTests(unittest.TestCase):
    """
    Named placeholders are substituted client-side. The old matcher
    only accepted letters, so :name1 and :first_name were left in the
    SQL untouched, and sequential str.replace let :a corrupt :ab.
    """

    def _sub(self, operation, parameters):
        return Cursor(None)._substitute_params(operation, parameters)

    def test_plain_name(self):
        self.assertEqual(
            self._sub("select * from t where a=:name", {"name": "x"}),
            "select * from t where a='x'")

    def test_name_with_digits(self):
        self.assertEqual(
            self._sub("select * from t where a=:name1", {"name1": 1}),
            "select * from t where a=1")

    def test_name_with_underscore(self):
        self.assertEqual(
            self._sub("select * from t where a=:first_name", {"first_name": "x"}),
            "select * from t where a='x'")

    def test_prefix_names_do_not_collide(self):
        self.assertEqual(
            self._sub("select * from t where a=:a and b=:ab", {"a": 1, "ab": 2}),
            "select * from t where a=1 and b=2")

    def test_repeated_name(self):
        self.assertEqual(
            self._sub("select :v, :v", {"v": 7}),
            "select 7, 7")

    def test_missing_name_raises(self):
        with self.assertRaises(sqlite.ProgrammingError):
            self._sub("select :a, :b", {"a": 1})

    def test_mapping_with_missing_is_honoured(self):
        class D(dict):
            def __missing__(self, key):
                return "dflt"
        self.assertEqual(self._sub("select :zzz", D()), "select 'dflt'")

    def test_qmark_count_mismatch_raises(self):
        with self.assertRaises(sqlite.ProgrammingError):
            self._sub("select ?, ?", (1,))

    def test_mixed_styles_raise(self):
        with self.assertRaises(sqlite.ProgrammingError):
            self._sub("select ?, :a", {"a": 1})


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
