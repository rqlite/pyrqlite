"""
Cursor behaviour that can be pinned without a running rqlited, plus a
few end-to-end checks against an ephemeral node.
"""

import unittest

import pyrqlite.dbapi2 as sqlite
from pyrqlite.cursors import Cursor, _statement_error


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


class StatementErrorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def setUp(self):
        self.cu = self.cx.cursor()
        self.cu.execute("create table test(x integer primary key)")

    def tearDown(self):
        self.cu.execute("drop table test")
        self.cu.close()

    def test_execute_raises_on_statement_error(self):
        with self.assertRaises(sqlite.Error):
            self.cu.execute("insert into nosuchtable values (1)")

    def test_executemany_raises_on_statement_error(self):
        # executemany used to log the error and report success.
        with self.assertRaises(sqlite.Error):
            self.cu.executemany("insert into nosuchtable values (?)", [(1,), (2,)])

    def test_empty_statement_is_a_noop(self):
        # The command sniffer used to raise IndexError on an empty
        # string before anything was sent. sqlite3 treats an empty
        # statement as a successful no-op, and so does rqlite.
        for stmt in ("", "   "):
            self.cu.execute(stmt)
            self.assertIsNone(self.cu.description)
            self.assertEqual(self.cu.fetchall(), [])


class RowcountTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def setUp(self):
        self.cu = self.cx.cursor()
        self.cu.execute("create table test(x integer)")

    def tearDown(self):
        self.cu.execute("drop table test")
        self.cu.close()

    def test_insert_reports_rows_affected(self):
        self.cu.execute("insert into test(x) values (1)")
        self.assertEqual(self.cu.rowcount, 1)
        self.cu.executemany("insert into test(x) values (?)", [(2,), (3,)])
        self.assertEqual(self.cu.rowcount, 2)

    def test_select_reports_rows_returned(self):
        self.cu.executemany("insert into test(x) values (?)", [(1,), (2,), (3,)])
        self.cu.execute("select x from test where x > 1")
        self.assertEqual(self.cu.rowcount, 2)
        self.assertEqual(len(self.cu.fetchall()), 2)

    def test_ddl_reports_zero(self):
        self.cu.execute("create table other(y)")
        self.assertEqual(self.cu.rowcount, 0)
        self.cu.execute("drop table other")


class ExceptionMappingTests(unittest.TestCase):
    """
    Statement errors used to be raised as the base Error with a JSON
    dump as the message. They now follow the sqlite3 mapping from the
    SQLite result code, with the server's message verbatim.
    """

    def test_constraint_is_integrity_error(self):
        item = {"error": "UNIQUE constraint failed: t.x",
                "error_v2": {"message": "x", "sqlite": {"code": 19, "extended_code": 1555}}}
        exc = _statement_error(item)
        self.assertIsInstance(exc, sqlite.IntegrityError)
        self.assertEqual(str(exc), "UNIQUE constraint failed: t.x")

    def test_generic_error_is_operational(self):
        item = {"error": "no such table: t",
                "error_v2": {"message": "x", "sqlite": {"code": 1, "extended_code": 1}}}
        self.assertIsInstance(_statement_error(item), sqlite.OperationalError)

    def test_unknown_code_is_database_error(self):
        item = {"error": "weird", "error_v2": {"sqlite": {"code": 999}}}
        exc = _statement_error(item)
        self.assertIs(type(exc), sqlite.DatabaseError)

    def test_missing_error_v2_is_operational(self):
        # Older rqlite sends only the message.
        exc = _statement_error({"error": "boom"})
        self.assertIsInstance(exc, sqlite.OperationalError)
        self.assertEqual(str(exc), "boom")

    def test_every_mapped_class_is_an_error_subclass(self):
        from pyrqlite.cursors import _SQLITE_ERROR_CLASSES
        for cls in _SQLITE_ERROR_CLASSES.values():
            if cls is MemoryError:
                continue
            self.assertTrue(issubclass(cls, sqlite.Error), cls)


class ExceptionMappingEndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def setUp(self):
        self.cu = self.cx.cursor()
        self.cu.execute("create table test(x integer primary key)")
        self.cu.execute("insert into test(x) values (1)")

    def tearDown(self):
        self.cu.execute("drop table test")
        self.cu.close()

    def test_unique_violation(self):
        with self.assertRaises(sqlite.IntegrityError) as cm:
            self.cu.execute("insert into test(x) values (1)")
        self.assertIn("UNIQUE constraint failed", str(cm.exception))

    def test_syntax_error(self):
        with self.assertRaises(sqlite.OperationalError) as cm:
            self.cu.execute("selec 1")
        self.assertIn("syntax error", str(cm.exception))

    def test_executemany_unique_violation(self):
        with self.assertRaises(sqlite.IntegrityError):
            self.cu.executemany("insert into test(x) values (?)", [(2,), (1,)])

    def test_http_error_includes_body(self):
        with self.assertRaises(sqlite.OperationalError) as cm:
            self.cu._request("POST", "/db/execute", body='"garbage"',
                             headers={'Content-Type': 'application/json'})
        self.assertIn("400", str(cm.exception))
        self.assertIn("invalid request", str(cm.exception))
