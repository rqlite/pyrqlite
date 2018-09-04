#-*- coding: utf-8 -*-
# pysqlite2/test/dbapi.py: tests for DB-API compliance
#
# Copyright (C) 2004-2010 Gerhard Höring <gh@ghaering.de>
#
# This file is part of pysqlite.
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.

from __future__ import print_function

import sys
try:
    import test.support as test_support
except ImportError:
    from test import test_support
import unittest

import pyrqlite.dbapi2 as sqlite

if sys.version_info[0] >= 3:
    StandardError = Exception


class ModuleTests(unittest.TestCase):
    def test_CheckAPILevel(self):
        self.assertEqual(sqlite.apilevel, "2.0",
                         "apilevel is %s, should be 2.0" % sqlite.apilevel)

    def test_CheckThreadSafety(self):
        self.assertEqual(sqlite.threadsafety, 1,
                         "threadsafety is %d, should be 1" % sqlite.threadsafety)

    def test_CheckParamStyle(self):
        self.assertEqual(sqlite.paramstyle, "qmark",
                         "paramstyle is '%s', should be 'qmark'" %
                         sqlite.paramstyle)

    def test_CheckWarning(self):
        self.assertTrue(issubclass(sqlite.Warning, StandardError),
                        "Warning is not a subclass of StandardError")

    def test_CheckError(self):
        self.assertTrue(issubclass(sqlite.Error, StandardError),
                        "Error is not a subclass of StandardError")

    def test_CheckInterfaceError(self):
        self.assertTrue(issubclass(sqlite.InterfaceError, sqlite.Error),
                        "InterfaceError is not a subclass of Error")

    def test_CheckDatabaseError(self):
        self.assertTrue(issubclass(sqlite.DatabaseError, sqlite.Error),
                        "DatabaseError is not a subclass of Error")

    def test_CheckDataError(self):
        self.assertTrue(issubclass(sqlite.DataError, sqlite.DatabaseError),
                        "DataError is not a subclass of DatabaseError")

    def test_CheckOperationalError(self):
        self.assertTrue(issubclass(sqlite.OperationalError, sqlite.DatabaseError),
                        "OperationalError is not a subclass of DatabaseError")

    def test_CheckIntegrityError(self):
        self.assertTrue(issubclass(sqlite.IntegrityError, sqlite.DatabaseError),
                        "IntegrityError is not a subclass of DatabaseError")

    def test_CheckInternalError(self):
        self.assertTrue(issubclass(sqlite.InternalError, sqlite.DatabaseError),
                        "InternalError is not a subclass of DatabaseError")

    def test_CheckProgrammingError(self):
        self.assertTrue(issubclass(sqlite.ProgrammingError, sqlite.DatabaseError),
                        "ProgrammingError is not a subclass of DatabaseError")

    def test_CheckNotSupportedError(self):
        self.assertTrue(issubclass(sqlite.NotSupportedError,
                                   sqlite.DatabaseError),
                        "NotSupportedError is not a subclass of DatabaseError")

class ConnectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    def setUp(self):
        cu = self.cx.cursor()
        cu.execute("create table test(id integer primary key, name text)")
        cu.execute("insert into test(name) values (?)", ("foo",))

    def tearDown(self):
        self.cx.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()
        del cls.cx

    def test_CheckCommit(self):
        self.cx.commit()

    def test_CheckCommitAfterNoChanges(self):
        """
        A commit should also work when no changes were made to the database.
        """
        self.cx.commit()
        self.cx.commit()

    def test_CheckRollback(self):
        self.cx.rollback()

    def test_CheckRollbackAfterNoChanges(self):
        """
        A rollback should also work when no changes were made to the database.
        """
        self.cx.rollback()
        self.cx.rollback()

    def test_CheckCursor(self):
        cu = self.cx.cursor()

    @unittest.skip('not implemented')
    def test_CheckFailedOpen(self):
        YOU_CANNOT_OPEN_THIS = "/foo/bar/bla/23534/mydb.db"
        try:
            con = sqlite.connect(YOU_CANNOT_OPEN_THIS)
        except sqlite.OperationalError:
            return
        self.fail("should have raised an OperationalError")

    def test_CheckClose(self):
        # This would interfere with other tests, and
        # tearDownClass exercises it already.
        #self.cx.close()
        pass

    def test_CheckExceptions(self):
        # Optional DB-API extension.
        self.assertEqual(self.cx.Warning, sqlite.Warning)
        self.assertEqual(self.cx.Error, sqlite.Error)
        self.assertEqual(self.cx.InterfaceError, sqlite.InterfaceError)
        self.assertEqual(self.cx.DatabaseError, sqlite.DatabaseError)
        self.assertEqual(self.cx.DataError, sqlite.DataError)
        self.assertEqual(self.cx.OperationalError, sqlite.OperationalError)
        self.assertEqual(self.cx.IntegrityError, sqlite.IntegrityError)
        self.assertEqual(self.cx.InternalError, sqlite.InternalError)
        self.assertEqual(self.cx.ProgrammingError, sqlite.ProgrammingError)
        self.assertEqual(self.cx.NotSupportedError, sqlite.NotSupportedError)

class CursorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    def setUp(self):
        self.cu = self.cx.cursor()
        self.cu.execute("create table test(id integer primary key, name text, income number)")
        self.cu.execute("insert into test(name) values (?)", ("foo",))

    def tearDown(self):
        self.cu.close()
        self.cx.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()
        del cls.cx

    def test_CheckExecuteNoArgs(self):
        self.cu.execute("delete from test")

    @unittest.skip('not implemented')
    def test_CheckExecuteIllegalSql(self):
        try:
            self.cu.execute("select asdf")
            self.fail("should have raised an OperationalError")
        except sqlite.OperationalError:
            return
        except:
            self.fail("raised wrong exception")

    @unittest.skip('not implemented')
    def test_CheckExecuteTooMuchSql(self):
        try:
            self.cu.execute("select 5+4; select 4+5")
            self.fail("should have raised a Warning")
        except sqlite.Warning:
            return
        except:
            self.fail("raised wrong exception")

    @unittest.skip('not implemented')
    def test_CheckExecuteTooMuchSql2(self):
        self.cu.execute("select 5+4; -- foo bar")

    @unittest.skip('not implemented')
    def test_CheckExecuteTooMuchSql3(self):
        self.cu.execute("""
            select 5+4;

            /*
            foo
            */
            """)

    def test_CheckExecuteWrongSqlArg(self):
        try:
            self.cu.execute(42)
            self.fail("should have raised a ValueError")
        except ValueError:
            return
        except:
            self.fail("raised wrong exception.")

    def test_CheckExecuteArgInt(self):
        self.cu.execute("insert into test(id) values (?)", (42,))

    def test_CheckExecuteArgFloat(self):
        self.cu.execute("insert into test(income) values (?)", (2500.32,))

    def test_CheckExecuteArgString(self):
        self.cu.execute("insert into test(name) values (?)", ("Hugo",))

    @unittest.expectedFailure
    def test_CheckExecuteArgStringWithZeroByte(self):
        self.cu.execute("insert into test(name) values (?)", ("Hu\x00go",))

        self.cu.execute("select name from test where id=?", (self.cu.lastrowid,))
        row = self.cu.fetchone()
        self.assertEqual(row[0], "Hu\x00go")

    def test_CheckExecuteWrongNoOfArgs1(self):
        # too many parameters
        try:
            self.cu.execute("insert into test(id) values (?)", (17, "Egon"))
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckExecuteWrongNoOfArgs2(self):
        # too little parameters
        try:
            self.cu.execute("insert into test(id) values (?)")
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckExecuteWrongNoOfArgs3(self):
        # no parameters, parameters are needed
        try:
            self.cu.execute("insert into test(id) values (?)")
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckExecuteParamList(self):
        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("select name from test where name=?", ["foo"])
        row = self.cu.fetchone()
        self.assertEqual(row[0], "foo")

    def test_CheckExecuteParamSequence(self):
        class L(object):
            def __len__(self):
                return 1
            def __getitem__(self, x):
                assert x == 0
                return "foo"

        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("select name from test where name=?", L())
        row = self.cu.fetchone()
        self.assertEqual(row[0], "foo")

    def test_CheckExecuteDictMapping(self):
        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("select name from test where name=:name", {"name": "foo"})
        row = self.cu.fetchone()
        self.assertEqual(row[0], "foo")

    def test_CheckExecuteDictMapping_Mapping(self):
        # Test only works with Python 2.5 or later
        if sys.version_info < (2, 5, 0):
            return

        class D(dict):
            def __missing__(self, key):
                return "foo"

        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("select name from test where name=:name", D())
        row = self.cu.fetchone()
        self.assertEqual(row[0], "foo")

    def test_CheckExecuteDictMappingTooLittleArgs(self):
        self.cu.execute("insert into test(name) values ('foo')")
        try:
            self.cu.execute("select name from test where name=:name and id=:id", {"name": "foo"})
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckExecuteDictMappingNoArgs(self):
        self.cu.execute("insert into test(name) values ('foo')")
        try:
            self.cu.execute("select name from test where name=:name")
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckExecuteNamedWithoutDict(self):
        self.cu.execute("insert into test(name) values ('foo')")
        try:
            self.cu.execute("select name from test where name=:name", ("name",))
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckExecuteDictMappingUnnamed(self):
        self.cu.execute("insert into test(name) values ('foo')")
        try:
            self.cu.execute("select name from test where name=?", {"name": "foo"})
            self.fail("should have raised ProgrammingError")
        except sqlite.ProgrammingError:
            pass

    def test_CheckClose(self):
        self.cu.close()
        self.cu = self.cx.cursor()

    def test_CheckRowcountExecute(self):
        self.cu.execute("delete from test")
        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("update test set name='bar'")
        self.assertEqual(self.cu.rowcount, 2)

    @unittest.skip('not implemented')
    def test_CheckRowcountSelect(self):
        """
        pysqlite does not know the rowcount of SELECT statements, because we
        don't fetch all rows after executing the select statement. The rowcount
        has thus to be -1.
        """
        self.cu.execute("select 5 union select 6")
        self.assertEqual(self.cu.rowcount, -1)

    def test_CheckRowcountExecutemany(self):
        self.cu.execute("delete from test")
        self.cu.executemany("insert into test(name) values (?)", [(1,), (2,), (3,)])
        self.assertEqual(self.cu.rowcount, 3)

    @unittest.skip('Cursor.total_changes is not implemented')
    def test_CheckTotalChanges(self):
        self.cu.execute("insert into test(name) values ('foo')")
        self.cu.execute("insert into test(name) values ('foo')")
        if self.cx.total_changes < 2:
            self.fail("total changes reported wrong value")

    # Checks for executemany:
    # Sequences are required by the DB-API, iterators
    # enhancements in pysqlite.

    def test_CheckExecuteManySequence(self):
        self.cu.executemany("insert into test(income) values (?)", [(x,) for x in range(100, 110)])

    def test_CheckExecuteManyIterator(self):
        class MyIter:
            def __init__(self):
                self.value = 5

            def __iter__(self):
                return self

            def __next__(self):
                if self.value == 10:
                    raise StopIteration
                else:
                    self.value += 1
                    return (self.value,)

            next = __next__

        self.cu.executemany("insert into test(income) values (?)", MyIter())

    def test_CheckExecuteManyGenerator(self):
        def mygen():
            for i in range(5):
                yield (i,)

        self.cu.executemany("insert into test(income) values (?)", mygen())

    def test_CheckExecuteManyWrongSqlArg(self):
        try:
            self.cu.executemany(42, [(3,)])
            self.fail("should have raised a ValueError")
        except ValueError:
            return
        except:
            self.fail("raised wrong exception.")

    @unittest.skip('not implemented')
    def test_CheckExecuteManySelect(self):
        try:
            self.cu.executemany("select ?", [(3,)])
            self.fail("should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            return
        except:
            self.fail("raised wrong exception.")

    def test_CheckExecuteManyNotIterable(self):
        try:
            self.cu.executemany("insert into test(income) values (?)", 42)
            self.fail("should have raised a TypeError")
        except TypeError:
            return
        except Exception as e:
            print("raised", e.__class__)
            self.fail("raised wrong exception.")

    def test_CheckFetchIter(self):
        # Optional DB-API extension.
        self.cu.execute("delete from test")
        self.cu.execute("insert into test(id) values (?)", (5,))
        self.cu.execute("insert into test(id) values (?)", (6,))
        self.cu.execute("select id from test order by id")
        lst = []
        for row in self.cu:
            lst.append(row[0])
        self.assertEqual(lst[0], 5)
        self.assertEqual(lst[1], 6)

    def test_CheckFetchone(self):
        self.cu.execute("select name from test")
        row = self.cu.fetchone()
        self.assertEqual(row[0], "foo")
        row = self.cu.fetchone()
        self.assertEqual(row, None)

    def test_CheckFetchoneNoStatement(self):
        cur = self.cx.cursor()
        row = cur.fetchone()
        self.assertEqual(row, None)

    def test_CheckArraySize(self):
        # must default ot 1
        self.assertEqual(self.cu.arraysize, 1)

        # now set to 2
        self.cu.arraysize = 2

        # now make the query return 3 rows
        self.cu.execute("delete from test")
        self.cu.execute("insert into test(name) values ('A')")
        self.cu.execute("insert into test(name) values ('B')")
        self.cu.execute("insert into test(name) values ('C')")
        self.cu.execute("select name from test")
        res = self.cu.fetchmany()

        self.assertEqual(len(res), 2)

    def test_CheckFetchmany(self):
        self.cu.execute("select name from test")
        res = self.cu.fetchmany(100)
        self.assertEqual(len(res), 1)
        res = self.cu.fetchmany(100)
        self.assertEqual(res, [])

    def test_CheckFetchmanyKwArg(self):
        """Checks if fetchmany works with keyword arguments"""
        self.cu.execute("select name from test")
        res = self.cu.fetchmany(size=100)
        self.assertEqual(len(res), 1)

    def test_CheckFetchall(self):
        self.cu.execute("select name from test")
        res = self.cu.fetchall()
        self.assertEqual(len(res), 1)
        res = self.cu.fetchall()
        self.assertEqual(res, [])

    @unittest.skip('Cursor.setinputsizes is not implemented')
    def test_CheckSetinputsizes(self):
        self.cu.setinputsizes([3, 4, 5])

    @unittest.skip('Cursor.setoutputsize is not implemented')
    def test_CheckSetoutputsize(self):
        self.cu.setoutputsize(5, 0)

    @unittest.skip('Cursor.setoutputsize is not implemented')
    def test_CheckSetoutputsizeNoColumn(self):
        self.cu.setoutputsize(42)

    def test_CheckCursorConnection(self):
        # Optional DB-API extension.
        self.assertEqual(self.cu.connection, self.cx)

    def test_CheckWrongCursorCallable(self):
        try:
            def f(): pass
            cur = self.cx.cursor(f)
            self.fail("should have raised a TypeError")
        except TypeError:
            return
        self.fail("should have raised a ValueError")

    @unittest.skip('not implemented')
    def test_CheckCursorWrongClass(self):
        class Foo: pass
        foo = Foo()
        try:
            cur = sqlite.Cursor(foo)
            self.fail("should have raised a ValueError")
        except TypeError:
            pass


class ConstructorTests(unittest.TestCase):
    def test_CheckDate(self):
        d = sqlite.Date(2004, 10, 28)

    def test_CheckTime(self):
        t = sqlite.Time(12, 39, 35)

    def test_CheckTimestamp(self):
        ts = sqlite.Timestamp(2004, 10, 28, 12, 39, 35)

    def test_CheckDateFromTicks(self):
        d = sqlite.DateFromTicks(42)

    def test_CheckTimeFromTicks(self):
        t = sqlite.TimeFromTicks(42)

    def test_CheckTimestampFromTicks(self):
        ts = sqlite.TimestampFromTicks(42)

    def test_CheckBinary(self):
        with (test_support.check_warnings() if sys.version_info[0] >= 3
            else test_support.check_py3k_warnings()):
            b = sqlite.Binary(chr(0).encode() + b"'"
                if sys.version_info[0] >= 3 else chr(0) + b"'")

class ExtensionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:")

    def tearDown(self):
        for row in self.con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            self.con.execute("drop table '{}'".format(row[0]))

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        del cls.con

    @unittest.skip('Cursor.executescript is not implemented')
    def test_CheckScriptStringSql(self):
        cur = self.con.cursor()
        cur.executescript("""
            -- bla bla
            /* a stupid comment */
            create table a(i);
            insert into a(i) values (5);
            """)
        cur.execute("select i from a")
        res = cur.fetchone()[0]
        self.assertEqual(res, 5)

    @unittest.skip('Cursor.executescript is not implemented')
    def test_CheckScriptStringUnicode(self):
        cur = self.con.cursor()
        cur.executescript(u"""
            create table a(i);
            insert into a(i) values (5);
            select i from a;
            delete from a;
            insert into a(i) values (6);
            """)
        cur.execute("select i from a")
        res = cur.fetchone()[0]
        self.assertEqual(res, 6)

    @unittest.skip('Cursor.executescript is not implemented')
    def test_CheckScriptSyntaxError(self):
        cur = self.con.cursor()
        raised = False
        try:
            cur.executescript("create table test(x); asdf; create table test2(x)")
        except sqlite.OperationalError:
            raised = True
        self.assertEqual(raised, True, "should have raised an exception")

    @unittest.skip('Cursor.executescript is not implemented')
    def test_CheckScriptErrorNormal(self):
        cur = self.con.cursor()
        raised = False
        try:
            cur.executescript("create table test(sadfsadfdsa); select foo from hurz;")
        except sqlite.OperationalError:
            raised = True
        self.assertEqual(raised, True, "should have raised an exception")

    def test_CheckConnectionExecute(self):
        result = self.con.execute("select 5").fetchone()[0]
        self.assertEqual(result, 5, "Basic test of Connection.execute")

    @unittest.skip('Connection.executemany is not implemented')
    def test_CheckConnectionExecutemany(self):
        con = self.con
        con.execute("create table test(foo)")
        con.executemany("insert into test(foo) values (?)", [(3,), (4,)])
        result = con.execute("select foo from test order by foo").fetchall()
        self.assertEqual(result[0][0], 3, "Basic test of Connection.executemany")
        self.assertEqual(result[1][0], 4, "Basic test of Connection.executemany")

    @unittest.skip('Connection.executescript is not implemented')
    def test_CheckConnectionExecutescript(self):
        con = self.con
        con.executescript("create table test(foo); insert into test(foo) values (5);")
        result = con.execute("select foo from test").fetchone()[0]
        self.assertEqual(result, 5, "Basic test of Connection.executescript")

@unittest.skip('not implemented')
class ClosedConTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:")
        cls.cur = cls.con.cursor()
        cls.con.close()

    @classmethod
    def tearDownClass(cls):
        del cls.cur
        del cls.con

    def test_CheckClosedConCursor(self):
        con = self.con
        try:
            cur = con.cursor()
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedConCommit(self):
        con = self.con
        try:
            con.commit()
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedConRollback(self):
        con = self.con
        try:
            con.rollback()
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedCurExecute(self):
        cur = self.cur
        try:
            cur.execute("select 4")
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedCreateFunction(self):
        con = self.con
        def f(x): return 17
        try:
            con.create_function("foo", 1, f)
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedCreateAggregate(self):
        con = self.con
        class Agg:
            def __init__(self):
                pass
            def step(self, x):
                pass
            def finalize(self):
                return 17
        try:
            con.create_aggregate("foo", 1, Agg)
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedSetAuthorizer(self):
        con = self.con
        def authorizer(*args):
            return sqlite.DENY
        try:
            con.set_authorizer(authorizer)
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedSetProgressCallback(self):
        con = self.con
        def progress(): pass
        try:
            con.set_progress_handler(progress, 100)
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

    def test_CheckClosedCall(self):
        con = self.con
        try:
            con()
            self.fail("Should have raised a ProgrammingError")
        except sqlite.ProgrammingError:
            pass
        except:
            self.fail("Should have raised a ProgrammingError")

@unittest.skip('not implemented')
class ClosedCurTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:")
        cls.cur = cls.con.cursor()
        cls.cur.close()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        del cls.cur
        del cls.con

    def test_CheckClosed(self):
        cur = self.cur

        for method_name in ("execute", "executemany", "executescript", "fetchall", "fetchmany", "fetchone"):
            if method_name in ("execute", "executescript"):
                params = ("select 4 union select 5",)
            elif method_name == "executemany":
                params = ("insert into foo(bar) values (?)", [(3,), (4,)])
            else:
                params = []

            try:
                method = getattr(cur, method_name)

                method(*params)
                self.fail("Should have raised a ProgrammingError: method " + method_name)
            except sqlite.ProgrammingError:
                pass
            except:
                self.fail("Should have raised a ProgrammingError: " + method_name)

def suite():
    loader = unittest.TestLoader()
    module_suite = loader.loadTestsFromTestCase(ModuleTests)
    connection_suite = loader.loadTestsFromTestCase(ConnectionTests)
    cursor_suite = loader.loadTestsFromTestCase(CursorTests)
    constructor_suite = loader.loadTestsFromTestCase(ConstructorTests)
    ext_suite = loader.loadTestsFromTestCase(ExtensionTests)
    closed_con_suite = loader.loadTestsFromTestCase(ClosedConTests)
    closed_cur_suite = loader.loadTestsFromTestCase(ClosedCurTests)
    return unittest.TestSuite((module_suite, connection_suite, cursor_suite, constructor_suite, ext_suite, closed_con_suite, closed_cur_suite))

def main():
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())

if __name__ == "__main__":
    main()
