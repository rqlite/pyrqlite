#-*- coding: utf-8 -*-
# pysqlite2/test/types.py: tests for type conversion and detection
#
# Copyright (C) 2005 Gerhard Höring <gh@ghaering.de>
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

import datetime
import sys
import unittest
import pyrqlite.dbapi2 as sqlite
try:
    import zlib
except ImportError:
    zlib = None


class SqliteTypeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:")
        cls.cur = cls.con.cursor()
        if cls.cur.execute("pragma table_info(test)").fetchall():
            cls.cur.execute("drop table test")

    def setUp(self):
        self.cur.execute("create table test(i integer, s varchar, f number, b blob)")

    def tearDown(self):
        self.cur.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.con.close()

    def test_CheckString(self):
        self.cur.execute("insert into test(s) values (?)", (u"Österreich",))
        self.cur.execute("select s from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], u"Österreich")

    def test_CheckStringNull(self):
        self.cur.execute("insert into test(s) values (?)", (None,))
        self.cur.execute("select s from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], None)

    def test_CheckSmallInt(self):
        self.cur.execute("insert into test(i) values (?)", (42,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], 42)

    def test_CheckLargeInt(self):
        num = 2**40
        self.cur.execute("insert into test(i) values (?)", (num,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], num)

    def test_CheckIntNull(self):
        self.cur.execute("insert into test(i) values (?)", (None,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], None)

    def test_CheckFloat(self):
        val = 3.14
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    def test_CheckFloatNull(self):
        self.cur.execute("insert into test(f) values (?)", (None,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], None)

    def test_CheckBlob(self):
        sample = b"\x99Guglhupf"
        val = sample
        self.cur.execute("insert into test(b) values (?)", (val,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], sample)

    def test_CheckBlobNull(self):
        self.cur.execute("insert into test(b) values (?)", (None,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], None)

    def test_CheckUnicodeExecute(self):
        self.cur.execute(u"select 'Österreich'")
        row = self.cur.fetchone()
        self.assertEqual(row[0], u"Österreich")

    def test_CheckNullExecute(self):
        self.cur.execute("select null")
        row = self.cur.fetchone()
        self.assertEqual(row[0], None)

    def test_PragmaTableInfo(self):
        self.cur.execute("pragma table_info('test')")
        rows = self.cur.fetchall()
        self.assertEqual(rows,
            [
                (0, 'i', 'integer', 0, None, 0),
                (1, 's', 'varchar', 0, None, 0),
                (2, 'f', 'number', 0, None, 0),
                (3, 'b', 'blob', 0, None, 0),
            ]
        )

class DeclTypesTests(unittest.TestCase):
    class Foo:
        def __init__(self, _val):
            if isinstance(_val, bytes):
                # sqlite3 always calls __init__ with a bytes created from a
                # UTF-8 string when __conform__ was used to store the object.
                _val = _val.decode('utf-8')
            self.val = _val

        def __eq__(self, other):
            if not isinstance(other, DeclTypesTests.Foo):
                return NotImplemented
            return self.val == other.val

        def __conform__(self, protocol):
            if protocol is sqlite.PrepareProtocol:
                return self.val
            else:
                return None

        def __str__(self):
            return "<%s>" % self.val

    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:",
            detect_types=sqlite.PARSE_DECLTYPES)
        cls.cur = cls.con.cursor()
        if cls.cur.execute("pragma table_info(test)").fetchall():
            cls.cur.execute("drop table test")

        # override float, make them always return the same number
        sqlite.converters["FLOAT"] = lambda x: 47.2

        # and implement two custom ones
        sqlite.converters["BOOL"] = lambda x: bool(int(x))
        sqlite.converters["FOO"] = DeclTypesTests.Foo
        sqlite.converters["WRONG"] = lambda x: "WRONG"
        sqlite.converters["NUMBER"] = float

    def setUp(self):
        self.cur.execute("create table test(i int, s str, f float, b bool, u unicode, foo foo, bin blob, n1 number, n2 number(5))")

    def tearDown(self):
        self.cur.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        del sqlite.converters["FLOAT"]
        del sqlite.converters["BOOL"]
        del sqlite.converters["FOO"]
        del sqlite.converters["WRONG"]
        del sqlite.converters["NUMBER"]
        cls.cur.close()
        cls.con.close()

    @unittest.skipIf(sys.version_info[0] >= 3, "AssertionError: b'foo' != 'foo'")
    def test_CheckString(self):
        # default
        self.cur.execute("insert into test(s) values (?)", ("foo",))
        self.cur.execute('select s as "s [WRONG]" from test')
        row = self.cur.fetchone()
        self.assertEqual(row[0], "foo")

    def test_CheckSmallInt(self):
        # default
        self.cur.execute("insert into test(i) values (?)", (42,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], 42)

    def test_CheckLargeInt(self):
        # default
        num = 2**40
        self.cur.execute("insert into test(i) values (?)", (num,))
        self.cur.execute("select i from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], num)

    def test_CheckFloat(self):
        # custom
        val = 3.14
        self.cur.execute("insert into test(f) values (?)", (val,))
        self.cur.execute("select f from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], 47.2)

    def test_CheckBool(self):
        # custom
        self.cur.execute("insert into test(b) values (?)", (False,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], False)

        self.cur.execute("delete from test")
        self.cur.execute("insert into test(b) values (?)", (True,))
        self.cur.execute("select b from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], True)

    @unittest.expectedFailure # binascii.Error: decoding with 'base64' codec failed (Error: Incorrect padding)
    def test_CheckUnicode(self):
        # default
        val = u"\xd6sterreich"
        self.cur.execute("insert into test(u) values (?)", (val,))
        self.cur.execute("select u from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    @unittest.expectedFailure # binascii.Error: decoding with 'base64' codec failed (Error: Incorrect padding)
    def test_CheckFoo(self):
        val = DeclTypesTests.Foo("bla")
        self.cur.execute("insert into test(foo) values (?)", (val,))
        self.cur.execute("select foo from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], val)

    def test_CheckUnsupportedSeq(self):
        class Bar: pass
        val = Bar()
        with self.assertRaises(sqlite.InterfaceError):
            self.cur.execute("insert into test(f) values (?)", (val,))

    @unittest.skip('named paramstyle is not implemented')
    def test_CheckUnsupportedDict(self):
        class Bar: pass
        val = Bar()
        with self.assertRaises(sqlite.InterfaceError):
            self.cur.execute("insert into test(f) values (:val)", {"val": val})

    def test_CheckBlob(self):
        # default
        sample = b"Guglhupf"
        val = sample
        self.cur.execute("insert into test(bin) values (?)", (val,))
        self.cur.execute("select bin from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], sample)

    def test_CheckBlobNull(self):
        self.cur.execute("insert into test(bin) values (?)", (None,))
        self.cur.execute("select bin from test")
        row = self.cur.fetchone()
        self.assertEqual(row[0], None)

    def test_CheckNumber1(self):
        self.cur.execute("insert into test(n1) values (5)")
        value = self.cur.execute("select n1 from test").fetchone()[0]
        # if the converter is not used, it's an int instead of a float
        self.assertEqual(type(value), float)

    def test_CheckNumber2(self):
        """Checks whether converter names are cut off at '(' characters"""
        self.cur.execute("insert into test(n2) values (5)")
        value = self.cur.execute("select n2 from test").fetchone()[0]
        # if the converter is not used, it's an int instead of a float
        self.assertEqual(type(value), float)

class ColNamesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:",
            detect_types=sqlite.PARSE_COLNAMES)
        cls.cur = cls.con.cursor()
        if cls.cur.execute("pragma table_info(test)").fetchall():
            cls.cur.execute("drop table test")

        sqlite.converters["FOO"] = lambda x: "[%s]" % x.decode("ascii")
        sqlite.converters["BAR"] = lambda x: "<%s>" % x.decode("ascii")
        sqlite.converters["EXC"] = lambda x: 5/0
        sqlite.converters["B1B1"] = lambda x: "MARKER"

    def setUp(self):
        self.cur.execute("create table test(x foo)")

    def tearDown(self):
        self.cur.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        del sqlite.converters["FOO"]
        del sqlite.converters["BAR"]
        del sqlite.converters["EXC"]
        del sqlite.converters["B1B1"]
        cls.cur.close()
        cls.con.close()

    @unittest.expectedFailure # binascii.Error: decoding with 'base64' codec failed (Error: Incorrect padding)
    def test_CheckDeclTypeNotUsed(self):
        """
        Assures that the declared type is not used when PARSE_DECLTYPES
        is not set.
        """
        self.cur.execute("insert into test(x) values (?)", ("xxx",))
        self.cur.execute("select x from test")
        val = self.cur.fetchone()[0]
        self.assertEqual(val, b"xxx")

    def test_CheckNone(self):
        self.cur.execute("insert into test(x) values (?)", (None,))
        self.cur.execute("select x from test")
        val = self.cur.fetchone()[0]
        self.assertEqual(val, None)

    @unittest.expectedFailure # binascii.Error: decoding with 'base64' codec failed (Error: Incorrect padding)
    def test_CheckColName(self):
        self.cur.execute("insert into test(x) values (?)", ("xxx",))
        self.cur.execute('select x as "x [bar]" from test')
        val = self.cur.fetchone()[0]
        self.assertEqual(val, "<xxx>")

        # Check if the stripping of colnames works. Everything after the first
        # whitespace should be stripped.
        self.assertEqual(self.cur.description[0][0], "x")

    @unittest.expectedFailure # https://github.com/rqlite/pyrqlite/issues/16
    def test_CheckCaseInConverterName(self):
        self.cur.execute("select 'other' as \"x [b1b1]\"")
        val = self.cur.fetchone()[0]
        self.assertEqual(val, "MARKER")

    def test_CheckCursorDescriptionNoRow(self):
        """
        cursor.description should at least provide the column name(s), even if
        no row returned.
        """
        self.cur.execute("select * from test where 0 = 1")
        self.assertEqual(self.cur.description[0][0], "x")

    def test_CheckCursorDescriptionInsert(self):
        self.cur.execute("insert into test values (1)")
        self.assertIsNone(self.cur.description)


@unittest.skipIf(sqlite.sqlite_version_info < (3, 8, 3), "CTEs not supported")
class CommonTableExpressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:")
        cls.cur = cls.con.cursor()
        if cls.cur.execute("pragma table_info(test)").fetchall():
            cls.cur.execute("drop table test")

    def setUp(self):
        self.cur.execute("create table test(x foo)")

    def tearDown(self):
        self.cur.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.con.close()

    ## Disabled until this is resolved: https://github.com/rqlite/rqlite/issues/255
    # def test_CheckCursorDescriptionCTESimple(self):
    #     self.cur.execute("with one as (select 1) select * from one")
    #     self.assertIsNotNone(self.cur.description)
    #     self.assertEqual(self.cur.description[0][0], "1")
    #
    # def test_CheckCursorDescriptionCTESMultipleColumns(self):
    #     self.cur.execute("insert into test values(1)")
    #     self.cur.execute("insert into test values(2)")
    #     self.cur.execute("with testCTE as (select * from test) select * from testCTE")
    #     self.assertIsNotNone(self.cur.description)
    #     self.assertEqual(self.cur.description[0][0], "x")
    #
    # def test_CheckCursorDescriptionCTE(self):
    #     self.cur.execute("insert into test values (1)")
    #     self.cur.execute("with bar as (select * from test) select * from test where x = 1")
    #     self.assertIsNotNone(self.cur.description)
    #     self.assertEqual(self.cur.description[0][0], "x")
    #     self.cur.execute("with bar as (select * from test) select * from test where x = 2")
    #     self.assertIsNotNone(self.cur.description)
    #     self.assertEqual(self.cur.description[0][0], "x")


class ObjectAdaptationTests(unittest.TestCase):
    def cast(obj):
        return float(obj)
    cast = staticmethod(cast)

    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:")
        cls.cur = cls.con.cursor()
        if cls.cur.execute("pragma table_info(test)").fetchall():
            cls.cur.execute("drop table test")
        try:
            del sqlite.adapters[int]
        except:
            pass
        sqlite.register_adapter(int, ObjectAdaptationTests.cast)

    @classmethod
    def tearDownClass(cls):
        del sqlite.adapters[(int, sqlite.PrepareProtocol)]
        cls.cur.close()
        cls.con.close()

    def test_CheckCasterIsUsed(self):
        self.cur.execute("select ?", (4,))
        val = self.cur.fetchone()[0]
        self.assertEqual(type(val), float)

@unittest.skipUnless(zlib, "requires zlib")
class BinaryConverterTests(unittest.TestCase):
    def convert(s):
        return zlib.decompress(s)
    convert = staticmethod(convert)

    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:",
            detect_types=sqlite.PARSE_COLNAMES)
        if cls.con.execute("pragma table_info(test)").fetchall():
            cls.con.execute("drop table test")
        sqlite.register_converter("bin", BinaryConverterTests.convert)

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    @unittest.expectedFailure # https://github.com/rqlite/pyrqlite/issues/17
    def test_CheckBinaryInputForConverter(self):
        testdata = b"abcdefg" * 10
        compressed = zlib.compress(testdata)
        result = self.con.execute('select ? as "x [bin]"', (compressed,)).fetchone()[0]
        self.assertEqual(testdata, result)

class DateTimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = sqlite.connect(":memory:",
            detect_types=sqlite.PARSE_DECLTYPES)
        cls.cur = cls.con.cursor()
        if cls.cur.execute("pragma table_info(test)").fetchall():
            cls.cur.execute("drop table test")

    def setUp(self):
        self.cur.execute("create table test(d date, ts timestamp, dt datetime)")

    def tearDown(self):
        self.cur.execute("drop table test")

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.con.close()

    def test_CheckSqliteDate(self):
        d = sqlite.Date(2004, 2, 14)
        self.cur.execute("insert into test(d) values (?)", (d,))
        self.cur.execute("select d from test")
        d2 = self.cur.fetchone()[0]
        self.assertEqual(d, d2)

    def test_CheckSqliteTimestamp(self):
        ts = sqlite.Timestamp(2004, 2, 14, 7, 15, 0)
        self.cur.execute("insert into test(ts) values (?)", (ts,))
        self.cur.execute("select ts from test")
        ts2 = self.cur.fetchone()[0]
        self.assertEqual(ts, ts2)

    @unittest.skipIf(sqlite.sqlite_version_info < (3, 1),
                     'the date functions are available on 3.1 or later')
    def test_CheckSqlTimestamp(self):
        now = datetime.datetime.utcnow()
        self.cur.execute("insert into test(ts) values (current_timestamp)")
        self.cur.execute("select ts from test")
        ts = self.cur.fetchone()[0]
        self.assertEqual(type(ts), datetime.datetime)
        self.assertEqual(ts.year, now.year)

    def test_CheckDateTimeSubSeconds(self):
        ts = sqlite.Timestamp(2004, 2, 14, 7, 15, 0, 500000)
        self.cur.execute("insert into test(ts) values (?)", (ts,))
        self.cur.execute("select ts from test")
        ts2 = self.cur.fetchone()[0]
        self.assertEqual(ts, ts2)

    def test_CheckDateTimeSubSecondsFloatingPoint(self):
        ts = sqlite.Timestamp(2004, 2, 14, 7, 15, 0, 510241)
        self.cur.execute("insert into test(ts) values (?)", (ts,))
        self.cur.execute("select ts from test")
        ts2 = self.cur.fetchone()[0]
        self.assertEqual(ts, ts2)

    def test_CheckSqlDatetime(self):
        now = datetime.datetime.utcnow()
        self.cur.execute("insert into test(dt) values (?)", (now,))
        self.cur.execute("select dt from test")
        dt = self.cur.fetchone()[0]
        self.assertEqual(dt, now.isoformat(' ').rstrip('0').rstrip('.'))

def suite():
    loader = unittest.TestLoader()
    sqlite_type_suite = loader.loadTestsFromTestCase(SqliteTypeTests)
    decltypes_type_suite = loader.loadTestsFromTestCase(DeclTypesTests)
    colnames_type_suite = loader.loadTestsFromTestCase(ColNamesTests)
    adaptation_suite = loader.loadTestsFromTestCase(ObjectAdaptationTests)
    bin_suite = loader.loadTestsFromTestCase(BinaryConverterTests)
    date_suite = loader.loadTestsFromTestCase(DateTimeTests)
    cte_suite = loader.loadTestsFromTestCase(CommonTableExpressionTests)
    return unittest.TestSuite((sqlite_type_suite, decltypes_type_suite, colnames_type_suite, adaptation_suite, bin_suite, date_suite, cte_suite))

def main():
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())

if __name__ == "__main__":
    main()
