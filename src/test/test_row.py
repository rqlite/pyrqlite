import unittest

import pyrqlite.dbapi2 as sqlite
from pyrqlite.row import Row


class NonUniqueColumnsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = sqlite.connect(":memory:")

    def setUp(self):
        """
        Create tables to join with some columns that have the same names,
        to demonstrated rows with non-unique column names.
        """
        self.cu = self.cx.cursor()
        self.cu.execute("create table tbl1(id integer primary key, name text)")
        self.cu.execute("insert into tbl1(id, name) values (1, 'foo')")
        self.cu.execute(
            "create table tbl2(id integer primary key, tbl1id integer, name text)"
        )
        self.cu.execute("insert into tbl2(id, tbl1id, name) values (2, 1, 'bar')")

    def tearDown(self):
        self.cu.close()

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()
        del cls.cx

    def testJoin(self):
        """
        Test a join that demonstrates rows with non-unique column names.
        """
        self.cu.execute("select * from tbl1 inner join tbl2 on tbl2.tbl1id = tbl1.id")
        row = self.cu.fetchone()
        self.assertEqual(tuple(row.keys()), ("id", "name", "id", "tbl1id", "name"))
        self.assertEqual(tuple(row.values()), (1, "foo", 2, 1, "bar"))
        self.assertEqual(row["name"], "foo")
        self.assertEqual(row["id"], 1)
        self.assertEqual(row["tbl1id"], 1)


def test_row():
    row = Row([('foo', 'foo'), ('bar', 'bar')])
    assert len(row) == 2
    assert row['foo'] == 'foo'
    assert row['bar'] == 'bar'
    assert row[0] == 'foo'
    assert row[1] == 'bar'

    try:
        row[2]
    except IndexError:
        pass
    else:
        assert False

    try:
        row['non-existent']
    except KeyError:
        pass
    else:
        assert False


def test_row_with_non_unique_columns():
    items = [("foo", "foo"), ("bar", "bar"), ("foo", "bar")]
    row = Row(items)
    assert len(row) == 3

    assert row["foo"] == "foo"
    assert row["bar"] == "bar"
    assert row[0] == "foo"
    assert row[1] == "bar"
    assert row[2] == "bar"
    assert list(row.items()) == items
    assert list(row.values()) == [item[1] for item in items]
