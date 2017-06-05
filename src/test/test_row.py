
from pyrqlite.row import Row


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
