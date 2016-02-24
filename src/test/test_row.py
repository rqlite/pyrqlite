
from pyrqlite.row import Row


def test_row():
    row = Row()
    assert len(row) == 0
    row['foo'] = 'foo'
    row['bar'] = 'bar'
    assert row['foo'] == 'foo'
    assert row['bar'] == 'bar'
    assert row[0] == 'foo'
    assert row[1] == 'bar'

    assert len(row) == 2

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
