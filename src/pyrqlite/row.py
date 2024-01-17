
try:
    # pylint: disable=no-name-in-module
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

from collections import OrderedDict


class Row(tuple, Mapping):

    def __new__(cls, items):
        return tuple.__new__(cls, (item[1] for item in items))

    def __init__(self, items):
        super(Row, self).__init__()
        self._items = tuple(items)
        # If the same column name appears more than once then only the
        # value for the first occurence is indexed (see __getitem__).
        d = {}
        for k, v in self._items:
            d.setdefault(k, v)
        self._dict = d

    def __getitem__(self, k):
        """
        If the same column name appears more than once then this returns
        the value from the first matching column just like sqlite3.Row.
        """
        try:
            return self._dict[k]
        except (KeyError, TypeError):
            if isinstance(k, (int, slice)):
                return tuple.__getitem__(self, k)
            else:
                raise

    def __iter__(self):
        """
        Return an iterator over the values of the row.

        View contains all values.
        """
        return tuple.__iter__(self)

    def items(self):
        """
        Return a new view of the row’s items ((key, value) pairs).

        View contains all items, multiple items can have the same key.
        """
        for item in self._items:
            yield item

    def values(self):
        """
        Return a new view of the rows’s values.

        View contains all values.
        """
        for item in self._items:
            yield item[1]

    def keys(self):
        """
        Returns a list of column names which are not necessarily
        unique, just like sqlite3.Row.
        """
        return [item[0] for item in self._items]

    def __len__(self):
        return tuple.__len__(self)

    def __delitem__(self, k):
        raise NotImplementedError(self)

    def pop(self, k):
        raise NotImplementedError(self)
