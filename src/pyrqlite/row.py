
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
        self._dict = OrderedDict(items)

    def __getitem__(self, k):
        try:
            return self._dict[k]
        except (KeyError, TypeError):
            if isinstance(k, (int, slice)):
                return tuple.__getitem__(self, k)
            else:
                raise

    def __iter__(self):
        return tuple.__iter__(self)

    def keys(self):
        for k in self._dict:
            if not isinstance(k, int):
                yield k

    def __len__(self):
        return tuple.__len__(self)

    def __str__(self):
        return str(self._dict)

    def __delitem__(self, k):
        raise NotImplementedError(self)

    def pop(self, k):
        raise NotImplementedError(self)
