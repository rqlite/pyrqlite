
try:
    # pylint: disable=no-name-in-module
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

from collections import OrderedDict


class Row(list, Mapping):

    def __init__(self):
        super(Row, self).__init__()
        self._dict = OrderedDict()

    def __setitem__(self, k, v):
        self.append(v)
        self._dict[k] = v

    def __getitem__(self, k):
        try:
            return self._dict[k]
        except KeyError:
            if isinstance(k, int):
                return list.__getitem__(self, k)
            else:
                raise

    def __iter__(self):
        return list.__iter__(self)

    def keys(self):
        for k in self._dict:
            if not isinstance(k, int):
                yield k

    def __len__(self):
        return list.__len__(self)

    def __str__(self):
        return str(self._dict)

    def __delitem__(self, k):
        raise NotImplementedError(self)

    def pop(self, k):
        raise NotImplementedError(self)
