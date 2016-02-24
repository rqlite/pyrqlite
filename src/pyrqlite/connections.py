
try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection

try:
    # pylint: disable=no-name-in-module
    from urllib.parse import urlencode
except ImportError:
    # pylint: disable=no-name-in-module
    from urllib import urlencode

from .constants import (
    PARSE_DECLTYPES,
)

from .cursors import Cursor

class Connection(object):

    def __init__(self, host=None, port=None, connect_timeout=None,
        detect_types=0):

        self.messages = []
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self._connection = HTTPConnection(host, port=port,
            timeout=None if self.connect_timeout is None else float(self.connect_timeout))
        self._parse_decltypes = bool(detect_types & PARSE_DECLTYPES)

    def close(self):
        """Close the connection now (rather than whenever .__del__() is
        called).

        The connection will be unusable from this point forward; an
        Error (or subclass) exception will be raised if any operation
        is attempted with the connection. The same applies to all
        cursor objects trying to use the connection. Note that closing
        a connection without committing the changes first will cause an
        implicit rollback to be performed."""
        self._connection.close()

    def commit(self):
        """Database modules that do not support transactions should
        implement this method with void functionality."""
        pass

    def rollback(self):
        """This method is optional since not all databases provide
        transaction support. """
        pass

    def cursor(self):
        """Return a new Cursor Object using the connection."""
        return Cursor(self)
