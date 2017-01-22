
import logging

try:
    from http.client import HTTPConnection
except ImportError:
    # pylint: disable=import-error
    from httplib import HTTPConnection

try:
    from urllib.parse import urlparse
except ImportError:
    # pylint: disable=import-error
    from urlparse import urlparse

from .constants import (
    UNLIMITED_REDIRECTS,
)

from .cursors import Cursor
from .extensions import PARSE_DECLTYPES, PARSE_COLNAMES


class Connection(object):

    def __init__(self, host='localhost', port=4001, connect_timeout=None,
                 detect_types=0, max_redirects=UNLIMITED_REDIRECTS):

        self.messages = []
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.max_redirects = max_redirects
        self.detect_types = detect_types
        self.parse_decltypes = detect_types & PARSE_DECLTYPES
        self.parse_colnames = detect_types & PARSE_COLNAMES
        self._connection = self._init_connection()

    def _init_connection(self):
        return HTTPConnection(self.host, port=self.port,
                              timeout=None if self.connect_timeout is None else float(self.connect_timeout))

    def _fetch_response(self, method, uri, body=None, headers={}):
        """
        Fetch a response, handling redirection.
        """
        self._connection.request(method, uri, body=body, headers=headers)
        response = self._connection.getresponse()
        redirects = 0

        while response.status == 301 and \
                response.getheader('Location') is not None and \
                (self.max_redirects == UNLIMITED_REDIRECTS or redirects < self.max_redirects):
            redirects += 1
            uri = response.getheader('Location')
            location = urlparse(uri)

            logging.debug("status: %s reason: '%s' location: '%s'",
                          response.status, response.reason, uri)

            if self.host != location.hostname or self.port != location.port:
                self._connection.close()
                self.host = location.hostname
                self.port = location.port
                self._connection = self._init_connection()

            self._connection.request(method, uri, body=body, headers=headers)
            response = self._connection.getresponse()

        return response

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

    def execute(self, *args, **kwargs):
        with self.cursor() as cursor:
            return cursor.execute(*args, **kwargs)

