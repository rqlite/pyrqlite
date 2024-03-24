
from __future__ import unicode_literals

import codecs
import logging
import warnings

try:
    from http.client import HTTPConnection, HTTPSConnection
except ImportError:
    # pylint: disable=import-error
    from httplib import HTTPConnection, HTTPSConnection

try:
    from urllib.parse import urlparse
except ImportError:
    # pylint: disable=import-error
    from urlparse import urlparse

from .constants import (
    UNLIMITED_REDIRECTS,
)

from .cursors import Cursor
from ._ephemeral import EphemeralRqlited as _EphemeralRqlited
from .extensions import PARSE_DECLTYPES, PARSE_COLNAMES


class Connection(object):

    from .exceptions import (
        Warning,
        Error,
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
        NotSupportedError,
    )

    def __init__(
        self,
        scheme="http",
        host="localhost",
        port=4001,
        ssl_context=None,
        user=None,
        password=None,
        connect_timeout=DeprecationWarning,
        timeout=None,
        detect_types=0,
        max_redirects=UNLIMITED_REDIRECTS,
    ):

        self.messages = []
        self.scheme = scheme
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self._headers = {}
        if not (user is None or password is None):
            self._headers['Authorization'] = 'Basic ' + \
                codecs.encode('{}:{}'.format(user, password).encode('utf-8'),
                              'base64').decode('utf-8').rstrip('\n')

        if connect_timeout is not DeprecationWarning:
            warnings.warn(
                "connect_timeout parameter is deprecated and renamed to timeout",
                DeprecationWarning,
                stacklevel=1,
            )
            timeout = connect_timeout
        self.timeout = timeout
        self.max_redirects = max_redirects
        self.detect_types = detect_types
        self.parse_decltypes = detect_types & PARSE_DECLTYPES
        self.parse_colnames = detect_types & PARSE_COLNAMES
        self._ephemeral = None
        if scheme == ':memory:':
            self._ephemeral = _EphemeralRqlited().__enter__()
            self.host, self.port = self._ephemeral.http
        self._connection = self._init_connection()

    @property
    def connect_timeout(self):
        warnings.warn(
            "connect_timeout attribute is deprecated and renamed to timeout",
            DeprecationWarning,
            stacklevel=1,
        )
        return self.timeout

    def _init_connection(self):
        timeout = None if self.timeout is None else float(self.timeout)
        if self.scheme in ('http', ':memory:'):
            return HTTPConnection(self.host, port=self.port, timeout=timeout)
        elif self.scheme == 'https':
            return HTTPSConnection(self.host, port=self.port, context=self.ssl_context,
                                   timeout=timeout)
        else:
            raise Connection.ProgrammingError('Unsupported scheme %r' % self.scheme)

    def _retry_request(self, method, uri, body=None, headers={}):
        tries = 10
        while tries:
            tries -= 1
            try:
                self._connection.request(method, uri, body=body,
                                         headers=dict(self._headers, **headers))
                return self._connection.getresponse()
            except Exception:
                if not tries:
                    raise
                self._connection.close()
                self._connection = self._init_connection()

    def _fetch_response(self, method, uri, body=None, headers={}):
        """
        Fetch a response, handling redirection.
        """
        response = self._retry_request(method, uri, body=body, headers=headers)
        redirects = 0

        while response.status == 301 and \
                response.getheader('Location') is not None and \
                (self.max_redirects == UNLIMITED_REDIRECTS or redirects < self.max_redirects):
            redirects += 1
            uri = response.getheader('Location')
            location = urlparse(uri)

            logging.getLogger(__name__).debug("status: %s reason: '%s' location: '%s'",
                                              response.status, response.reason, uri)

            if self.host != location.hostname or self.port != location.port:
                self._connection.close()
                self.host = location.hostname
                self.port = location.port
                self._connection = self._init_connection()

            response = self._retry_request(method, uri, body=body, headers=headers)

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
        if self._ephemeral is not None:
            self._ephemeral.__exit__(None, None, None)
            self._ephemeral = None

    def __del__(self):
        self.close()

    def commit(self):
        """Database modules that do not support transactions should
        implement this method with void functionality."""
        pass

    def rollback(self):
        """This method is optional since not all databases provide
        transaction support. """
        pass

    def cursor(self, factory=None):
        """Return a new Cursor Object using the connection."""
        if factory:
            return factory(self)
        else:
            return Cursor(self)

    def execute(self, *args, **kwargs):
        return self.cursor().execute(*args, **kwargs)

    def ping(self, reconnect=True):
        if self._connection.sock is None:
            if reconnect:
                self._connection = self._init_connection()
            else:
                raise self.Error("Already closed")
        try: 
            self.execute("SELECT 1")
        except Exception:
            if reconnect:
                self._connection = self._init_connection()
                self.ping(False)
            else:
                raise
                
