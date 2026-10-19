
from collections import OrderedDict
import json
import logging
import re
from urllib.parse import urlencode

from .exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InternalError,
    OperationalError,
    ProgrammingError,
)

from .row import Row
from .extensions import _convert_to_python, _adapt_from_python, _column_stripper


# SQLite primary result codes, mapped to DB-API exception classes the
# same way CPython's sqlite3 module does it. Anything not listed is a
# DatabaseError. Codes: https://www.sqlite.org/rescode.html
_SQLITE_ERROR_CLASSES = {
    1: OperationalError,    # SQLITE_ERROR
    2: InternalError,       # SQLITE_INTERNAL
    3: OperationalError,    # SQLITE_PERM
    4: OperationalError,    # SQLITE_ABORT
    5: OperationalError,    # SQLITE_BUSY
    6: OperationalError,    # SQLITE_LOCKED
    7: MemoryError,         # SQLITE_NOMEM
    8: OperationalError,    # SQLITE_READONLY
    9: OperationalError,    # SQLITE_INTERRUPT
    10: OperationalError,   # SQLITE_IOERR
    11: DatabaseError,      # SQLITE_CORRUPT
    12: InternalError,      # SQLITE_NOTFOUND
    13: OperationalError,   # SQLITE_FULL
    14: OperationalError,   # SQLITE_CANTOPEN
    15: OperationalError,   # SQLITE_PROTOCOL
    16: OperationalError,   # SQLITE_EMPTY
    17: OperationalError,   # SQLITE_SCHEMA
    18: DataError,          # SQLITE_TOOBIG
    19: IntegrityError,     # SQLITE_CONSTRAINT
    20: IntegrityError,     # SQLITE_MISMATCH
    21: ProgrammingError,   # SQLITE_MISUSE
    25: ProgrammingError,   # SQLITE_RANGE
}


def _statement_error(item):
    """
    Build the exception for a result item carrying an "error" key.

    rqlite 9 and later also send "error_v2" with the SQLite result
    code, which selects the DB-API exception class. Older servers only
    send the message, which is reported as an OperationalError.
    """
    message = item.get('error', 'unknown error')
    try:
        code = item['error_v2']['sqlite']['code']
    except (KeyError, TypeError):
        return OperationalError(message)
    return _SQLITE_ERROR_CLASSES.get(code, DatabaseError)(message)


# A named placeholder is a colon followed by an identifier: a letter or
# underscore, then letters, digits or underscores.
_NAMED_RE = re.compile(r':([a-zA-Z_][a-zA-Z0-9_]*)')
_QMARK_RE = re.compile(r'\?')


class Cursor(object):
    arraysize = 1

    def __init__(self, connection):
        self._connection = connection
        self.messages = []
        self.lastrowid = None
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._rows = None
        self._column_type_cache = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    @property
    def connection(self):
        return self._connection

    def close(self):
        self._rows = None

    def _request(self, method, uri, body=None, headers={}):
        logger = logging.getLogger(__name__)
        debug = logger.getEffectiveLevel() < logging.DEBUG
        logger.debug(
            'request method: %s uri: %s headers: %s body: %s',
            method,
            uri,
            headers,
            body)
        response = self.connection._fetch_response(
            method, uri, body=body, headers=headers)
        logger.debug(
            "status: %s reason: %s",
            response.status,
            response.reason)
        response_text = response.read().decode('utf-8')
        if response.status != 200:
            raise OperationalError(
                "received unexpected http status: %d %s: %s" % (
                    response.status, response.reason, response_text.strip()))
        logger.debug("raw response: %s", response_text)
        response_json = json.loads(
            response_text, object_pairs_hook=OrderedDict)
        if debug:
            logger.debug(
                "formatted response: %s",
                json.dumps(
                    response_json,
                    indent=4))
        return response_json

    def _substitute_params(self, operation, parameters):
        '''
        SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and
        NULL
        '''
        qmark_matches = _QMARK_RE.findall(operation)
        named_matches = _NAMED_RE.findall(operation)
        param_matches = len(qmark_matches) + len(named_matches)

        # Matches but no parameters
        if param_matches > 0 and parameters is None:
            raise ProgrammingError('parameter required but not given: %s' %
                                   operation)

        # No regex matches and no parameters.
        if parameters is None:
            return operation

        if len(qmark_matches) > 0 and len(named_matches) > 0:
            raise ProgrammingError('different parameter types in operation not '
                                   'permitted: %s %s' %
                                   (operation, parameters))

        if isinstance(parameters, dict):
            # parameters is a dict or a dict subclass
            if len(qmark_matches) > 0:
                raise ProgrammingError('Unnamed binding used, but you supplied '
                                       'a dictionary (which has only names): '
                                       '%s %s' % (operation, parameters))

            def replace(match):
                try:
                    value = parameters[match.group(1)]
                except KeyError:
                    raise ProgrammingError('the named parameters given do not '
                                           'match operation: %s %s' %
                                           (operation, parameters))
                return _adapt_from_python(value)

            # A single regex pass substitutes each placeholder exactly
            # once, so :a cannot clobber the start of :ab.
            operation = _NAMED_RE.sub(replace, operation)
        else:
            # parameters is a sequence
            if param_matches != len(parameters):
                raise ProgrammingError('incorrect number of parameters '
                                       '(%s != %s): %s %s' % (param_matches, 
                                       len(parameters), operation, parameters))
            if len(named_matches) > 0:
                raise ProgrammingError('Named binding used, but you supplied a'
                                       ' sequence (which has no names): %s %s' %
                                       (operation, parameters))
            parts = operation.split('?')
            subst = []
            for i, part in enumerate(parts):
                subst.append(part)
                if i < len(parameters):
                    subst.append(_adapt_from_python(parameters[i]))
            operation = ''.join(subst)

        return operation

    def _get_sql_command(self, sql_str):
        parts = sql_str.split(None, 1)
        return parts[0].upper() if parts else ''

    @staticmethod
    def _execute_path(queue, wait):
        path = "/db/execute?transaction"
        if queue:
            path += "&queue"
        if wait:
            path += "&wait"
        return path

    def _handle_results(self, payload, command):
        """
        Populate the cursor from the JSON payload of a /db/query or
        /db/execute response. Raises on the first statement error.
        """
        last_insert_id = None
        rows_affected = -1
        payload_rows = {}
        results = payload.get("results")
        if results is not None:
            rows_affected = 0
            for item in results:
                if 'error' in item:
                    logging.getLogger(__name__).debug(json.dumps(item))
                    raise _statement_error(item)
                rows_affected += item.get('rows_affected', 0)
                if 'last_insert_id' in item:
                    last_insert_id = item['last_insert_id']
                if 'columns' in item:
                    payload_rows = item

        self.rownumber = 0
        if 'columns' not in payload_rows:
            # Not a result set: DDL or DML.
            self.description = None
            self._rows = []
            if command == 'INSERT':
                self.lastrowid = last_insert_id
            self.rowcount = rows_affected
            return

        fields = payload_rows['columns']
        self.description = tuple(
            (_column_stripper(field, parse_colnames=self.connection.parse_colnames),
             None, None, None, None, None, None)
            for field in fields)

        rows = []
        values = payload_rows.get('values')
        types = payload_rows.get('types')
        if values and types is not None:
            converters = [_convert_to_python(field, type_,
                          parse_decltypes=self.connection.parse_decltypes,
                          parse_colnames=self.connection.parse_colnames)
                          for field, type_ in zip(fields, types)]
            for payload_row in values:
                row = []
                for field, converter, value in zip(fields, converters, payload_row):
                    row.append((field, (value if converter is None
                                        else converter(value))))
                rows.append(Row(row))
        self._rows = rows
        self.rowcount = len(rows)

    def execute(self, operation, parameters=None, queue=False, wait=False, consistency=None):
        if not isinstance(operation, str):
            raise ValueError(
                "argument must be a string, not '{}'".format(type(operation).__name__))

        operation = self._substitute_params(operation, parameters)

        command = self._get_sql_command(operation)
        if command in ('SELECT', 'PRAGMA'):
            # POST the statement in the body rather than in the URL.
            # Parameters are inlined client-side, so a large text
            # parameter can push a GET past the server's request
            # header limit; a body has no such limit.
            path = "/db/query"
            if consistency:
                path += "?" + urlencode({"level": consistency})
            payload = self._request("POST", path,
                                    headers={'Content-Type': 'application/json'},
                                    body=json.dumps([operation]))
        else:
            payload = self._request("POST", self._execute_path(queue, wait),
                                    headers={'Content-Type': 'application/json'},
                                    body=json.dumps([operation]))

        self._handle_results(payload, command)
        return self

    def executemany(self, operation, seq_of_parameters=None, queue=False, wait=False):
        if not isinstance(operation, str):
            raise ValueError(
                "argument must be a string, not '{}'".format(type(operation).__name__))

        statements = []
        for parameters in seq_of_parameters:
            statements.append(self._substitute_params(operation, parameters))

        payload = self._request("POST", self._execute_path(queue, wait),
                                headers={'Content-Type': 'application/json'},
                                body=json.dumps(statements))
        self._handle_results(payload, self._get_sql_command(operation))

    def _remaining(self):
        """
        Number of rows not yet fetched. This is based on the rows
        actually held, not on rowcount, which for UPDATE and DELETE
        reports rows affected rather than rows returned.
        """
        if self._rows is None:
            return 0
        return max(len(self._rows) - self.rownumber, 0)

    def fetchone(self):
        ''' Fetch the next row '''
        if not self._remaining():
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        remaining = self.arraysize if size is None else size
        remaining = min(remaining, self._remaining())
        return [self.fetchone() for i in range(remaining)]

    def fetchall(self):
        return [self.fetchone() for i in range(self._remaining())]

    def setinputsizes(self, sizes):
        raise NotImplementedError(self)

    def setoutputsize(self, size, column=None):
        raise NotImplementedError(self)

    def scroll(self, value, mode='relative'):
        raise NotImplementedError(self)

    def next(self):
        raise NotImplementedError(self)

    def __iter__(self):
        while self._remaining():
            yield self.fetchone()
