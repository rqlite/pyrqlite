
from collections import OrderedDict
import json
import logging

try:
    # pylint: disable=no-name-in-module
    from urllib.parse import urlencode
except ImportError:
    # pylint: disable=no-name-in-module
    from urllib import urlencode

import re
import sqlparse

from .exceptions import ProgrammingError

from .row import Row

from .sql import select_identifier_map

from .types import (
    NUMBER,
    ROWID,
)

# https://www.sqlite.org/datatype3.html#affname
_AFFINITY = re.compile(
    r'(?P<int>INT)|'
    #r'(?P<text>TEXT|CHAR|CLOB)|'
    #r'(?P<blob>BLOB)'
    r'(?P<real>REAL|FLOA|DOUB)',
    #r'(?P<bool>BOOL)'
    re.VERBOSE
)

_convert_cache = {}

def _get_converter(column_type):
    """Get a converter for the specified column_type."""
    try:
        return _convert_cache[column_type]
    except KeyError:
        pass

    converter = None

    m = _AFFINITY.search(column_type)
    if m is not None:
        if m.group('int') is not None:
            converter = ROWID
        elif m.group('real') is not None:
            converter = NUMBER

    _convert_cache[column_type] = converter

    return converter


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

    def _escape_string(self, s):
        return "'%s'" % s.replace("'", "''")

    def _get_table_info(self, table_name):
        q = "PRAGMA table_info(%s)" % \
            self._escape_string(table_name)
        payload = self._request("GET", "/db?explain&" + urlencode({'q': q}))
        if payload['rows']:
            return payload['rows']
        return None

    def _get_table_column_types(self, table_name):
        try:
            return self._column_type_cache[table_name]
        except KeyError:
            pass
        table_info = self._get_table_info(table_name)
        if table_info is None:
            self._column_type_cache[table_name] = None
            return None
        column_types = {}
        for row in table_info:
            column_types[row['name']] = row['type'].upper()

        logging.debug('table: %s column types: %s', table_name, column_types)
        self._column_type_cache[table_name] = column_types
        return column_types

    def _request(self, method, uri, body=None):
        debug = logging.getLogger().getEffectiveLevel() < logging.DEBUG
        conn = self.connection._connection
        conn.request(method, uri, body=body)
        response = conn.getresponse()
        logging.debug("status: %s reason: %s", response.status, response.reason)
        response_text = response.read().decode('utf-8')
        logging.debug("raw response: %s", response_text)
        response_json = json.loads(response_text, object_pairs_hook=OrderedDict)
        if debug:
            logging.debug("formatted response: %s", json.dumps(response_json, indent=4))
        return response_json

    def _substitute_params(self, operation, parameters):
        subst = []
        parts = operation.split('?')
        if len(parts) - 1 != len(parameters):
            raise ProgrammingError('incorrect number of parameters (%s != %s): %s %s' %
                (len(parts) - 1, len(parameters), operation, parameters))
        for i, part in enumerate(parts):
            subst.append(part)
            if i < len(parameters):
                if isinstance(parameters[i], int):
                    subst.append("{}".format(parameters[i]))
                else:
                    subst.append(self._escape_string(parameters[i]))
        return ''.join(subst)

    def execute(self, operation, parameters=None):

        if parameters:
            operation = self._substitute_params(operation, parameters)

        # Since rqlite json responses have the column keys in random
        # order, use sqlparse decide how to order the columns in the
        # returned row lists.
        statements = sqlparse.parse(operation)
        if len(statements) != 1:
            raise ProgrammingError('more than one statement: {}'.format(operation))
        id_map = None
        command = statements[0].tokens[0].value.upper()
        if command == 'SELECT':
            # TODO: add rqlite support to preserve column order
            # https://github.com/otoolep/rqlite/issues/51
            id_map = select_identifier_map(statements[0])
            payload = self._request("GET", "/db?explain&" + urlencode({'q': operation}))
        else:
            payload = self._request("POST", "/db?explain&transaction", body=operation)

        if payload.get('failures'):
            for failure in payload['failures']:
                logging.error(json.dumps(failure))

        payload_rows = payload.get('rows')
        if id_map:
            parse_decltypes = self._connection._parse_decltypes
            rows = []
            description = []
            types = []
            for field, (table, column) in id_map.items():

                conv = None
                if table is None or not parse_decltypes:
                    # SELECT CAST('test plain returns' AS VARCHAR(60)) AS anon_1
                    field_type = None
                else:
                    column_types = self._get_table_column_types(table)
                    field_type = column_types[column]
                    conv = _get_converter(field_type)

                types.append((field, field_type))

                description.append((
                    field,
                    conv,
                    None,
                    None,
                    None,
                    None,
                    None,
                ))

            for payload_row in payload_rows:
                row = Row()
                for field, field_type in types:
                    v = payload_row[field]
                    # TODO: support custom converters like sqlite3 module
                    if field_type is not None:
                        conv = _get_converter(field_type)
                        if conv is not None:
                            v = conv(v)
                    row[field] = v
                rows.append(row)
            self._rows = rows
            self.description = tuple(description)
        else:
            self.description = None
            self._rows = []
            if command == 'INSERT':
                # TODO: add rqlite support to do this in the same transaction as the insert
                # https://github.com/otoolep/rqlite/issues/50
                payload = self._request("GET", "/db?explain&" + urlencode({'q': "SELECT last_insert_rowid() as rowid"}))
                self.lastrowid = int(payload['rows'][0]['rowid'])
        self.rownumber = 0
        if command == 'UPDATE':
            # sqalchemy's _emit_update_statements function asserts
            # rowcount for each update
            self.rowcount = 1
            # TODO: add rqlite support to return the update rowcount
            # https://github.com/otoolep/rqlite/issues/4
            #payload = self._request("GET", "/db?explain&" + urlencode({'q': "SELECT changes() as changes"}))
            #self.rowcount = int(payload['rows'][0]['changes'])
        else:
            self.rowcount = len(self._rows)
        return self.rowcount

    def executemany(self, operation, seq_of_parameters=None):
        statements = []
        for parameters in seq_of_parameters:
            statements.append(self._substitute_params(operation, parameters))
        payload = self._request("POST", "/db?explain&transaction", body='; '.join(statements))
        if payload.get('failures'):
            for failure in payload['failures']:
                logging.error(json.dumps(failure))
        self._rows = []
        self.rownumber = 0
        self.rowcount = -1
        #payload = self._request("GET", "/db?explain&" + urlencode({'q': "SELECT changes() as changes"}))
        #self.rowcount = int(payload['rows'][0]['changes'])

    def fetchone(self):
        ''' Fetch the next row '''
        if self._rows is None or self.rownumber >= len(self._rows):
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        raise NotImplementedError(self)

    def fetchall(self):
        rows = []
        while self.rownumber < self.rowcount:
            rows.append(self.fetchone())
        return rows

    def setinputsizes(self, sizes):
        raise NotImplementedError(self)

    def setoutputsize(self, size, column=None):
        raise NotImplementedError(self)

    def scroll(self, value, mode='relative'):
        raise NotImplementedError(self)

    def next(self):
        raise NotImplementedError(self)

    def __iter__(self):
        return self
