
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
        payload = self._request("GET", "/db/query?" + urlencode({'q': q}))
        try:
            for result in payload['results']:
                if 'columns' in result and 'values' in result:
                    return [dict(zip(result['columns'], row))
                        for row in result['values']]
        except KeyError:
            pass
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
        type_index = table_info
        for row in table_info:
            column_types[row['name']] = row['type'].upper()

        logging.debug('table: %s column types: %s', table_name, column_types)
        self._column_type_cache[table_name] = column_types
        return column_types

    def _request(self, method, uri, body=None, headers={}):
        debug = logging.getLogger().getEffectiveLevel() < logging.DEBUG
        conn = self.connection._connection
        logging.debug('request method: %s uri: %s headers: %s body: %s', method, uri, headers, body)
        conn.request(method, uri, body=body, headers=headers)
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

    def _get_sql_command(self, sql_str):
        return sql_str.split(maxsplit=1)[0].upper()

    def execute(self, operation, parameters=None):

        if parameters:
            operation = self._substitute_params(operation, parameters)

        id_map = None
        command = self._get_sql_command(operation)
        if command == 'SELECT':
            payload = self._request("GET", "/db/query?" + urlencode({'q': operation}))
        else:
            payload = self._request("POST", "/db/execute?transaction",
                headers={'Content-Type': 'application/json'}, body=json.dumps([operation]))

        last_insert_id = None
        rows_affected = -1
        payload_rows = {}
        try:
            results = payload["results"]
        except KeyError:
            pass
        else:
            rows_affected = 0
            for item in results:
                if 'error' in item:
                    logging.error(json.dumps(item))
                try:
                    rows_affected += item['rows_affected']
                except KeyError:
                    pass
                try:
                    last_insert_id = item['last_insert_id']
                except KeyError:
                    pass
                if 'columns' in item:
                    payload_rows = item

        try:
            fields = payload_rows['columns']
        except KeyError:
            self.description = None
            self._rows = []
            if command == 'INSERT':
                self.lastrowid = last_insert_id
        else:
            if self._connection._parse_decltypes:
                id_map = select_identifier_map(operation)
            else:
                id_map = None
            rows = []
            description = []
            types = []
            for field in fields:

                conv = None
                if id_map is None:
                    table, column = None, None
                else:
                    table, column = id_map[field]

                if table is None:
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

            try:
                values = payload_rows['values']
            except KeyError:
                pass
            else:
                for payload_row in values:
                    row = Row()
                    for i, (field, field_type) in enumerate(types):
                        v = payload_row[i]
                        # TODO: support custom converters like sqlite3 module
                        if field_type is not None:
                            conv = _get_converter(field_type)
                            if conv is not None:
                                v = conv(v)
                        row[field] = v
                    rows.append(row)
            self._rows = rows
            self.description = tuple(description)

        self.rownumber = 0
        if command == 'UPDATE':
            # sqalchemy's _emit_update_statements function asserts
            # rowcount for each update
            self.rowcount = rows_affected
        else:
            self.rowcount = len(self._rows)
        return self.rowcount

    def executemany(self, operation, seq_of_parameters=None):
        statements = []
        for parameters in seq_of_parameters:
            statements.append(self._substitute_params(operation, parameters))
        payload = self._request("POST", "/db/execute?transaction",
            headers={'Content-Type': 'application/json'},
            body=json.dumps(statements))
        rows_affected = -1
        try:
            results = payload["results"]
        except KeyError:
            pass
        else:
            rows_affected = 0
            for item in results:
                if 'error' in item:
                    logging.error(json.dumps(item))
                try:
                    rows_affected += item['rows_affected']
                except KeyError:
                    pass
        self._rows = []
        self.rownumber = 0
        self.rowcount = rows_affected

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
