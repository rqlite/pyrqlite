
from collections import OrderedDict
import json
import logging
import sqlite3

try:
    # pylint: disable=no-name-in-module
    from urllib.parse import urlencode
except ImportError:
    # pylint: disable=no-name-in-module
    from urllib import urlencode

from .exceptions import Error, InterfaceError

from .row import Row
from .extensions import _convert_to_python, _adapt_from_python, _column_stripper


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
        debug = logging.getLogger().getEffectiveLevel() < logging.DEBUG
        logging.debug(
            'request method: %s uri: %s headers: %s body: %s',
            method,
            uri,
            headers,
            body)
        response = self.connection._fetch_response(
            method, uri, body=body, headers=headers)
        logging.debug(
            "status: %s reason: %s",
            response.status,
            response.reason)
        response_text = response.read().decode('utf-8')
        logging.debug("raw response: %s", response_text)
        response_json = json.loads(
            response_text, object_pairs_hook=OrderedDict)
        if debug:
            logging.debug(
                "formatted response: %s",
                json.dumps(
                    response_json,
                    indent=4))
        return response_json

    def _substitute_params(self, operation, parameters):
        '''
        SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and NULL
        '''
        subst = []
        parts = operation.split('?')
        if len(parts) - 1 != len(parameters):
            raise InterfaceError('incorrect number of parameters (%s != %s): %s %s' %
                                   (len(parts) - 1, len(parameters), operation, parameters))
        for i, part in enumerate(parts):
            subst.append(part)
            if i < len(parameters):
                subst.append(_adapt_from_python(parameters[i]))
        return ''.join(subst)

    def _get_sql_command(self, sql_str):
        return sql_str.split(None, 1)[0].upper()

    def execute(self, operation, parameters=None):

        if parameters:
            operation = self._substitute_params(operation, parameters)

        command = self._get_sql_command(operation)
        if command in ('SELECT', 'PRAGMA'):
            payload = self._request("GET",
                                    "/db/query?" + urlencode({'q': operation}))
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
                    raise Error(json.dumps(item))
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
            rows = []
            description = []
            for field in fields:
                description.append((
                    _column_stripper(field, parse_colnames=self.connection.parse_colnames),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ))

            try:
                values = payload_rows['values']
                types = payload_rows['types']
            except KeyError:
                pass
            else:
                for payload_row in values:
                    row = Row()
                    for field, type_, value in zip(fields, types, payload_row):
                        row[field] = _convert_to_python(field, type_, value,
                                                        parse_decltypes=self.connection.parse_decltypes,
                                                        parse_colnames=self.connection.parse_colnames)
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
        return self

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
        raise NotImplementedError(self)
