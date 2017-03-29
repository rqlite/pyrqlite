from __future__ import unicode_literals

"""
SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and NULL.
And RQLite always answers 'bytes' values.

Converters transforms RQLite answers to Python native types.
Adapters transforms Python native types to RQLite-aware values.
"""

import binascii
import datetime
import numbers
import re
import sqlite3

from .exceptions import InterfaceError


PARSE_DECLTYPES = 1
PARSE_COLNAMES = 2


def _decoder(conv_func):
    """ The Python sqlite3 interface returns always byte strings.
        This function converts the received value to a regular string before
        passing it to the receiver function.
    """
    return lambda s: conv_func(s.decode('utf-8'))


def _adapt_date(val):
    return val.isoformat()

def _adapt_datetime(val):
    return val.isoformat(b" ")

def _convert_date(val):
    return datetime.date(*map(int, val.split('T')[0].split("-")))

def _convert_timestamp(val):
    datepart, timepart = val.split("T")
    year, month, day = map(int, datepart.split("-"))
    timepart_full = timepart.strip('Z').split(".")
    hours, minutes, seconds = map(int, timepart_full[0].split(":"))
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1].decode()))
    else:
        microseconds = 0

    val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)
    return val


adapters = {
    float: lambda x: x,
    int: lambda x: x,
    bool: int,
    unicode: lambda x: x.encode('utf-8'),
    type(None): lambda x: None,
    datetime.date: _adapt_date,
    datetime.datetime: _adapt_datetime,

}
adapters = {(type_, sqlite3.PrepareProtocol): val for type_, val in adapters.items()}

converters = {
    'UNICODE': lambda x: x.decode('utf-8'),
    'INTEGER': int,
    'BOOL': bool,
    'FLOAT': float,
    'REAL': float,
    'NULL': lambda x: None,
    'BLOB': lambda x: x,
    'DATE': _convert_date,
    'TIMESTAMP': _convert_timestamp,
    # For PRAGMA results, the go-sqlite3 SQLiteRows.DeclTypes method
    # returns empty strings for all of the types, but the rows can
    # contain int or None values that need to pass through here.
    '': lambda x: x.encode('utf-8') if hasattr(x, 'encode') else x,
}

# Non-native converters will be decoded from base64 before fed into converter
_native_converters = ('BOOL', 'FLOAT', 'INTEGER', 'REAL', 'NUMBER', 'NULL', 'DATE', 'TIMESTAMP', '')

# SQLite TEXT affinity: https://www.sqlite.org/datatype3.html
_text_affinity_re = re.compile(r'CHAR|CLOB|TEXT')


def register_converter(type_string, function):
    converters[type_string.upper()] = function


def register_adapter(type_, function):
    adapters[(type_, sqlite3.PrepareProtocol)] = function


def _convert_to_python(column_name, type_, value, parse_decltypes=False, parse_colnames=False):
    """
    Tries to mimic stock sqlite3 module behaviours.

    PARSE_COLNAMES have precedence over PARSE_DECLTYPES on _sqlite/cursor.c code
    """
    converter = None
    type_upper = None

    if type_ == '':     # q="select 3.0" -> type='' column_name='3.0' value=3
        if column_name.isdigit():
            type_ = 'int'
        elif all([slice.isdigit() for slice in column_name.partition('.')[::2]]):   # 3.14
            type_ = 'real'

    if '[' in column_name and ']' in column_name and parse_colnames:
        type_upper = column_name.upper().partition('[')[-1].partition(']')[0]
        if type_upper in converters:
            converter = converters[type_upper]

    if not converter:
        type_upper = type_.upper()
        if parse_decltypes:
            ## From: https://github.com/python/cpython/blob/c72b6008e0578e334f962ee298279a23ba298856/Modules/_sqlite/cursor.c#L167
            # /* Converter names are split at '(' and blanks.
            #  * This allows 'INTEGER NOT NULL' to be treated as 'INTEGER' and
            #  * 'NUMBER(10)' to be treated as 'NUMBER', for example.
            #  * In other words, it will work as people expect it to work.*/
            type_upper = type_upper.partition('(')[0].partition(' ')[0]
        if type_upper in converters:
            if type_upper in _native_converters or parse_decltypes:
                converter = converters[type_upper]

    if converter:
        if type_upper not in _native_converters:
            value = value.decode('base64')
        value = converter(value)
    elif _text_affinity_re.search(type_upper):
        # Python's sqlite3 module has a text_factory attribute which
        # returns unicode by default.
        pass
    elif isinstance(value, basestring):
        value = value.decode('base64')

    return value


def _adapt_from_python(value):
    if not isinstance(value, basestring):
        try:
            adapted = adapters[(type(value), sqlite3.PrepareProtocol)](value)
        except KeyError, e:
            # No adapter registered. Let the object adapt itself via PEP-246.
            # It has been rejected by the BDFL, but is still implemented
            # on stdlib sqlite3 module even on Python 3 !!
            if hasattr(value, '__adapt__'):
                adapted = value.__adapt__(sqlite3.PrepareProtocol)
            elif hasattr(value, '__conform__'):
                adapted = value.__conform__(sqlite3.PrepareProtocol)
            else:
                raise InterfaceError(e)
    else:
        adapted = value

    # The adapter could had returned a string
    if isinstance(adapted, basestring):
        adapted = _escape_string(adapted)
    elif adapted is None:
        adapted = 'NULL'
    else:
        adapted = str(adapted)

    return adapted


def _escape_string(value):
    return (type(value)(b"'%s'")) % (value.replace(b"'", b"''"))

def _column_stripper(column_name, parse_colnames=False):
    return column_name.partition(' ')[0] if parse_colnames else column_name


