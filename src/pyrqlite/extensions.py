from __future__ import unicode_literals

"""
SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and NULL.
And RQLite always answers 'bytes' values.

Converters transforms RQLite answers to Python native types.
Adapters transforms Python native types to RQLite-aware values.
"""

import codecs
import datetime
import functools
import re
import sqlite3
import sys

from .exceptions import InterfaceError

if sys.version_info[0] >= 3:
    basestring = str
    unicode = str

PARSE_DECLTYPES = 1
PARSE_COLNAMES = 2


def _decoder(conv_func):
    """ The Python sqlite3 interface returns always byte strings.
        This function converts the received value to a regular string before
        passing it to the receiver function.
    """
    return lambda s: conv_func(s.decode('utf-8'))

if sys.version_info[0] >= 3:

    def _escape_string(value):
        if isinstance(value, bytes):
            return "X'{}'".format(
                codecs.encode(value, 'hex').decode('utf-8'))

        return "'{}'".format(value.replace("'", "''"))

    def _adapt_datetime(val):
        return val.isoformat(" ")
else:

    def _escape_string(value):
        if isinstance(value, bytes):
            try:
                value = value.decode('utf-8')
            except UnicodeDecodeError:
                # Encode as a BLOB literal containing hexadecimal data
                return "X'{}'".format(
                    codecs.encode(value, 'hex').decode('utf-8'))

        return "'{}'".format(value.replace("'", "''"))

    def _adapt_datetime(val):
        return val.isoformat(b" ")

def _adapt_date(val):
    return val.isoformat()

def _convert_date(val):
    return datetime.date(*map(int, val.split('T')[0].split("-")))

def _convert_timestamp(val):
    datepart, timepart = val.split("T")
    year, month, day = map(int, datepart.split("-"))
    timepart_full = timepart.strip('Z').split(".")
    hours, minutes, seconds = map(int, timepart_full[0].split(":"))
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1]))
    else:
        microseconds = 0

    val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)
    return val


def _null_wrapper(converter, value):
    if value is not None:
        value = converter(value)
    return value


adapters = {
    bytes: lambda x: x,
    float: lambda x: x,
    int: lambda x: x,
    bool: int,
    unicode: lambda x: x.encode('utf-8'),
    type(None): lambda x: None,
    datetime.date: _adapt_date,
    datetime.datetime: _adapt_datetime,

}
adapters = {(type_, sqlite3.PrepareProtocol): val for type_, val in adapters.items()}
_default_adapters = adapters.copy()

converters = {
    'UNICODE': functools.partial(_null_wrapper, lambda x: x.decode('utf-8')),
    'INTEGER': functools.partial(_null_wrapper, int),
    'BOOL': functools.partial(_null_wrapper, bool),
    'FLOAT': functools.partial(_null_wrapper, float),
    'REAL': functools.partial(_null_wrapper, float),
    'NULL': lambda x: None,
    'BLOB': lambda x: x,
    'DATE': functools.partial(_null_wrapper, _convert_date),
    'DATETIME': lambda x: x.replace('T', ' ').rstrip('Z'),
    'TIMESTAMP': functools.partial(_null_wrapper, _convert_timestamp),
}

# Non-native converters will be decoded from base64 before fed into converter
_native_converters = ('BOOL', 'FLOAT', 'INTEGER', 'REAL', 'NUMBER', 'NULL', 'DATE', 'DATETIME', 'TIMESTAMP')

# SQLite TEXT affinity: https://www.sqlite.org/datatype3.html
_text_affinity_re = re.compile(r'CHAR|CLOB|TEXT')


def register_converter(type_string, function):
    converters[type_string.upper()] = function


def register_adapter(type_, function):
    adapters[(type_, sqlite3.PrepareProtocol)] = function


def _convert_to_python(column_name, type_, parse_decltypes=False, parse_colnames=False):
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
            converter = functools.partial(_decode_base64_converter, converter)
    elif not type_upper or _text_affinity_re.search(type_upper):
        # Python's sqlite3 module has a text_factory attribute which
        # returns unicode by default.
        pass
    else:
        converter = _conditional_string_decode_base64

    return converter


def _adapt_from_python(value):
    if not isinstance(value, basestring):
        adapter_key = (type(value), sqlite3.PrepareProtocol)
        adapter = adapters.get(adapter_key)
        try:
            if adapter is None:
                # Fall back to _default_adapters, so that ObjectAdaptationTests
                # teardown will correctly restore the default state.
                adapter = _default_adapters[adapter_key]
        except KeyError as e:
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
            adapted = adapter(value)
    else:
        adapted = value

    # The adapter could had returned a string
    if isinstance(adapted, (bytes, unicode)):
        adapted = _escape_string(adapted)
    elif adapted is None:
        adapted = 'NULL'
    else:
        adapted = str(adapted)

    return adapted

def _column_stripper(column_name, parse_colnames=False):
    return column_name.partition(' ')[0] if parse_colnames else column_name

def _decode_base64_converter(converter, value):
    if value is not None:
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        value = converter(codecs.decode(value, 'base64'))
    return value

def _conditional_string_decode_base64(value):
    if isinstance(value, basestring):
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        value = codecs.decode(value, 'base64')
    return value
