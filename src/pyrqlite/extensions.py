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

def _adapt_bytes(value):
    # Use byte array for the params API
    if not isinstance(value, bytes):
        value = value.encode('utf-8')
    return list(value)

if sys.version_info[0] >= 3:
    def _adapt_datetime(val):
        return val.isoformat(" ")
else:
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


# Adapters only need to adapt Python values before JSON serialization for the rqlite API.
# Types that are already serialized in an acceptable way by json.JSONEncoder without an adapter
# are not included anymore.
adapters = {
    bytes: _adapt_bytes,
    datetime.date: _adapt_date,
    datetime.datetime: _adapt_datetime,
}
_default_adapters = adapters.copy()

# Registered converters when PARSE_DECLTYPES or PARSE_COLNAMES is applicable
converters = {
    'UNICODE': functools.partial(_null_wrapper, lambda x: x.decode('utf-8')),
    'BOOL': functools.partial(_null_wrapper, bool),
    'FLOAT': functools.partial(_null_wrapper, float),
    'DATE': functools.partial(_null_wrapper, _convert_date),
    'TIMESTAMP': functools.partial(_null_wrapper, _convert_timestamp),
}
# Default converters are always applied for primitive types
_default_converters = {
    'INTEGER': functools.partial(_null_wrapper, int),
    'REAL': functools.partial(_null_wrapper, float),
    'BLOB': lambda x: x,
    'NULL': lambda x: None,
    'DATE': lambda x: x.split('T')[0] if x is not None else None,
    'DATETIME': lambda x: x.replace('T', ' ').rstrip('Z') if x is not None else None,
    'TIMESTAMP': lambda x: x.replace('T', ' ').rstrip('Z') if x is not None else None,
}

# Non-native converters will be decoded from base64 before fed into converter
_native_converters = ('BOOL', 'FLOAT', 'INTEGER', 'REAL', 'NUMBER', 'NULL', 'DATE', 'DATETIME', 'TIMESTAMP')

# SQLite TEXT affinity: https://www.sqlite.org/datatype3.html
_text_affinity_re = re.compile(r'CHAR|CLOB|TEXT')


def register_converter(type_string, function):
    converters[type_string.upper()] = function


def register_adapter(type_, function):
    adapters[type_] = function


def _convert_to_python(column_name, type_, parse_decltypes=False, parse_colnames=False):
    """
    Tries to mimic stock sqlite3 module behaviours.

    PARSE_COLNAMES have precedence over PARSE_DECLTYPES on _sqlite/cursor.c code
    """
    converter = None
    type_upper = None

    # if type_ is blank try to infer the type from the column_name
    # e.g. q="select 3.0" -> type='' column_name='3.0' value=3
    if type_ == '':
        if column_name.isdigit():
            type_ = 'int'
        elif all([slice.isdigit() for slice in column_name.partition('.')[::2]]):   # 3.14
            type_ = 'real'

    # Try default converters first based on the type_ parameter
    if type_:
        type_upper = type_.upper()
        if type_upper in _default_converters:
            converter = _default_converters[type_upper]

        # If parse_decltypes is enabled, a registered converter based on type_ takes precedence
        if parse_decltypes:
            ## From: https://github.com/python/cpython/blob/c72b6008e0578e334f962ee298279a23ba298856/Modules/_sqlite/cursor.c#L167
            # /* Converter names are split at '(' and blanks.
            #  * This allows 'INTEGER NOT NULL' to be treated as 'INTEGER' and
            #  * 'NUMBER(10)' to be treated as 'NUMBER', for example.
            #  * In other words, it will work as people expect it to work.*/
            type_upper = type_upper.partition('(')[0].partition(' ')[0]
            if type_upper in converters:
                converter = converters[type_upper]

    # If parse_colnames is enabled, a registered converter based on column name takes precedence
    if parse_colnames:
        if '[' in column_name and ']' in column_name:
            type_from_col = column_name.upper().partition('[')[-1].partition(']')[0]
            if type_from_col:
                type_upper = type_from_col.upper()
                if type_upper in converters:
                    converter = converters[type_upper]

    if converter:
        if type_upper not in _native_converters:
            # If the converter is not a native one, it will be decoded from base64
            # before being passed to the converter
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
        adapter_key = type(value)
        # Use default adapter first, then check for registered adapters
        adapter = _default_adapters.get(adapter_key)
        adapter = adapters.get(adapter_key, adapter)
        if adapter:
            return adapter(value)

    return value

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
