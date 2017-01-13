from __future__ import unicode_literals

"""
SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and NULL.
And RQLite always answers 'bytes' values.

Converters transforms RQLite answers to Python native types.
Adapters transforms Python native types to RQLite-aware values.
"""

import binascii
import numbers
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


adapters = {
    float: lambda x: x,
    int: lambda x: x,
    bool: lambda x: int(x),
    unicode: lambda x: x.encode('utf-8')
}
converters = {
    'TEXT': str,
    'VARCHAR': lambda x: x.encode('utf-8'),
    'UNICODE': lambda x: x.decode('utf-8'),
    'INTEGER': int,
    'BOOL': bool,
    'FLOAT': float,
    'REAL': float,
    'NULL': lambda x: None,
    'BLOB': lambda x: x,
    '': lambda x: x.encode('utf-8'),
}

# Non-native converters will be decoded from base64 before fed into converter
_native_converters = ('BOOL', 'FLOAT', 'INTEGER', 'REAL', 'NUMBER', 'TEXT', 'VARCHAR', 'NULL', '')


def register_converter(type_string, value_string):
    raise NotImplementedError()


def register_adapter(type_, function):
    raise NotImplementedError()


def _convert_to_python(column_name, type_, value, parse_decltypes=False, parse_colnames=False):
    """
    Tries to mimic stock sqlite3 module behaviours.

    PARSE_COLNAMES have precedence over PARSE_DECLTYPES on _sqlite/cursor.c code
    """
    converter = None
    type_upper = None

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
            converter = converters[type_upper]

    if converter:
        if type_upper not in _native_converters:
            try:
                value = value.decode('base64')
            except binascii.Error:
                pass
        value = converter(value)
    elif isinstance(value, basestring):
        value = value.decode('base64')

    return value


def _adapt_from_python(value):
    if not isinstance(value, basestring):
        try:
            adapted = adapters[type(value)](value)
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
    else:
        adapted = str(adapted)

    return adapted


def _escape_string(value):
    return (type(value)(b"'%s'")) % (value.replace(b"'", b"''"))

def _column_stripper(column_name, parse_colnames=False):
    return column_name.partition(' ')[0] if parse_colnames else column_name


