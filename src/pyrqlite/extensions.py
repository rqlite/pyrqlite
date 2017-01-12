from __future__ import unicode_literals

"""
SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and NULL.
And RQLite always answers 'bytes' values.

Converters transforms RQLite answers to Python native types.
Adapters transforms Python native types to RQLite-aware values.
"""

import numbers


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
}
converters = {
    'TEXT': str,
    'VARCHAR': lambda x: x.encode('utf-8'),
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


def _convert_to_python(type_, value):
    type_upper = type_.upper()
    if type_upper in converters:
        if type_upper not in _native_converters:
            value = value.decode('base64')
        return converters[type_upper](value)
    return value


def _adapt_from_python(value):
    if not isinstance(value, basestring):
        try:
            adapted = adapters[type(value)](value)
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
