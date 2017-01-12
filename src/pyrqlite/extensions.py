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


converters = {
    'TEXT': str,
    'VARCHAR': lambda x: x.encode('utf-8'),
    'INTEGER': int,
    'REAL': float,
    'NULL': lambda x: None,
    'BLOB': lambda x: x.decode('base64'),
    '': lambda x: x.encode('utf-8'),
}


adapters = {
    bool: int,
    #str: lambda x: x.decode('utf-8')
}


def register_converter(type_string, value_string):
    raise NotImplementedError()


def register_adapter(type_, function):
    raise NotImplementedError()


def _convert_to_python(type_, value):
    if type_.upper() in converters:
        return converters[type_.upper()](value)
    return value


def _adapt_from_python(value):
    if isinstance(value, basestring):
        escaped = value.replace(b"'", b"''")
        adapted = (type(value)(b"'%s'")) % escaped
    else:
        if isinstance(value, numbers.Number):
            adapted = value
        else:
            adapted = adapters[type(value)](value)

    if not isinstance(adapted, basestring):
        adapted = str(adapted)

    return adapted
