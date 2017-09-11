
from __future__ import unicode_literals

import time

from .constants import (
    UNLIMITED_REDIRECTS,
)

from .connections import Connection
connect = Connection

from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DataError,
    DatabaseError,
    OperationalError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    ProgrammingError,
)

from .types import (
    Binary,
    Date,
    Time,
    Timestamp,
    STRING,
    BINARY,
    NUMBER,
    DATETIME,
    ROWID,
)

# Compat with native sqlite module
from .extensions import converters, adapters, register_converter, register_adapter
from sqlite3.dbapi2 import PrepareProtocol


paramstyle = "qmark"

threadsafety = 1

apilevel = "2.0"


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

# accessed by sqlalchemy sqlite dialect
sqlite_version_info = (3, 10, 0)

# Compat with native sqlite module
from .extensions import PARSE_DECLTYPES, PARSE_COLNAMES
