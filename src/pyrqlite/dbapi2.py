
import time

from .constants import (
    UNLIMITED_REDIRECTS,
)

from .connections import Connection as connect

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
