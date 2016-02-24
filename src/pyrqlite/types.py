
import datetime
import sys

Binary = bytes
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime

# pylint: disable=undefined-variable
STRING = str if sys.version_info[0] >= 3 else unicode
BINARY = bytes
NUMBER = float
DATETIME = Timestamp
# pylint: disable=undefined-variable
ROWID = int if sys.version_info[0] >= 3 else long
