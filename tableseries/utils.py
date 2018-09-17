from datetime import datetime

import dateutil


def parser_datetime_to_timestamp(_datetime):
    if isinstance(_datetime, datetime):
        return _datetime.timestamp()
    else:
        _datetime = dateutil.parser.parse(_datetime)
        return _datetime.timestamp()
