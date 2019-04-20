from datetime import datetime

import dateutil


def parser_datetime_to_timestamp(_datetime):
    """
    :param _datetime:
    :return:
    """
    if isinstance(_datetime, datetime):
        return _datetime.timestamp()
    elif isinstance(_datetime, str):
        _datetime = dateutil.parser.parse(_datetime)
        return _datetime.timestamp()
    else:
        return _datetime
