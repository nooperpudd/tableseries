# import pytest
import datetime

import numpy
import pandas


def prepare_dataframe(periods,freq="S"):
    """
    :return:
    """
    columns = ["value1", "value2", "value3"]
    start_date = datetime.datetime.now()
    index = pandas.date_range(start_date, periods=periods, freq=freq)
    values = numpy.random.rand(periods, len(columns))
    data_frame = pandas.DataFrame(data=values, index=index, columns=columns)
    return data_frame



def test_read_data():
    """
    :return:
    """


def test_search_data():
    """
    :return:
    """


def test_append_data():
    """
    :return:
    """
