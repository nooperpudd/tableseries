import pytest
import datetime

import numpy
import pandas
import tableseries

def prepare_dataframe(length, freq="S"):
    """
    :return:
    """
    columns = ["value1", "value2", "value3"]
    start_date = datetime.datetime.now()
    index = pandas.date_range(start_date, periods=length, freq=freq)
    values = numpy.random.rand(length, len(columns))
    data_frame = pandas.DataFrame(data=values, index=index, columns=columns)
    return data_frame

@pytest.fixture(scope="module")
@pytest.fixture()
def h5_series_day():
    columns_dtype = [("value1","i8"),("value2","i8"),("value3","i8")]
    hdf5_series = tableseries.TableSeries("day","hdf5_day.h5",columns_dtype,
                                          compress_level=0)
    yield hdf5_series
    hdf5_series.close()

@pytest.fixture(scope="module",params=[(5,""),(9,"")])
def h5_series_month(request):
    columns_dtype = [("value1", "i8"), ("value2", "i8"), ("value3", "i8")]
    hdf5_series = tableseries.TableSeries("month", "hdf5_month.h5", columns_dtype,
                                          compress_level=request.param[0])
    yield hdf5_series
    hdf5_series.close()

@pytest.fixture
def h5_series_year():
    pass


def test_add_data():
    pass

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
