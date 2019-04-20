# encoding:utf-8
import os
import unittest
from datetime import datetime

import pandas
import pytz
import numpy
import tables


from tableseries import TimeSeriesTable


class TableSeriesMixin(object):
    """
    """
    def prepare_dataframe(self, columns=("values",),length=1000 , timezone=pytz.UTC,
                          freq="S"):
        now = datetime.now()
        date_range = pandas.date_range(now, periods=length,
                                       freq=freq, tz=timezone)

        return pandas.DataFrame([i + 1 for i in range(len(date_range))],
                                index=date_range, columns=columns)

    def test_append_data_with_data_frame(self,):
        name = "APPL"
        pass

    def test_append_data_with_data_series(self):
        """
        :return:
        """
    def test_add_repeated_time_index(self):
        pass

    def test_delete_data(self):
        """
        :return:
        """
    def test_delete_data_with_slice(self):
        """
        :return:
        """
    def test_iter_data(self):
        """
        :return:
        """
    def test_get_tail_data(self):
        """
        :return:
        """
    def test_get_length(self):
        """
        :return:
        """
        pass

    def test_get_slice_chunks(self):
        pass

    def test_get_slice_with_start_time(self):
        pass

    def test_get_slice_with_start_time_and_end_time(self):
        pass

    def test_get_slice_with_where_condition(self):
        """
        :return:
        """
        pass


class TableSeriesHourUnitTest(unittest.TestCase, TableSeriesMixin):
    """
    """
    def setUp(self):
        self.hdf5_file = "temp.h5"
        self.timezone = pytz.UTC
        self.data_frame = self.prepare_dataframe(100000, freq="S")

        self.h5_series = TimeSeriesTable("temp.h5", time_granularity="Day" )

    # def tearDown(self):
    #     os.remove(self.hdf5_file)

    def test_append_data(self):
        name = "APPL"
        self.h5_series.append(name=name, data_frame=self.data_frame)
        response_data = self.h5_series.get_slice(name=name)
        print(response_data)
        # numpy.testing.assert_array_equal(response_data,)

    def get_slice_chunks(self):
        pass

    def get_slice_with_start_timestamp(self):
        pass

    def get_add_repeated_dated_chunks(self):
        pass

    def get_slice_with_start_timestamp_end_timestamp(self):
        pass


class TableSeriesDayUnitTest(unittest.TestCase,TableSeriesMixin):
    """
    """
    pass

class TableSeriesMonthUnitTest(unittest.TestCase,TableSeriesMixin):
    """
    """

class TableSeriesYearUnitTest(unittest.TestCase,TableSeriesMixin):
    """
    """
    pass





class TableSeriesGranularityTGUnitTest(unittest.TestCase, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp.h5"
        self.timezone = pytz.UTC
        self.columns = ["price"]
        self.data_frame = self.prepare_dataframe(100000, self.columns)

        self.dtypes = {"timestamp": "time64", "price": "int32"}
        self.h5_series = TimeSeriesTable("temp.h5", dtypes=self.dtypes,
                                         columns=self.columns, granularity="second")

    def tearDown(self):
        os.remove(self.hdf5_file)

