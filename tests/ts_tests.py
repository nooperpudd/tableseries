# encoding:utf-8
import os
import unittest
from datetime import datetime

import pandas
import pytz

from tableseries import TimeSeriesTable


class TableSeriesMixin(object):
    """
    """
    def prepare_dataframe(self, length, columns, timezone=pytz.UTC,
                          freq="second"):
        now = datetime.now()

        date_range = pandas.date_range(now, periods=length,
                                       freq=freq, tz=timezone)

        return pandas.DataFrame([i + 1 for i in range(len(date_range))],
                                index=date_range, columns=columns)

    def append_data(self):
        name = "APPL"

        pass

    def get_slice_chunks(self):
        pass

    def get_slice_with_start_timestamp(self):
        pass

    def get_add_repeated_dated_chunks(self):
        pass

    def get_slice_with_start_timestamp_end_timestamp(self):
        pass


class TableSeriesUnitTest(unittest.TestCase, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp.h5"
        self.timezone = pytz.UTC
        self.columns = ["price"]
        self.data_frame = self.prepare_dataframe(100000, self.columns, freq="hour")
        self.dtypes = {"timestamp": "time64", "price": "int32"}
        self.h5_series = TimeSeriesTable("temp.h5", dtypes=self.dtypes,
                                         columns=self.columns, granularity="hour")

    def tearDown(self):
        os.remove(self.hdf5_file)

    def append_data(self):
        name = "APPL"

        self.h5_series.append(name=name,data=self.data_frame)


    def get_slice_chunks(self):
        pass

    def get_slice_with_start_timestamp(self):
        pass

    def get_add_repeated_dated_chunks(self):
        pass

    def get_slice_with_start_timestamp_end_timestamp(self):
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


class TableSeriesInMemeryTestCase(unittest.TestCase, TableSeriesMixin):

    def setUp(self):
        self.hdf5_file = "temp.h5"
        self.timezone = pytz.UTC
        self.columns = ["price"]
        self.data_frame = self.prepare_dataframe(100000, self.columns, freq="hour")
        self.dtypes = {"timestamp": "time64", "price": "int32"}
        self.h5_series = TimeSeriesTable("temp.h5", dtypes=self.dtypes,
                                         columns=self.columns, granularity="hour",
                                         in_memory=True)

    def tearDown(self):
        os.remove(self.hdf5_file)
