# encoding:utf-8
import random
import unittest
from datetime import datetime

import numpy
import pandas
import pytz
import tables

from tableseries import TimeSeriesTable


class TableSeriesMixin(object):
    """
    """

    def prepare_dataframe(self, columns=("value1", "value2"), length=1000,
                          freq="S"):
        now = datetime.now()
        date_range = pandas.date_range(now, periods=length,
                                       freq=freq)

        range_array = numpy.arange(length)
        random_array = numpy.random.randint(0, 100, size=length)
        string_list = [random.choice(["a", "b", "c"]) for _ in range(length)]
        # combined = numpy.vstack((range_array, random_array, string_list)).T

        return pandas.DataFrame({"value1": random_array,
                                 "value2": random_array
                                 }, index=date_range, columns=columns)

    # def test_append_data_with_data_frame(self, ):
    #     name = "APPL"
    #     pass
    #
    # def test_append_data_with_data_series(self):
    #     """
    #     :return:
    #     """
    #
    # def test_add_repeated_time_index(self):
    #     pass
    #
    # def test_delete_data(self):
    #     """
    #     :return:
    #     """
    #
    # def test_delete_data_with_slice(self):
    #     """
    #     :return:
    #     """
    #
    # def test_iter_data(self):
    #     """
    #     :return:
    #     """
    #
    # def test_get_tail_data(self):
    #     """
    #     :return:
    #     """
    #
    # def test_get_length(self):
    #     """
    #     :return:
    #     """
    #     pass
    #
    # def test_get_slice_chunks(self):
    #     pass
    #
    # def test_get_slice_with_start_time(self):
    #     pass
    #
    # def test_get_slice_with_start_time_and_end_time(self):
    #     pass
    #
    # def test_get_slice_with_where_condition(self):
    #     """
    #     :return:
    #     """
    #     pass


class TableSeriesUnitTest(unittest.TestCase, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp8.h5"
        self.timezone = pytz.UTC
        self.data_frame = self.prepare_dataframe(length=10000, freq="min")
        dtypes = [("value1", "int64"), ("value2", "int64")]

        class Ttime(tables.IsDescription):
            timestamp = tables.Int64Col(pos=0)
            value1 = tables.Int64Col(pos=1)
            value2 = tables.Int64Col(pos=2)

        self.h5_series = TimeSeriesTable(self.hdf5_file, time_granularity="day", dtypes=dtypes,
                                         table_description=Ttime)

    def tearDown(self):
        self.h5_series.close()
        # os.remove(self.hdf5_file)

    def test_validate_name_exception(self):
        """
        :return:
        """
        name1 = "/fdsa"
        name2 = "1233fdsa"
        name3 = ".111"
        name4 = "11"
        self.assertRaises(ValueError, self.h5_series.append, name1, self.data_frame)
        self.assertRaises(ValueError, self.h5_series.append, name2, self.data_frame)
        self.assertRaises(ValueError, self.h5_series.append, name3, self.data_frame)
        self.assertRaises(ValueError, self.h5_series.append, name4, self.data_frame)

    def test_append_data(self):
        name = "APPL"
        # print(self.data_frame.dtypes)
        # print(self.data_frame)
        self.h5_series.append(name=name, data_frame=self.data_frame)
        print(self.h5_series.h5_store)
        # response_data = self.h5_series.get_slice(name=name)
        # print(response_data)
        # numpy.testing.assert_array_equal(response_data,)

    def test_delete_group(self):
        """
        :return:
        """
        name = "APPL"
        now = datetime.now()
        self.h5_series.delete(name, year=now.year, month=now.month, day=now.day)
    # def get_slice_chunks(self):
    #     pass
    #
    # def get_slice_with_start_timestamp(self):
    #     pass
    #
    # def get_add_repeated_dated_chunks(self):
    #     pass
    #
    # def get_slice_with_start_timestamp_end_timestamp(self):
    #     pass

# class TableSeriesMonthUnitTest(unittest.TestCase, TableSeriesMixin):
#     """
#     """
#
#     def setUp(self):
#         self.hdf5_file = "temp.h5"
#         self.timezone = pytz.UTC
#         self.data_frame = self.prepare_dataframe(100000, freq="D")
#         self.h5_series = TimeSeriesTable("temp.h5", time_granularity="month")
#
#
# class TableSeriesYearUnitTest(unittest.TestCase, TableSeriesMixin):
#     """
#     """
#
#     def setUp(self):
#         self.hdf5_file = "temp.h5"
#         self.timezone = pytz.UTC
#         self.data_frame = self.prepare_dataframe(100000, freq="min")
#         self.h5_series = TimeSeriesTable("temp.h5", time_granularity="year")
#
#
# class TableSeriesGranularityTGUnitTest(unittest.TestCase, TableSeriesMixin):
#     """
#     """
#
#     def setUp(self):
#         self.hdf5_file = "temp.h5"
#         self.timezone = pytz.UTC
#         self.columns = ["price"]
#         self.data_frame = self.prepare_dataframe(100000, self.columns)
#
#         self.dtypes = {"timestamp": "time64", "price": "int32"}
#         self.h5_series = TimeSeriesTable("temp.h5", dtypes=self.dtypes,
#                                          columns=self.columns, granularity="second")
#
#     def tearDown(self):
#         os.remove(self.hdf5_file)
