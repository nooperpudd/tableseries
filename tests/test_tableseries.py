# encoding:utf-8
import os
import unittest
from datetime import datetime, timedelta

import numpy
import pandas
import pytz

from tableseries import TimeSeriesDayPartition


class TableSeriesMixin(object):
    """
    """

    def prepare_dataframe(self, date, columns=("value1", "value2"), length=1000,
                          freq="S"):
        date_range = pandas.date_range(date, periods=length, freq=freq)

        range_array = numpy.arange(length)
        random_array = numpy.random.randint(0, 100, size=length)

        return pandas.DataFrame({"value1": range_array,
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


class TableSeriesDayUnitTest(unittest.TestCase, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp8.h5"
        self.timezone = pytz.UTC
        self.start_datetime = datetime(year=2018, month=1, day=1, hour=1, minute=1, second=0)
        self.data_frame = self.prepare_dataframe(date=self.start_datetime, length=5000, freq="min")
        self.name = "APPL"
        dtypes = [("value1", "int64"), ("value2", "int64")]

        self.h5_series = TimeSeriesDayPartition(self.hdf5_file, column_dtypes=dtypes)

    def tearDown(self):
        self.h5_series.close()
        os.remove(self.hdf5_file)

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
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        data_frame = None

        for frame in self.h5_series.get_granularity_range(name=self.name, start_datetime=self.start_datetime):
            if data_frame is not None:
                data_frame = data_frame.append(frame)
            else:
                data_frame = frame
        pandas.testing.assert_frame_equal(self.data_frame, data_frame)

    def test_append_repeated_data(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        repeated_data = self.data_frame.iloc[0:10]
        self.h5_series.append(name=self.name, data_frame=repeated_data)
        data_frame = None
        for frame in self.h5_series.get_granularity_range(name=self.name, start_datetime=self.start_datetime):
            if data_frame is not None:
                data_frame = data_frame.append(frame)
            else:
                data_frame = frame
        pandas.testing.assert_frame_equal(self.data_frame, data_frame)

    def test_delete_by_group_name(self):
        """
        :return:
        """
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        self.h5_series.delete(name=self.name,
                              year=self.start_datetime.year,
                              month=self.start_datetime.month,
                              day=self.start_datetime.day)
        delete_date = self.start_datetime.date()
        groups = self.h5_series.date_groups(name=self.name)
        date_tuple = [((delete_date.year, delete_date.month, delete_date.day),
                       "/" + self.name + delete_date.strftime("/y%Y/m%m/d%d"))]
        self.assertNotIn(date_tuple, groups)

    def test_get_granularity_by_date(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = datetime(self.start_datetime.year, self.start_datetime.month, self.start_datetime.day,
                                  hour=0, minute=0, second=0)
        end_datetime = datetime(self.start_datetime.year, self.start_datetime.month, self.start_datetime.day,
                                hour=23, minute=59, second=59)
        filter_frame = self.data_frame.loc[(self.data_frame.index >= start_datetime)
                                           & (self.data_frame.index <= end_datetime)]
        print("Fdsfafsafsfa")
        result_frame = self.h5_series.get_granularity(self.name, iterable=False, year=self.start_datetime.year,
                                                      month=self.start_datetime.month, day=self.start_datetime.day)
        # pandas.testing.assert_frame_equal(filter_frame, result_frame)

    def test_get_granularity_range_with_start_datetime_end_datetime(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = self.start_datetime + timedelta(days=1)
        end_datetime = self.start_datetime + timedelta(days=3)
        data_frame = None
        filter_frame = self.data_frame.loc[(self.data_frame.index >= start_datetime)
                                           & (self.data_frame.index <= end_datetime)]
        for frame in self.h5_series.get_granularity_range(name=self.name,
                                                          start_datetime=start_datetime,
                                                          end_datetime=end_datetime):
            if data_frame is None:
                data_frame = frame
            else:
                data_frame = data_frame.append(frame)

        pandas.testing.assert_frame_equal(filter_frame, data_frame)

    def test_get_granularity_range_with_start_datetime(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = self.start_datetime + timedelta(days=2)
        data_frame = None
        filter_frame = self.data_frame.loc[self.data_frame.index >= start_datetime]
        for frame in self.h5_series.get_granularity_range(name=self.name,
                                                          start_datetime=start_datetime):
            if data_frame is None:
                data_frame = frame
            else:
                data_frame = data_frame.append(frame)
        pandas.testing.assert_frame_equal(filter_frame, data_frame)

    def test_get_granularity_range_start_date_equal_end_date(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = self.start_datetime
        end_datetime = self.start_datetime + timedelta(hours=10)
        data_frame = None
        filter_frame = self.data_frame.loc[(self.data_frame.index >= start_datetime)
                                           & (self.data_frame.index <= end_datetime)]
        for frame in self.h5_series.get_granularity_range(name=self.name,
                                                          start_datetime=start_datetime,
                                                          end_datetime=end_datetime):
            if data_frame is None:
                data_frame = frame
            else:
                data_frame = data_frame.append(frame)
        pandas.testing.assert_frame_equal(filter_frame, data_frame)

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
