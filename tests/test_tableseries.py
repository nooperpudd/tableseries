# encoding:utf-8
import os
import unittest
from datetime import datetime, timedelta

import numpy
import pandas
import pytz

from tableseries.ts import TimeSeriesDayPartition, TimeSeriesMonthPartition, TimeSeriesYearPartition


class EqualMinx(object):
    """
    """

    def prepare_dataframe(self, date, tz, columns=("value1", "value2"), length=1000, freq="S"):
        date_range = pandas.date_range(date, periods=length, freq=freq, tz=tz)
        range_array = numpy.arange(length, dtype=numpy.int64)
        random_array = numpy.random.randint(0, 100, size=length, dtype=numpy.int64)

        return pandas.DataFrame({"value1": range_array,
                                 "value2": random_array},
                                index=date_range, columns=columns)

    def assert_frame_equal(self, filter_frame, start_datetime, end_datetime=None):
        data_frame = None
        for frame in self.h5_series.get_granularity_range(self.name,
                                                          start_datetime=start_datetime,
                                                          end_datetime=end_datetime):
            if data_frame is None:
                data_frame = frame
            else:
                data_frame = data_frame.append(frame)
        data_frame.sort_index(inplace=True)

        pandas.testing.assert_frame_equal(filter_frame, data_frame)


class TableSeriesMixin(object):
    """
    """

    def test_append_data_with_table(self):
        data_frame = self.prepare_dataframe(date=self.start_datetime, length=20, freq="min", tz=pytz.UTC)
        extra_data_frame = self.prepare_dataframe(date=self.start_datetime + timedelta(minutes=20), length=10,
                                                  freq="min", tz=pytz.UTC)
        self.h5_series.append(name=self.name, data_frame=data_frame)
        self.h5_series.append(name=self.name, data_frame=extra_data_frame)

        filter_frame = data_frame.append(extra_data_frame)
        self.assert_frame_equal(filter_frame, start_datetime=self.start_datetime)

    def test_get_granularity_range_with_start_datetime(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = self.start_datetime + timedelta(days=1)

        filter_frame = self.data_frame.loc[self.data_frame.index >= start_datetime]

        self.assert_frame_equal(filter_frame, start_datetime=start_datetime)

    def test_get_granularity_range_with_start_datetime_end_datetime(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = self.start_datetime + timedelta(days=1)
        end_datetime = self.start_datetime + timedelta(days=3)
        filter_frame = self.data_frame.loc[(self.data_frame.index >= start_datetime)
                                           & (self.data_frame.index <= end_datetime)]

        self.assert_frame_equal(filter_frame, start_datetime=start_datetime, end_datetime=end_datetime)


class TableSeriesDayUnitTest(unittest.TestCase, EqualMinx, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp.h5"
        self.timezone = pytz.UTC
        # self.start_datetime = datetime(year=2018, month=1, day=1, hour=1, minute=1, second=0, tzinfo=pytz.UTC)
        self.start_datetime = datetime.now(tz=pytz.UTC)
        self.data_frame = self.prepare_dataframe(date=self.start_datetime, tz=pytz.UTC,
                                                 length=50000, freq="min")
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

    def test_get_length(self):
        """
        :return:
        """
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        length = self.h5_series.length(self.name)
        self.assertEqual(self.data_frame.shape[0], length)

    def test_append_data(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        self.assert_frame_equal(self.data_frame, start_datetime=self.start_datetime)

    def test_append_repeated_data(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        repeated_data = self.data_frame.iloc[0:10]
        self.h5_series.append(name=self.name, data_frame=repeated_data)

        self.assert_frame_equal(self.data_frame, start_datetime=self.start_datetime)

    def test_get_granularity_range_start_date_equal_end_date(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        start_datetime = self.start_datetime
        end_datetime = self.start_datetime + timedelta(hours=10)

        filter_frame = self.data_frame.loc[(self.data_frame.index >= start_datetime)
                                           & (self.data_frame.index <= end_datetime)]
        self.assert_frame_equal(filter_frame, start_datetime=self.start_datetime, end_datetime=end_datetime)

    def test_delete_by_group_name_day(self):
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

    def test_delete_by_group_name_month(self):
        """
        :return:
        """
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        self.h5_series.delete(name=self.name,
                              year=self.start_datetime.year,
                              month=self.start_datetime.month)
        date_index = self.data_frame.index
        date_list = date_index.strftime("%Y-%m-%d").to_list()
        date_list = list(set(date_list))
        result_data = []
        group_list = self.h5_series.date_groups(self.name)
        for date_item in date_list:
            date_ = datetime.strptime(date_item, "%Y-%m-%d")
            if date_.year == self.start_datetime.year and \
                    date_.month == self.start_datetime.month:
                continue
            else:
                result_data.append((
                    (date_.year, date_.month, date_.day),
                    "/{name}/y{year}/m{month:02d}/d{day:02d}".format(
                        name=self.name,
                        year=date_.year,
                        month=date_.month,
                        day=date_.day
                    )
                ))
        result_data = sorted(result_data, key=lambda x: x[0])
        self.assertListEqual(result_data, group_list)


class TableSeriesTimezoneUnitTest(unittest.TestCase, EqualMinx):
    """
    """

    def setUp(self) -> None:
        self.hdf5_file = "temp_tzinfo.h5"
        self.start_datetime = datetime(year=2018, month=1, day=1, hour=1, minute=1,
                                       second=0, tzinfo=pytz.timezone("Etc/GMT+8"))
        self.data_frame = self.prepare_dataframe(date=self.start_datetime, tz="Etc/GMT+8",
                                                 length=50, freq="min")
        self.name = "APPL"
        dtypes = [("value1", "int64"), ("value2", "int64")]

        self.h5_series = TimeSeriesDayPartition(self.hdf5_file,
                                                column_dtypes=dtypes,
                                                tzinfo=pytz.timezone("Etc/GMT+8"))

    def tearDown(self) -> None:
        self.h5_series.close()
        os.remove(self.hdf5_file)

    def test_get_range_with_timezone(self):
        self.h5_series.append(name=self.name, data_frame=self.data_frame)
        self.assert_frame_equal(self.data_frame, start_datetime=self.start_datetime)

    def test_get_with_different_timezone(self):
        """
        :return:
        """
        self.h5_series.append(name=self.name, data_frame=self.data_frame)

        start_datetime = datetime(year=2018, month=1, day=1, hour=1, minute=10,
                                  second=0, tzinfo=pytz.timezone("UTC"))
        start_date_timezone = start_datetime.astimezone(pytz.timezone("Etc/GMT+8"))
        filter_frame = self.data_frame.loc[(self.data_frame.index >= start_date_timezone)]

        self.assert_frame_equal(filter_frame, start_datetime=start_datetime)


class TableSeriesMonthUnitTest(unittest.TestCase, EqualMinx, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp_month.h5"
        self.timezone = pytz.UTC
        self.name = "APPL"
        self.start_datetime = datetime.now(tz=pytz.UTC)

        self.data_frame = self.prepare_dataframe(date=self.start_datetime,
                                                 length=2000, freq="min", tz=pytz.UTC)
        self.h5_series = TimeSeriesMonthPartition(self.hdf5_file, [("value1", "int64"), ("value2", "int64")])

    def tearDown(self) -> None:
        self.h5_series.close()
        os.remove(self.hdf5_file)


class TableSeriesYearUnitTest(unittest.TestCase, EqualMinx, TableSeriesMixin):
    """
    """

    def setUp(self):
        self.hdf5_file = "temp_year.h5"
        self.timezone = pytz.UTC
        self.name = "APPL"
        self.start_datetime = datetime.now(tz=pytz.UTC)

        self.data_frame = self.prepare_dataframe(date=self.start_datetime, length=2000, freq="min", tz=pytz.UTC)
        self.h5_series = TimeSeriesYearPartition(self.hdf5_file, [("value1", "int64"), ("value2", "int64")])

    def tearDown(self) -> None:
        self.h5_series.close()
        os.remove(self.hdf5_file)
