import functools
import sys
import threading

import numpy
import pandas
import pytz
import tables

import tableseries.utils


class TimeSeriesTable(object):
    """
    """

    def __init__(self, filename, dtypes, columns=None,
                 index_name="timestamp",
                 table_name="data",
                 timezone=pytz.UTC,
                 compress_level=5,
                 chunks_size=100000, in_memory=False, granularity="second"):
        """
        :param filename:
        :param dtypes:
        :param granularity:
        :param index_name:
        :param timezone:
        :param compress_level:
        :param chunks_size:
        :param columns:
        :param in_memory:
        """

        # http://www.pytables.org/cookbook/inmemory_hdf5_files.html

        # driver
        #   * H5FD_SEC2: this driver uses POSIX file-system functions like read
        #   and write to perform I/O to a single, permanent file on local
        #   disk with no system buffering.
        #   This driver is POSIX-compliant and is the default file driver for
        #   all systems.
        #
        # * H5FD_DIRECT: this is the H5FD_SEC2 driver except data is written
        #   to or read from the file synchronously without being cached by
        #   the system.
        #
        # * H5FD_WINDOWS: this driver was modified in HDF5-1.8.8 to be a
        #   wrapper of the POSIX driver, H5FD_SEC2. This change should not
        #   affect user applications.
        #
        # * H5FD_STDIO: this driver uses functions from the standard C
        #   stdio.h to perform I/O to a single, permanent file on local disk
        #   with additional system buffering.
        #
        # * H5FD_CORE: with this driver, an application can work with a file
        #   in memory for faster reads and writes. File contents are kept in
        #   memory until the file is closed. At closing, the memory version
        #   of the file can be written back to disk or abandoned.
        #
        # * H5FD_SPLIT: this file driver splits a file into two parts.
        #   One part stores metadata, and the other part stores raw data.
        #   This splitting a file into two parts is a limited case of the
        #   Multi driver.
        if in_memory:
            driver = "H5FD_CORE"
        else:
            if sys.platform == "win32":
                driver = "H5FD_WINDOWS"
            else:
                driver = "H5FD_SEC2"
        if granularity not in ["second", "minute", "hour",
                               "day", "week", "month"]:
            raise ValueError("granularity values must in 'second','minute','hour','day','week','month'")

        # https://github.com/kiyo-masui/bitshuffle
        # tips set up the compress with
        self.comp_filter = tables.Filters(complevel=compress_level,
                                     complib="blosc")

        if in_memory:

            self.h5file = tables.open_file(filename=filename, mode="a",
                                           driver=driver,
                                           driver_core_backing_store=0,
                                           filters=self.comp_filter)
        else:
            self.h5file = tables.open_file(filename=filename, mode="a",
                                           driver=driver,
                                           filters=self.comp_filter)

        # self.h5file.attrs.dtypes = dtypes
        self.timezone = timezone
        self._lock = threading.RLock()

        self.index_name = index_name
        self.dtypes = dtypes
        self.columns = columns

        self._root = self.h5file.root

        self.chunks_size = chunks_size
        self.time_granularity = granularity
        self.table_name = table_name

    @property
    def attribute(self):
        """
        :return:
        """
        return self.h5file.attrs

    @property
    def groups(self):
        """
        :return:
        """
        groups = []
        for item in self.h5file.walk_groups(self._root):
            groups.append(item)
        return groups

    @functools.lru_cache(maxsize=2048)
    def _get_or_create_parent_group(self, name):
        """
        :param name:
        :return:
        """
        if name in self._root:
            # get group node
            return self.h5file.get_node(self._root, name=name,
                                        classname="Group")
        else:
            return self.h5file.create_group(self._root, name=name)

    @functools.lru_cache(maxsize=2048)
    def _get_or_create_time_partition_group(self):
        """
        :return:
        """
        pass

    @functools.lru_cache(maxsize=2048)
    def _get_or_create_table(self, name, start_dt=None, end_dt=None):
        """
        :param name:
        :return:
        """
        parent_group = self._get_or_create_parent_group(name)

        print(self.time_granularity)
        # aggregration timestamp with time granularity
        if self.time_granularity in ["second", "minute"]:

            # group as monthly
            date_range = self._partition_date(start_dt, end_dt)
            for year in date_range:
                year_group = self.h5file.create_group(parent_group, year)
                months = date_range[year]
                for month in months:
                    month_group = self.h5file.create_group(year_group, month)

                    data_table = self.h5file.create_table(month_group, name=self.table_name, description=self.dtypes)

                    # to create index on index column
                    self._create_index(data_table)

                    return data_table

        else:
            print("sfdsfdfsa")

            print(parent_group)
            print(parent_group)

            if self.table_name in parent_group:

                return self.h5file.get_node(parent_group,name=self.table_name,classname="Table")
            else:
                data_table = self.h5file.create_table(parent_group, name=self.table_name,
                                                      description=self.dtypes)

                self._create_index(data_table)
                return data_table

        # if self.time_granularity in ["second", "minute"]:
        #     pass
        # else:
        #     print(parent_group)
        #
        #     return self.h5file.get_node(parent_group, name=self.table_name,
        #                                 classname="Table")

    def get_max_timestamp(self, name):

        data_table = self._get_or_create_table(name)
        return data_table.cols[self.index_name][data_table.colindexes[self.index_name][-1]]

    def get_min_timestamp(self, name):

        data_table = self._get_or_create_table(name)

        return data_table.cols[self.index_name][data_table.colindexes[self.index_name][0]]

    def _validate_append_data(self, data_frame):
        """
        validate repeated index
        :return:
        """
        date_index = data_frame.index
        unique_date = date_index[date_index.duplicated()].unique()
        if not unique_date.empty:
            raise ValueError("DataFrame index can't contains duplicated index data")

    def append(self, name, data):

        # validate data frame
        if not isinstance(data, pandas.DataFrame):
            raise TypeError("data parameter's type must be a pandas.DataFrame")
        if not isinstance(data.index, pandas.DatetimeIndex):
            raise TypeError("DataFrame index must be pandas.DateTimeIndex type")

        data_frame = data.sort_index()

        # check timestamp repeated
        self._validate_append_data(data_frame)

        datetime_index = data_frame.index
        max_datetime = data_frame.idxmax()[0].timestamp()
        min_datetime = data_frame.idxmin()[0].timestamp()

        if self.time_granularity in ["second", "minute"]:
            pass
        else:
            exist_timestamps = self.get_slice(name, start_datetime=min_datetime,
                                              end_datetime=max_datetime,
                                              field=self.index_name)
            if exist_timestamps.size > 0:
                self._validate_append(datetime_index, exist_timestamps)

            data_table = self._get_or_create_table(name)
            print(data_table)

            print(len(data_table))
            print(data_frame.values)
            print(data_frame.dtypes)
            data_table.append(data_frame.values)
            data_table.flush()

    def _validate_append(self, data_index, compare_index):
        """
        :return:
        """
        if data_index and compare_index:
            results = numpy.intersect1d(data_index, compare_index)
            if results:
                raise IndexError("duplicated index insert")

    def get_slice(self, name, start_datetime=None, end_datetime=None, limit=0, field=None):

        if start_datetime:
            start_datetime = tableseries.utils.parser_datetime_to_timestamp(start_datetime)
        if end_datetime:
            end_datetime = tableseries.utils.parser_datetime_to_timestamp(end_datetime)

        if start_datetime > end_datetime:
            raise ValueError("end_datetime must > start_datetime")

        if self.time_granularity in ["second", "minute"]:
            pass
        else:
            if start_datetime and end_datetime is None:
                where_filter = "( {index_name} >= {start_dt} )".format(index_name=self.index_name,
                                                                       start_dt=start_datetime)
            elif start_datetime is None and end_datetime:
                where_filter = "( {index_name} <= {end_dt} )".format(index_name=self.index_name,
                                                                     end_dt=end_datetime)
            else:
                where_filter = '( {index_name} >= {start_dt} ) & ( {index_name} <= {end_dt} )'.format(
                    index_name=self.index_name,
                    start_dt=start_datetime,
                    end_dt=end_datetime
                )
            data_table = self._get_or_create_table(name)
            response_data = data_table.read_where(where_filter, field=field)
            print("fdsafdfaf",response_data)
            return response_data

    def length(self, name):
        """
        :param name:
        :return:
        """
        if self.time_granularity in ["second", "minute"]:
            pass
        else:
            data_table = self._get_table(name)
            return len(data_table)

    def iter_data(self, name, start_datetime=None, end_datetime=None, chunks=None):
        """
        :param name:
        :param chunks:
        :return:
        """
        if chunks:
            pass
        else:
            pass
        pass

    def delete(self, name, start_datetime=None, end_datetime=None):
        pass

        self.h5file.flush()

    def close(self):
        """
        :return:
        """
        self.h5file.close()

    def _create_index(self, data_table):

        if hasattr(data_table.cols, self.index_name):
            col = getattr(data_table.cols, self.index_name)
            col.create_csindex(filters=self.comp_filter)  # create completely sorted index

    def _create_table(self, parent_group, start_datetime, end_datetime):
        """
        :param name:
        :param start_datetime:
        :param end_datetime:
        :return:
        """
        # if name not in self.h5file:
        #     key_group = self.h5file.create_group(self._root, name=name)
        # else:
        #     key_group = self.h5file[name]

        print(self.time_granularity)
        # aggregration timestamp with time granularity
        if self.time_granularity in ["second", "minute"]:

            # group as monthly
            date_range = self._partition_date(start_datetime, end_datetime)
            for year in date_range:
                year_group = self.h5file.create_group(parent_group, year)
                months = date_range[year]
                for month in months:
                    month_group = self.h5file.create_group(year_group, month)

                    data_table = self.h5file.create_table(month_group, name=self.table_name, description=self.dtypes)

                    # to create index on index column
                    self._create_index(data_table)

                    yield data_table

        else:
            print("sfdsfdfsa")

            data_table = self.h5file.create_table(parent_group, name=self.table_name, description=self.dtypes)

            self._create_index(data_table)

            return data_table

    def _fetch_group(self):
        """
        :return:
        """
        pass

    def _partition_date(self, start_date, end_date):
        """
        :param start_date:
        :param end_date:
        :return:
        """
        date_range = {}
        diff_years = end_date.year - start_date.year
        if diff_years == 0:
            months = [start_date.month + i for i in range(end_date.month - start_date.month)]
            date_range[start_date] = months
        else:
            for diff in diff_years:
                if start_date.year + diff != end_date.year:
                    months = [start_date.month + i for i in range(12 - start_date.month)]
                    date_range[start_date.year + diff] = months
                else:
                    months = [end_date.month + i for i in range(end_date.month - 1)]
                    date_range[start_date.year + diff] = months

        return date_range
