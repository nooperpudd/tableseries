import sys
import threading

import pandas
import pytz
import tables

import tableseries.utils
import tableseries.utils


class TimeSeriesTable(object):
    """
    """

    def __init__(self, filename, dtypes,columns=None,
                 index_name="timestamp",
                 table_name="data",
                 timezone=pytz.UTC,
                 compress_level=5,
                 chunks_size=100000,in_memory=False, granularity="second"):
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
        comp_filter = tables.Filters(complevel=compress_level,
                                     complib="blosc")

        if in_memory:

            self.h5file = tables.open_file(filename=filename, mode="a",
                                           driver=driver,
                                           driver_core_backing_store=0,
                                           filters=comp_filter)
        else:
            self.h5file = tables.open_file(filename=filename, mode="a",
                                           driver=driver,
                                           filters=comp_filter)

        self.h5file.attrs.dtypes = dtypes
        self.timezone = timezone
        self._lock = threading.RLock()

        self.index_name = index_name
        self.dtypes = dtypes
        self.columns = columns
        self._root = self.h5file.root
        self.chunks_size = chunks_size
        self.time_granularity = granularity
        self.table_name = table_name

    def attribute(self, name=None):
        return {}

    @property
    def groups(self):
        """
        :return:
        """
        groups = []
        for item in self.h5file.walk_groups(self._root):
            groups.append(item)
        return groups

    def _get_group(self, name):
        """
        :param name:
        :return:
        """
        return self.h5file.get_node(self._root, name=name,
                                    classname="Group")

    def _get_table(self, name):
        """
        :param name:
        :return:
        """
        if self.time_granularity in ["second", "minute"]:
            pass
        else:
            sub_group = self._get_group(name)

            return self.h5file.get_node(sub_group, name=self.table_name,
                                        classname="Table")

    def get_max_timestamp(self, name):
        if self.time_granularity in ["second", "minute"]:
            pass
        else:
            data_table = self._get_table(name)
            return data_table.cols[self.index_name][data_table.colindexes[self.index_name][-1]]

    def get_min_timestamp(self, name):
        if self.time_granularity in ["second", "minute"]:
            pass
        else:
            data_table = self._get_table(name)

            return data_table.cols[self.index_name][data_table.colindexes[self.index_name][0]]

    def append(self, name, data):

        if not isinstance(data, pandas.DataFrame):
            raise TypeError("data parameter's type must be a pandas.DataFrame")
        if not isinstance(data.index, pandas.DatetimeIndex):
            raise TypeError("DataFrame index must be pandas.DateTimeIndex type")

        data_frame = data.sort_index()

        datetime_index = data_frame.index
        max_datetime = data_frame.idxmin()
        min_datetime = data_frame.idxmax()
        

        self.h5file.flush()

    def get_slice(self, name, start_datetime=None, end_datetime=None, limit=10000):

        if start_datetime:
            start_datetime = tableseries.utils.parser_datetime_to_timestamp(start_datetime)
        if end_datetime:
            end_datetime = tableseries.utils.parser_datetime_to_timestamp(end_datetime)
        if start_datetime > end_datetime:
            raise ValueError("end_datetime must > start_datetime")

    def length(self, name):
        pass

    def iter_data(self, name, chunks=None):
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
            col.create_csindex()  # create completely sorted index

    def _add_group_name(self, name, start_datetime, end_datetime):
        """
        :param name:
        :param start_datetime:
        :param end_datetime:
        :return:
        """
        if name not in self.h5file:
            key_group = self.h5file.create_group(self._root, name=name)
        else:
            key_group = self.h5file[name]

        # aggregration timestamp with time granularity
        if self.time_granularity in ["second", "minute"]:
            # group as monthly
            date_range = self._partition_date(start_datetime, end_datetime)
            for year in date_range:
                year_group = self.h5file.create_group(key_group, year)
                months = date_range[year]
                for month in months:
                    month_group = self.h5file.create_group(year_group, month)

                    data_table = self.h5file.create_table(month_group, name=self.table_name, description=self.dtypes)

                    # to create index on index column
                    self._create_index(data_table)

                    yield data_table

        else:
            data_table = self.h5file.create_table(key_group, name=self.table_name, description=self.dtypes)

            self._create_index(data_table)

            yield data_table

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
