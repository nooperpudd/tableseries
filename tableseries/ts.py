import re
import sys
import threading
from datetime import datetime
from decimal import Decimal, ROUND_HALF_DOWN

import numpy
import pandas
import pytz
import tables

from .compare import DateCompare


def round_timestamp(timestamp):
    """
    :return:
    """
    timestamp = Decimal(timestamp)
    timestamp = timestamp.quantize(Decimal(".000001"), rounding=ROUND_HALF_DOWN)
    return int(timestamp * Decimal(1e9))


class TableSeriesError(Exception):
    pass


class TableBase(object):
    """
    http://www.pytables.org/cookbook/threading.html
    https://www.pytables.org/usersguide/optimization.html?highlight=bitshuffle
    """
    DATE_FORMAT = None
    FREQ = None
    GROUP_REGEX = None

    NUMBER_REGEX = re.compile(r"(\d+)")
    NAME_REGEX = re.compile(r'^([a-zA-Z]+)([0-9]*)$')

    def __init__(self, filename, column_dtypes, index_name="timestamp",
                 complib="blosc:blosclz",
                 in_memory=False,
                 compress_level=5,
                 bitshuffle=False,
                 tzinfo=pytz.UTC):
        """
        :param filename:
        :param column_dtypes:
        :param complib:
        :param index_name:
        :param compress_level:
        :param bitshuffle:
        :param in_memory:
        """
        self._lock = threading.RLock()
        if in_memory:
            driver = "H5FD_CORE"
        else:
            if sys.platform == "win32":
                driver = "H5FD_WINDOWS"
            else:
                driver = "H5FD_SEC2"

        if isinstance(tzinfo, str):
            self.tzinfo = pytz.timezone(tzinfo)
        else:
            self.tzinfo = tzinfo

        self.filters = tables.Filters(complevel=compress_level,
                                      complib=complib,
                                      bitshuffle=bitshuffle)

        self.h5_store = tables.open_file(filename=filename, mode="a",
                                         driver=driver)
        # self.h5_store = tables.open_file(filename=filename, mode="a",
        # driver=driver, filters=self.filters)

        self.index_name = index_name
        # index int64
        self._column_dtypes = column_dtypes

        # pytable table datatype.
        self._convert_dtypes = numpy.dtype([(index_name, "<i8")] + column_dtypes)
        self._table_description = self._convert_dtypes

    def length(self, name):
        """
        :param name:
        :return:
        """
        path = "/" + name
        total_length = 0
        for table_node in self.h5_store.walk_nodes(path, classname="Table"):
            total_length += table_node.nrows
        return total_length

    def _validate_name(self, name):
        """
        # validate group name in the group path
        :return:
        """
        if not self.NAME_REGEX.match(name):
            raise ValueError("name must match ^([a-zA-Z]+)([0-9]*)$ regex")

    def _get_or_create_group(self, where, group_path):
        """
        get or create group
        :param where:
        :param group_path
        :return:
        """
        if group_path in self.h5_store.get_node(where):
            return self.h5_store.get_node(where=where, name=group_path)
        else:
            return self.h5_store.create_group(where=where, name=group_path)

    def _create_group_path(self, group_path, root="/"):
        """
        create group
        :return:
        """
        group_list = group_path.split("/")
        # groups have "" in the list
        group_list.pop(0)
        start_group = root
        for group in group_list:
            self._get_or_create_group(start_group, group)
            start_group = start_group + "/" + group

    def _create_index(self, data_table, index_name):
        """
        create table index
        :param data_table:
        :param index_name:
        :return:
        """
        if hasattr(data_table.cols, index_name):
            col = getattr(data_table.cols, index_name)
            # create completely sorted index
            col.create_csindex()

    def _get_or_create_table(self, name, parent_group_path):
        """
        :param name: table name
        :return:
        """
        table_path = parent_group_path + "/" + name
        if table_path in self.h5_store:
            data_table = self.h5_store.get_node(where=parent_group_path, name=name)
        else:
            data_table = self.h5_store.create_table(parent_group_path, name=name, description=self._table_description)
        return data_table

    def _partition_date_frame(self, date_frame):
        """
        :return:
        """
        for date_key, frame in date_frame.groupby(pandas.Grouper(freq=self.FREQ)):
            yield (date_key, frame)

    def delete(self, name, year=None, month=None, day=None):
        """
        :param self:
        :param name:
        :param year:
        :param month:
        :param day:
        :return:
        """
        self._validate_name(name)
        root = "/"
        if year and month and day:
            path = root + name + "/y{year}/m{month:02d}".format(year=year,
                                                                month=month)
            node = "d{day:02d}".format(day=day)
        elif year and month:
            path = root + name + "/y{year}".format(year=year)
            node = "m{month:02d}".format(month=month)
        elif year:
            path = root + name
            node = "y{year}".format(year=year)
        else:
            path = root
            node = name
        self.h5_store.remove_node(path, name=node, recursive=True)
        self.h5_store.flush()

    def _check_repeated(self, name, data_frame):
        """
        :param data_frame:
        :return:
        """
        data_frame.sort_index(inplace=True)
        datetime_index = data_frame.index

        max_datetime = datetime_index.max()
        min_datetime = datetime_index.min()
        max_datetime = max_datetime.to_pydatetime()
        min_datetime = min_datetime.to_pydatetime()

        for filter_frame in self.get_granularity_range(name, min_datetime, max_datetime):
            if filter_frame is not None and not filter_frame.empty:
                data_frame = data_frame.drop(filter_frame.index)
        return data_frame

    def date_groups(self, name):
        """
        :param name:
        :return:
        """
        root_path = "/" + name
        return self._walk_groups(root_path, self.GROUP_REGEX)

    def _validate_datetime(self, start_datetime, end_datetime):
        """
        :param start_datetime:
        :param end_datetime:
        :return:
        """
        if start_datetime.tzinfo is None:
            start_datetime = self.tzinfo.localize(start_datetime)
        elif start_datetime.tzinfo != self.tzinfo:
            start_datetime = start_datetime.astimezone(self.tzinfo)

        start_date = start_datetime.date()
        end_date = None
        if end_datetime:
            if end_datetime.tzinfo is None:
                end_datetime = self.tzinfo.localize(end_datetime)
            elif end_datetime.tzinfo != self.tzinfo:
                end_datetime = end_datetime.astimezone(self.tzinfo)
            end_date = end_datetime.date()
        start_timestamp = round_timestamp(start_datetime.timestamp())
        end_timestamp = None
        if end_datetime:
            end_timestamp = round_timestamp(end_datetime.timestamp())

            if end_timestamp < start_timestamp:
                raise ValueError("start datetime: {0} > end datetime: {1}".format(
                    start_datetime.strftime("%Y-%m-%d %H:%m:%s"),
                    end_datetime.strftime("%Y-%m-%d %H:%m:%s")
                ))

        return start_date, end_date, start_timestamp, end_timestamp

    def append(self, name, data_frame):
        """
        append data frame data into datatable
        :param name:
        :param data_frame:
        :return:
        """
        self._validate_name(name)

        if not isinstance(data_frame, pandas.DataFrame):
            raise TypeError("data parameter's type must be a pandas.DataFrame")
        if not isinstance(data_frame.index, pandas.DatetimeIndex):
            raise TypeError("DataFrame index must be pandas.DateTimeIndex type")
        if self.index_name in data_frame.columns:
            raise ValueError("DataFrame columns contains index name:{0}".format(self.index_name))

        # try to convert data frame index timezone
        tzinfo = data_frame.index.tzinfo
        if tzinfo and tzinfo != self.tzinfo:
            data_frame.index = data_frame.index.tz_convert(self.tzinfo)
        elif not tzinfo:
            data_frame.index = data_frame.index.tz_localize(self.tzinfo)

        # check duplicated index data
        duplicated_index = data_frame.index[data_frame.index.duplicated()]
        if duplicated_index.size > 0:
            raise TableSeriesError("DataFrame index are duplicated")

        data_frame = self._check_repeated(name, data_frame)

        for date_key, chunk_frame in self._partition_date_frame(data_frame):
            date_group = date_key.strftime(self.DATE_FORMAT)
            group_path = "/" + name + "/" + date_group
            self._create_group_path(group_path)

            array = chunk_frame.to_records(index=True)
            numpy_dtypes = numpy.dtype([(self.index_name, "<M8[ns]")] + self._column_dtypes)

            array = array.astype(numpy.dtype(numpy_dtypes))
            # default timezone is UTC + 0
            array = numpy.rec.array(array, dtype=self._convert_dtypes)

            table_node = self._get_or_create_table("table", group_path)
            table_node.append(array)

            if not table_node.indexed:
                self._create_index(table_node, self.index_name)
            else:
                table_node.reindex_dirty()

    def _walk_groups(self, root_path, regex):
        """
        filter group path
        :param root_path:
        :param regex:
        :return:
        """
        group_list = []
        for group_path in self.h5_store.walk_groups(root_path):
            path_name = group_path._v_pathname
            search = regex.search(path_name)
            if search:
                date_tuple = search.groups()
                date_tuple = tuple(map(int, date_tuple))
                group_list.append((date_tuple, path_name))

        return group_list

    def _to_pandas_frame(self, records, sort=False):
        """
        convert records to pandas data frame
        :param records:
        :param sort:
        :return:
        """

        data_frame = pandas.DataFrame.from_records(
            records,
            index=records[self.index_name].astype("datetime64[ns]"),
            exclude=[self.index_name])

        index = data_frame.index.tz_localize("UTC")
        index = index.tz_convert(self.tzinfo)
        # after convert data
        data_frame.index = index
        # sorted index
        if sort:
            data_frame.sort_index(inplace=True)
        return data_frame

    def _read_where(self, table_node, where_filter, field=None):

        result = table_node.read_where(where_filter, field=field)
        return self._to_pandas_frame(result, sort=True)

    def _read_table(self, table_node, field=None):
        """
        :param table_node:
        :return:
        """
        result = table_node.read_sorted(sortby=self.index_name, field=field)
        return self._to_pandas_frame(result)

    def _filter_field_type(self, fields):
        """
        :return:
        """
        dtype_result = []
        for dtype in self._convert_dtypes:
            if dtype.name in fields:
                dtype_result.append(dtype)
        return numpy.dtype(dtype_result)

    def _get_granularity(self, name, year=None, month=None, day=None):
        """
        :param name:
        :param year:
        :param month:
        :param day:
        :return:
        """
        self._validate_name(name)
        root = "/" + name

        if year and month and day:
            path = root + "/y{year}/m{month:02d}/d{day:02d}".format(year=year, month=month, day=day)
        elif year and month:
            path = root + "/y{year}/m{month:02d}".format(year=year, month=month)
        elif year:
            path = root + "/y{year}".format(year=year)
        else:
            path = root
        return path

    def get_granularity(self, name, field=None, year=None, month=None, day=None):
        """
        :param name:
        :param field:
        :param year:
        :param month:
        :param day:
        :return:
        """
        path = self._get_granularity(name, year, month, day)

        if field:
            result = numpy.empty(shape=0, dtype=self._filter_field_type(fields=field))
        else:
            result = numpy.empty(shape=0, dtype=self._convert_dtypes)

        for table_node in self.h5_store.walk_nodes(path, classname="Table"):
            sorted_data = table_node.read_sorted(sortby=self.index_name, field=field)
            result = numpy.concatenate((result, sorted_data))
        if result.size > 0:
            return self._to_pandas_frame(result)

    def get_granularity_iter(self, name, field=None, year=None, month=None, day=None):
        """
        :param name:
        :param field:
        :param year:
        :param month:
        :param day:
        :return:
        """
        path = self._get_granularity(name, year, month, day)
        for table_node in self.h5_store.walk_nodes(path, classname="Table"):
            yield self._read_table(table_node, field=field)

    def _get_granularity_range_table(self, name, start_date, end_date=None):
        self._validate_name(name)
        root = "/"
        root_path = root + name
        group_list = self._walk_groups(root_path, self.GROUP_REGEX)

        result_groups = self._filter_groups(group_list, start_date, end_date)

        for result in result_groups:
            # result[0] -> (2016, 1, 2)
            # result[1] -> /APPL/y2016/m01/d02
            yield result[0], self.h5_store.get_node(result[1], "table", "Table")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __repr__(self):
        """
        :return:
        """
        return repr(self.h5_store)

    def close(self):
        """
        :return:
        """
        self.h5_store.close()

    def _format_date(self, *date_tuple):
        """
        :return:
        """
        if self.FREQ == "D":
            return DateCompare(date_tuple[0], date_tuple[1], date_tuple[2])
        elif self.FREQ == "M":
            return DateCompare(date_tuple[0], date_tuple[1])
        else:
            return DateCompare(date_tuple[0])

    def _filter_groups(self, group_list, start_dt, end_dt=None):
        """
        :param group_list:
        :param start_dt:
        :param end_dt:
        :return:
        """
        results = []
        start_date_cmp = self._format_date(start_dt.year, start_dt.month, start_dt.day)
        end_date_cmp = None

        if end_dt:
            end_date_cmp = self._format_date(end_dt.year, end_dt.month, end_dt.day)

        for date_group in group_list:
            date_tuple = date_group[0]
            date_tuple_cmp = self._format_date(*date_tuple)

            if date_tuple_cmp >= start_date_cmp and end_dt is None:
                results.append(date_group)  # path name
            elif end_dt and start_date_cmp <= date_tuple_cmp <= end_date_cmp:
                results.append(date_group)  # path name
        return results

    def get_granularity_range(self, name, start_datetime: datetime, end_datetime: datetime = None, fields=None):
        """
        :param name:
        :param start_datetime:
        :param end_datetime:
        :param fields:
        :return:
        """
        start_date, end_date, start_timestamp, end_timestamp = self._validate_datetime(start_datetime, end_datetime)
        start_date_cmp = self._format_date(start_date.year, start_date.month, start_date.day)
        end_date_cmp = None
        if end_date:
            end_date_cmp = self._format_date(end_date.year, end_date.month, end_date.day)
        if "/" + name in self.h5_store:
            for group, table_node in self._get_granularity_range_table(name, start_date, end_date):

                group_date_cmp = self._format_date(*group)
                if end_date is None:
                    if group_date_cmp == start_date_cmp:

                        where_filter = "( {index_name} >= {start_timestamp} )".format(index_name=self.index_name,
                                                                                      start_timestamp=start_timestamp)
                        yield self._read_where(table_node, where_filter, field=fields)
                    else:
                        yield self._read_table(table_node, field=fields)

                elif end_date and start_date_cmp == end_date_cmp:

                    where_filter = "( {index_name} >= {start_timestamp} ) & " \
                                   "( {index_name} <= {end_timestamp} )".format(index_name=self.index_name,
                                                                                start_timestamp=start_timestamp,
                                                                                end_timestamp=end_timestamp)
                    yield self._read_where(table_node, where_filter, field=fields)

                elif end_date and start_date_cmp < end_date_cmp:
                    if group_date_cmp == start_date_cmp:
                        where_filter = "( {index_name} >= {start_timestamp} )".format(index_name=self.index_name,
                                                                                      start_timestamp=start_timestamp)
                        yield self._read_where(table_node, where_filter, field=fields)

                    elif group_date_cmp == end_date_cmp:
                        where_filter = "( {index_name} <= {end_timestamp} )".format(index_name=self.index_name,
                                                                                    end_timestamp=end_timestamp)

                        yield self._read_where(table_node, where_filter, field=fields)
                    else:
                        yield self._read_table(table_node, field=fields)


class TimeSeriesDayPartition(TableBase):
    """
    daily group hdf5 storage
    """
    DATE_FORMAT = "y%Y/m%m/d%d"
    FREQ = "D"
    GROUP_REGEX = re.compile(r"/y(\d{4})/m(\d{2})/d(\d{2})")


class TimeSeriesMonthPartition(TableBase):
    """
    Monthly group Hdf5 storage
    """
    DATE_FORMAT = "y%Y/m%m"
    FREQ = "M"
    GROUP_REGEX = re.compile(r"/y(\d{4})/m(\d{2})")


class TimeSeriesYearPartition(TableBase):
    """
    yearly group hdf5 storage
    """
    DATE_FORMAT = "y%Y"
    FREQ = "Y"
    GROUP_REGEX = re.compile(r"/y(\d{4})")


class TableSeries(object):
    """
    """

    def __new__(cls, cls_name, filename, column_dtypes, *args, **kwargs):
        if cls_name not in ["year", "month", "day"]:
            raise TableSeriesError("class name parameter must be in year, month or day")
        if cls_name == "year":
            return TimeSeriesYearPartition(filename, column_dtypes, *args, **kwargs)
        elif cls_name == "month":
            return TimeSeriesMonthPartition(filename, column_dtypes, *args, **kwargs)
        else:
            return TimeSeriesDayPartition(filename, column_dtypes, *args, **kwargs)
