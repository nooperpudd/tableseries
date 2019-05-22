import functools
import operator
import re
import sys
import threading

import numpy
import pandas
import tables
from dateutil import relativedelta

import tableseries.utils


class TimeSeriesTable(object):
    """
    https://github.com/kiyo-masui/bitshuffle
    http://www.pytables.org/cookbook/threading.html
    """
    MAX_TABLE_PARTITION_SIZE = 86400000  # million seconds
    READ_BUFFER = 0  # TODO
    FREQ_MAP = {
        "day": "D",
        "month": "M",
        "year": "A"
    }
    DATE_MAP = {
        "day": "y%Y/m%m/d%d",
        "month": "y%Y/m%m",
        "year": "y%Y"
    }
    NUMBER_REGEX = re.compile(r"(\d+)")
    NAME_REGEX = re.compile(r'^([a-zA-Z]+)([0-9]*)$')

    def __init__(self, filename, time_granularity, dtypes,
                 complib="blosc:blosclz",
                 in_memory=False,
                 index_name="timestamp",
                 compress_level=5,
                 bitshuffle=True,
                 encoding="utf-8"):
        """
                dtype([
    ("name"     , "S16"),
    ("TDCcount" , uint8),
    ("ADCcount" , uint16),
    ("xcoord"   , float32),
    ("ycoord"   , float32)
    ])
    # https://www.pytables.org/usersguide/optimization.html?highlight=bitshuffle

        :param filename:
        :param time_granularity:
        :param dtype:
        :param compress_level:
        :param bitshuffle:
        :param in_memory:
        """
        self._lock = threading.RLock()
        if time_granularity not in ["day", "month", "year"]:
            raise ValueError("granularity values must in 'day','month' or 'year'")

        if in_memory:
            driver = "H5FD_CORE"
        else:
            if sys.platform == "win32":
                driver = "H5FD_WINDOWS"
            else:
                driver = "H5FD_SEC2"

        self.filters = tables.Filters(complevel=compress_level,
                                      complib=complib,
                                      bitshuffle=bitshuffle)

        self.h5_store = tables.open_file(filename=filename, mode="a",
                                         driver=driver, filters=self.filters)

        self._time_granularity = time_granularity
        self.index_name = index_name
        # default for time index type
        self.columns_dtypes = dtypes
        index_dtype = [(index_name, "datetime64")]
        self.dtypes = numpy.dtype(index_dtype + dtypes)
        self._table_description = self._dtype_to_pytable(self.dtypes)

    @property
    def parent_groups(self):
        """
        get root sub groups
        :return:
        """
        sub_groups = {}
        children = self.h5_store.root._v_children.values()
        for child in children:
            sub_groups[child._v_name] = child
        return sub_groups

    def _dtype_to_pytable(self, dtype):
        """

        :param dtype:
        :return:
        """
        d = {}
        for pos, name in enumerate(dtype.names):
            dt, _ = dtype.fields[name]
            if issubclass(dt.type, numpy.datetime64):
                tdtype = tables.Description({name: tables.Time64Col(pos=pos)}),
            else:
                tdtype = tables.descr_from_dtype(numpy.dtype([(name, dt)]))
            el = tdtype[0]  # removed dependency on toolz -DJC
            getattr(el, name)._v_pos = pos
            d.update(el._v_colobjects)
        return d
        # tables_dtype = {}
        # for pos, name in enumerate(dtype.names):
        #     dt, _ = dtype.fields[name]
        #     if issubclass(dt.type, numpy.datetime64):
        #         tdtype = tables.Description({name: tables.Time64Col(pos=pos)})
        #     else:
        #         tdtype = tables.descr_from_dtype(numpy.dtype([(name, dt)]))
        #     element = tdtype[0]  # removed dependency on toolz -DJC
        #     getattr(element, name)._v_pos = pos
        #     tables_dtype.update(element._v_colobjects)
        # return tables_dtype

    @property
    def groups(self):
        """
        list all groups
        :return:
        """
        return self.h5_store.groups()

    @property
    def info(self):
        """
        :return:
        """
        return self.h5_store.info()

    def _validate_name(self, name):
        """
        # validate group name in the group path
        :return:
        """
        if not self.NAME_REGEX.match(name):
            raise ValueError("name must match ^([a-zA-Z]+)([0-9]*)$ regex")

    def _get_or_create_group(self, where, group_path):
        """
        :param name:
        :return:
        """
        if group_path in self.h5_store.get_node(where):
            return self.h5_store.get_node(where=where, name=group_path)
        else:
            return self.h5_store.create_group(where=where, name=group_path)

    def _generate_time_span(self, start_time, end_time):
        """
        :return:
        """
        date_span = relativedelta.relativedelta(start_time, end_time)

        date_span_dict = {}

        def _generate_year():
            for year in range(date_span.years + 1):
                start_year = start_time.year
                date_span_dict[start_year + year] = {}

        def _generate_month():
            for month in range(date_span.months + 1):
                month_time = start_time + relativedelta.relativedelta(months=month)
                date_span_dict[month_time.year][month_time.month] = {}

        def _generate_day():
            for day in range(date_span.days + 1):
                day_time = start_time + relativedelta.relativedelta(days=day)
                date_span_dict[day_time.year][day_time.month][day_time.day] = {}

        if self._time_granularity == "year":
            _generate_year()

        if self._time_granularity == "month":
            _generate_year()
            _generate_month()

        if self._time_granularity == "day":
            _generate_year()
            _generate_month()
            _generate_day()

        return date_span_dict

    def _create_date_groups(self, root, data_groups):

        for key, value in data_groups.items():
            sub_path = self._get_or_create_key_group(root, key)
            if value and isinstance(value, dict):
                self._create_date_groups(sub_path, value)

    def _partition_date_frame(self, date_frame):
        """
        :return:
        """
        freq = self.FREQ_MAP[self._time_granularity]
        for date_key, frame in date_frame.groupby(pandas.Grouper(freq=freq)):
            yield (date_key, frame)

    def _get_or_create_time_partition_group(self, key_group, start_time, end_time):
        """
        :return:
        """
        # group = self.h5_store.get_node(key_group)

        date_groups = self._generate_time_span(start_time, end_time)

        self._create_date_groups(key_group, date_groups)

        # for year_path, month_data in date_groups:
        #     year_node = self._get_or_create_key_group(key_group,year_path)
        #     if month_data:
        #         for month_path, day_data in month_data:
        #
        #
        #     for year_path,_ in date_groups:
        #         self._get_or_create_key_group(key_group,year_path)
        # elif self._time_granularity=="month":
        #     for
        #
        #
        # if self._time_granularity == "day":
        #     for i in range(date_span.days+1):                                                                                                                                                                                                                                                                                                                                                                                                                                          ;
        #         group_time = start_time + timedelta(days=i)
        #         time_span_group = group_time.strftime("%Y/%m/%d")
        #         table_group_node = self._get_or_create_key_group(key_group, time_span_group)
        #         self.h5_store._write_to_group()
        # elif self._time_granularity == "month":
        #     pass
        # elif self._time_granularity == "year":
        #     pass
        #
        # for i in range(date_span.days + 1):
        #     group_time = start_time + timedelta(days=i)
        #     strip_date = group_time.strftime("%Y/%m/%d")
        #     if strip_date in key_group:
        #         return key_group.get_node(strip_date)
        #     else:
        #         return key_group.create_group(strip_date)

    # def _get_or_create_table(self, group, table_name="ts_table"):
    #     """
    #     :param table_name:
    #     :return:
    #     """
    #     if self.h5_store.get_node(group + "/" + table_name):
    #         ts_table = self.h5_store.get_node(group + "/" + table_name)
    #     else:
    #         ts_table = self.h5_store._handle.create_table(group=group,
    #                                                       table_name=table_name)
    #     return ts_table

    def _search_group(self, name, start_time, end_time):
        """
        :param name:
        :param start_time:
        :param end_time:
        :return:
        """
        if start_time and end_time:
            pass

    def delete(self, name, year=None, month=None, day=None):
        """
        :param self:
        :param name:
        :return:
        """
        self._validate_name(name)
        root = "/"
        if month:
            month = "0" + str(month) if len(str(month)) == 1 else str(month)
        if day:
            day = "0" + str(day) if len(str(day)) == 1 else str(day)
        if year and month and day:
            path = root + name + "/y" + str(year) + "/m" + month
            node = "d" + day
        elif year and month:
            path = root + name + "/y" + str(year)
            node = "m" + month
        elif year:
            path = root + name
            node = "y" + str(year)
        else:
            path = root
            node = name
        self.h5_store.remove_node(path, name=node, recursive=True)
        self.h5_store.flush()

    def _generate_date_group_path(self, date):
        """
        generate the group path for hdf5
        :param date:
        :return:
        """
        return date.strftime(self.DATE_MAP[self._time_granularity])

    def _create_group_path(self, group_path, root="/"):
        """
        create group or not
        :return:
        """
        group_list = group_path.split("/")
        # groups have "" in the list
        group_list.pop(0)
        start_group = root
        for group in group_list:
            self._get_or_create_group(start_group, group)
            start_group = start_group + "/" + group

    def append(self, name, data_frame):
        """
        :param name:
        :param data_frame:
        :return:
        """
        self._validate_name(name)

        if not isinstance(data_frame, pandas.DataFrame):
            raise TypeError("data parameter's type must be a pandas.DataFrame")
        if not isinstance(data_frame.index, pandas.DatetimeIndex):
            raise TypeError("DataFrame index must be pandas.DateTimeIndex type")

        data_frame.sort_index(inplace=True)
        datetime_index = data_frame.index

        # data_frame.insert(0,self.index_name,data_frame.index.to_pydatetime())

        index_name = data_frame.index.name or self.index_name

        max_datetime = datetime_index.max()
        min_datetime = datetime_index.min()
        # todo check data time exists?
        # hdf5 groups
        for date_key, chunk_frame in self._partition_date_frame(data_frame):
            date_group = self._generate_date_group_path(date_key)
            group_key = "/" + name + "/" + date_group
            self._create_group_path(group_key)

            # array_index = chunk_frame.index.to_numpy()
            # array_index = array_index.reshape(-1, 1)
            # date_array = chunk_frame.to_numpy()
            # date_array = date_array.astype("O") # invalid type promotion
            # combined = numpy.hstack([array_index,date_array])
            # index = numpy.apply_along_axis(lambda x:x[0]/1000000000,1,combined)
            # index = index.reshape(-1,1)
            # combined = numpy.hstack([index,date_array])
            # # index = combined
            # print(combined)
            # combined = combined.astype(self.dtypes)
            # print(combined)
            # print(combined.dtype)

            ts_table = self._get_or_create_table("table", group_key, index_name)

        self.h5_store.flush()

    def _create_index(self, data_table, index_name):

        if hasattr(data_table.cols, index_name):
            col = getattr(data_table.cols, index_name)
            # create completely sorted index
            col.create_csindex(filters=self.filters)

    def read_range(self, name, start_time=None, end_time=None,
                   columns=None, chunk_size=1000000):
        """
        :param name:
        :param start_time:
        :param end_time:
        :param chunk_size:
        :return:
        """
        self._validate_name(name)
        if start_time and end_time:
            if start_time > end_time:
                raise ValueError("start_time must be <= end_time")
        if start_time:
            pass
        if end_time:
            pass
        self.h5_store.select(name, "index>=%s")

    def _get_sub_group_path(self, root, operator_func):
        """
        get sub group path based on the max value
        :param root:
        :param operator_func:
        :return:
        """
        group = self.h5_store.get_node(root)

        max_value = 0
        result_path = None
        for child in group._v_children.values():
            name = child._v_name
            path_name = child._v_pathname
            match_value = self.NUMBER_REGEX.search(name)
            match_value = int(match_value.group())
            if match_value and operator_func(max_value, match_value):
                max_value = match_value
                result_path = path_name
        return result_path

    def _filter_group_table(self, name, operator_func):
        """
        :return:
        """

        group_path = ""
        if self._time_granularity == "year":
            group_path = self._get_sub_group_path(name, operator_func)
        elif self._time_granularity == "month":
            year_path = self._get_sub_group_path(name, operator_func)
            group_path = self._get_sub_group_path(year_path, operator_func)
        elif self._time_granularity == "day":
            year_path = self._get_sub_group_path(name, operator_func)
            month_path = self._get_sub_group_path(year_path, operator_func)
            group_path = self._get_sub_group_path(month_path, operator_func)

        return self.h5_store.get_storer(group_path).table

    def get_max_timestamp(self, name):
        """
        :param name:
        :return:
        """
        self._validate_name(name)
        if self.parent_groups.get(name):
            operator_func = operator.lt
            data_table = self._filter_group_table(name, operator_func)
            result_value = data_table.cols.index[-1]
            return result_value

    def get_min_timestamp(self, name):
        """
        return data_table.cols[self.index_name][data_table.colindexes[self.index_name][0]]
        :param name:
        :return:
        """
        self._validate_name(name)
        if self.parent_groups.get(name):
            operator_func = operator.gt
            data_table = self._filter_group_table(name, operator_func)
            result_value = data_table.cols.index[0]
            return result_value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _get_or_create_table(self, name, parent_group_path, index_name):
        """
        :param name:
        :return:
        """
        table_path = parent_group_path + "/" + name
        if table_path not in self.h5_store:
            data_table = self.h5_store.create_table(parent_group_path, name=name,
                                                    description=self._table_description)
            # create table then create index
            self._create_index(data_table, index_name)
        else:
            data_table = self.h5_store.get_node(where=parent_group_path, name="table")
        return data_table

    # @functools.lru_cache(maxsize=2048)
    # def _get_or_create_table(self, name, start_dt=None, end_dt=None):
    #     """
    #     :param name:
    #     :return:
    #     """
    #     parent_group = self._get_or_create_parent_group(name)
    #
    #     print(self.time_granularity)
    #     # aggregration timestamp with time granularity
    #     if self.time_granularity in ["second", "minute"]:
    #
    #         # group as monthly
    #         date_range = self._partition_date(start_dt, end_dt)
    #         for year in date_range:
    #             year_group = self.h5file.create_group(parent_group, year)
    #             months = date_range[year]
    #             for month in months:
    #                 month_group = self.h5file.create_group(year_group, month)
    #
    #                 data_table = self.h5file.create_table(month_group, name=self.table_name,
    #                                                       description=self.dtypes)
    #
    #                 # to create index on index column
    #                 self._create_index(data_table)
    #
    #                 return data_table
    #
    #     else:
    #         print("sfdsfdfsa")
    #
    #         print(parent_group)
    #         print(parent_group)
    #
    #         if self.table_name in parent_group:
    #
    #             return self.h5file.get_node(parent_group, name=self.table_name, classname="Table")
    #         else:
    #             data_table = self.h5file.create_table(parent_group, name=self.table_name,
    #                                                   description=self.dtypes)
    #
    #             self._create_index(data_table)
    #             return data_table

    # if self.time_granularity in ["second", "minute"]:
    #     pass
    # else:
    #     print(parent_group)
    #
    #     return self.h5file.get_node(parent_group, name=self.table_name,
    #                                 classname="Table")

    def get_slice(self, name, start_datetime=None, end_datetime=None, limit=0, columns=None):

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
            print("fdsafdfaf", response_data)
            return response_data

    def _iter_groups(self, root, start_value, end_value, operator_func):
        """
        :return:
        """

        group = self.h5_store.get_node(root)

        result_list = []
        result_path = None
        for child in group._v_children.values():
            name = child._v_name
            path_name = child._v_pathname
            match_value = self.NUMBER_REGEX.search(name)
            match_value = int(match_value.group())
            if match_value and start_value and end_value and \
                    operator.ge(match_value, start_value) and \
                    operator.le(match_value, start_value):
                pass
            if match_value and operator.ge(match_value, start_value):
                result_list.append(match_value)
                result_path = path_name
        return result_path

    def _iter_all_groups_date_span(self, root, start_date, end_date):
        """
        :param root:
        :param start_path:
        :param end_path:
        :return:
        """

        start_year = start_date.year
        end_year = end_date.year
        start_month = start_date.month
        end_month = start_date.month
        start_day = start_date.day
        end_day = end_date.day

        between_date = lambda start, end, x: start <= x <= end
        parliar_func = functools.partial(between_date, start_year, end_year)

        time_span = self._generate_time_span(start_date, end_date)
        group_path_list = []
        end_path_list = []
        # if self._time_granularity == "year":
        #     start_year = start_date.year
        #     end_year = end_date.year
        #
        # elif self._time_granularity == "month":
        #     start_year = start_date.year
        #     end_year = end_date.year
        #     start_month = start_date.month
        #     end_month = end_date.month
        #
        #     # group_path = self._iter_groups(root, operator_func)
        # elif self._time_granularity == "month":
        #
        #     year_path = self._get_sub_group_path(name, operator_func)
        #     group_path = self._get_sub_group_path(year_path, operator_func)
        # elif self._time_granularity == "day":
        #     year_path = self._get_sub_group_path(name, operator_func)
        #     month_path = self._get_sub_group_path(year_path, operator_func)
        #     group_path = self._get_sub_group_path(month_path, operator_func)

    def length(self, name, start_time=None, end_time=None):
        """
        :param name:
        :return:
        """
        start_path = None
        end_path = None
        if start_time:
            start_path = self._generate_date_group_path(start_time)
        if end_time:
            end_path = self._generate_date_group_path(end_time)

        if start_path and end_path:
            if start_path == end_path:
                pass
            else:
                pass
        elif start_path and end_path is None:
            pass
        elif start_path is None and end_path:
            pass
        else:
            pass

        # if self.time_granularity in ["second", "minute"]:
        #     pass
        # else:
        #     data_table = self._get_table(name)
        #     return len(data_table)

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

    def close(self):
        """
        :return:
        """
        self.h5_store.close()

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
