import operator
import re
import sys
import threading

import dateutil.parser
import pandas
import tables
from dateutil import relativedelta
from pandas.io.pytables import HDFStore

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
    # NAME_RE = r"^[1-9_].*"
    NUMBER_REGEX = re.compile(r"(\d+)")

    def __init__(self, filename, time_granularity,
                 complib="blosc:blosclz",
                 in_memory=False,
                 compress_level=5,
                 bitshuffle=True, encoding="utf-8"):
        """
        :param filename:
        :param granularity:
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

        self.comp_filter = tables.Filters(complevel=compress_level,
                                          complib=complib,
                                          bitshuffle=bitshuffle)

        self.h5_store = HDFStore(path=filename, mode="a", driver=driver,
                                 complib=complib, compress_level=compress_level)

        self._time_granularity = time_granularity

        self.encoding = encoding

    @property
    def parent_groups(self):
        """
        get root sub groups
        :return:
        """
        sub_groups = []
        children = self.h5_store.root._v_children.values()
        for child in children:
            sub_groups.append(child)
        return sub_groups

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

    def _get_or_create_key_group(self, root, group_path):
        """
        :param name:
        :return:
        """
        if group_path in root:
            return self.h5_store.get_node(key=group_path)
        else:
            return self.h5_store._handle.create_group(root, group_path)



    def _walk_create_group(self, root, group_paths):
        """
        :param root:
        :param group_path:
        :return:
        """

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

    def _partiation_date_frame(self, date_frame):
        """
        :return:
        """
        freq = self.FREQ_MAP[self._time_granularity]
        return {
            date_key: frame for date_key, frame in date_frame.groupby(pandas.Grouper(freq=freq))
        }

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

    def _get_or_create_table(self, group, table_name="ts_table"):
        """
        :param table_name:
        :return:
        """
        if self.h5_store.get_node(group + "/" + table_name):
            ts_table = self.h5_store.get_node(group + "/" + table_name)
        else:
            ts_table = self.h5_store._handle.create_table(group=group,
                                                          table_name=table_name)
        return ts_table

    def _create_index(self):
        """
        :return:
        """
        pass

    def delete(self, name, size, start_time=None, end_time=None):
        """
        :param self:
        :param name:
        :param size:
        :param start_time:
        :param end_time:
        :return:
        """
        pass

    def append(self, name, data_frame):
        """
        :param name:
        :param data_frame:
        :return:
        """
        if not isinstance(data_frame, pandas.DataFrame):
            raise TypeError("data parameter's type must be a pandas.DataFrame")
        if not isinstance(data_frame.index, pandas.DatetimeIndex):
            raise TypeError("DataFrame index must be pandas.DateTimeIndex type")

        data_frame.sort_index(inplace=True)
        datetime_index = data_frame.index

        max_datetime = datetime_index.max()
        min_datetime = datetime_index.min()

        # hdf5 group
        # key_group = self._get_or_create_key_group(self.root, name)

        data_frame_chunks = self._partiation_date_frame(data_frame)
        for date_key, chunk_frame in data_frame_chunks.items():
            date_group = date_key.strftime(self.DATE_MAP[self._time_granularity])
            key = name + "/" + date_group
            self.h5_store.append(key, value=data_frame, append=True, encoding=self.encoding,
                                 expectedrows=self.MAX_TABLE_PARTITION_SIZE)
        # node = self._get_or_create_time_partition_group(key_group, min_datetime, max_datetime)

        # self.h5_store.append(key=node, value=data_frame)

        self.h5_store.flush()

    def read_range(self, name, start_time, end_time=None, chunk_size=100000):
        """
        :param name:
        :param start_time:
        :param end_time:
        :param chunk_size:
        :return:
        """
        if isinstance(start_time, str):
            start_time = dateutil.parser.parse(start_time)
        if end_time and isinstance(end_time, str):
            end_time = dateutil.parser.parse(end_time)

        if self._time_granularity in ["second", "minute"]:
            pass
        else:
            self.h5_store.select(name, "index>=%s")

    def get_max_sub_group(self):
        """
        :return:
        """
        pass

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

    def _filer_group_table(self,name, operator_func):
        """
        :return:
        """
        if self.h5_store.get_node(name):
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
        operator_func = operator.lt
        data_table = self._filer_group_table(name,operator_func)

        result_value = data_table.cols.index[-1]
        return result_value

    def get_min_timestamp(self, name):
        """
        return data_table.cols[self.index_name][data_table.colindexes[self.index_name][0]]
        :param name:
        :return:
        """
        operator_func = operator.gt
        data_table = self._filer_group_table(name, operator_func)
        result_value = data_table.cols.index[0]
        return result_value



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

    def length(self, name, start_time, end_time):
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
        self.h5_store.close()

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
