import operator
import re
import sys
import threading

import numpy
import pandas
import tables

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
                 table_description,
                 complib="blosc:blosclz",
                 in_memory=False,
                 index_name="timestamp",
                 compress_level=5,
                 bitshuffle=True,
                 ):
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
        numpy_index_dtype = [(index_name, "<M8[ms]")]  # index dtype datetime64
        table_index_dtype = [(index_name, "<i8")]  # index int64
        self._numpy_dtypes = numpy.dtype(numpy_index_dtype + dtypes)
        self._convert_dtypes = numpy.dtype(table_index_dtype + dtypes)
        # self._table_description = self._dtype_to_pytable(self._convert_dtypes)
        self._table_description = table_description

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
        # todo
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

    def _partition_date_frame(self, date_frame):
        """
        :return:
        """
        freq = self.FREQ_MAP[self._time_granularity]
        for date_key, frame in date_frame.groupby(pandas.Grouper(freq=freq)):
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

        index_name = data_frame.index.name or self.index_name

        max_datetime = datetime_index.max()
        min_datetime = datetime_index.min()

        # todo check data time exists?
        # hdf5 groups
        for date_key, chunk_frame in self._partition_date_frame(data_frame):
            date_group = self._generate_date_group_path(date_key)
            group_key = "/" + name + "/" + date_group
            self._create_group_path(group_key)

            array = chunk_frame.to_records(index=True, convert_datetime64=False)
            array = array.astype(numpy.dtype(self._numpy_dtypes))
            array = numpy.rec.array(array, dtype=self._convert_dtypes)
            ts_table = self._get_or_create_table("table", group_key, index_name)

            ts_table.append(array)

    def _create_index(self, data_table, index_name):

        if hasattr(data_table.cols, index_name):
            col = getattr(data_table.cols, index_name)
            # create completely sorted index
            col.create_csindex()

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

    def close(self):
        """
        :return:
        """
        self.h5_store.close()
