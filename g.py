import pandas
import numpy
from datetime import datetime
import pytz
from tableseries.ts import  TableBase
#  filename, column_dtypes, index_name="timestamp",
#                  complib="blosc:blosclz",
#                  in_memory=False,
#                  compress_level=5,
#                  bitshuffle=False,
#                  tzinfo=pytz.UTC
dtypes = [("value1", "int64"), ("value2", "int64")]
columns= ("value1", "value2")
date = datetime.now()

date_range = pandas.date_range(date, periods=100, freq="S", tz=pytz.UTC)
range_array = numpy.arange(100, dtype=numpy.int64)
random_array = numpy.random.randint(0, 100, size=100, dtype=numpy.int64)

data = pandas.DataFrame({"value1": range_array,
                         "value2": random_array},
                        index=date_range, columns=columns)

b= TableBase("day","g.h5",dtypes)

b.append("yes",data)

b.close()
