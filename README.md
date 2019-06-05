## TableSeries
Based on HDF5 file storage to store time-series data, with high performance.

[![Build Status](https://travis-ci.org/nooperpudd/tableseries.svg?branch=master)](https://travis-ci.org/nooperpudd/tableseries)
[![Build status](https://ci.appveyor.com/api/projects/status/5ncwofnor67vljpt/branch/master?svg=true)](https://ci.appveyor.com/project/nooperpudd/tableseries/branch/master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b8b4113198184f83b4bd265af6271374)](https://www.codacy.com/app/nooperpudd/tableseries?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=nooperpudd/tableseries&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/nooperpudd/tableseries/branch/master/graph/badge.svg)](https://codecov.io/gh/nooperpudd/tableseries)

[![pypi](https://img.shields.io/pypi/v/tableseries.svg)](https://pypi.python.org/pypi/tableseries)
[![status](https://img.shields.io/pypi/status/tableseries.svg)](https://pypi.python.org/pypi/tableseries)
[![pyversion](https://img.shields.io/pypi/pyversions/tableseries.svg)](https://pypi.python.org/pypi/tableseries)
[![Downloads](https://pepy.tech/badge/tableseries)](https://pepy.tech/project/tableseries)

In-kernel searches

in-kernel searches on uncompressed tables are generally much faster (10x) than standard queries as well as PostgreSQL (5x).

s name is shuffle, and because it can greatly benefit compression and it does not take many CPU resources (see below for a justification), it is active by default in PyTables whenever compression is activated (independently of the chosen compressor). It is deactivated when compression is off (which is 