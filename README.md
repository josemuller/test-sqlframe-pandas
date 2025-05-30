How to run?
===========

1. `pip install -r requirements.txt`
2. `python data_pandas.py` -> shows you the running time for pandas
3. `python data_polars.py` -> shows you the running time for polars
4. `python data_sqlframe.py` -> shows you the running time for sqlframe[duckdb]


Benchmark
=========


| Library               | First Run Time   | Consecutive Run Time |
|-----------------------|------------------|----------------------|
| Pandas                | 17 ms            | 11.0 ms              |
| Polars                | 35 ms            | 5.0 ms               |
| SqlFrame [DuckDB]     | 2465 ms (2.46 s) | N/A                  |
| SparkleFrame [Polars] | 74 ms            | 8.0ms                |


Data is in `transformation/raw_data`.

Repo created for discussion in https://github.com/eakmanrq/sqlframe/issues/409
SparkFrame repo https://github.com/flypipe/sparkleframe