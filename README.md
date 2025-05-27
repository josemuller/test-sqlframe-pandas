How to run?
===========

1. `pip install requirements.txt`
2. `python data_pandas.py` -> shows you the running time for pandas
3. `python data_sqlframe.py` -> shows you the running time for sqlframe[duckdb]


Benchmark
=========

Pandas between 17 ms (first run) and 11.0 ms (consecutive runs)
Polars between 35 ms (first run) and 5.0 ms (consecutive runs)
SqlFrame[DuckDB] (2465 ms or 2.46 sec)

Data is in `transformation/raw_data`