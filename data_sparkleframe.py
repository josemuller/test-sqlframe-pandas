# ruff: noqa: E402
import os.path
from time import time

start = time()

from sparkleframe.activate import activate

activate()

import pandas as pd

from pyspark.sql import SparkSession
from transformation.pyspark.data import data
from transformation.pyspark.func5 import func5
from transformation.pyspark.prep_func2 import prep_func2
from transformation.pyspark.prep_func3 import prep_func3
from transformation.pyspark.prep_func4 import prep_func4
from transformation.pyspark.prep_func1 import prep_func1

spark = SparkSession.builder.getOrCreate()
df_func1 = spark.createDataFrame(
    pd.read_parquet("./transformation/raw_data/func1.parquet")
)

df_func2 = spark.createDataFrame(
    pd.read_parquet("./transformation/raw_data/func2.parquet")
)

df_func3 = spark.createDataFrame(
    pd.read_parquet("./transformation/raw_data/func3.parquet")
)

df_func4 = spark.createDataFrame(
    pd.read_parquet("./transformation/raw_data/func4.parquet")
)

df_func5 = spark.createDataFrame(
    pd.read_parquet("./transformation/raw_data/func5.parquet")
)
time_loaded_files = time()
df_func1 = prep_func1(df_func1)
df_func2 = prep_func2(df_func2)
df_func3 = prep_func3(df_func3)
df_func4 = prep_func4(df_func4)
df_func5 = func5(df_func5)

df = data(df_func2, df_func1, df_func3, df_func4, df_func5)
print(type(df))
df.show()
print(f"load files in {round((time_loaded_files - start) * 1000, 0)} milliseconds")
print(
    f"transformations ran in {round((time() - time_loaded_files) * 1000, 0)} milliseconds"
)
